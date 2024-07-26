package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

type ConnHandler struct {
	terminated     bool
	conn           net.Conn
	respMap        map[string]chan AnyMessage
	respMapMux     sync.Mutex
	requestHandler func(*ConnHandler, *AnyMessage)
}

func newConnHandler(conn net.Conn, requestHandler func(*ConnHandler, *AnyMessage)) *ConnHandler {
	ch := ConnHandler{
		conn:           conn,
		respMap:        make(map[string]chan AnyMessage),
		requestHandler: requestHandler}
	go ch.readMessageLoop()
	return &ch
}

func (ch *ConnHandler) SendRequest(message *AnyMessage) (*AnyMessage, error) {
	responseChan := make(chan AnyMessage, 1)

	ch.respMapMux.Lock()
	ch.respMap[message.RequestID] = responseChan
	ch.respMapMux.Unlock()
	err := ch.SendUntracked(message)
	if err != nil {
		return nil, err
	}

	select {
	case resp := <-responseChan:
		return &resp, nil
	case <-time.After(5 * time.Second):
		ch.respMapMux.Lock()
		delete(ch.respMap, message.RequestID)
		ch.respMapMux.Unlock()
		return nil, fmt.Errorf("timeout waiting for operation response")
	}
}

func (ch *ConnHandler) SendUntracked(message *AnyMessage) error {
	fmt.Printf("Sending message: %+v\n", message)
	// Serialize the request to JSON
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}

	// Write the length of the message followed by the message itself
	length := uint16(len(data))
	err = binary.Write(ch.conn, binary.BigEndian, length)
	if err != nil {
		return err
	}
	_, err = ch.conn.Write(data)
	if err != nil {
		return fmt.Errorf("error sending request: %v", err)
	}
	return nil
}

func (ch *ConnHandler) readMessageLoop() {
	defer fmt.Printf("Shutdown connection listener loop\n")
	for !ch.terminated {
		ch.readNextSingleMessage()
	}
}

func (ch *ConnHandler) readNextSingleMessage() {
	var length uint16
	err := binary.Read(ch.conn, binary.BigEndian, &length)
	if err != nil {
		if err == io.EOF {
			log.Println("Connection closed by peer")
			ch.terminated = true
			return
		}
		log.Panicf("Error reading size for next packet: %e", err)
	}
	message := make([]byte, length)
	_, err = io.ReadFull(ch.conn, message)
	if err != nil {
		log.Panicf("Error reading message of expected size %d: %e", length, err)
	}
	// Now check message type
	anyMessage, err := parseMessage(message)
	if err != nil {
		log.Panicf("Error parsing message: %v", err)
		return
	}
	fmt.Printf("Received message: %+v\n", anyMessage)
	// Handle callback
	ch.respMapMux.Lock()
	respChan, ok := ch.respMap[anyMessage.RequestID]
	ch.respMapMux.Unlock()
	if ok {
		fmt.Printf("It was a response, and handling it now\n")
		// This is a response to a request
		delete(ch.respMap, anyMessage.RequestID)
		respChan <- anyMessage
	} else {
		// This is a request and needs a response
		fmt.Printf("It was a request and forwarding it to the handler\n")
		ch.requestHandler(ch, &anyMessage)
	}
}

func parseMessage(message []byte) (AnyMessage, error) {
	var anyMessage AnyMessage

	err := json.Unmarshal(message, &anyMessage)
	if err == nil {
		return anyMessage, nil
	}

	return AnyMessage{}, fmt.Errorf("Unknown message type: %+v", message)
}
