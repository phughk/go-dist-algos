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
	conn           net.Conn
	respMap        map[string]chan AnyMessage
	respMapMux     sync.Mutex
	requestHandler func(*ConnHandler, *AnyMessage) AnyMessage
}

func newConnHandler(conn net.Conn, requestHandler func(*ConnHandler, *AnyMessage) AnyMessage) *ConnHandler {
	ch := ConnHandler{
		conn:           conn,
		respMap:        make(map[string]chan AnyMessage),
		requestHandler: requestHandler}
	go ch.readMessages()
	return &ch
}

func (ch *ConnHandler) SendRequest(message *AnyMessage) (*AnyMessage, error) {
	responseChan := make(chan AnyMessage, 1)

	ch.respMapMux.Lock()
	ch.respMap[message.RequestID] = responseChan
	ch.respMapMux.Unlock()

	// Serialize the request to JSON
	data, err := json.Marshal(message)
	if err != nil {
		return nil, err
	}

	// Write the length of the message followed by the message itself
	length := uint16(len(data))
	err = binary.Write(ch.conn, binary.BigEndian, length)
	if err != nil {
		return nil, err
	}
	_, err = ch.conn.Write(data)
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

func (ch *ConnHandler) readMessages() {
	var length uint16
	err := binary.Read(ch.conn, binary.BigEndian, &length)
	if err != nil {
		log.Panicf("Error reading size for next packet", err)
	}
	message := make([]byte, length)
	_, err = io.ReadFull(ch.conn, message)
	if err != nil {
		log.Panicf("Error reading message of expected size %d", length, err)
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
		// This is a response to a request
		delete(ch.respMap, anyMessage.RequestID)
		respChan <- anyMessage
	} else {
		// This is a request and needs a response
		ch.requestHandler(ch, &anyMessage)
	}
}

func parseMessage(message []byte) (AnyMessage, error) {
	var anyMessage AnyMessage

	err := json.Unmarshal(message, anyMessage.OperationRequest)
	if err == nil {
		return anyMessage, nil
	}

	err = json.Unmarshal(message, anyMessage.OperationResponse)
	if err == nil {
		return anyMessage, nil
	}

	return AnyMessage{}, fmt.Errorf("Unknown message type")
}
