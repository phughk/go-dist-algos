package main

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"log"
	"net"
	"strings"
	"time"
)

func client(c *cli.Context) error {
	logrus.Debugf("Running client...")
	bootstrap := c.String("cluster")
	client_id := uuid.New().String()
	logrus.Debugf("Client ID: %s\n", client_id)
	servers := strings.Split(bootstrap, ";")
	connections := make([]*ConnHandler, len(servers))
	for i, server := range servers {
		conn, err := net.Dial("tcp", server)
		if err != nil {
			return err
		}
		connections[i] = newConnHandler(conn, clientRequestHandler)
		defer func() {
			err := conn.Close()
			if err != nil {
				logrus.Errorf("Error closing connection to server: %v", err)
			}
		}()
		logrus.Debugf("Connected to server: %+v", server)
		client := &Client{Connections: connections}
		ClientRepl(client)
	}
	return nil
}

type Client struct {
	Connections []*ConnHandler
}

func (c *Client) SendOperationRequest(readSet []string, writeSet map[string]PutcOp) (*OperationResponse, error) {
	// Response channel
	responseChan := make(chan *AnyMessage, len(c.Connections))
	// WriteSet is values that haven't been read
	// WriteCSet is values that have been read
	writeCSet := make(map[string]PutcOp)
	for _, v := range readSet {
		_, ok := writeSet[v]
		if ok {
			writeCSet[v] = writeSet[v]
			delete(writeSet, v)
		}
	}
	newWriteSet := make(map[string]string)
	for k, v := range writeSet {
		newWriteSet[k] = v.Proposed
	}
	// Send message to all servers
	operationRequest := &OperationRequest{
		Mode: Inconsistent,
		Propose: &Operation{
			ReadSet:   readSet,
			WriteSet:  newWriteSet,
			WriteCSet: writeCSet,
		},
		TransactionID: uuid.New().String(),
	}
	for _, conn := range c.Connections {
		go func(conn *ConnHandler) {
			request := AnyMessage{
				RequestID:        uuid.New().String(),
				OperationRequest: operationRequest,
			}
			resp, err := conn.SendRequest(&request)
			if err != nil {
				log.Panicf("Error sending operation request to server: %v\n", err)
				return
			}
			responseChan <- resp
		}(conn)
	}
	// Wait for all responses with timeout 5s
	logrus.Tracef("Received %d out of %d responses\n", len(c.Connections), len(responseChan))
	var responses []*OperationResponse
	for i := 0; i < len(c.Connections); i++ {
		select {
		case resp := <-responseChan:
			responses = append(responses, resp.OperationResponse)
		case <-time.After(5 * time.Second):
			break
		}
	}

	// TODO: Decide value
	decidedValue := responses[0]
	fmt.Printf("Decided value: %+v\n", decidedValue)
	return decidedValue, nil
}

func clientRequestHandler(ch *ConnHandler, m *AnyMessage) {
	if m.Ping != 0 {
		logrus.Tracef("Client received ping: %+v\n", m)
		err := ch.SendUntracked(&AnyMessage{RequestID: m.RequestID, Pong: m.Ping})
		if err != nil {
			log.Panicf("Error sending pong: %v", err)
		}
	} else {
		log.Panicf("Client does not handle responses")
	}
}
