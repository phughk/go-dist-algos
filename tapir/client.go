package main

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/urfave/cli/v2"
	"log"
	"net"
	"strings"
	"time"
)

func client(c *cli.Context) error {
	fmt.Println("Running client...")
	bootstrap := c.String("bootstrap")
	servers := strings.Split(bootstrap, ";")
	connections := make([]*ConnHandler, len(servers))
	for i, server := range servers {
		conn, err := net.Dial("tcp", server)
		if err != nil {
			return err
		}
		connections[i] = newConnHandler(conn, clientRequestHandler)
		defer conn.Close()
		fmt.Println("Connected to server:", server)
		client := &Client{Connections: connections}
		resp, err := client.SendOperationRequest(OperationRequest{})
		if err != nil {
			return err
		}
		fmt.Printf("Received response: %+v\n", resp)
	}
	return nil
}

type Client struct {
	Connections []*ConnHandler
}

func (c *Client) SendOperationRequest(request OperationRequest) (*OperationResponse, error) {
	// Response channel
	responseChan := make(chan *AnyMessage, len(c.Connections))
	// Send message to all servers
	for _, conn := range c.Connections {
		go func(conn *ConnHandler) {
			request := AnyMessage{
				RequestID:        uuid.New().String(),
				OperationRequest: &request,
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
	fmt.Printf("Waiting for all responses...\n")
	var responses []*OperationResponse
	for i := 0; i < len(c.Connections); i++ {
		select {
		case resp := <-responseChan:
			responses = append(responses, resp.OperationResponse)
		case <-time.After(5 * time.Second):
			return nil, fmt.Errorf("Timeout waiting for operation response from all servers")
		}
	}
	// TODO: Decide value
	decidedValue := responses[0]
	fmt.Printf("Decided value: %+v\n", decidedValue)
	return decidedValue, nil
}

func clientRequestHandler(ch *ConnHandler, m *AnyMessage) {
	if m.Ping != 0 {
		fmt.Printf("Client received ping: %+v\n", m)
		err := ch.SendUntracked(&AnyMessage{RequestID: m.RequestID, Pong: m.Ping})
		if err != nil {
			log.Panicf("Error sending pong: %v", err)
		}
	} else {
		log.Panicf("Client does not handle responses")
	}
}
