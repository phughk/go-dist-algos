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
		repl(client)
	}
	return nil
}

func repl(client *Client) {
	fmt.Println("Interactive client, type 'help' for list of commands.")
	for {
		fmt.Print("> ")
		var input string
		fmt.Scanln(&input)
		parts := strings.SplitN(input, " ", 2)
		command := strings.ToLower(parts[0])
		args := parts[1:]
		switch command {
		case "start", "s", "begin", "b":
			logrus.Debugf("Started transaction...\n")
			resp, err := client.SendOperationRequest(&OperationRequest{})
			if err != nil {
				logrus.Warnf("Error starting transaction: %v\n", err)
			}
			logrus.Debugf("Received response: %+v\n", resp)
		case "read", "r", "get", "g":
			if len(args) < 1 {
				fmt.Println("Usage: read/r/get/g <key>")
				continue
			}
			logrus.Debugf("Reading key: %s...\n", args[0])
			resp, err := client.SendOperationRequest(&OperationRequest{})
			if err != nil {
				logrus.Warnf("Error reading key: %v\n", err)
			}
			logrus.Debugf("Received response: %+v\n", resp)
		case "write", "w", "put", "p":
			if len(args) < 2 {
				fmt.Println("Usage: write/w/put/p <key> <value>")
				continue
			}
			logrus.Debugf("Writing key: %s, value: %s...\n", args[0], args[1])
			resp, err := client.SendOperationRequest(&OperationRequest{})
			if err != nil {
				logrus.Warnf("Error writing key: %v\n", err)
			}
			logrus.Debugf("Received response: %+v\n", resp)
		case "commit", "c":
			logrus.Debugf("Committing transaction...\n")
			resp, err := client.SendOperationRequest(&OperationRequest{})
			if err != nil {
				logrus.Warnf("Error committing transaction: %v\n", err)
			}
			logrus.Debugf("Received response: %+v\n", resp)
		case "cancel", "end", "e", "rollback":
			logrus.Debugf("Rolling back transaction...\n")
			resp, err := client.SendOperationRequest(&OperationRequest{})
			if err != nil {
				logrus.Warnf("Error cancelling transaction: %v\n", err)
			}
			logrus.Debugf("Received response: %+v\n", resp)
		case "help", "h":
			fmt.Println("Available commands:")
			fmt.Println("start/s/begin/b: Start a new transaction")
			fmt.Println("read/r/get/g <key>: Read the value of a key")
			fmt.Println("write/w/put/p <key> <value>: Write a key-value pair")
			fmt.Println("commit/c: Commit the current transaction")
			fmt.Println("cancel/end/e/rollback: Roll back the current transaction")
			fmt.Println("help/h: Show this help message")
			fmt.Println("exit: Exit the client")
		case "exit":
			// TODO cancel transaction?
			return
		default:
			fmt.Println("Invalid command")
		}
	}
}

type Client struct {
	Connections []*ConnHandler
}

func (c *Client) SendOperationRequest(request *OperationRequest) (*OperationResponse, error) {
	// Response channel
	responseChan := make(chan *AnyMessage, len(c.Connections))
	// Send message to all servers
	for _, conn := range c.Connections {
		go func(conn *ConnHandler) {
			request := AnyMessage{
				RequestID:        uuid.New().String(),
				OperationRequest: request,
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
		logrus.Tracef("Client received ping: %+v\n", m)
		err := ch.SendUntracked(&AnyMessage{RequestID: m.RequestID, Pong: m.Ping})
		if err != nil {
			log.Panicf("Error sending pong: %v", err)
		}
	} else {
		log.Panicf("Client does not handle responses")
	}
}
