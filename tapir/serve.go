package main

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/urfave/cli/v2"
	"log"
	"net"
	"time"
)

func serve(c *cli.Context) error {
	fmt.Println("Running server...")

	store_filepath := c.String("filepath")
	db, err := NewStorageEngine(store_filepath)
	if err != nil {
		return err
	}
	defer db.Close()

	bind_port := c.Int("port")
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", bind_port))
	if err != nil {
		return err
	}
	port := listener.Addr().(*net.TCPAddr).Port
	defer listener.Close()
	fmt.Println("Listening on port:", port)
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		go serveConnection(conn)
	}
	return nil
}

func serveConnection(conn net.Conn) {
	defer func() {
		fmt.Println("Connection closed: ", conn.RemoteAddr())
		err := conn.Close()
		if err != nil {
			fmt.Println("Error closing connection:", err)
		}
	}()

	fmt.Println("New connection from: ", conn.RemoteAddr())
	ch := newConnHandler(conn, handleRequest)
	// Every 1s send a ping
	for !ch.terminated {
		resp, err := ch.SendRequest(&AnyMessage{RequestID: uuid.New().String(), Ping: 1})
		if err != nil {
			fmt.Println("Error sending ping:", err)
			break
		} else {
			fmt.Printf("Ping response: %+v\n", resp)
		}
		time.Sleep(1 * time.Second)
	}
}

func handleRequest(ch *ConnHandler, m *AnyMessage) {
	fmt.Printf("Server inbound request: %+v\n", m)
	if m.Pong != 0 {
		fmt.Printf("Server received pong: %+v\n", m)
	} else if m.OperationRequest != nil {
		err := ch.SendUntracked(&AnyMessage{RequestID: m.RequestID, OperationResponse: &OperationResponse{}})
		if err != nil {
			log.Panicf("Error sending response: %e", err)
		}
	} else {
		fmt.Printf("Server unhandled request: %+v\n", m)
	}
}
