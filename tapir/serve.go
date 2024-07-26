package main

import (
	"fmt"
	"github.com/urfave/cli/v2"
	"net"
	"time"
)

func serve(c *cli.Context) error {
	fmt.Println("Running server...")
	listener, err := net.Listen("tcp", ":0")
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
	defer conn.Close()

	fmt.Println("New connection from: ", conn.RemoteAddr())
	ch := newConnHandler(conn, handleRequest)
	// Every 1s send a ping
	for {
		resp, err := ch.SendRequest(&AnyMessage{Ping: 1})
		if err != nil {
			fmt.Println("Error sending ping:", err)
			break
		} else {
			fmt.Printf("Ping response: %+v\n", resp)
		}
		time.Sleep(1 * time.Second)
	}
}

func handleRequest(ch *ConnHandler, m *AnyMessage) AnyMessage {
	// TODO: Handle request
	return AnyMessage{}
}
