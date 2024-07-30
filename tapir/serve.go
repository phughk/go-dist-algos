package main

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"net"
	"time"
)

func serve(c *cli.Context) error {
	logrus.Debugf("Running server...")

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
	logrus.Infof("Listening on port: %d", port)
	test_properties := &TestProperties{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ServerRepl(ctx, test_properties)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			conn, err := listener.Accept()
			if err != nil {
				return err
			}
			go serveConnection(conn)
		}
	}
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
			logrus.Warnf("Error sending ping: %e", err)
			break
		} else {
			logrus.Tracef("Ping response: %+v", resp)
		}
		time.Sleep(1 * time.Second)
	}
}

func handleRequest(ch *ConnHandler, m *AnyMessage) {
	logrus.Tracef("Server inbound request: %+v", m)
	if m.Pong != 0 {
		logrus.Tracef("Server received pong: %+v", m)
	} else if m.OperationRequest != nil {
		err := ch.SendUntracked(&AnyMessage{RequestID: m.RequestID, OperationResponse: &OperationResponse{}})
		if err != nil {
			logrus.Panicf("Error sending response: %e", err)
		}
	} else {
		logrus.Errorf("Server unhandled request: %+v", m)
	}
}
