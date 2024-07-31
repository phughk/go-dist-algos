package main

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"net"
	"strings"
)

func serve(c *cli.Context) error {
	logrus.Debugf("Running server...")

	store_filepath := c.String("filepath")
	db, err := NewStorageEngine(store_filepath)
	if err != nil {
		return err
	}
	defer db.Close()

	members_raw := c.String("cluster")
	if err != nil {
		return err
	}
	members := processMembers(members_raw)

	bind_port := c.Int("port")
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", bind_port))
	if err != nil {
		return err
	}
	port := listener.Addr().(*net.TCPAddr).Port
	host := listener.Addr().(*net.TCPAddr).IP.String()
	ctx := context.Background()
	ir := NewInconsistentReplicationProtocol(ctx, fmt.Sprintf("%s:%d", host, port), members, db)
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
			go serveConnectionInbound(conn, ir)
		}
	}
}

func processMembers(members_raw string) []string {
	members_split := strings.Split(members_raw, ",")
	logrus.Infof("Processing members: %+v", members_split)
	members := make([]string, 0)
	for _, member := range members_split {
		trimmed := strings.TrimSpace(member)
		if trimmed != "" {
			logrus.Infof("Adding member to list: '%s'", trimmed)
			members = append(members, trimmed)
		}
	}
	return members
}

func serveConnectionInbound(conn net.Conn, ir *InconsistentReplicationProtocol) {
	defer func() {
		fmt.Println("Connection closed: ", conn.RemoteAddr())
		err := conn.Close()
		if err != nil {
			fmt.Println("Error closing connection:", err)
		}
	}()

	fmt.Println("New connection from: ", conn.RemoteAddr())
	ctx := context.Background()
	ch := newPeerConnection(ctx, conn, ir)
	ch.blockingPingLoop()
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
