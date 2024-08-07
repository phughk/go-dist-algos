package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"net"
	"strings"
	"syscall"
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
	host := normaliseIp(listener.Addr().(*net.TCPAddr).IP)
	ctx := context.Background()
	test_properties := &TestProperties{
		viewChangePeriod: time.Duration(1) * time.Second,
	}
	ir := NewInconsistentReplicationProtocol(ctx, fmt.Sprintf("%s:%d", host, port), members, db, test_properties)
	defer listener.Close()
	logrus.Infof("Listening on port: %d", port)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ServerRepl(ctx, test_properties, ir)
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

func normaliseIp(ip net.IP) string {
	if ip.IsLoopback() || ip.IsMulticast() || ip.IsUnspecified() {
		// We need to do this because binding to `:0` etc causes multi-host bind, but we need a specific address for membership identity
		return "127.0.0.1"
	}
	if v4 := ip.To4(); v4 != nil {
		return fmt.Sprintf("%d.%d.%d.%d", v4[0], v4[1], v4[2], v4[3])
	}
	panic(fmt.Sprintf("Invalid IPv6 address: %s", ip))
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
			if !(errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED)) {
				logrus.Warnf("Error closing connection: %+v", err)
			}
		}
	}()

	fmt.Println("New connection from: ", conn.RemoteAddr())
	ctx := context.Background()
	ch := newPeerConnection(ctx, conn, ir)
	ch.blockingPingLoop()
}
