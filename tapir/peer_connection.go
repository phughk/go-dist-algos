package main

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"net"
	"time"
)

// PeerConnection Peer connection handles inbound unclassified requests
// During the lifecycle we need to determine if it is a client or a server
type PeerConnection struct {
	ch     *ConnHandler
	server bool
	ir     *InconsistentReplicationProtocol
}

func newPeerConnection(ctx context.Context, conn net.Conn, ir *InconsistentReplicationProtocol) *PeerConnection {
	pc := &PeerConnection{
		ch:     nil,
		server: false,
		ir:     ir,
	}
	pc.ch = newConnHandler(ctx, conn, pc.handle)
	return pc
}

func (pc *PeerConnection) handle(ch *ConnHandler, m *AnyMessage) {
	fmt.Println("Received message")
	if pc.server {
	} else {
		if m.Hello != nil {
			if m.Hello.Type == ClientTypeServer {
				pc.server = true
				pc.ir.AddPeer(ch.conn.RemoteAddr().String(), ch, m.Hello.ViewID)
			}
		}
	}
}

func (pc *PeerConnection) blockingPingLoop() {
	for !pc.ch.terminated {
		resp, err := pc.ch.SendRequest(&AnyMessage{RequestID: uuid.New().String(), Ping: 1})
		if err != nil {
			logrus.Warnf("Error sending ping: %e", err)
			pc.ch.Close()
			break
		} else {
			logrus.Tracef("Ping response: %+v", resp)
		}
		time.Sleep(1 * time.Second)
	}
}
