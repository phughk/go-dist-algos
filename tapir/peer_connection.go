package main

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"log"
	"net"
	"time"
)

// PeerConnection Peer connection handles inbound unclassified requests
// During the lifecycle we need to determine if it is a client or a server
type PeerConnection struct {
	ch       *ConnHandler
	server   bool
	memberID string
	ir       *InconsistentReplicationProtocol
}

func newPeerConnection(ctx context.Context, conn net.Conn, ir *InconsistentReplicationProtocol) *PeerConnection {
	pc := &PeerConnection{
		ch:     nil,
		server: false,
		ir:     ir,
	}
	// We use a no-op shutdown hook because we don't know if its a client or peer node
	// When we discover its a peer we change the shutdown hook
	shutdownHook := func() {}
	pc.ch = newConnHandler(ctx, conn, pc.handleClient, shutdownHook)
	return pc
}

// Since we cannot differentiate between client and server, this code handles p2p server upgrade and client comms in one
func (pc *PeerConnection) handleClient(ch *ConnHandler, m *AnyMessage) {
	fmt.Println("Received message")
	if m.Ping != 0 {
		logrus.Tracef("Client received ping: %+v\n", m)
		err := ch.SendUntracked(&AnyMessage{RequestID: m.RequestID, Pong: m.Ping})
		if err != nil {
			log.Panicf("Error sending pong: %v", err)
		}
	} else if m.Pong != 0 {
		// This shouldn't happen because ping is synchronous...?
		logrus.Warnf("Received unhandled pong but that is a synchronous request")
	} else if m.Hello != nil {
		if m.Hello.Type == ClientTypeServer {
			pc.server = true
			pc.memberID = m.Hello.ID
			pc.ir.AddPeer(ch.conn.RemoteAddr().String(), ch, m.Hello.ViewID)
			ch.SetShutdownHook(func() {
				// We need the service address
				pc.ir.RemovePeer(pc.memberID)
			})
			// Upgrade protocol to server comms
			ch.SetHandler(func(ch *ConnHandler, m *AnyMessage) {
				pc.ir.handleMessage(pc.memberID, ch, m)
			})
			// Now invoke the hello inbound path of the server handler to respond to hello
			ch.requestHandler(ch, m)
		}
	} else if m.OperationRequest != nil {
		err := ch.SendUntracked(&AnyMessage{RequestID: m.RequestID, OperationResponse: &OperationResponse{}})
		if err != nil {
			logrus.Panicf("Error sending response: %e", err)
		}
	} else {
		logrus.Errorf("Server unhandled request: %+v", m)
	}
}

func (pc *PeerConnection) blockingPingLoop() {
	for !pc.ch.terminated.Load() {
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
