package main

import (
	"context"
	"github.com/sirupsen/logrus"
	"net"
)

type InconsistentReplicationProtocol struct {
	self  string
	db    *StorageEngine
	peers map[string]*ConnHandler
	view  *View
}

type View struct {
	currentViewID int
	leader        string
	members       []string
}

func NewInconsistentReplicationProtocol(ctx context.Context, self string, members []string, db *StorageEngine) *InconsistentReplicationProtocol {
	ir := &InconsistentReplicationProtocol{
		self:  self,
		peers: make(map[string]*ConnHandler),
		db:    db,
		// We discover the view from local store and members
		view: nil,
	}
	for _, member := range members {
		conn, err := net.Dial("tcp", member)
		if err != nil {
			logrus.Warnf("Error connecting to peer: %v", err)
		} else {
			thisMember := member
			peer := newConnHandler(conn, func(ch *ConnHandler, m *AnyMessage) {
				ir.handleMessage(thisMember, ch, m)
			})
			ir.peers[member] = peer
		}
	}
	go ir.protocolExecution(ctx)
	return ir
}

func (p *InconsistentReplicationProtocol) handleMessage(thisMember string, ch *ConnHandler, m *AnyMessage) {

}

func (p *InconsistentReplicationProtocol) protocolExecution(ctx context.Context) {
	// Read local view
	// Check member views
	for {
		select {
		case <-ctx.Done():
			return
		default:
			p.protocolIteration()
		}
	}
}

func (p *InconsistentReplicationProtocol) protocolIteration() {
	// Check last messages
	// Confirm view and members
	// Validate leader and check view change need
}
