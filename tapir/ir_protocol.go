package main

import (
	"context"
	"github.com/sirupsen/logrus"
	"net"
	"sync"
	"time"
)

type InconsistentReplicationProtocol struct {
	self string
	db   *StorageEngine
	// NOTE: this list can contain peers that are not members, and can miss peers that should be members
	peers map[string]*PeerTracker
	view  *View
	mx    sync.Mutex
}

type PeerTracker struct {
	conn         *ConnHandler
	lastPingTime time.Time
	ViewID       int
}

type View struct {
	currentViewID int
	leader        string
	members       []string
}

func NewInconsistentReplicationProtocol(ctx context.Context, self string, members []string, db *StorageEngine) *InconsistentReplicationProtocol {
	ir := &InconsistentReplicationProtocol{
		self:  self,
		peers: make(map[string]*PeerTracker),
		db:    db,
		// We discover the view from local store and members
		view: nil,
	}
	logrus.Infof("Initialized InconsistentReplicationProtocol with self '%s' and members(%d) '%+v'", self, len(members), members)
	for _, member := range members {
		if member == self {
			continue
		}
		conn, err := net.Dial("tcp", member)
		if err != nil {
			logrus.Warnf("Error connecting to peer '%+v': %v", member, err)
		} else {
			thisMember := member
			logrus.Infof("Connected to peer: %s", thisMember)
			peer := newConnHandler(ctx, conn, func(ch *ConnHandler, m *AnyMessage) {
				ir.handleMessage(thisMember, ch, m)
			})
			ir.AddPeer(thisMember, peer, 0)
		}
	}
	go ir.protocolExecution(ctx)
	return ir
}

func (p *InconsistentReplicationProtocol) handleMessage(peer string, ch *ConnHandler, m *AnyMessage) {
	logrus.Warnf("Unhandled message from %s: %+v", peer, m)
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

func (p *InconsistentReplicationProtocol) AddPeer(s string, ch *ConnHandler, ViewID int) {
	// The lock is important both for iterating over membership but also for detail changes
	p.mx.Lock()
	defer p.mx.Unlock()
	previous, ok := p.peers[s]
	if ok {
		delete(p.peers, s)
		previous.conn.Close()
	}
	p.peers[s] = &PeerTracker{
		conn:         ch,
		lastPingTime: time.Now(),
		ViewID:       ViewID,
	}
}
