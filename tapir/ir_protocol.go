package main

import (
	"context"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"net"
	"sync"
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
	conn   *ConnHandler
	ViewID int
}

type View struct {
	currentViewID int
	leader        string
	members       []string
}

func NewInconsistentReplicationProtocol(ctx context.Context, self string, members []string, db *StorageEngine) *InconsistentReplicationProtocol {
	// Read local store view, default is 0 with provided config
	view := &View{
		currentViewID: 0,
		leader:        "",
		members:       members,
	}
	ir := &InconsistentReplicationProtocol{
		self:  self,
		peers: make(map[string]*PeerTracker),
		db:    db,
		view:  view,
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
			peer := newConnHandler(ctx, conn,
				func(ch *ConnHandler, m *AnyMessage) {
					ir.handleMessage(thisMember, ch, m)
				},
				func() {
					ir.RemovePeer(thisMember)
				},
			)
			ir.AddPeer(thisMember, peer, 0)
			peer.SetShutdownHook(func() {
				ir.RemovePeer(thisMember)
			})
			go ir.peerInit(ctx, peer)
		}
	}
	go ir.protocolExecution(ctx)
	return ir
}

// / handleMessage is called by a node acting as a peer-client to another node
func (p *InconsistentReplicationProtocol) handleMessage(peer string, ch *ConnHandler, m *AnyMessage) {
	if m.Ping != 0 {
		err := ch.SendUntracked(&AnyMessage{RequestID: m.RequestID, Pong: m.Ping})
		if err != nil {
			logrus.Warnf("Error sending pong: %v", err)
		}
	} else if m.Hello != nil {
		logrus.Warnf("Received hello message: %+v", m.Hello)
		err := ch.SendUntracked(&AnyMessage{RequestID: m.RequestID,
			HelloResponse: &HelloResponse{ViewID: p.view.currentViewID,
				Members: p.view.members,
				Leader:  p.view.leader,
			}})
		if err != nil {
			logrus.Warnf("Error sending hello response: %v", err)
			ch.Close()
		}
	} else {
		logrus.Infof("Unhandled message from peer '%+v': %+v", peer, m)
	}
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
	// Validate Leader and check view change need
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
		conn:   ch,
		ViewID: ViewID,
	}
}

func (p *InconsistentReplicationProtocol) RemovePeer(member string) {
	p.mx.Lock()
	defer p.mx.Unlock()
	delete(p.peers, member)
	logrus.Infof("Removed peer: %s, peers now are: %+v", member, p.peers)
}

func (p *InconsistentReplicationProtocol) peerInit(ctx context.Context, peer *ConnHandler) {
	resp, err := peer.SendRequest(&AnyMessage{
		RequestID: uuid.New().String(),
		Hello:     NewHelloMessageFromServer(p.self, p.view.members, p.view.currentViewID, p.view.leader),
	})
	if err != nil {
		logrus.Warnf("Error sending hello message to peer '%+v': %v", peer.conn.RemoteAddr().String(), err)
		peer.Close()
		return
	}
	if resp.HelloResponse == nil {
		logrus.Warnf("Received unexpected hello response from peer '%+v': %+v", peer.conn.RemoteAddr().String(), resp)
		peer.Close()
		return
	}
	if resp.HelloResponse.ViewID > p.view.currentViewID {
		p.catchupToView(ctx, resp.HelloResponse)
	}
}

func (p *InconsistentReplicationProtocol) catchupToView(ctx context.Context, hello *HelloResponse) {
	logrus.Infof("TODO Catching up to view ID %d'", hello.ViewID)
}
