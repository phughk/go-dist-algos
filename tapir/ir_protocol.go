package main

import (
	"context"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"math"
	"net"
	"sort"
	"sync"
	"time"
)

type InconsistentReplicationProtocol struct {
	self string
	tp   *TestProperties
	db   *StorageEngine
	// NOTE: this list can contain peers that are not members, and can miss peers that should be members
	peers map[string]*PeerTracker
	view  View
	mx    sync.RWMutex
}

type PeerTracker struct {
	conn   *ConnHandler
	ViewID int
}

type View struct {
	currentViewID int
	when          time.Time
	leader        string
	members       []string
	ViewState     ViewState
}

type ViewState struct {
	Normal int
	// Changing On receiving a message with a view number that is higher than its current view,
	// a replica moves to the VIEW-CHANGING state and requests the master record from any replica
	// in the higher view. It replaces its own record with the master record and upcalls into the application
	// protocol with Sync before returning to NORMAL state.
	Changing *ViewStateChanging
	Recovery *ViewStateRecovery
}

type ViewStateChanging struct {
	FromViewID      int
	ToViewID        int
	proposedMembers []string
}

type ViewStateRecovery struct {
	FromViewID int
	ToViewID   int
}

func NewInconsistentReplicationProtocol(ctx context.Context, self string, members []string, db *StorageEngine, tp *TestProperties) *InconsistentReplicationProtocol {
	// Read local store view, default is 0 with provided config
	ir := &InconsistentReplicationProtocol{
		self:  self,
		tp:    tp,
		peers: make(map[string]*PeerTracker),
		db:    db,
		view: View{
			currentViewID: 0,
			leader:        "",
			members:       members,
			when:          time.Now(),
			ViewState:     ViewState{Normal: 0, Changing: nil, Recovery: nil},
		},
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
	if p.tp.DecDropReplica() {
		return
	}
	if m.Ping != 0 {
		if p.tp.DecDropPing() {
			logrus.Tracef("Dropping ping message from peer '%s'", peer)
			return
		}
		err := ch.SendUntracked(&AnyMessage{RequestID: m.RequestID, Pong: m.Ping})
		if err != nil {
			logrus.Warnf("Error sending pong: %v", err)
		}
	} else if m.Hello != nil {
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
		logrus.Warnf("Unhandled message from peer '%+v': %+v", peer, m)
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

func (p *InconsistentReplicationProtocol) peersThatAreMembers() []string {
	p.mx.RLock()
	defer p.mx.RUnlock()
	memberPeers := make([]string, 0, len(p.peers))
	for s := range p.peers {
		for _, candidate := range p.view.members {
			if candidate == s {
				memberPeers = append(memberPeers, s)
			}
		}
	}
	return memberPeers
}

func (p *InconsistentReplicationProtocol) fastQuorum(count int) bool {
	total := len(p.view.members)
	// (3f/2)+1 > total
	// f > (total - 1) * (2/3)
	return float64(count) > math.Ceil((float64(total)-1.0)*(2.0/3.0))
}

func (p *InconsistentReplicationProtocol) majorityQuorum(count int) bool {
	total := len(p.view.members)
	// we tolerate f failures in a 2f+1 group
	// majority quorum is for f > (total-1)/2
	return float64(count) > math.Ceil((float64(total)-1.0)/2.0)
}

func (p *InconsistentReplicationProtocol) shouldBeNextLeader() bool {
	sorted := make([]string, 0, len(p.view.members))
	// Add self :)
	sorted = append(sorted, p.self)
	// Add only live nodes that are part of the current view
	p.mx.RLock()
	defer p.mx.RUnlock()
	for _, member := range p.view.members {
		if peer, ok := p.peers[member]; ok {
			if peer.conn.lastMessageTime.After(time.Now().Add(-p.tp.timeout)) {
				sorted = append(sorted, member)
			}
		}
	}
	// Check if we are the next leader
	sort.Strings(sorted)
	return sorted[0] == p.self
}

func (p *InconsistentReplicationProtocol) viewChangeNeeded() bool {
	// View changes are needed if the view has expired AND membership needs updating
	// This prevents membership being too flaky
	if p.view.when.IsZero() {
		panic("Current view 'when' is not set")
	}
	if p.tp.viewChangePeriod.Milliseconds() == 0 {
		panic("The view change period is set to 0ms")
	}
	viewChangeTimeoutExpired := p.view.when.Add(p.tp.viewChangePeriod).Before(time.Now())
	// Do we need to add anyone
	peersAreMembers := func() bool {
		p.mx.RLock()
		defer p.mx.RUnlock()
		// all members in peers, no need to vote anyone out
		for _, member := range p.view.members {
			if peer, ok := p.peers[member]; ok {
				if !peer.conn.lastMessageTime.After(time.Now().Add(-p.tp.timeout)) {
					return false
				}
			} else {
				return false
			}
		}
		return false
	}()
	// Do we need to remove anyone
	contains := func(item string, set map[string]*PeerTracker) bool {
		for k, _ := range set {
			if item == k {
				return true
			}
		}
		return false
	}
	membersArePeers := func() bool {
		p.mx.RLock()
		defer p.mx.RUnlock()
		for peer := range p.peers {
			if !contains(peer, p.peers) {
				return false
			}
		}
		return true
	}()
	if viewChangeTimeoutExpired {
		logrus.Tracef("The view timeout has expired but peersAreMember=%b and membersArePeers=%b", peersAreMembers, membersArePeers)
	}
	return viewChangeTimeoutExpired && !(peersAreMembers && membersArePeers)
}

func (p *InconsistentReplicationProtocol) protocolIteration() {
	// Check when the last view was
	if p.viewChangeNeeded() {
		p.proposeViewChange()
	}
	// Validate Leader and check view change need
}

// proposeViewChange During a view change, the leader has just one task: to make at least f + 1 replicas up-to-date
// (i.e., they have applied all operations in the operation set) and consistent with each other (i.e., they have applied the same consensus results).
// IR view changes require a leader because polling inconsistent replicas can lead to conflicting sets of operations and consensus results.
// Thus, the leader must decide on a master record that replicas can then use to synchronize with each other.
//
// Each view change is coordinated by a leader, which is unique per view and deterministically chosen.
// There are three key differences. First, in IR the leader merges records during a view change rather than simply
// taking the longest log from the latest view. The reason for this is that, with inconsistent replicas and unordered
// operations, any single record could be incomplete. Second, in VR, the leader is used to process operations in the
// normal case, but IR uses the leader only for performing view changes. Finally, on recovery, an IR replica performs
// a view change, rather than simply interrogating a single replica. This makes sure that the recovering replica either
// receives all operations it might have sent a reply for, or prevents them from completing.
//
// (RE TAPIR Lock service example)
// Sync for the lock server matches up all corresponding Lock and Unlock by id;
// if there are unmatched Locks, it sets locked = TRUE; otherwise, locked = FALSE.
func (p *InconsistentReplicationProtocol) proposeViewChange() {
	currentViewID := p.view.currentViewID
	p.view = View{
		currentViewID: currentViewID + 1,
		when:          time.Now(),
		leader:        "",
		members:       p.view.members,
		ViewState: ViewState{Normal: 0, Changing: &ViewStateChanging{
			FromViewID:      currentViewID,
			ToViewID:        currentViewID + 1,
			proposedMembers: p.livePeers(),
		},
			Recovery: nil,
		},
	}
	client := p.clusterOnlyClient()
	_, err := client.SendViewChangeRequest(&p.view)
	if err != nil {
		logrus.Warnf("Failed to change view: %s", err.Error())
	} else {
		logrus.Infof("Successfully changed view change")
	}
}

func (p *InconsistentReplicationProtocol) livePeers() []string {
	peers := make([]string, 0, len(p.peers))
	for members := range p.peers {
		peers = append(peers, members)
	}
	return peers
}

func (p *InconsistentReplicationProtocol) clusterOnlyClient() *Client {
	peer_connections := make([]*ConnHandler, 0, len(p.peers))
	for _, peer := range p.peers {
		peer_connections = append(peer_connections, peer.conn)
	}
	return &Client{Connections: peer_connections}
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
