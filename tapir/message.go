package main

import (
	"fmt"
	"github.com/google/uuid"
)

type OperationRequestMode int

const (
	// Inconsistent mode operations are similar to operations in weak consistency
	//replication protocols: they can execute in different orders at each replica,
	//and the application protocol must resolve conflicts afterwards
	Inconsistent OperationRequestMode = iota

	// Consensus mode operations allow the application protocol to decide
	//the outcome of conflicts (by executing a decide function specified by the
	//application protocol) and recover that decision afterwards by ensuring that
	//the chosen result persists across failures as the consensus result.
	//
	// If replicas return conflicting/non-matching results for a consensus operation,
	// IR allows the application protocol to decide the operation’s outcome by
	// invoking the decide function – passed in by the application protocol to
	// InvokeConsensus – in the client-side library. The decide function takes the list of
	// returned results (the candidate results) and returns a single result,
	//which IR ensures will persist as the consensus result. The application
	//protocol can later recover the consensus result to find out its decision to
	//conflicting operations.
	Consensus
)

type OperationRequest struct {
	Mode          OperationRequestMode
	ClientID      string
	TransactionID string
	Propose       *Operation
	// Finalize can be its own message or piggy-backed onto next client proposed message
	Finalize *Operation
}

type Operation struct {
	ReadSet   []string
	WriteCSet map[string]PutcOp
	WriteSet  map[string]string
}

type PutcOp struct {
	Previous string
	Proposed string
}

func (o *OperationRequest) String() string {
	if o == nil {
		return "nil"
	} else {
		return fmt.Sprintf("%+v", *o)
	}
}

// OperationResponse is effectively the reply message
type OperationResponse struct {
	success    bool
	ReadValues map[string]string
}

func (o *OperationResponse) String() string {
	if o == nil {
		return "nil"
	} else {
		return fmt.Sprintf("%+v", *o)
	}
}

type AnyMessage struct {
	RequestID          string
	Hello              *HelloMessage
	HelloResponse      *HelloResponse
	OperationRequest   *OperationRequest
	OperationResponse  *OperationResponse
	ViewChangeRequest  *ViewChangeRequest
	ViewChangeResponse *ViewChangeResponse
	Ping               int
	Pong               int
}

type ViewChangeRequest struct {
	ViewID  int
	Members []string
}

func (v *ViewChangeRequest) String() string {
	if v == nil {
		return "nil"
	} else {
		return fmt.Sprintf("%+v", *v)
	}
}

type ViewChangeResponse struct {
	ViewID  int
	Members []string
}

func (v *ViewChangeResponse) String() string {
	if v == nil {
		return "nil"
	} else {
		return fmt.Sprintf("%+v", *v)
	}
}

type ClientType int

const (
	ClientTypeClient ClientType = iota
	ClientTypeServer
)

type HelloMessage struct {
	Type    ClientType
	ID      string
	Members []string
	ViewID  int
	Leader  string
}

func (m *HelloMessage) String() string {
	if m == nil {
		return "nil"
	} else {
		return fmt.Sprintf("%+v", *m)
	}
}

func NewHelloMessageFromClient(members []string, viewID int) *HelloMessage {
	return &HelloMessage{
		Type: ClientTypeClient,
		// The client ID doesn't matter as long as it's unique
		ID:      uuid.New().String(),
		Members: members,
		ViewID:  viewID,
	}
}

func NewHelloMessageFromServer(id string, members []string, viewID int, leader string) *HelloMessage {
	return &HelloMessage{
		Type:    ClientTypeServer,
		ID:      id,
		Members: members,
		ViewID:  viewID,
		Leader:  leader,
	}
}

type HelloResponse struct {
	ViewID  int
	Members []string
	Leader  string
}

func (m *HelloResponse) String() string {
	if m == nil {
		return "nil"
	} else {
		return fmt.Sprintf("%+v", *m)
	}
}
