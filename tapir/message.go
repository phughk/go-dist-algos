package main

import "fmt"

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
	Mode OperationRequestMode
}

func (o *OperationRequest) String() string {
	if o == nil {
		return "nil"
	} else {
		return fmt.Sprintf("%+v", *o)
	}
}

type OperationResponse struct {
	Dummy string
}

func (o *OperationResponse) String() string {
	if o == nil {
		return "nil"
	} else {
		return fmt.Sprintf("%+v", *o)
	}
}

type AnyMessage struct {
	RequestID         string
	OperationRequest  *OperationRequest
	OperationResponse *OperationResponse
	Ping              int
	Pong              int
}
