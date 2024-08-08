package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"log"
	"math"
	"net"
	"strings"
	"syscall"
	"time"
)

func client(c *cli.Context) error {
	logrus.Debugf("Running client...")
	bootstrap := c.String("cluster")
	client_id := uuid.New().String()
	logrus.Debugf("Client ID: %s\n", client_id)
	servers := strings.Split(bootstrap, ";")
	connections := make([]*ConnHandler, len(servers))
	ctx, cancel := context.WithCancel(context.Background())
	for i, server := range servers {
		conn, err := net.Dial("tcp", server)
		if err != nil {
			return err
		}
		connections[i] = newConnHandler(ctx, conn, clientRequestHandler, func() {
			cancel()
		})
		defer func() {
			cancel()
			err := conn.Close()
			if err != nil {
				if !(errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED)) {
					logrus.Errorf("Error closing connection to server: %v", err)
				}
			}
		}()
		logrus.Debugf("Connected to server: %+v", server)
		client := &Client{Connections: connections}
		ClientRepl(ctx, client)
	}
	return nil
}

type Client struct {
	Connections    []*ConnHandler
	TestProperties *TestProperties
}

// SendOperationRequest IR replicas send their current view number in every response to clients. For an operation to
// be considered successful, the IR client must receive responses with matching view numbers. For consensus operations,
// the view numbers in REPLY and CONFIRM must match as well. If a client receives responses with different view numbers,
// it notifies the replicas in the older view.
func (c *Client) SendOperationRequest(readSet []string, writeSet map[string]PutcOp) (*OperationResponse, error) {
	// Response channel
	responseChan := make(chan *AnyMessage, len(c.Connections))
	// WriteSet is values that haven't been read
	// WriteCSet is values that have been read
	writeCSet := make(map[string]PutcOp)
	for _, v := range readSet {
		_, ok := writeSet[v]
		if ok {
			writeCSet[v] = writeSet[v]
			delete(writeSet, v)
		}
	}
	newWriteSet := make(map[string]string)
	for k, v := range writeSet {
		newWriteSet[k] = v.Proposed
	}
	// Send message to all servers
	operationRequest := &OperationRequest{
		Mode: Inconsistent,
		Propose: &Operation{
			ReadSet:   readSet,
			WriteSet:  newWriteSet,
			WriteCSet: writeCSet,
		},
		TransactionID: uuid.New().String(),
	}
	for _, conn := range c.Connections {
		go func(conn *ConnHandler) {
			request := AnyMessage{
				RequestID:        uuid.New().String(),
				OperationRequest: operationRequest,
			}
			resp, err := conn.SendRequest(&request)
			if err != nil {
				log.Panicf("Error sending operation request to server: %v\n", err)
				return
			}
			responseChan <- resp
		}(conn)
	}
	// Wait for all responses with timeout 5s
	logrus.Tracef("Received %d out of %d responses\n", len(c.Connections), len(responseChan))
	var responses []*OperationResponse
	for i := 0; i < len(c.Connections); i++ {
		select {
		case resp := <-responseChan:
			responses = append(responses, resp.OperationResponse)
		case <-time.After(c.TestProperties.timeout):
			break
		}
	}

	// TODO: Decide value
	decidedValue := responses[0]
	fmt.Printf("Decided value: %+v\n", decidedValue)
	return decidedValue, nil
}

type MaybeError struct {
	Error    error
	Response *AnyMessage
}

func (c *Client) SendViewChangeRequest(view *View) (*View, error) {
	clusterMembers := view.members
	membersNotSelf := view.members
	// Remove self from members
	if view.self == "" {
		panic("View's self was empty")
	}
	for i := 0; i < len(membersNotSelf); i++ {
		if membersNotSelf[i] == view.self {
			membersNotSelf = append(membersNotSelf[:i], membersNotSelf[i+1:]...)
			break
		}
	}
	expectedMemberResults := make(chan *MaybeError)
	// We need f+1 results for membership to pass
	for _, peer := range c.Connections {
		go func() {
			// Request timeout is handled outside this function by catch-all
			res, err := peer.SendRequest(&AnyMessage{
				RequestID: uuid.New().String(),
				ViewChangeRequest: &ViewChangeRequest{
					ViewID:  view.ViewState.Changing.ToViewID,
					Members: view.ViewState.Changing.proposedMembers,
				},
			})
			if err != nil {
				logrus.Errorf("Below formatting error is for this: %v", err.Error())
			}
			expectedMemberResults <- &MaybeError{Error: err, Response: res}
		}()
	}
	// Now collect all responses or timeout
	responses := make([]*AnyMessage, 0, len(membersNotSelf))
	for i := 0; i < len(membersNotSelf); i++ {
		select {
		case resp := <-expectedMemberResults:
			if resp.Error != nil {
				logrus.Warnf("Failed to make peer request to change view: %s", resp.Error.Error())
			} else {
				responses = append(responses, resp.Response)
			}
		case <-time.After(5 * time.Second):
			logrus.Warnf("Not all members responded to change view: received %d out of %d responses (%+v)", len(responses), len(membersNotSelf), membersNotSelf)
		}
	}
	// check if we have quorum results, if not then fail
	// this is majority (slow, classic) quorum of f+1 in a 2f+1 cluster
	// So if total is 2f + 1, and we want at least n, then n > f+1
	classic_quorum := len(clusterMembers)/2 + 1
	// The responses do not include ourselves, so we need to remove one to account for ourselves
	classic_quorum -= 1
	if len(responses) < classic_quorum {
		return nil, fmt.Errorf("not enough responses to change view: received %d, required %d, cluster %d", len(responses), classic_quorum, len(membersNotSelf))
	}
	// All the responses for quorum must have the same view
	// - remove responses that are older
	// - invalidate results if there is a matching or higher view
	// This effectively is reduced to "only responses that are on the same current view"
	latest_view := view.currentViewID
	resp_in_view := make([]*AnyMessage, 0, len(responses))
	for _, response := range responses {
		if response.ViewChangeResponse.ViewID != view.currentViewID {
			// We want to track the latest view in case we are behind
			latest_view = int(math.Max(float64(latest_view), float64(response.ViewChangeResponse.ViewID)))
			// Members that arent caught up in current view can be rejected
			continue
		}
		resp_in_view = append(resp_in_view, response)
	}
	// Re-verify we have quorum of correct view responses
	if len(resp_in_view) < classic_quorum {
		return nil, fmt.Errorf("not enough responses to change view as the views were in different state: matching views %d, received %d, required %d, cluster %d", len(resp_in_view), len(responses), classic_quorum, len(membersNotSelf))
	}
	return nil, nil
}

func clientRequestHandler(ch *ConnHandler, m *AnyMessage) {
	if m.Ping != 0 {
		logrus.Tracef("Client received ping: %+v\n", m)
		err := ch.SendUntracked(&AnyMessage{RequestID: m.RequestID, Pong: m.Ping})
		if err != nil {
			log.Panicf("Error sending pong: %v", err)
		}
	} else {
		log.Panicf("Client does not handleClient responses")
	}
}
