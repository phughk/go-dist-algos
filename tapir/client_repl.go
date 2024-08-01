package main

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
)

// ClientTransaction is a client-side representation of a transaction
type ClientTransaction struct {
	// Cache of read values
	ReadSet map[string]string
	// Values being written, used alongside ReadSet for compare and swap
	WriteSet map[string]string
}

func (ct *ClientTransaction) Empty() bool {
	return ct == nil || (ct.ReadSet == nil && ct.WriteSet == nil)
}

func ClientRepl(ctx context.Context, client *Client) {
	var transaction *ClientTransaction = nil
	repl := newRepl("Interactive client, type 'help' for list of commands.", []*Command{
		{
			Catches: []string{"start", "s", "begin", "b"},
			Help:    "Start a new transaction",
			MinArgs: 0,
			Execute: func(args []string) error {
				if !transaction.Empty() {
					fmt.Println("Abandoning previous transaction")
				}
				transaction = &ClientTransaction{
					// TODO version
					ReadSet:  make(map[string]string),
					WriteSet: make(map[string]string),
				}
				return nil
			},
		},
		{
			Catches: []string{"read", "r", "get", "g"},
			Help:    "Read a value from the database",
			MinArgs: 1,
			Execute: func(args []string) error {
				logrus.Debugf("Reading keys: %+v...\n", args)
				resp, err := client.SendOperationRequest(args, nil)
				if err != nil {
					logrus.Warnf("Error reading key: %v\n", err)
				}
				for k, v := range resp.ReadValues {
					if transaction != nil {
						transaction.ReadSet[k] = v
					}
					fmt.Printf("%+v=%+v\n", k, v)
				}
				return nil
			},
		},
		{
			Catches: []string{"write", "w", "put", "p"},
			Help:    "Write a value to the database",
			MinArgs: 2,
			Execute: func(args []string) error {
				if transaction != nil {
					// Active transaction, cache writes
					transaction.WriteSet[args[0]] = args[1]
				} else {
					// Transaction is not active so this operation is standalone
					logrus.Debugf("Writing key: %s, value: %s...\n", args[0], args[1])
					resp, err := client.SendOperationRequest([]string{}, map[string]PutcOp{args[0]: {
						Previous: "",
						Proposed: args[1],
					}})
					if err != nil {
						logrus.Warnf("Error writing key: %v", err)
					}
					for k, v := range resp.ReadValues {
						fmt.Printf("%+v=%+v\n", k, v)
					}
				}
				return nil
			},
		},
		{
			Catches: []string{"commit", "c"},
			Help:    "Commit the transaction",
			MinArgs: 0,
			Execute: func(args []string) error {
				if transaction.Empty() {
					transaction = nil
				} else {
					logrus.Debugf("Committing transaction...\n")
					// make a new write set including reads as no-op
					newWriteSet := make(map[string]PutcOp)
					for k, v := range transaction.WriteSet {
						newWriteSet[k] = PutcOp{
							Previous: transaction.ReadSet[k],
							Proposed: v,
						}
					}
					newReadSet := make([]string, 0, len(transaction.ReadSet))
					for k := range transaction.ReadSet {
						newReadSet = append(newReadSet, k)
					}
					resp, err := client.SendOperationRequest(newReadSet, newWriteSet)
					if err != nil {
						logrus.Warnf("Error committing transaction: %v\n", err)
					}
					logrus.Debugf("Received response: %+v\n", resp)
				}
				return nil
			},
		},
		{
			Catches: []string{"cancel", "end", "e", "rollback"},
			Help:    "Cancel or rollback the transaction",
			MinArgs: 0,
			Execute: func(args []string) error {
				logrus.Debugf("Rolling back transaction...\n")
				transaction = nil
				return nil
			},
		},
	})
	repl.Loop(ctx)
}
