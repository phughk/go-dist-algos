package main

import (
	"bufio"
	"fmt"
	"github.com/sirupsen/logrus"
	"os"
	"strings"
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

func ClientRepl(client *Client) {
	fmt.Println("Interactive client, type 'help' for list of commands.")
	var transaction *ClientTransaction = nil
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("> ")
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading input:", err)
			continue
		}
		// Remove the last character (newline) from the input
		input = input[:len(input)-1]
		parts := strings.SplitN(input, " ", 2)
		command := strings.ToLower(parts[0])
		args := parts[1:]
		// Args is an array of 1, so we "join" it (effectively) no-op
		args = strings.Fields(strings.Join(args, " "))
		logrus.Debugf("Received command: %s, args: %+v\n", command, args)
		switch command {
		case "start", "s", "begin", "b":
			if !transaction.Empty() {
				fmt.Println("Abandoning previous transaction")
			}
			transaction = &ClientTransaction{
				ReadSet:  make(map[string]string),
				WriteSet: make(map[string]string),
			}
		case "read", "r", "get", "g":
			if len(args) < 1 {
				fmt.Println("Usage: read/r/get/g <keyN>")
				continue
			}
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
		case "write", "w", "put", "p":
			if len(args) < 2 {
				fmt.Println("Usage: write/w/put/p <key> <value>")
				fmt.Printf("Args(%d) were %+v\n", len(args), args)
				continue
			}
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
		case "commit", "c":
			if transaction.Empty() {
				transaction = nil
				continue
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
		case "cancel", "end", "e", "rollback":
			logrus.Debugf("Rolling back transaction...\n")
			transaction = nil
		case "help", "h":
			fmt.Println("Available commands:")
			fmt.Println("start/s/begin/b: Start a new transaction")
			fmt.Println("read/r/get/g <key>: Read the value of a key")
			fmt.Println("write/w/put/p <key> <value>: Write a key-value pair")
			fmt.Println("commit/c: Commit the current transaction")
			fmt.Println("cancel/end/e/rollback: Roll back the current transaction")
			fmt.Println("help/h: Show this help message")
			fmt.Println("exit: Exit the client")
		case "exit":
			// TODO cancel transaction?
			return
		default:
			fmt.Printf("Invalid command: %s, arguments: %v\n", command, args)
		}
	}
}
