package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func ServerRepl(ctx context.Context, tp *TestProperties) {
	fmt.Println("Server REPL started. Type 'help' for more information.")
	for {
		select {
		case <-ctx.Done():
			return
		default:
			fmt.Print("> ")
			var input string
			_, err := fmt.Scanln(&input)
			if err != nil {
				fmt.Println("Error reading input:", err)
				continue
			}
			parts := strings.SplitN(input, " ", 2)
			command := strings.ToLower(parts[0])
			args := parts[1:]
			switch command {
			case "latency", "l":
				if len(args) != 1 {
					fmt.Println("Usage: latency <latency_ms>")
					continue
				}
				latency, err := strconv.Atoi(args[0])
				if err != nil {
					fmt.Println("Invalid latency value. Please enter a positive integer.")
					continue
				}
				tp.SetLatency(time.Duration(latency) * time.Millisecond)
			case "drop_ping":
				if len(args) < 1 {
					fmt.Println("Usage: drop_ping/d <number_of_messages>")
					continue
				}
				num, err := strconv.Atoi(args[0])
				if err != nil {
					fmt.Println("Invalid number of messages to drop_ping. Please enter a positive integer.")
					continue
				}
				tp.AddDropPing(num)
			case "drop_replica":
				if len(args) < 1 {
					fmt.Println("Usage: drop_replica/r <number_of_messages>")
					continue
				}
				num, err := strconv.Atoi(args[0])
				if err != nil {
					fmt.Println("Invalid number of messages to drop_replica. Please enter a positive integer.")
					continue
				}
				tp.AddDropReplica(num)
			case "drop_client":
				if len(args) < 1 {
					fmt.Println("Usage: drop_client/c <number_of_messages>")
					continue
				}
				num, err := strconv.Atoi(args[0])
				if err != nil {
					fmt.Println("Invalid number of messages to drop_client. Please enter a positive integer.")
					continue
				}
				tp.AddDropClient(num)
			case "help", "h":
				fmt.Println("Available commands:")
				fmt.Println("  latency, l: Set the latency for incoming requests")
				fmt.Println("  drop_ping, d: Set the number of messages to drop_ping before processing")
				fmt.Println("  help, h: Display this help message")
				fmt.Println("  exit, quit: Terminate the server")
			case "exit", "quit":
				fmt.Println("Server terminated.")

				os.Exit(0)
			default:
				fmt.Println("Unknown command. Type 'help' for more information.")
			}
		}
	}
}
