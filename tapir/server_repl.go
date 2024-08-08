package main

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

func ServerRepl(ctx context.Context, tp *TestProperties, ir *InconsistentReplicationProtocol) {
	repl := NewRepl("TAPIR KV Server REPL, type 'help' for list of commands.",
		[]*Command{
			{
				Catches: []string{"status", "s"},
				Help:    "Display status of replica",
				MinArgs: 0,
				Execute: func(args []string) error {
					fmt.Printf("Self: %s\n", ir.self)
					fmt.Printf("View: %+v\n", ir.view)
					return nil
				},
			},
			{
				Catches: []string{"latency", "l"},
				Help:    "Set the latency for incoming requests",
				MinArgs: 1,
				Execute: func(args []string) error {
					latency, err := strconv.Atoi(args[0])
					if err != nil {
						return fmt.Errorf("invalid latency value: %w", err)
					}
					tp.SetLatency(time.Duration(latency) * time.Millisecond)
					return nil
				},
			},
			{
				Catches: []string{"view_change_period", "vc"},
				Help:    "Set the view change period",
				MinArgs: 1,
				Execute: func(args []string) error {
					period, err := strconv.Atoi(args[0])
					if err != nil {
						return fmt.Errorf("invalid view change period: %w", err)
					}
					tp.SetViewChangePeriod(time.Duration(period) * time.Second)
					return nil
				},
			},
			{
				Catches: []string{"timeout", "t"},
				Help:    "Set the timeout for incoming requests",
				MinArgs: 1,
				Execute: func(args []string) error {
					timeout, err := strconv.Atoi(args[0])
					if err != nil {
						return fmt.Errorf("invalid timeout value: %w", err)
					}
					tp.SetTimeout(time.Duration(timeout) * time.Millisecond)
					return nil
				},
			},
			{
				Catches: []string{"drop_ping", "dp"},
				Help:    "Set the number of messages to drop_ping before processing",
				MinArgs: 1,
				Execute: func(args []string) error {
					num, err := strconv.Atoi(args[0])
					if err != nil {
						return fmt.Errorf("invalid number of messages to drop_ping: %w", err)
					}
					tp.AddDropPing(num)
					return nil
				},
			},
			{
				Catches: []string{"drop_replica", "dr"},
				Help:    "Set the number of messages to drop_replica before processing",
				MinArgs: 1,
				Execute: func(args []string) error {
					i, err := strconv.Atoi(args[0])
					if err != nil {
						return fmt.Errorf("invalid number of messages to drop_replica: %w", err)
					}
					tp.AddDropReplica(i)
					return nil
				},
			},
			{
				Catches: []string{"drop_client", "dc"},
				Help:    "Set the number of messages to drop_client before processing",
				MinArgs: 1,
				Execute: func(args []string) error {
					num, err := strconv.Atoi(args[0])
					if err != nil {
						return fmt.Errorf("invalid number of messages to drop_client: %w", err)
					}
					tp.AddDropClient(num)
					return nil
				},
			},
			{
				Catches: []string{"peers"},
				Help:    "List the active peers",
				MinArgs: 0,
				Execute: func(args []string) error {
					fmt.Println("Active peers:")
					for member, peer := range ir.peers {
						fmt.Printf("peer - %s\n", member)
						fmt.Printf("     - Last message time: %s\n", peer.conn.lastMessageTime.Format(time.RFC3339Nano))
						fmt.Printf("     - View ID: %d\n", peer.ViewID)
					}
					return nil
				},
			},
			{
				Catches: []string{"members"},
				Help:    "List the active members",
				MinArgs: 0,
				Execute: func(args []string) error {
					fmt.Printf("Active members for view %d:\n", ir.view.currentViewID)
					for _, member := range ir.view.members {
						fmt.Printf("member - %s\n", member)
					}
					return nil
				},
			},
		},
	)
	repl.Loop(ctx)
}
