package main

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

func ServerRepl(ctx context.Context, tp *TestProperties) {
	repl := &Repl{
		Intro: "TAPIR KV Server REPL, type 'help' for list of commands.",
		Commands: []*Command{
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
		},
	}
	repl.Loop(ctx)
}
