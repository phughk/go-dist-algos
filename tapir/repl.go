package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
)

type Repl struct {
	Intro    string
	Commands []*Command
}

func newRepl(intro string, commands []*Command) *Repl {
	return &Repl{
		Intro:    intro,
		Commands: commands,
	}
}

type Command struct {
	Catches []string
	Help    string
	MinArgs int
	Execute func(args []string) error
}

func (r *Repl) Loop(ctx context.Context) {
	fmt.Println(r.Intro)
	reader := bufio.NewReader(os.Stdin)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			err := r.Iteration(reader)
			if err == ExitRequest(0) {
				os.Exit(0)
			} else if err != nil {
				fmt.Printf("Error executing command: %v\n", err)
			}
		}
	}
}

type ExitRequest int

func (e ExitRequest) Error() string {
	return "received exit request"
}

func (r *Repl) Iteration(reader *bufio.Reader) error {
	fmt.Print("> ")
	var input string
	input, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading input:", err)
		return err
	}
	// Remove the last character (newline) from the input
	input = input[:len(input)-1]
	parts := strings.SplitN(input, " ", 2)
	command := strings.ToLower(parts[0])
	args := parts[1:]
	// Args is an array of 1, so we "join" it (effectively) no-op
	args = strings.Fields(strings.Join(args, " "))
	if command == "help" {
		fmt.Println("Available commands:")
		for _, cmd := range r.Commands {
			fmt.Printf("  %s: %s\n", strings.Join(cmd.Catches, ", "), cmd.Help)
		}
		return nil
	} else if command == "exit" || command == "quit" {
		return ExitRequest(0)
	} else {
		for _, cmd := range r.Commands {
			if strings.Contains(strings.Join(cmd.Catches, "|"), command) {
				if len(args) < cmd.MinArgs {
					fmt.Printf("Usage: %s\n", cmd.Help)
					return nil
				}
				if err := cmd.Execute(args); err != nil {
					fmt.Println("Error executing command:", err)
				}
				return nil
			}
		}
		fmt.Println("Unknown command. Type 'help' for more information.")
		return nil
	}
}
