package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/chzyer/readline"
	"github.com/sirupsen/logrus"
	"os"
	"strings"
)

type Repl struct {
	Intro    string
	Commands []*Command
	Cli      *readline.Instance
}

func NewRepl(intro string, commands []*Command) *Repl {
	l, err := readline.NewEx(&readline.Config{
		Prompt:      "\033[31m»\033[0m ",
		HistoryFile: "/tmp/readline.tmp",
		//AutoComplete:    completer,
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",

		HistorySearchFold: true,
		//FuncFilterInputRune: filterInput,
	})
	if err != nil {
		panic(err)
	}
	l.CaptureExitSignal()
	logrus.SetOutput(l.Stdout())
	return &Repl{
		Intro:    intro,
		Commands: commands,
		Cli:      l,
	}
}

type Command struct {
	Catches []string
	Help    string
	MinArgs int
	Execute func(args []string) error
}

func (r *Repl) Loop(ctx context.Context) {
	defer func() {
		err := r.Cli.Close()
		if err != nil {
			panic(err)
		}
	}()
	fmt.Println(r.Intro)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			err := r.Iteration()
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

func (r *Repl) Iteration() error {
	input, err := r.Cli.Readline()
	if err != nil {
		if errors.Is(err, readline.ErrInterrupt) {
			return ExitRequest(0)
		}
		fmt.Println("Error reading input:", err)
		return err
	}
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
		fmt.Printf("Unknown command '%+v'. Type 'help' for more information.\n", command)
		return nil
	}
}
