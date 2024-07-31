package main

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"os"
)

func main() {
	setLogLevel()

	app := cli.App{
		Usage: "A tapir client and server for a KV store backed by bbolt",
		Commands: []*cli.Command{
			{
				Name:  "serve",
				Usage: "Run the server connecting to a cluster",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "cluster",
						Aliases:  []string{"c"},
						Required: true,
						Usage:    "comma-separated list of bootstrap servers",
					}, &cli.IntFlag{
						Name:     "port",
						Aliases:  []string{"p"},
						Required: false,
						Value:    0,
						Usage:    "port to listen on",
					},
					&cli.StringFlag{
						Name:     "filepath",
						Aliases:  []string{"f"},
						Required: false,
						Value:    fmt.Sprintf("%s.db", uuid.New().String()),
						Usage:    "filepath for storage",
					},
					&cli.IntFlag{
						Name:     "min-cluster-size",
						Aliases:  []string{"m"},
						Required: false,
						Value:    0,
						Usage:    "minimum cluster size, below this size operations will be rejected even if there is quorum",
					},
				},

				Action: serve,
			},
			{
				Name:  "client",
				Usage: "Run the client connecting to a tapir cluster",
				Flags: []cli.Flag{&cli.StringFlag{
					Name:     "cluster",
					Aliases:  []string{"c"},
					Required: true,
					Usage:    "comma-separated list of bootstrap servers",
				}},
				Action: client,
			},
		},
	}
	app.Run(os.Args)
}

func setLogLevel() {
	// Retrieve log level from environment variable
	logLevelStr := os.Getenv("LOG_LEVEL")

	// Default to info level if environment variable is not set
	logLevel := logrus.InfoLevel
	if logLevelStr != "" {
		var err error
		logLevel, err = logrus.ParseLevel(logLevelStr)
		if err != nil {
			logrus.Fatalf("Invalid log level: %s", logLevelStr)
		}
	}

	// Set the log level
	logrus.SetLevel(logLevel)
}
