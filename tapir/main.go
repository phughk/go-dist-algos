package main

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/urfave/cli/v2"
	"os"
)

func main() {
	app := cli.App{
		Commands: []*cli.Command{
			{
				Name:  "serve",
				Usage: "Run the server connecting to a cluster",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "cluster",
						Aliases:  []string{"c"},
						Required: true,
						Usage:    "semi-colon-separated list of bootstrap servers",
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
					Usage:    "semi-colon-separated list of bootstrap servers",
				}},
				Action: client,
			},
		},
	}
	app.Run(os.Args)
}
