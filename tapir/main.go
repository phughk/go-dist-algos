package main

import (
	"github.com/urfave/cli/v2"
	"os"
)

func main() {
	app := cli.App{
		Commands: []*cli.Command{
			{
				Name:  "serve",
				Usage: "Run the server connecting to a cluster",
				Flags: []cli.Flag{&cli.StringFlag{
					Name:     "bootstrap",
					Aliases:  []string{"b"},
					Required: true,
					Usage:    "semi-colon-separated list of bootstrap servers",
				}},
				Action: serve,
			},
			{
				Name:  "client",
				Usage: "Run the client connecting to a tapir cluster",
				Flags: []cli.Flag{&cli.StringFlag{
					Name:     "bootstrap",
					Aliases:  []string{"b"},
					Required: true,
					Usage:    "semi-colon-separated list of bootstrap servers",
				}},
				Action: client,
			},
		},
	}
	app.Run(os.Args)
}
