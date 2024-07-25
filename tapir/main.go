package main

import (
	"fmt"
	"github.com/urfave/cli/v2"
	"net"
	"os"
	"strings"
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

func serve(c *cli.Context) error {
	fmt.Println("Running server...")
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return err
	}
	port := listener.Addr().(*net.TCPAddr).Port
	defer listener.Close()
	fmt.Println("Listening on port:", port)
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		go serve_connection(conn)
	}
	return nil
}

func serve_connection(conn net.Conn) {
	defer conn.Close()
	fmt.Println("New connection from: ", conn.RemoteAddr())
}

func client(c *cli.Context) error {
	fmt.Println("Running client...")
	bootstrap := c.String("bootstrap")
	servers := strings.Split(bootstrap, ";")
	connections := make([]net.Conn, len(servers))
	for i, server := range servers {
		conn, err := net.Dial("tcp", server)
		if err != nil {
			return err
		}
		connections[i] = conn
		defer conn.Close()
		fmt.Println("Connected to server:", server)
	}
	return nil
}
