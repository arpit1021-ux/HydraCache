package main

import (
	"flag"
	"fmt"
	"os"
)

const version = "1.0.0"

func main() {
	cfg := ParseConfig()

	if flag.NArg() == 0 {
		flag.Usage()
		return
	}

	cmd := flag.Arg(0)
	args := flag.Args()[1:]

	switch cmd {
	case "version":
		fmt.Printf("HydraCache CLI v%s\n", version)
		return
	case "help":
		flag.Usage()
		return
	}

	c, err := NewClient(cfg.Address())
	if err != nil {
		fmt.Fprintf(os.Stderr, "(error) %v\n", err)
		os.Exit(1)
	}
	defer c.Close()

	executeCommand(c, cmd, args)
}
