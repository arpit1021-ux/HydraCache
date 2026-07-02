package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/hydracache/hydracache/internal/protocol"
)

func printResponse(resp *protocol.Response) {
	switch resp.Type {
	case protocol.ResponseSimpleString:
		fmt.Println(resp.Str)
	case protocol.ResponseError:
		fmt.Fprintf(os.Stderr, "(error) %s\n", resp.Str)
	case protocol.ResponseInteger:
		fmt.Printf("(integer) %d\n", resp.Integer)
	case protocol.ResponseBulkString:
		if resp.Data == nil {
			fmt.Println("(nil)")
		} else {
			fmt.Println(string(resp.Data))
		}
	case protocol.ResponseArray:
		if resp.Items == nil {
			fmt.Println("(nil)")
			return
		}
		for i, item := range resp.Items {
			fmt.Printf("%d) %s\n", i+1, formatValue(item))
		}
	case protocol.ResponseNull:
		fmt.Println("(nil)")
	}
}

func formatValue(resp *protocol.Response) string {
	switch resp.Type {
	case protocol.ResponseSimpleString:
		return resp.Str
	case protocol.ResponseError:
		return "(error) " + resp.Str
	case protocol.ResponseInteger:
		return fmt.Sprintf("(integer) %d", resp.Integer)
	case protocol.ResponseBulkString:
		if resp.Data == nil {
			return "(nil)"
		}
		return string(resp.Data)
	case protocol.ResponseNull:
		return "(nil)"
	default:
		return ""
	}
}

func handlePing(c *Client, args []string) {
	cmdArgs := []string{"PING"}
	cmdArgs = append(cmdArgs, args...)
	resp, err := c.Send(cmdArgs...)
	if err != nil {
		fatal(err)
	}
	printResponse(resp)
}

func handleSet(c *Client, args []string) {
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "(error) wrong number of arguments for 'set' command")
		os.Exit(1)
	}
	resp, err := c.Send(append([]string{"SET"}, args...)...)
	if err != nil {
		fatal(err)
	}
	printResponse(resp)
}

func handleGet(c *Client, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "(error) wrong number of arguments for 'get' command")
		os.Exit(1)
	}
	resp, err := c.Send("GET", args[0])
	if err != nil {
		fatal(err)
	}
	printResponse(resp)
}

func handleDel(c *Client, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "(error) wrong number of arguments for 'del' command")
		os.Exit(1)
	}
	resp, err := c.Send(append([]string{"DEL"}, args...)...)
	if err != nil {
		fatal(err)
	}
	printResponse(resp)
}

func handleExists(c *Client, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "(error) wrong number of arguments for 'exists' command")
		os.Exit(1)
	}
	resp, err := c.Send(append([]string{"EXISTS"}, args...)...)
	if err != nil {
		fatal(err)
	}
	printResponse(resp)
}

func handleTTL(c *Client, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "(error) wrong number of arguments for 'ttl' command")
		os.Exit(1)
	}
	resp, err := c.Send("TTL", args[0])
	if err != nil {
		fatal(err)
	}
	printResponse(resp)
}

func handleInfo(c *Client, args []string) {
	cmdArgs := []string{"INFO"}
	cmdArgs = append(cmdArgs, args...)
	resp, err := c.Send(cmdArgs...)
	if err != nil {
		fatal(err)
	}
	printResponse(resp)
}

func handleCluster(c *Client, args []string) {
	if len(args) < 1 {
		fmt.Println("Cluster subcommands: info, nodes, slots")
		return
	}
	cmdArgs := append([]string{"CLUSTER"}, args...)
	resp, err := c.Send(cmdArgs...)
	if err != nil {
		fatal(err)
	}
	printResponse(resp)
}

func handleDBSize(c *Client) {
	resp, err := c.Send("DBSIZE")
	if err != nil {
		fatal(err)
	}
	printResponse(resp)
}

func handleFlushAll(c *Client) {
	resp, err := c.Send("FLUSHALL")
	if err != nil {
		fatal(err)
	}
	printResponse(resp)
}

func handleKeys(c *Client, args []string) {
	pattern := "*"
	if len(args) > 0 {
		pattern = args[0]
	}
	resp, err := c.Send("KEYS", pattern)
	if err != nil {
		fatal(err)
	}
	printResponse(resp)
}

func handleExpire(c *Client, args []string) {
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "(error) wrong number of arguments for 'expire' command")
		os.Exit(1)
	}
	resp, err := c.Send("EXPIRE", args[0], args[1])
	if err != nil {
		fatal(err)
	}
	printResponse(resp)
}

func handlePersist(c *Client, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "(error) wrong number of arguments for 'persist' command")
		os.Exit(1)
	}
	resp, err := c.Send("PERSIST", args[0])
	if err != nil {
		fatal(err)
	}
	printResponse(resp)
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "(error) %v\n", err)
	os.Exit(1)
}

func executeCommand(c *Client, cmd string, args []string) {
	switch strings.ToLower(cmd) {
	case "ping":
		handlePing(c, args)
	case "set":
		handleSet(c, args)
	case "get":
		handleGet(c, args)
	case "del", "delete":
		handleDel(c, args)
	case "exists":
		handleExists(c, args)
	case "ttl":
		handleTTL(c, args)
	case "info":
		handleInfo(c, args)
	case "cluster":
		handleCluster(c, args)
	case "dbsize":
		handleDBSize(c)
	case "flushall":
		handleFlushAll(c)
	case "keys":
		handleKeys(c, args)
	case "expire":
		handleExpire(c, args)
	case "persist":
		handlePersist(c, args)
	default:
		fmt.Fprintf(os.Stderr, "(error) unknown command '%s'\n", cmd)
		os.Exit(1)
	}
}
