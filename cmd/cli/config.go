package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	Host string
	Port int
}

func ParseConfig() *Config {
	cfg := &Config{}

	flag.StringVar(&cfg.Host, "host", "localhost", "Server hostname")
	flag.IntVar(&cfg.Port, "port", 7379, "Server port")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "HydraCache CLI v1.0.0\n\n")
		fmt.Fprintf(os.Stderr, "Usage: hc [options] <command> [arguments]\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		fmt.Fprintf(os.Stderr, "  --host <hostname>  Server host (default: localhost)\n")
		fmt.Fprintf(os.Stderr, "  --port <port>      Server port (default: 7379)\n")
		fmt.Fprintf(os.Stderr, "  --help             Show this help\n\n")
		fmt.Fprintf(os.Stderr, "Commands:\n")
		fmt.Fprintf(os.Stderr, "  ping [message]              Ping the server\n")
		fmt.Fprintf(os.Stderr, "  set <key> <value> [EX s]    Set a key-value pair\n")
		fmt.Fprintf(os.Stderr, "  get <key>                   Get value for a key\n")
		fmt.Fprintf(os.Stderr, "  del <key> [key ...]         Delete one or more keys\n")
		fmt.Fprintf(os.Stderr, "  exists <key> [key ...]      Check if keys exist\n")
		fmt.Fprintf(os.Stderr, "  ttl <key>                   Get TTL of a key\n")
		fmt.Fprintf(os.Stderr, "  info [section]              Get server information\n")
		fmt.Fprintf(os.Stderr, "  cluster <subcommand>        Cluster management\n")
		fmt.Fprintf(os.Stderr, "  dbsize                      Get number of keys\n")
		fmt.Fprintf(os.Stderr, "  flushall                    Delete all keys\n")
		fmt.Fprintf(os.Stderr, "  version                     Show CLI version\n")
		fmt.Fprintf(os.Stderr, "  help                        Show this help\n\n")
		fmt.Fprintf(os.Stderr, "Examples:\n")
		fmt.Fprintf(os.Stderr, "  hc ping\n")
		fmt.Fprintf(os.Stderr, "  hc --host 10.0.0.1 --port 7380 ping\n")
		fmt.Fprintf(os.Stderr, "  hc set user:123 \"John Doe\"\n")
		fmt.Fprintf(os.Stderr, "  hc set session:abc \"data\" EX 3600\n")
		fmt.Fprintf(os.Stderr, "  hc get user:123\n")
		fmt.Fprintf(os.Stderr, "  hc del user:123\n")
		fmt.Fprintf(os.Stderr, "  hc exists user:123\n")
		fmt.Fprintf(os.Stderr, "  hc ttl session:abc\n")
	}

	flag.Parse()
	return cfg
}

func (c *Config) Address() string {
	return c.Host + ":" + strconv.Itoa(c.Port)
}
