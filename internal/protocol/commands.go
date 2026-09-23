package protocol

import (
	"fmt"
	"strings"
)

type CommandDef struct {
	Name     string
	MinArgs  int
	MaxArgs  int
	ReadOnly bool
}

var Commands = map[string]CommandDef{
	"PING":               {Name: "PING", MinArgs: 0, MaxArgs: 1, ReadOnly: true},
	"AUTH":               {Name: "AUTH", MinArgs: 1, MaxArgs: 2, ReadOnly: true},
	"SET":                {Name: "SET", MinArgs: 2, MaxArgs: 5, ReadOnly: false},
	"SETNX":              {Name: "SETNX", MinArgs: 2, MaxArgs: 2, ReadOnly: false},
	"GET":                {Name: "GET", MinArgs: 1, MaxArgs: 1, ReadOnly: true},
	"DEL":                {Name: "DEL", MinArgs: 1, MaxArgs: -1, ReadOnly: false},
	"EXISTS":             {Name: "EXISTS", MinArgs: 1, MaxArgs: -1, ReadOnly: true},
	"TTL":                {Name: "TTL", MinArgs: 1, MaxArgs: 1, ReadOnly: true},
	"PTTL":               {Name: "PTTL", MinArgs: 1, MaxArgs: 1, ReadOnly: true},
	"EXPIRE":             {Name: "EXPIRE", MinArgs: 2, MaxArgs: 2, ReadOnly: false},
	"PERSIST":            {Name: "PERSIST", MinArgs: 1, MaxArgs: 1, ReadOnly: false},
	"KEYS":               {Name: "KEYS", MinArgs: 1, MaxArgs: 1, ReadOnly: true},
	"DBSIZE":             {Name: "DBSIZE", MinArgs: 0, MaxArgs: 0, ReadOnly: true},
	"FLUSHALL":           {Name: "FLUSHALL", MinArgs: 0, MaxArgs: 0, ReadOnly: false},
	"INFO":               {Name: "INFO", MinArgs: 0, MaxArgs: 1, ReadOnly: true},
	"CLUSTER":            {Name: "CLUSTER", MinArgs: 1, MaxArgs: -1, ReadOnly: true},
	"HELLO":              {Name: "HELLO", MinArgs: 0, MaxArgs: -1, ReadOnly: true},
	"CLIENT":             {Name: "CLIENT", MinArgs: 1, MaxArgs: -1, ReadOnly: true},
	"GOSSIP":             {Name: "GOSSIP", MinArgs: 1, MaxArgs: 1, ReadOnly: false},
	"REPLICATE":          {Name: "REPLICATE", MinArgs: 1, MaxArgs: 1, ReadOnly: false},
	"REPLICA_SYNC":       {Name: "REPLICA_SYNC", MinArgs: 1, MaxArgs: 1, ReadOnly: true},
	"ELECTION_VOTE":      {Name: "ELECTION_VOTE", MinArgs: 1, MaxArgs: 1, ReadOnly: false},
	"ELECTION_HEARTBEAT": {Name: "ELECTION_HEARTBEAT", MinArgs: 1, MaxArgs: 1, ReadOnly: false},
}

func ValidateCommand(cmd *Command) error {
	def, ok := Commands[cmd.Name]
	if !ok {
		return fmt.Errorf("ERR unknown command '%s'", cmd.Name)
	}
	argsLen := len(cmd.Args)
	if argsLen < def.MinArgs {
		return fmt.Errorf("ERR wrong number of arguments for '%s' command", strings.ToLower(cmd.Name))
	}
	if def.MaxArgs > 0 && argsLen > def.MaxArgs {
		return fmt.Errorf("ERR wrong number of arguments for '%s' command", strings.ToLower(cmd.Name))
	}
	return nil
}

func ParseSetFlags(args []string) (value string, ttl int64, flags []string, err error) {
	if len(args) < 2 {
		return "", 0, nil, fmt.Errorf("ERR wrong number of arguments for 'set' command")
	}
	value = args[1]
	i := 2
	for i < len(args) {
		switch strings.ToUpper(args[i]) {
		case "EX":
			if i+1 >= len(args) {
				return "", 0, nil, fmt.Errorf("ERR syntax error")
			}
			var sec int64
			_, parseErr := fmt.Sscanf(args[i+1], "%d", &sec)
			if parseErr != nil {
				return "", 0, nil, fmt.Errorf("ERR value is not an integer or out of range")
			}
			if sec <= 0 {
				return "", 0, nil, fmt.Errorf("ERR invalid expire time in 'set' command")
			}
			ttl = sec * int64(1e9)
			flags = append(flags, "EX")
			i += 2
		case "PX":
			if i+1 >= len(args) {
				return "", 0, nil, fmt.Errorf("ERR syntax error")
			}
			var ms int64
			_, parseErr := fmt.Sscanf(args[i+1], "%d", &ms)
			if parseErr != nil {
				return "", 0, nil, fmt.Errorf("ERR value is not an integer or out of range")
			}
			if ms <= 0 {
				return "", 0, nil, fmt.Errorf("ERR invalid expire time in 'set' command")
			}
			ttl = ms * int64(1e6)
			flags = append(flags, "PX")
			i += 2
		case "NX":
			flags = append(flags, "NX")
			i++
		case "XX":
			flags = append(flags, "XX")
			i++
		default:
			return "", 0, nil, fmt.Errorf("ERR syntax error")
		}
	}
	return value, ttl, flags, nil
}

func FormatDuration(nanos int64) string {
	if nanos < 0 {
		return "-1"
	}
	return fmt.Sprintf("%d", nanos/1e9)
}
