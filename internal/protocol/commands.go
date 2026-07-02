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
	"PING":     {Name: "PING", MinArgs: 0, MaxArgs: 1, ReadOnly: true},
	"SET":      {Name: "SET", MinArgs: 2, MaxArgs: 5, ReadOnly: false},
	"GET":      {Name: "GET", MinArgs: 1, MaxArgs: 1, ReadOnly: true},
	"DEL":      {Name: "DEL", MinArgs: 1, MaxArgs: -1, ReadOnly: false},
	"EXISTS":   {Name: "EXISTS", MinArgs: 1, MaxArgs: -1, ReadOnly: true},
	"TTL":      {Name: "TTL", MinArgs: 1, MaxArgs: 1, ReadOnly: true},
	"PTTL":     {Name: "PTTL", MinArgs: 1, MaxArgs: 1, ReadOnly: true},
	"EXPIRE":   {Name: "EXPIRE", MinArgs: 2, MaxArgs: 2, ReadOnly: false},
	"PERSIST":  {Name: "PERSIST", MinArgs: 1, MaxArgs: 1, ReadOnly: false},
	"KEYS":     {Name: "KEYS", MinArgs: 1, MaxArgs: 1, ReadOnly: true},
	"DBSIZE":   {Name: "DBSIZE", MinArgs: 0, MaxArgs: 0, ReadOnly: true},
	"FLUSHALL": {Name: "FLUSHALL", MinArgs: 0, MaxArgs: 0, ReadOnly: false},
	"INFO":     {Name: "INFO", MinArgs: 0, MaxArgs: 1, ReadOnly: true},
	"CLUSTER":  {Name: "CLUSTER", MinArgs: 1, MaxArgs: -1, ReadOnly: true},
}

func ValidateCommand(cmd *Command) error {
	def, ok := Commands[cmd.Name]
	if !ok {
		return fmt.Errorf("unknown command '%s'", cmd.Name)
	}
	argsLen := len(cmd.Args)
	if argsLen < def.MinArgs {
		return fmt.Errorf("wrong number of arguments for '%s' command", cmd.Name)
	}
	if def.MaxArgs > 0 && argsLen > def.MaxArgs {
		return fmt.Errorf("wrong number of arguments for '%s' command", cmd.Name)
	}
	return nil
}

func ParseSetFlags(args []string) (value string, ttl int64, flags []string, err error) {
	if len(args) < 2 {
		return "", 0, nil, fmt.Errorf("SET requires at least key and value")
	}
	value = args[1]
	i := 2
	for i < len(args) {
		switch strings.ToUpper(args[i]) {
		case "EX":
			if i+1 >= len(args) {
				return "", 0, nil, fmt.Errorf("missing value for EX")
			}
			sec, parseErr := fmt.Sscanf(args[i+1], "%d", &ttl)
			if sec != 1 || parseErr != nil {
				return "", 0, nil, fmt.Errorf("invalid EX value")
			}
			ttl *= int64(1e9)
			flags = append(flags, "EX")
			i += 2
		case "PX":
			if i+1 >= len(args) {
				return "", 0, nil, fmt.Errorf("missing value for PX")
			}
			sec, parseErr := fmt.Sscanf(args[i+1], "%d", &ttl)
			if sec != 1 || parseErr != nil {
				return "", 0, nil, fmt.Errorf("invalid PX value")
			}
			ttl *= int64(1e6)
			flags = append(flags, "PX")
			i += 2
		case "NX":
			flags = append(flags, "NX")
			i++
		case "XX":
			flags = append(flags, "XX")
			i++
		default:
			return "", 0, nil, fmt.Errorf("unexpected SET flag: %s", args[i])
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
