package protocol

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type Command struct {
	Name string
	Args []string
}

type Parser struct {
	reader *bufio.Reader
}

func NewParser(r io.Reader) *Parser {
	return &Parser{reader: bufio.NewReader(r)}
}

func (p *Parser) ReadCommand() (*Command, error) {
	line, err := p.readLine()
	if err != nil {
		return nil, err
	}

	if len(line) == 0 {
		return nil, fmt.Errorf("empty line")
	}

	switch line[0] {
	case '*':
		return p.parseArray(line)
	case '$':
		return nil, fmt.Errorf("unexpected bulk string")
	default:
		return p.parseInline(line)
	}
}

func (p *Parser) parseInline(line string) (*Command, error) {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty command")
	}
	return &Command{
		Name: strings.ToUpper(parts[0]),
		Args: parts[1:],
	}, nil
}

func (p *Parser) parseArray(line string) (*Command, error) {
	count, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, fmt.Errorf("invalid array count: %w", err)
	}

	if count < 0 {
		return nil, fmt.Errorf("null array")
	}

	args := make([]string, 0, count)
	for i := 0; i < count; i++ {
		bulkLine, err := p.readLine()
		if err != nil {
			return nil, err
		}
		if len(bulkLine) == 0 || bulkLine[0] != '$' {
			return nil, fmt.Errorf("expected bulk string, got: %s", bulkLine)
		}

		strLen, err := strconv.Atoi(bulkLine[1:])
		if err != nil {
			return nil, fmt.Errorf("invalid bulk string length: %w", err)
		}

		data := make([]byte, strLen+2)
		_, err = io.ReadFull(p.reader, data)
		if err != nil {
			return nil, err
		}

		args = append(args, string(data[:strLen]))
	}

	if len(args) == 0 {
		return nil, fmt.Errorf("empty command")
	}

	return &Command{
		Name: strings.ToUpper(args[0]),
		Args: args[1:],
	}, nil
}

func (p *Parser) readLine() (string, error) {
	var line []byte
	for {
		part, isPrefix, err := p.reader.ReadLine()
		if err != nil {
			return "", err
		}
		line = append(line, part...)
		if !isPrefix {
			break
		}
	}
	return string(line), nil
}
