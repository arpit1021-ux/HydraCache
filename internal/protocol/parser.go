package protocol

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Default safety limits, chosen to match Redis's own defaults closely
// enough not to surprise an operator already familiar with them:
// proto-max-bulk-len defaults to 512MB there; the multibulk (array)
// element count Redis hardcodes at 1024*1024 and doesn't expose as
// configurable, so this doesn't either.
const (
	DefaultMaxBulkLen  = 512 * 1024 * 1024
	DefaultMaxArrayLen = 1024 * 1024
)

type Command struct {
	Name string
	Args []string
}

type Parser struct {
	reader      *bufio.Reader
	maxBulkLen  int
	maxArrayLen int
}

func NewParser(r io.Reader) *Parser {
	return NewParserWithLimits(r, DefaultMaxBulkLen, DefaultMaxArrayLen)
}

// NewParserWithLimits creates a Parser with explicit bounds on a single
// bulk string's length and a command array's element count. Both are
// load-bearing for safety, not just resource hygiene: without them, a
// single client claiming a negative length (e.g. "$-5\r\n") makes the
// subsequent make([]byte, strLen+2) panic — a Go slice-length panic that
// nothing upstream of this parser recovers from, crashing the entire
// process on one malformed command from any connection, authenticated or
// not, since parsing happens before the AUTH gate. Claiming a huge
// positive length (array count or bulk length) instead exhausts memory
// with a single command's worth of allocation.
func NewParserWithLimits(r io.Reader, maxBulkLen, maxArrayLen int) *Parser {
	return &Parser{reader: bufio.NewReader(r), maxBulkLen: maxBulkLen, maxArrayLen: maxArrayLen}
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
	if count > p.maxArrayLen {
		return nil, fmt.Errorf("array count %d exceeds maximum of %d", count, p.maxArrayLen)
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
		if strLen < 0 {
			return nil, fmt.Errorf("negative bulk string length: %d", strLen)
		}
		if strLen > p.maxBulkLen {
			return nil, fmt.Errorf("bulk string length %d exceeds maximum of %d", strLen, p.maxBulkLen)
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
