package network

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

type Client struct {
	addr    string
	conn    net.Conn
	reader  *bufio.Reader
	writer  *bufio.Writer
	mu      sync.Mutex
	connected bool
}

func NewClient(addr string) *Client {
	return &Client{addr: addr}
}

func (c *Client) Connect() error {
	conn, err := net.DialTimeout("tcp", c.addr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", c.addr, err)
	}
	c.conn = conn
	c.reader = bufio.NewReader(conn)
	c.writer = bufio.NewWriter(conn)
	c.connected = true
	return nil
}

func (c *Client) Send(args ...interface{}) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.connected {
		return "", fmt.Errorf("not connected")
	}

	cmd := fmt.Sprintf("*%d\r\n", len(args))
	for _, arg := range args {
		s := fmt.Sprintf("%v", arg)
		cmd += fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
	}

	if _, err := c.writer.WriteString(cmd); err != nil {
		return "", err
	}
	if err := c.writer.Flush(); err != nil {
		return "", err
	}

	line, err := c.readLine()
	if err != nil {
		return "", err
	}

	if len(line) == 0 {
		return "", fmt.Errorf("empty response")
	}

	switch line[0] {
	case '+':
		return line[1:], nil
	case '-':
		return "", fmt.Errorf("error: %s", line[1:])
	case ':':
		return line[1:], nil
	case '$':
		var strLen int
		fmt.Sscanf(line[1:], "%d", &strLen)
		data := make([]byte, strLen+2)
		_, err := c.reader.Read(data)
		if err != nil {
			return "", err
		}
		return string(data[:strLen]), nil
	default:
		return line, nil
	}
}

func (c *Client) readLine() (string, error) {
	var line []byte
	for {
		part, isPrefix, err := c.reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				c.connected = false
			}
			return "", err
		}
		line = append(line, part...)
		if !isPrefix {
			break
		}
	}
	return string(line), nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connected = false
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *Client) IsConnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connected
}
