package main

import (
	"bufio"
	"fmt"
	"net"
	"time"

	"github.com/hydracache/hydracache/internal/protocol"
)

type Client struct {
	conn    net.Conn
	encoder *protocol.Encoder
	decoder *protocol.Decoder
	reader  *bufio.Reader
}

func NewClient(addr string) (*Client, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("cannot connect to %s: %w", addr, err)
	}

	reader := bufio.NewReaderSize(conn, 64*1024)
	writer := bufio.NewWriterSize(conn, 64*1024)

	return &Client{
		conn:    conn,
		encoder: protocol.NewEncoder(writer),
		decoder: protocol.NewDecoder(reader),
		reader:  reader,
	}, nil
}

func (c *Client) Send(args ...string) (*protocol.Response, error) {
	if err := c.encoder.WriteArray(args); err != nil {
		return nil, fmt.Errorf("failed to send command: %w", err)
	}
	if err := c.encoder.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush: %w", err)
	}

	resp, err := c.decoder.Decode()
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}
	return resp, nil
}

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *Client) RemoteAddr() string {
	if c.conn != nil {
		return c.conn.RemoteAddr().String()
	}
	return ""
}
