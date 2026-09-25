package chaostest

import (
	"fmt"
	"time"

	"github.com/hydracache/hydracache/internal/network"
)

// RESPClient wraps the internal RESP client with typed helpers for chaos testing.
type RESPClient struct {
	addr string
	conn *network.Client
}

func NewRESPClient(addr string) *RESPClient {
	return &RESPClient{addr: addr}
}

func (c *RESPClient) Connect() error {
	c.conn = network.NewClientWithTimeout(c.addr, 5*time.Second)
	return c.conn.Connect()
}

func (c *RESPClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *RESPClient) Set(key, value string) error {
	if err := c.ensureConnected(); err != nil {
		return err
	}
	resp, err := c.conn.Send("SET", key, value)
	if err != nil {
		return fmt.Errorf("SET %s failed: %w", key, err)
	}
	if resp == "" || resp == "OK" || resp[0] != '-' {
		return nil
	}
	return fmt.Errorf("SET %s: %s", key, resp)
}

func (c *RESPClient) Get(key string) (string, bool, error) {
	if err := c.ensureConnected(); err != nil {
		return "", false, err
	}
	resp, err := c.conn.Send("GET", key)
	if err != nil {
		return "", false, fmt.Errorf("GET %s failed: %w", key, err)
	}
	if resp == "" {
		return "", false, nil
	}
	return resp, true, nil
}

func (c *RESPClient) DBSize() (int64, error) {
	if err := c.ensureConnected(); err != nil {
		return 0, err
	}
	resp, err := c.conn.Send("DBSIZE")
	if err != nil {
		return 0, fmt.Errorf("DBSIZE failed: %w", err)
	}
	var count int64
	if _, err = fmt.Sscanf(resp, "%d", &count); err != nil {
		return 0, fmt.Errorf("DBSIZE: parse reply %q: %w", resp, err)
	}
	return count, nil
}

func (c *RESPClient) Ping() error {
	if err := c.ensureConnected(); err != nil {
		return err
	}
	resp, err := c.conn.Send("PING")
	if err != nil {
		return fmt.Errorf("PING failed: %w", err)
	}
	if resp != "PONG" {
		return fmt.Errorf("PING returned %q, expected PONG", resp)
	}
	return nil
}

func (c *RESPClient) ensureConnected() error {
	if c.conn != nil && c.conn.IsConnected() {
		return nil
	}
	return c.Connect()
}
