package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server  ServerConfig  `yaml:"server"`
	Cache   CacheConfig   `yaml:"cache"`
	Cluster ClusterConfig `yaml:"cluster"`
	WAL     WALConfig     `yaml:"wal"`
	Log     LogConfig     `yaml:"log"`
	HTTP    HTTPConfig    `yaml:"http"`
}

type ServerConfig struct {
	Addr         string        `yaml:"addr"`
	MaxConns     int           `yaml:"max_conns"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
}

type CacheConfig struct {
	EvictionPolicy       string        `yaml:"eviction_policy"`
	EvictionCapacity     int           `yaml:"eviction_capacity"`
	ActiveExpiration     bool          `yaml:"active_expiration"`
	ExpirationInterval   time.Duration `yaml:"expiration_interval"`
	ExpirationSampleSize int           `yaml:"expiration_sample_size"`
	ReplicationFactor    int           `yaml:"replication_factor"`

	// ReplicationMode is "async" (default: return to the client as soon
	// as the local write is durable) or "sync" (block for ReplicationAckCount
	// replica acks, bounded by ReplicationSyncTimeout).
	ReplicationMode        string        `yaml:"replication_mode"`
	ReplicationAckCount    int           `yaml:"replication_ack_count"`
	ReplicationSyncTimeout time.Duration `yaml:"replication_sync_timeout"`

	// MaxPromotionLag and PromotionGateTimeout bound failover promotion:
	// a dead primary's replica is only promoted once its lag is at or
	// below MaxPromotionLag, waiting up to PromotionGateTimeout before
	// promoting the best available candidate anyway.
	MaxPromotionLag      int64         `yaml:"max_promotion_lag"`
	PromotionGateTimeout time.Duration `yaml:"promotion_gate_timeout"`
}

type ClusterConfig struct {
	NodeID            string        `yaml:"node_id"`
	AdvertiseAddr     string        `yaml:"advertise_addr"`
	SeedNodes         []string      `yaml:"seed_nodes"`
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
	ElectionTimeout   time.Duration `yaml:"election_timeout"`
	// ElectionLeaseTimeout is how long an elected leader keeps acting as
	// leader after its last quorum-acknowledged heartbeat round before
	// stepping down — the fencing mechanism that stops a partitioned-away
	// leader from believing it holds leadership indefinitely.
	ElectionLeaseTimeout time.Duration `yaml:"election_lease_timeout"`
	// ElectionRPCTimeout bounds every individual vote/heartbeat RPC.
	ElectionRPCTimeout time.Duration `yaml:"election_rpc_timeout"`
	PhiThreshold       float64       `yaml:"phi_threshold"`
	SuspectTimeout     time.Duration `yaml:"suspect_timeout"`
	VirtualNodes       int           `yaml:"virtual_nodes"`
}

type WALConfig struct {
	Enabled          bool          `yaml:"enabled"`
	Dir              string        `yaml:"dir"`
	MaxSize          int64         `yaml:"max_size"`
	SyncMode         string        `yaml:"sync_mode"`
	EnabledSnapshot  bool          `yaml:"enabled_snapshot"`
	SnapshotInterval time.Duration `yaml:"snapshot_interval"`
}

type LogConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
	Output string `yaml:"output"`
}

type HTTPConfig struct {
	Enabled bool   `yaml:"enabled"`
	Addr    string `yaml:"addr"`
}

func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Addr:         ":7379",
			MaxConns:     10000,
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,
		},
		Cache: CacheConfig{
			EvictionPolicy:         "lru",
			EvictionCapacity:       100000,
			ActiveExpiration:       true,
			ExpirationInterval:     time.Second,
			ExpirationSampleSize:   10,
			ReplicationFactor:      2,
			ReplicationMode:        "async",
			ReplicationAckCount:    1,
			ReplicationSyncTimeout: 2 * time.Second,
			MaxPromotionLag:        100,
			PromotionGateTimeout:   5 * time.Second,
		},
		Cluster: ClusterConfig{
			HeartbeatInterval:    100 * time.Millisecond,
			ElectionTimeout:      300 * time.Millisecond,
			ElectionLeaseTimeout: 1 * time.Second,
			ElectionRPCTimeout:   200 * time.Millisecond,
			PhiThreshold:         8.0,
			SuspectTimeout:       5 * time.Second,
			VirtualNodes:         150,
		},
		WAL: WALConfig{
			Enabled:          true,
			Dir:              "./data/wal",
			MaxSize:          100 * 1024 * 1024,
			SyncMode:         "everysec",
			EnabledSnapshot:  true,
			SnapshotInterval: 60 * time.Second,
		},
		Log: LogConfig{
			Level:  "info",
			Format: "text",
			Output: "stdout",
		},
		HTTP: HTTPConfig{
			Enabled: true,
			Addr:    ":8379",
		},
	}
}

func LoadConfig(path string) (*Config, error) {
	cfg := DefaultConfig()

	if path == "" {
		return cfg, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	if err := parseYAML(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	return cfg, nil
}

func parseYAML(data []byte, cfg *Config) error {
	if len(data) == 0 {
		return nil
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(false)
	if err := dec.Decode(cfg); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return fmt.Errorf("yaml decode: %w", err)
	}
	return nil
}
