package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultConfig_ReturnsPopulatedValues(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Server.Addr != ":7379" {
		t.Errorf("Server.Addr = %q, want %q", cfg.Server.Addr, ":7379")
	}
	if cfg.Server.MaxConns != 10000 {
		t.Errorf("Server.MaxConns = %d, want 10000", cfg.Server.MaxConns)
	}
	if cfg.Server.ReadTimeout != 30*time.Second {
		t.Errorf("Server.ReadTimeout = %v, want 30s", cfg.Server.ReadTimeout)
	}
	if cfg.Server.WriteTimeout != 30*time.Second {
		t.Errorf("Server.WriteTimeout = %v, want 30s", cfg.Server.WriteTimeout)
	}
	if cfg.Cache.EvictionPolicy != "lru" {
		t.Errorf("Cache.EvictionPolicy = %q, want %q", cfg.Cache.EvictionPolicy, "lru")
	}
	if cfg.Cache.EvictionCapacity != 100000 {
		t.Errorf("Cache.EvictionCapacity = %d, want 100000", cfg.Cache.EvictionCapacity)
	}
	if !cfg.Cache.ActiveExpiration {
		t.Error("Cache.ActiveExpiration should be true")
	}
	if cfg.Cache.ExpirationInterval != time.Second {
		t.Errorf("Cache.ExpirationInterval = %v, want 1s", cfg.Cache.ExpirationInterval)
	}
	if cfg.Cache.ExpirationSampleSize != 10 {
		t.Errorf("Cache.ExpirationSampleSize = %d, want 10", cfg.Cache.ExpirationSampleSize)
	}
	if cfg.Cache.ReplicationFactor != 2 {
		t.Errorf("Cache.ReplicationFactor = %d, want 2", cfg.Cache.ReplicationFactor)
	}
	if cfg.Cluster.HeartbeatInterval != 100*time.Millisecond {
		t.Errorf("Cluster.HeartbeatInterval = %v, want 100ms", cfg.Cluster.HeartbeatInterval)
	}
	if cfg.Cluster.ElectionTimeout != 300*time.Millisecond {
		t.Errorf("Cluster.ElectionTimeout = %v, want 300ms", cfg.Cluster.ElectionTimeout)
	}
	if cfg.Cluster.PhiThreshold != 8.0 {
		t.Errorf("Cluster.PhiThreshold = %f, want 8.0", cfg.Cluster.PhiThreshold)
	}
	if cfg.Cluster.SuspectTimeout != 5*time.Second {
		t.Errorf("Cluster.SuspectTimeout = %v, want 5s", cfg.Cluster.SuspectTimeout)
	}
	if cfg.Cluster.VirtualNodes != 150 {
		t.Errorf("Cluster.VirtualNodes = %d, want 150", cfg.Cluster.VirtualNodes)
	}
	if !cfg.WAL.Enabled {
		t.Error("WAL.Enabled should be true")
	}
	if cfg.WAL.Dir != "./data/wal" {
		t.Errorf("WAL.Dir = %q, want %q", cfg.WAL.Dir, "./data/wal")
	}
	if cfg.WAL.MaxSize != 100*1024*1024 {
		t.Errorf("WAL.MaxSize = %d, want %d", cfg.WAL.MaxSize, 100*1024*1024)
	}
	if cfg.WAL.SyncMode != "batch" {
		t.Errorf("WAL.SyncMode = %q, want %q", cfg.WAL.SyncMode, "batch")
	}
	if !cfg.WAL.EnabledSnapshot {
		t.Error("WAL.EnabledSnapshot should be true")
	}
	if cfg.WAL.SnapshotInterval != 60*time.Second {
		t.Errorf("WAL.SnapshotInterval = %v, want 60s", cfg.WAL.SnapshotInterval)
	}
	if cfg.Log.Level != "info" {
		t.Errorf("Log.Level = %q, want %q", cfg.Log.Level, "info")
	}
	if cfg.Log.Format != "text" {
		t.Errorf("Log.Format = %q, want %q", cfg.Log.Format, "text")
	}
	if cfg.Log.Output != "stdout" {
		t.Errorf("Log.Output = %q, want %q", cfg.Log.Output, "stdout")
	}
	if !cfg.HTTP.Enabled {
		t.Error("HTTP.Enabled should be true")
	}
	if cfg.HTTP.Addr != ":8379" {
		t.Errorf("HTTP.Addr = %q, want %q", cfg.HTTP.Addr, ":8379")
	}
}

func TestDefaultConfig_ReturnsNewPointer(t *testing.T) {
	cfg1 := DefaultConfig()
	cfg2 := DefaultConfig()
	if cfg1 == cfg2 {
		t.Error("DefaultConfig should return distinct pointers")
	}
	cfg1.Server.Addr = "changed"
	if cfg2.Server.Addr == "changed" {
		t.Error("mutating one DefaultConfig result should not affect another")
	}
}

func TestLoadConfig_EmptyPath_ReturnsDefaults(t *testing.T) {
	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Server.Addr != ":7379" {
		t.Errorf("expected default addr, got %q", cfg.Server.Addr)
	}
}

func TestLoadConfig_NonExistentFile_ReturnsDefaults(t *testing.T) {
	cfg, err := LoadConfig("/nonexistent/path/config.yaml")
	if err != nil {
		t.Fatalf("non-existent file should return defaults, got error: %v", err)
	}
	if cfg.Cache.EvictionPolicy != "lru" {
		t.Errorf("expected default eviction policy, got %q", cfg.Cache.EvictionPolicy)
	}
}

func TestLoadConfig_EmptyFile_ReturnsDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.yaml")
	if err := os.WriteFile(path, []byte{}, 0644); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Server.Addr != ":7379" {
		t.Errorf("expected defaults for empty file, got addr %q", cfg.Server.Addr)
	}
}

func TestLoadConfig_ValidYAML_OverridesDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "valid.yaml")
	yaml := []byte(`
server:
  addr: ":9999"
  max_conns: 500
`)
	if err := os.WriteFile(path, yaml, 0644); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Server.Addr != ":9999" {
		t.Errorf("Server.Addr = %q, want :9999", cfg.Server.Addr)
	}
	if cfg.Server.MaxConns != 500 {
		t.Errorf("Server.MaxConns = %d, want 500", cfg.Server.MaxConns)
	}
}

func TestLoadConfig_MalformedYAML_ReturnsError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	yaml := []byte(`:::invalid yaml[[[` + "\n  bad: [")
	if err := os.WriteFile(path, yaml, 0644); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}
	_, err := LoadConfig(path)
	if err == nil {
		t.Error("malformed YAML should return error, not silently succeed")
	}
}

func TestLoadConfig_DirectoryPath_ReturnsError(t *testing.T) {
	dir := t.TempDir()
	_, err := LoadConfig(dir)
	if err == nil {
		t.Error("expected error when path is a directory")
	}
}

func TestConfig_StructFields_AllSubConfigsInitialized(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Server == (ServerConfig{}) {
		t.Error("Server config is zero value")
	}
	if cfg.Cache == (CacheConfig{}) {
		t.Error("Cache config is zero value")
	}
	if cfg.WAL == (WALConfig{}) {
		t.Error("WAL config is zero value")
	}
	if cfg.Log == (LogConfig{}) {
		t.Error("Log config is zero value")
	}
	if cfg.HTTP == (HTTPConfig{}) {
		t.Error("HTTP config is zero value")
	}
	if cfg.Cluster.PhiThreshold == 0 {
		t.Error("Cluster config is zero value")
	}
}

func TestDefaultConfig_ClusterDefaults_SeedNodesEmpty(t *testing.T) {
	cfg := DefaultConfig()
	if len(cfg.Cluster.SeedNodes) != 0 {
		t.Errorf("Cluster.SeedNodes should be empty by default, got %v", cfg.Cluster.SeedNodes)
	}
	if cfg.Cluster.NodeID != "" {
		t.Errorf("Cluster.NodeID should be empty by default, got %q", cfg.Cluster.NodeID)
	}
}

func TestLoadConfig_PartialYAML_KeepsUnsetDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "partial.yaml")
	yaml := []byte(`
server:
  addr: ":1234"
`)
	if err := os.WriteFile(path, yaml, 0644); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Server.Addr != ":1234" {
		t.Errorf("Server.Addr = %q, want :1234", cfg.Server.Addr)
	}
	if cfg.Server.MaxConns != 10000 {
		t.Errorf("Server.MaxConns = %d, want 10000 (default)", cfg.Server.MaxConns)
	}
	if cfg.Cache.EvictionPolicy != "lru" {
		t.Errorf("Cache.EvictionPolicy = %q, want lru (default)", cfg.Cache.EvictionPolicy)
	}
}

func TestLoadConfig_UnknownFields_Ignored(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "extra.yaml")
	yaml := []byte(`
server:
  addr: ":1234"
  unknown_field: true
cluster:
  node_id: "node-1"
  something_future: 42
`)
	if err := os.WriteFile(path, yaml, 0644); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("unknown fields should be ignored, got error: %v", err)
	}
	if cfg.Server.Addr != ":1234" {
		t.Errorf("Server.Addr = %q, want :1234", cfg.Server.Addr)
	}
	if cfg.Cluster.NodeID != "node-1" {
		t.Errorf("Cluster.NodeID = %q, want node-1", cfg.Cluster.NodeID)
	}
}

func TestLoadConfig_AllSections(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "full.yaml")
	yaml := []byte(`
server:
  addr: ":1111"
  max_conns: 5000
  read_timeout: 10s
  write_timeout: 15s
cache:
  eviction_policy: fifo
  eviction_capacity: 5000
  active_expiration: false
  expiration_interval: 2s
  expiration_sample_size: 20
  replication_factor: 3
cluster:
  node_id: "node-alpha"
  seed_nodes:
    - "10.0.0.1:7000"
    - "10.0.0.2:7000"
  heartbeat_interval: 200ms
  election_timeout: 500ms
  phi_threshold: 10.0
  suspect_timeout: 10s
  virtual_nodes: 200
wal:
  enabled: false
  dir: "/tmp/wal"
  max_size: 200000000
  sync_mode: always
  enabled_snapshot: false
  snapshot_interval: 30s
log:
  level: debug
  format: json
  output: /var/log/hc.log
http:
  enabled: false
  addr: ":9999"
`)
	if err := os.WriteFile(path, yaml, 0644); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Server.Addr != ":1111" {
		t.Errorf("Server.Addr = %q", cfg.Server.Addr)
	}
	if cfg.Server.MaxConns != 5000 {
		t.Errorf("Server.MaxConns = %d", cfg.Server.MaxConns)
	}
	if cfg.Server.ReadTimeout != 10*time.Second {
		t.Errorf("Server.ReadTimeout = %v", cfg.Server.ReadTimeout)
	}
	if cfg.Server.WriteTimeout != 15*time.Second {
		t.Errorf("Server.WriteTimeout = %v", cfg.Server.WriteTimeout)
	}
	if cfg.Cache.EvictionPolicy != "fifo" {
		t.Errorf("Cache.EvictionPolicy = %q", cfg.Cache.EvictionPolicy)
	}
	if cfg.Cache.EvictionCapacity != 5000 {
		t.Errorf("Cache.EvictionCapacity = %d", cfg.Cache.EvictionCapacity)
	}
	if cfg.Cache.ActiveExpiration {
		t.Error("Cache.ActiveExpiration should be false")
	}
	if cfg.Cache.ReplicationFactor != 3 {
		t.Errorf("Cache.ReplicationFactor = %d", cfg.Cache.ReplicationFactor)
	}
	if cfg.Cluster.NodeID != "node-alpha" {
		t.Errorf("Cluster.NodeID = %q", cfg.Cluster.NodeID)
	}
	if len(cfg.Cluster.SeedNodes) != 2 {
		t.Errorf("Cluster.SeedNodes len = %d, want 2", len(cfg.Cluster.SeedNodes))
	}
	if cfg.Cluster.PhiThreshold != 10.0 {
		t.Errorf("Cluster.PhiThreshold = %f", cfg.Cluster.PhiThreshold)
	}
	if cfg.WAL.Enabled {
		t.Error("WAL.Enabled should be false")
	}
	if cfg.WAL.Dir != "/tmp/wal" {
		t.Errorf("WAL.Dir = %q", cfg.WAL.Dir)
	}
	if cfg.WAL.SyncMode != "always" {
		t.Errorf("WAL.SyncMode = %q", cfg.WAL.SyncMode)
	}
	if cfg.Log.Level != "debug" {
		t.Errorf("Log.Level = %q", cfg.Log.Level)
	}
	if cfg.Log.Format != "json" {
		t.Errorf("Log.Format = %q", cfg.Log.Format)
	}
	if cfg.Log.Output != "/var/log/hc.log" {
		t.Errorf("Log.Output = %q", cfg.Log.Output)
	}
	if cfg.HTTP.Enabled {
		t.Error("HTTP.Enabled should be false")
	}
	if cfg.HTTP.Addr != ":9999" {
		t.Errorf("HTTP.Addr = %q", cfg.HTTP.Addr)
	}
}
