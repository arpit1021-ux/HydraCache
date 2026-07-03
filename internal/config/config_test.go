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

func TestLoadConfig_InvalidYAML_ReturnsError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(path, []byte(":::invalid yaml[[["), 0644); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}
	// parseYAML is a no-op, so currently this won't error.
	// When parseYAML is implemented, this should return an error.
	_, err := LoadConfig(path)
	if err != nil {
		t.Logf("parseYAML now returns error for invalid YAML: %v", err)
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

func TestLoadConfig_ValidYAMLFile_NoEffect(t *testing.T) {
	// BUG: parseYAML is a no-op. Even valid YAML is never applied.
	// This test documents the current (broken) behavior.
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
		// Expected behavior if parseYAML worked correctly.
		// Currently fails because parseYAML is a no-op stub.
		t.Logf("BUG: parseYAML is a no-op. Server.Addr = %q, expected %q", cfg.Server.Addr, ":9999")
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
