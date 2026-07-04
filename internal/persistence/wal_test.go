package persistence

import (
	"testing"
	"time"
)

func TestWALAppendReplay(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewWAL(dir, 1024*1024, SyncEveryWrite)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer wal.Close()

	_ = wal.Append(WALEntry{Cmd: "SET", Key: "key1", Value: []byte("val1")})
	_ = wal.Append(WALEntry{Cmd: "SET", Key: "key2", Value: []byte("val2")})
	_ = wal.Append(WALEntry{Cmd: "DEL", Key: "key1"})

	entries, err := wal.Replay()
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}
	if len(entries) != 3 {
		t.Errorf("expected 3 entries, got %d", len(entries))
	}
}

func TestWALRecovery(t *testing.T) {
	dir := t.TempDir()

	wal1, _ := NewWAL(dir, 1024*1024, SyncEveryWrite)
	_ = wal1.Append(WALEntry{Cmd: "SET", Key: "key1", Value: []byte("val1")})
	wal1.Close()

	wal2, err := NewWAL(dir, 1024*1024, SyncEveryWrite)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer wal2.Close()

	entries, _ := wal2.Replay()
	if len(entries) != 1 {
		t.Errorf("expected 1 recovered entry, got %d", len(entries))
	}
}

func TestSnapshotSaveLoad(t *testing.T) {
	dir := t.TempDir()
	snap, err := NewSnapshotter(dir, time.Hour, "test-node")
	if err != nil {
		t.Fatalf("NewSnapshotter failed: %v", err)
	}

	data := SnapshotData{
		Entries: map[string]SnapshotEntry{
			"key1": {Key: "key1", Value: []byte("val1")},
		},
		Seq: 42,
	}

	if err = snap.Save(data); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	loaded, loadErr := snap.Load()
	if loadErr != nil {
		t.Fatalf("Load failed: %v", loadErr)
	}
	if loaded == nil {
		t.Fatal("expected non-nil snapshot")
	}
	if loaded.Seq != 42 {
		t.Errorf("expected seq 42, got %d", loaded.Seq)
	}
	if len(loaded.Entries) != 1 {
		t.Errorf("expected 1 entry, got %d", len(loaded.Entries))
	}
}

func TestRecovererRecover(t *testing.T) {
	dir := t.TempDir()
	wal, _ := NewWAL(dir, 1024*1024, SyncEveryWrite)
	_ = wal.Append(WALEntry{Cmd: "SET", Key: "k1", Value: []byte("v1")})
	_ = wal.Append(WALEntry{Cmd: "SET", Key: "k2", Value: []byte("v2")})
	_ = wal.Append(WALEntry{Cmd: "DEL", Key: "k1"})
	wal.Close()

	wal2, _ := NewWAL(dir, 1024*1024, SyncEveryWrite)
	defer wal2.Close()

	rec := NewRecoverer(wal2, nil)
	state, err := rec.Recover()
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	if len(state.Entries) != 1 {
		t.Errorf("expected 1 entry after recovery, got %d", len(state.Entries))
	}
	if _, ok := state.Entries["k2"]; !ok {
		t.Error("expected k2 to exist")
	}
}

// --- Fix 2: snapshot→recovery preserves TTL semantics ---

func TestSnapshotRecoveryPreservesTTL(t *testing.T) {
	dir := t.TempDir()
	snap, err := NewSnapshotter(dir, time.Hour, "test-node")
	if err != nil {
		t.Fatalf("NewSnapshotter failed: %v", err)
	}

	// Key with long TTL (expires 1 hour from now).
	longExpiry := time.Now().Add(1 * time.Hour).UnixNano()
	// Key that has already expired.
	alreadyExpired := time.Now().Add(-5 * time.Minute).UnixNano()

	data := SnapshotData{
		Entries: map[string]SnapshotEntry{
			"alive": {Key: "alive", Value: []byte("v1"), ExpiresAt: longExpiry, CreatedAt: time.Now().UnixNano()},
			"stale": {Key: "stale", Value: []byte("v2"), ExpiresAt: alreadyExpired, CreatedAt: time.Now().UnixNano()},
			"perm":  {Key: "perm", Value: []byte("v3"), ExpiresAt: 0, CreatedAt: time.Now().UnixNano()},
		},
		Seq: 1,
	}

	if err = snap.Save(data); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	walDir := t.TempDir()
	wal, err := NewWAL(walDir, 1024*1024, SyncEveryWrite)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer wal.Close()

	rec := NewRecoverer(wal, snap)
	state, err := rec.Recover()
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	// "alive" key: remaining TTL should be roughly 1 hour (within a few seconds of tolerance).
	if entry, ok := state.Entries["alive"]; !ok {
		t.Fatal("expected 'alive' key in recovered state")
	} else {
		remaining := time.Duration(entry.TTL)
		if remaining < 55*time.Minute || remaining > time.Hour+5*time.Second {
			t.Errorf("alive key TTL should be ~1h, got %v", remaining)
		}
	}

	// "stale" key: TTL should be clamped to 0 (already expired at snapshot time).
	if entry, ok := state.Entries["stale"]; !ok {
		t.Fatal("expected 'stale' key in recovered state")
	} else if entry.TTL != 0 {
		t.Errorf("stale key TTL should be 0 (clamped), got %d", entry.TTL)
	}

	// "perm" key: ExpiresAt=0 means no expiry, TTL should remain 0.
	if entry, ok := state.Entries["perm"]; !ok {
		t.Fatal("expected 'perm' key in recovered state")
	} else if entry.TTL != 0 {
		t.Errorf("perm key TTL should be 0, got %d", entry.TTL)
	}
}
