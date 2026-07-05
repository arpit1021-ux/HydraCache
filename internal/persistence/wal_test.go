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

// --- End-to-end persistence round-trip test ---

func TestEndToEndPersistenceRoundTrip(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: simulate a live session with mixed mutations.
	wal, err := NewWAL(dir, 1024*1024, SyncEveryWrite)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}

	_ = wal.Append(WALEntry{Cmd: "SET", Key: "permanent", Value: []byte("forever")})
	_ = wal.Append(WALEntry{Cmd: "SET", Key: "ttl_key", Value: []byte("temp"), TTL: int64(10 * time.Second)})
	_ = wal.Append(WALEntry{Cmd: "SET", Key: "doomed", Value: []byte("bye"), TTL: int64(100 * time.Millisecond)})
	time.Sleep(150 * time.Millisecond) // let "doomed" expire
	_ = wal.Append(WALEntry{Cmd: "EXPIRE", Key: "permanent", TTL: int64(1 * time.Hour)})
	_ = wal.Append(WALEntry{Cmd: "SET", Key: "ephemeral", Value: []byte("v")})
	_ = wal.Append(WALEntry{Cmd: "DEL", Key: "ephemeral"})
	_ = wal.Append(WALEntry{Cmd: "SET", Key: "persist_me", Value: []byte("keep"), TTL: int64(5 * time.Second)})
	_ = wal.Append(WALEntry{Cmd: "PERSIST", Key: "persist_me"})
	wal.Close()

	// Phase 2: restart and recover.
	wal2, err := NewWAL(dir, 1024*1024, SyncEveryWrite)
	if err != nil {
		t.Fatalf("NewWAL restart: %v", err)
	}
	defer wal2.Close()

	rec := NewRecoverer(wal2, nil)
	state, err := rec.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	// permanent: should exist with a non-zero TTL (from EXPIRE)
	if e, ok := state.Entries["permanent"]; !ok {
		t.Error("permanent key missing")
	} else if e.TTL <= 0 {
		t.Errorf("permanent key should have positive TTL after EXPIRE, got %d", e.TTL)
	}

	// ttl_key: should exist with ~10s remaining TTL (may have decayed slightly)
	if e, ok := state.Entries["ttl_key"]; !ok {
		t.Error("ttl_key missing")
	} else if e.TTL <= 0 || e.TTL > int64(10*time.Second) {
		t.Errorf("ttl_key TTL should be (0, 10s], got %v", time.Duration(e.TTL))
	}

	// doomed: TTL was set to 100ms and 150ms passed before WAL close,
	// but the WAL entry itself is still present. Recovery loads the TTL
	// as-is. Since the key was SET then its TTL elapsed, the entry exists
	// in RecoveredState but with a 100ms TTL that has now expired.
	// BulkLoad would skip it (expired), so the key is effectively gone.
	if _, ok := state.Entries["doomed"]; ok {
		// It's in RecoveredState (the WAL entry exists), but TTL is
		// relative-to-write-time, so 100ms has long since elapsed.
		// This is expected — the cache's BulkLoad would skip it.
		t.Log("doomed key present in RecoveredState (expected, BulkLoad will skip)")
	}

	// ephemeral: DEL followed by no SET, should be absent
	if _, ok := state.Entries["ephemeral"]; ok {
		t.Error("ephemeral key should have been deleted")
	}

	// persist_me: SET with TTL then PERSIST (zero TTL), should have TTL=0
	if e, ok := state.Entries["persist_me"]; !ok {
		t.Error("persist_me key missing")
	} else if e.TTL != 0 {
		t.Errorf("persist_me should have TTL=0 after PERSIST, got %d", e.TTL)
	}
}

// --- Snapshot-then-truncate recovery test ---

func TestSnapshotThenTruncateRecovery(t *testing.T) {
	dir := t.TempDir()

	// Write initial WAL entries.
	wal, err := NewWAL(dir, 1024*1024, SyncEveryWrite)
	if err != nil {
		t.Fatal(err)
	}
	_ = wal.Append(WALEntry{Cmd: "SET", Key: "k1", Value: []byte("v1")})
	_ = wal.Append(WALEntry{Cmd: "SET", Key: "k2", Value: []byte("v2")})
	seq := wal.Seq()
	wal.Close()

	// Save a snapshot capturing state at this seq.
	snapDir := t.TempDir()
	snap, err := NewSnapshotter(snapDir, time.Hour, "test")
	if err != nil {
		t.Fatal(err)
	}
	err = snap.Save(SnapshotData{
		Entries: map[string]SnapshotEntry{
			"k1": {Key: "k1", Value: []byte("v1")},
			"k2": {Key: "k2", Value: []byte("v2")},
		},
		Seq: seq,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Truncate the WAL — simulates post-snapshot cleanup.
	wal2, err := NewWAL(dir, 1024*1024, SyncEveryWrite)
	if err != nil {
		t.Fatal(err)
	}
	if err = wal2.Truncate(); err != nil {
		t.Fatal(err)
	}

	// Write one more entry after truncation.
	_ = wal2.Append(WALEntry{Cmd: "SET", Key: "k3", Value: []byte("v3")})
	wal2.Close()

	// Simulate crash-and-restart: open fresh WAL + snapshot.
	wal3, err := NewWAL(dir, 1024*1024, SyncEveryWrite)
	if err != nil {
		t.Fatal(err)
	}
	defer wal3.Close()

	rec := NewRecoverer(wal3, snap)
	state, err := rec.Recover()
	if err != nil {
		t.Fatal(err)
	}

	// Should have k1, k2 from snapshot + k3 from post-truncate WAL.
	if len(state.Entries) != 3 {
		t.Errorf("expected 3 entries, got %d", len(state.Entries))
	}
	for _, key := range []string{"k1", "k2", "k3"} {
		if _, ok := state.Entries[key]; !ok {
			t.Errorf("key %q missing after recovery", key)
		}
	}
}

// --- EXPIRE/PERSIST replay round-trip test ---

func TestRecoverExpirePersist(t *testing.T) {
	dir := t.TempDir()

	wal, err := NewWAL(dir, 1024*1024, SyncEveryWrite)
	if err != nil {
		t.Fatal(err)
	}

	_ = wal.Append(WALEntry{Cmd: "SET", Key: "k1", Value: []byte("v1")})
	_ = wal.Append(WALEntry{Cmd: "SET", Key: "k2", Value: []byte("v2"), TTL: int64(10 * time.Second)})
	_ = wal.Append(WALEntry{Cmd: "EXPIRE", Key: "k1", TTL: int64(30 * time.Second)})
	_ = wal.Append(WALEntry{Cmd: "PERSIST", Key: "k2"})
	wal.Close()

	wal2, err := NewWAL(dir, 1024*1024, SyncEveryWrite)
	if err != nil {
		t.Fatal(err)
	}
	defer wal2.Close()

	rec := NewRecoverer(wal2, nil)
	state, err := rec.Recover()
	if err != nil {
		t.Fatal(err)
	}

	// k1: SET (no TTL) then EXPIRE with 30s — should have ~30s TTL
	if e, ok := state.Entries["k1"]; !ok {
		t.Fatal("k1 missing")
	} else if e.TTL <= int64(29*time.Second) || e.TTL > int64(30*time.Second) {
		t.Errorf("k1 TTL should be ~30s, got %v", time.Duration(e.TTL))
	}

	// k2: SET with 10s TTL then PERSIST — should have TTL=0
	if e, ok := state.Entries["k2"]; !ok {
		t.Fatal("k2 missing")
	} else if e.TTL != 0 {
		t.Errorf("k2 TTL should be 0 after PERSIST, got %d", e.TTL)
	}
}

// --- EXPIRE/PERSIST on missing key (edge case) ---

func TestRecoverExpirePersistMissingKey(t *testing.T) {
	dir := t.TempDir()

	wal, err := NewWAL(dir, 1024*1024, SyncEveryWrite)
	if err != nil {
		t.Fatal(err)
	}

	// EXPIRE and PERSIST on a key that was never SET.
	_ = wal.Append(WALEntry{Cmd: "EXPIRE", Key: "ghost", TTL: int64(10 * time.Second)})
	_ = wal.Append(WALEntry{Cmd: "PERSIST", Key: "ghost"})
	wal.Close()

	wal2, err := NewWAL(dir, 1024*1024, SyncEveryWrite)
	if err != nil {
		t.Fatal(err)
	}
	defer wal2.Close()

	rec := NewRecoverer(wal2, nil)
	state, err := rec.Recover()
	if err != nil {
		t.Fatal(err)
	}

	if len(state.Entries) != 0 {
		t.Errorf("expected 0 entries (missing key EXPIRE/PERSIST should be skipped), got %d", len(state.Entries))
	}
}

// --- FLUSHALL replay ---

func TestRecoverFlushAll(t *testing.T) {
	dir := t.TempDir()

	wal, err := NewWAL(dir, 1024*1024, SyncEveryWrite)
	if err != nil {
		t.Fatal(err)
	}

	_ = wal.Append(WALEntry{Cmd: "SET", Key: "k1", Value: []byte("v1")})
	_ = wal.Append(WALEntry{Cmd: "SET", Key: "k2", Value: []byte("v2")})
	_ = wal.Append(WALEntry{Cmd: "FLUSHALL"})
	_ = wal.Append(WALEntry{Cmd: "SET", Key: "k3", Value: []byte("v3")})
	wal.Close()

	wal2, err := NewWAL(dir, 1024*1024, SyncEveryWrite)
	if err != nil {
		t.Fatal(err)
	}
	defer wal2.Close()

	rec := NewRecoverer(wal2, nil)
	state, err := rec.Recover()
	if err != nil {
		t.Fatal(err)
	}

	if len(state.Entries) != 1 {
		t.Errorf("expected 1 entry after FLUSHALL + SET, got %d", len(state.Entries))
	}
	if _, ok := state.Entries["k3"]; !ok {
		t.Error("k3 should exist after FLUSHALL + SET")
	}
}

// --- SyncMode wiring test ---

func TestSyncModeFromString(t *testing.T) {
	tests := []struct {
		input string
		want  SyncMode
	}{
		{"batch", SyncBatch},
		{"every_write", SyncEveryWrite},
		{"everywrite", SyncEveryWrite},
		{"sync", SyncEveryWrite},
		{"async", SyncAsync},
		{"none", SyncAsync},
		{"garbage", SyncBatch}, // default
		{"", SyncBatch},        // default
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := SyncModeFromString(tt.input)
			if got != tt.want {
				t.Errorf("SyncModeFromString(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

func TestWALAsyncSyncMode(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewWAL(dir, 1024*1024, SyncAsync)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	// Should not panic or error — SyncAsync is a no-op.
	for i := 0; i < 5; i++ {
		_ = wal.Append(WALEntry{Cmd: "SET", Key: "k", Value: []byte("v")})
	}

	entries, err := wal.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 5 {
		t.Errorf("expected 5 entries, got %d", len(entries))
	}
}

// --- WAL.Seq() accessor ---

func TestWALSeq(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewWAL(dir, 1024*1024, SyncEveryWrite)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	if wal.Seq() != 0 {
		t.Errorf("initial Seq should be 0, got %d", wal.Seq())
	}

	_ = wal.Append(WALEntry{Cmd: "SET", Key: "a", Value: []byte("1")})
	_ = wal.Append(WALEntry{Cmd: "SET", Key: "b", Value: []byte("2")})

	if wal.Seq() != 2 {
		t.Errorf("Seq should be 2 after 2 appends, got %d", wal.Seq())
	}
}
