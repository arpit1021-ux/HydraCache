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

	wal.Append(WALEntry{Cmd: "SET", Key: "key1", Value: []byte("val1")})
	wal.Append(WALEntry{Cmd: "SET", Key: "key2", Value: []byte("val2")})
	wal.Append(WALEntry{Cmd: "DEL", Key: "key1"})

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
	wal1.Append(WALEntry{Cmd: "SET", Key: "key1", Value: []byte("val1")})
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

	if err := snap.Save(data); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	loaded, err := snap.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
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
	wal.Append(WALEntry{Cmd: "SET", Key: "k1", Value: []byte("v1")})
	wal.Append(WALEntry{Cmd: "SET", Key: "k2", Value: []byte("v2")})
	wal.Append(WALEntry{Cmd: "DEL", Key: "k1"})
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
