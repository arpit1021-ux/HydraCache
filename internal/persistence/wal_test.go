package persistence

import (
	"encoding/binary"
	"os"
	"path/filepath"
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

// corruptTail truncates the WAL file to length+extra bytes of garbage
// appended, or just truncates it shorter, simulating a crash mid-write:
// the last record on disk is torn (a partial header, a partial payload,
// or a length field pointing past EOF), not merely absent.
func corruptTail(t *testing.T, dir string, truncateTo int64, garbage []byte) {
	t.Helper()
	path := filepath.Join(dir, "wal.log")
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("open wal.log for corruption: %v", err)
	}
	defer f.Close()
	if err := f.Truncate(truncateTo); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if len(garbage) > 0 {
		if _, err := f.WriteAt(garbage, truncateTo); err != nil {
			t.Fatalf("write garbage: %v", err)
		}
	}
}

// TestWALRecovery_TornTailIsTruncatedNotPermanentlyStuck is the direct
// regression test for the audit finding: recover() used to break out of
// the replay loop on a bad record but then seek to the file's ACTUAL end
// (past the garbage) rather than truncating it off. Because the file is
// opened O_APPEND, every subsequent Append landed after that garbage, and
// every future restart's replay hit the same corrupt bytes at the same
// offset and stopped there again — silently and permanently discarding
// every record ever written after the crash point, with no error ever
// surfaced. This proves a torn tail is truncated once, and normal
// operation (including surviving a SECOND restart) resumes cleanly.
func TestWALRecovery_TornTailIsTruncatedNotPermanentlyStuck(t *testing.T) {
	dir := t.TempDir()

	wal1, err := NewWAL(dir, 1024*1024, SyncEveryWrite)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	_ = wal1.Append(WALEntry{Cmd: "SET", Key: "good1", Value: []byte("v1")})
	_ = wal1.Append(WALEntry{Cmd: "SET", Key: "good2", Value: []byte("v2")})
	goodSize := wal1.Size()
	// This record's bytes will be torn off after the file handle closes.
	_ = wal1.Append(WALEntry{Cmd: "SET", Key: "torn", Value: []byte("this-wont-survive")})
	fullSize := wal1.Size()
	if err = wal1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if fullSize <= goodSize {
		t.Fatal("test setup invariant broken: third append didn't grow the file")
	}

	// Simulate a crash mid-write: chop off the last record partway
	// through, leaving a torn header/payload, not a clean boundary.
	tornAt := goodSize + (fullSize-goodSize)/2
	corruptTail(t, dir, tornAt, nil)

	// First restart: recovery must truncate the torn bytes and come up
	// with exactly the 2 good records, not error out, not hang, and not
	// silently keep the garbage in place.
	wal2, err := NewWAL(dir, 1024*1024, SyncEveryWrite)
	if err != nil {
		t.Fatalf("NewWAL after torn tail: %v", err)
	}
	entries, err := wal2.Replay()
	if err != nil {
		t.Fatalf("Replay after torn tail: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 surviving good entries after torn-tail recovery, got %d", len(entries))
	}

	// Write a new record after recovery — this is exactly what the old
	// bug prevented from ever being readable again.
	if err = wal2.Append(WALEntry{Cmd: "SET", Key: "after-recovery", Value: []byte("v3")}); err != nil {
		t.Fatalf("Append after recovery: %v", err)
	}
	if err = wal2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Second restart: the record written after the first recovery must
	// actually be there. Under the old bug, replay would hit leftover
	// garbage at the same offset on THIS restart too and silently drop
	// it, or (if truncation only happened once) it would already be
	// fine — the real assertion is that it is fine now, not "eventually."
	wal3, err := NewWAL(dir, 1024*1024, SyncEveryWrite)
	if err != nil {
		t.Fatalf("NewWAL on second restart: %v", err)
	}
	defer wal3.Close()
	entries, err = wal3.Replay()
	if err != nil {
		t.Fatalf("Replay on second restart: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries (2 original + 1 post-recovery) on second restart, got %d", len(entries))
	}
	if entries[2].Key != "after-recovery" {
		t.Errorf("expected third entry to be 'after-recovery', got %q", entries[2].Key)
	}
}

// TestWALRecovery_TornAtRecordHeaderBoundary covers the other torn-write
// shape: the crash happens before even the CRC/length header of the next
// record is fully written (as opposed to a torn payload after a complete
// header) — a shorter, more common crash window in practice.
func TestWALRecovery_TornAtRecordHeaderBoundary(t *testing.T) {
	dir := t.TempDir()

	wal1, err := NewWAL(dir, 1024*1024, SyncEveryWrite)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	_ = wal1.Append(WALEntry{Cmd: "SET", Key: "good", Value: []byte("v1")})
	goodSize := wal1.Size()
	if err = wal1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Append 3 garbage bytes — less than the 8-byte crc+length header —
	// directly onto the file, simulating a crash after only a partial
	// header hit disk.
	corruptTail(t, dir, goodSize, []byte{0xDE, 0xAD, 0xBE})

	wal2, err := NewWAL(dir, 1024*1024, SyncEveryWrite)
	if err != nil {
		t.Fatalf("NewWAL after torn header: %v", err)
	}
	entries, err := wal2.Replay()
	if err != nil {
		t.Fatalf("Replay after torn header: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 surviving entry, got %d", len(entries))
	}

	if err = wal2.Append(WALEntry{Cmd: "SET", Key: "new", Value: []byte("v2")}); err != nil {
		t.Fatalf("Append after recovery: %v", err)
	}
	if err = wal2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	wal3, err := NewWAL(dir, 1024*1024, SyncEveryWrite)
	if err != nil {
		t.Fatalf("NewWAL on second restart: %v", err)
	}
	defer wal3.Close()
	entries, err = wal3.Replay()
	if err != nil {
		t.Fatalf("Replay on second restart: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries on second restart, got %d", len(entries))
	}
}

// TestWALRecovery_LengthFieldPointsPastEOF covers a corrupted length
// field (not just missing bytes): a record whose header claims more
// payload than actually exists in the file. io.ReadFull returns
// io.ErrUnexpectedEOF in that case, which recover() must treat as a torn
// tail (truncate) rather than propagating an error that fails startup.
func TestWALRecovery_LengthFieldPointsPastEOF(t *testing.T) {
	dir := t.TempDir()

	wal1, err := NewWAL(dir, 1024*1024, SyncEveryWrite)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	_ = wal1.Append(WALEntry{Cmd: "SET", Key: "good", Value: []byte("v1")})
	goodSize := wal1.Size()
	if err = wal1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// A record header claiming a huge payload length, with no payload
	// bytes actually present: [4-byte CRC][4-byte length=9999][nothing].
	header := make([]byte, 8)
	binary.BigEndian.PutUint32(header[0:4], 0x12345678)
	binary.BigEndian.PutUint32(header[4:8], 9999)
	corruptTail(t, dir, goodSize, header)

	wal2, err := NewWAL(dir, 1024*1024, SyncEveryWrite)
	if err != nil {
		t.Fatalf("NewWAL after bad length field: %v", err)
	}
	defer wal2.Close()
	entries, err := wal2.Replay()
	if err != nil {
		t.Fatalf("Replay after bad length field: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 surviving entry, got %d", len(entries))
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
		input   string
		want    SyncMode
		wantErr bool
	}{
		{input: "always", want: SyncEveryWrite},
		{input: "every_write", want: SyncEveryWrite},
		{input: "everywrite", want: SyncEveryWrite},
		{input: "sync", want: SyncEveryWrite},
		{input: "everysec", want: SyncEverySec},
		{input: "batch", want: SyncEverySec}, // back-compat alias
		{input: "never", want: SyncNever},
		{input: "no", want: SyncNever},
		{input: "async", want: SyncNever},
		{input: "none", want: SyncNever},
		{input: "garbage", wantErr: true},
		{input: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := SyncModeFromString(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("SyncModeFromString(%q) expected an error, got mode %v", tt.input, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("SyncModeFromString(%q) unexpected error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("SyncModeFromString(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

func TestWALNeverSyncMode(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewWAL(dir, 1024*1024, SyncNever)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	// Should not panic or error — SyncNever never calls fsync explicitly.
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

func TestWALEverySecSyncMode_BackgroundSyncRunsAndStopsCleanly(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewWAL(dir, 1024*1024, SyncEverySec)
	if err != nil {
		t.Fatal(err)
	}

	_ = wal.Append(WALEntry{Cmd: "SET", Key: "k", Value: []byte("v")})

	// Close must stop the periodic-sync goroutine and return promptly,
	// not hang waiting on a ticker that never fires again.
	done := make(chan struct{})
	go func() {
		_ = wal.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("WAL.Close() did not return — periodic sync goroutine likely leaked")
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
