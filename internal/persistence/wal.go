package persistence

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

type WALEntry struct {
	Seq       int64
	Cmd       string
	Args      []string
	Key       string
	Value     []byte
	TTL       int64
	Timestamp int64
}

type WAL struct {
	mu         sync.RWMutex
	file       *os.File
	dir        string
	seq        int64
	size       int64
	maxSize    int64
	syncMode   SyncMode
	writeCount int64

	stopCh   chan struct{}
	wg       sync.WaitGroup
	stopOnce sync.Once
}

// SyncMode is the WAL's fsync policy, matching the three modes real
// durability tools (Redis's appendfsync, PostgreSQL's synchronous_commit)
// expose, because "configurable in some novel way" is worse than
// matching an operator's existing mental model:
//
//   - SyncEveryWrite ("always"): fsync after every single Append. Zero
//     data loss on crash or power failure. Highest per-write latency —
//     bounded by the storage device's fsync cost, typically ~1-10ms on
//     an SSD, far more on spinning disk or some network-attached volumes.
//   - SyncEverySec ("everysec"): a background goroutine fsyncs at most
//     once per second, decoupled from write volume. Bounded loss on crash:
//     up to ~1 second of the most recently acknowledged writes. This is
//     the recommended default — the same tradeoff Redis's own default
//     (everysec) makes.
//   - SyncNever ("never"): no explicit fsync; durability depends entirely
//     on the OS flushing dirty pages on its own schedule (commonly within
//     30s, but not guaranteed by this WAL). Highest throughput, largest
//     and least predictable loss window. Only appropriate for pure-cache
//     workloads where the WAL is a performance nicety, not a guarantee.
type SyncMode int

const (
	SyncEveryWrite SyncMode = iota
	SyncEverySec
	SyncNever
)

func (m SyncMode) String() string {
	switch m {
	case SyncEveryWrite:
		return "always"
	case SyncEverySec:
		return "everysec"
	case SyncNever:
		return "never"
	default:
		return "unknown"
	}
}

func NewWAL(dir string, maxSize int64, syncMode SyncMode) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	walPath := filepath.Join(dir, "wal.log")
	f, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	w := &WAL{
		file:     f,
		dir:      dir,
		maxSize:  maxSize,
		syncMode: syncMode,
		stopCh:   make(chan struct{}),
	}

	if err := w.recover(); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("WAL recovery failed: %w", err)
	}

	if syncMode == SyncEverySec {
		w.wg.Add(1)
		go w.periodicSync(time.Second)
	}

	return w, nil
}

// recover replays the WAL to find the highest sequence number and,
// critically, truncates any torn tail record left by a crash mid-write.
// Without this, a corrupt/short record at the end of the file would be
// silently re-encountered at the same offset on every future restart —
// readEntry's error would end the replay loop there every time, forever
// discarding every record written after the crash point, with no error
// ever surfaced. Truncating the torn bytes now means a future Append
// picks up cleanly right after the last verified-good record.
func (w *WAL) recover() error {
	info, err := w.file.Stat()
	if err != nil {
		return fmt.Errorf("stat WAL file: %w", err)
	}
	if info.Size() == 0 {
		return nil
	}

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek to start: %w", err)
	}

	var maxSeq int64
	var validEnd int64
	for {
		pos, err := w.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("seek current: %w", err)
		}

		entry, err := w.readEntry()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Printf("[wal] torn or corrupt record at offset %d, truncating tail: %v", pos, err)
			}
			validEnd = pos
			break
		}
		if entry.Seq > maxSeq {
			maxSeq = entry.Seq
		}
	}

	if validEnd < info.Size() {
		// Truncate via the path rather than w.file.Truncate(): on
		// Windows, a handle opened with O_APPEND can be denied the
		// access rights SetEndOfFile needs even though it was also
		// opened O_RDWR. Closing, truncating, and reopening sidesteps
		// that platform quirk and matches the pattern Truncate() (the
		// full-wipe method below) already uses.
		name := w.file.Name()
		if err := w.file.Close(); err != nil {
			return fmt.Errorf("close WAL file before truncating torn tail: %w", err)
		}
		if err := os.Truncate(name, validEnd); err != nil {
			return fmt.Errorf("truncate torn WAL tail at offset %d: %w", validEnd, err)
		}
		f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
		if err != nil {
			return fmt.Errorf("reopen WAL file after truncating torn tail: %w", err)
		}
		w.file = f
	}

	w.seq = maxSeq
	w.size = validEnd
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek to end: %w", err)
	}
	return nil
}

func (w *WAL) Append(entry WALEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry.Seq = atomic.AddInt64(&w.seq, 1)
	entry.Timestamp = time.Now().UnixNano()

	data := encodeWALEntry(entry)
	crc := crc32.ChecksumIEEE(data)

	_, err := w.file.Write(encodeRecord(crc, data))
	if err != nil {
		return fmt.Errorf("failed to write WAL entry: %w", err)
	}

	w.size += int64(len(data) + 8)
	atomic.AddInt64(&w.writeCount, 1)

	if w.syncMode == SyncEveryWrite {
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("WAL sync failed: %w", err)
		}
	}
	// SyncEverySec: handled by the background periodicSync goroutine,
	// decoupled from write volume. SyncNever: no explicit fsync at all.

	return nil
}

// periodicSync fsyncs at most once per interval for SyncEverySec. It
// always exits: either interval fires and it loops, or stopCh closes and
// it returns — Close() joins it via wg before returning.
func (w *WAL) periodicSync(interval time.Duration) {
	defer w.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
			w.mu.Lock()
			if err := w.file.Sync(); err != nil {
				log.Printf("[wal] periodic sync failed: %v", err)
			}
			w.mu.Unlock()
		}
	}
}

func (w *WAL) readEntry() (*WALEntry, error) {
	var crc uint32
	if err := binary.Read(w.file, binary.BigEndian, &crc); err != nil {
		return nil, err
	}

	var length uint32
	if err := binary.Read(w.file, binary.BigEndian, &length); err != nil {
		return nil, err
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(w.file, data); err != nil {
		return nil, err
	}

	actualCRC := crc32.ChecksumIEEE(data)
	if actualCRC != crc {
		return nil, fmt.Errorf("CRC mismatch: expected %x, got %x", crc, actualCRC)
	}

	return decodeWALEntry(data), nil
}

func (w *WAL) Replay() ([]WALEntry, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	var entries []WALEntry
	for {
		entry, err := w.readEntry()
		if err != nil {
			break
		}
		entries = append(entries, *entry)
	}

	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}

	return entries, nil
}

func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	name := w.file.Name()
	w.file.Close()

	if err := os.Remove(name); err != nil && !os.IsNotExist(err) {
		return err
	}

	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	w.file = f
	w.size = 0

	return nil
}

func (w *WAL) Sync() error {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.file.Sync()
}

func (w *WAL) Size() int64 {
	return atomic.LoadInt64(&w.size)
}

func (w *WAL) Seq() int64 {
	return atomic.LoadInt64(&w.seq)
}

// SyncModeFromString converts a config string to a SyncMode, matching
// Redis's appendfsync naming (always/everysec/no) as the canonical
// spelling, with a few tolerated aliases. Unlike the previous version,
// an unrecognized string is a hard error rather than a silent fallback to
// SyncEverySec — a typo in sync_mode should fail startup loudly, not
// quietly downgrade the durability guarantee an operator asked for.
func SyncModeFromString(s string) (SyncMode, error) {
	switch s {
	case "always", "every_write", "everywrite", "sync":
		return SyncEveryWrite, nil
	case "everysec", "batch":
		return SyncEverySec, nil
	case "never", "no", "async", "none":
		return SyncNever, nil
	default:
		return SyncEveryWrite, fmt.Errorf("unknown wal sync_mode %q: must be one of always, everysec, never", s)
	}
}

func (w *WAL) Close() error {
	w.stopOnce.Do(func() { close(w.stopCh) })
	w.wg.Wait()

	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.file.Sync(); err != nil {
		return err
	}
	return w.file.Close()
}

func encodeRecord(crc uint32, data []byte) []byte {
	buf := make([]byte, 4+4+len(data))
	binary.BigEndian.PutUint32(buf[0:4], crc)
	binary.BigEndian.PutUint32(buf[4:8], uint32(len(data)))
	copy(buf[8:], data)
	return buf
}

func encodeWALEntry(entry WALEntry) []byte {
	keyBytes := []byte(entry.Key)
	valueBytes := entry.Value

	buf := make([]byte, 0, 8+8+len(keyBytes)+4+len(valueBytes)+8+8)
	buf = binary.BigEndian.AppendUint64(buf, uint64(entry.Seq))
	buf = append(buf, byte(len(entry.Cmd)))
	buf = append(buf, entry.Cmd...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(keyBytes)))
	buf = append(buf, keyBytes...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(valueBytes)))
	buf = append(buf, valueBytes...)
	buf = binary.BigEndian.AppendUint64(buf, uint64(entry.TTL))
	buf = binary.BigEndian.AppendUint64(buf, uint64(entry.Timestamp))

	return buf
}

func decodeWALEntry(data []byte) *WALEntry {
	entry := &WALEntry{}
	offset := 0

	entry.Seq = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	cmdLen := int(data[offset])
	offset++
	entry.Cmd = string(data[offset : offset+cmdLen])
	offset += cmdLen

	keyLen := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	entry.Key = string(data[offset : offset+keyLen])
	offset += keyLen

	valLen := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	entry.Value = make([]byte, valLen)
	copy(entry.Value, data[offset:offset+valLen])
	offset += valLen

	entry.TTL = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	entry.Timestamp = int64(binary.BigEndian.Uint64(data[offset:]))

	return entry
}
