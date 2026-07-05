package persistence

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
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
}

type SyncMode int

const (
	SyncEveryWrite SyncMode = iota
	SyncBatch
	SyncAsync
)

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
	}

	w.recover()

	return w, nil
}

func (w *WAL) recover() {
	info, err := w.file.Stat()
	if err != nil || info.Size() == 0 {
		return
	}

	_, _ = w.file.Seek(0, io.SeekStart)
	var maxSeq int64

	for {
		entry, err := w.readEntry()
		if err != nil {
			break
		}
		if entry.Seq > maxSeq {
			maxSeq = entry.Seq
		}
	}
	w.seq = maxSeq
	_, _ = w.file.Seek(0, io.SeekEnd)
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

	switch w.syncMode {
	case SyncEveryWrite:
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("WAL sync failed: %w", err)
		}
	case SyncBatch:
		if atomic.LoadInt64(&w.writeCount)%100 == 0 {
			if err := w.file.Sync(); err != nil {
				return fmt.Errorf("WAL sync failed: %w", err)
			}
		}
	case SyncAsync:
		// Explicit no-op: rely on OS page cache flush. Writes are
		// durable only after the kernel flushes dirty pages (typically
		// within 30s). Acceptable for throughput-oriented workloads
		// where up to 30s of writes may be lost on power failure.
	}

	return nil
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

// SyncModeFromString converts a config string to a SyncMode constant.
func SyncModeFromString(s string) SyncMode {
	switch s {
	case "every_write", "everywrite", "sync":
		return SyncEveryWrite
	case "async", "none":
		return SyncAsync
	default:
		return SyncBatch
	}
}

func (w *WAL) Close() error {
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
