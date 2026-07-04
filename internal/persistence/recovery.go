package persistence

import (
	"log"
	"time"
)

type Recoverer struct {
	wal         *WAL
	snapshotter *Snapshotter
	recovered   bool
}

func NewRecoverer(wal *WAL, snapshotter *Snapshotter) *Recoverer {
	return &Recoverer{
		wal:         wal,
		snapshotter: snapshotter,
	}
}

type RecoveredState struct {
	Entries map[string]WALEntry
	Seq     int64
}

func (r *Recoverer) Recover() (*RecoveredState, error) {
	state := &RecoveredState{
		Entries: make(map[string]WALEntry),
	}

	if r.snapshotter != nil {
		snapshot, err := r.snapshotter.Load()
		if err != nil {
			return nil, err
		}
		if snapshot != nil {
			now := time.Now().UnixNano()
			for key, entry := range snapshot.Entries {
				// SnapshotEntry.ExpiresAt is an absolute UnixNano timestamp.
				// WALEntry.TTL must be a *remaining* relative duration (nanoseconds)
				// so the field name's contract is honestly satisfied.  Clamp to 0
				// when the key has already elapsed — the caller should skip it.
				var remaining int64
				if entry.ExpiresAt > 0 {
					remaining = entry.ExpiresAt - now
					if remaining < 0 {
						remaining = 0
					}
				}
				state.Entries[key] = WALEntry{
					Key:       entry.Key,
					Value:     entry.Value,
					TTL:       remaining,
					Timestamp: entry.CreatedAt,
				}
			}
			state.Seq = snapshot.Seq
			log.Printf("[recovery] restored %d entries from snapshot", len(snapshot.Entries))
		}
	}

	if r.wal != nil {
		entries, err := r.wal.Replay()
		if err != nil {
			return nil, err
		}

		applied := 0
		for _, entry := range entries {
			if entry.Seq <= state.Seq {
				continue
			}

			if entry.Cmd == "SET" {
				state.Entries[entry.Key] = entry
				applied++
			} else if entry.Cmd == "DEL" {
				delete(state.Entries, entry.Key)
				applied++
			}

			if entry.Seq > state.Seq {
				state.Seq = entry.Seq
			}
		}
		log.Printf("[recovery] applied %d WAL entries (seq=%d)", applied, state.Seq)
	}

	r.recovered = true
	return state, nil
}

func (r *Recoverer) IsRecovered() bool {
	return r.recovered
}

type RecoveryStats struct {
	SnapshotEntries int
	WALEntries      int
	TotalEntries    int
	RecoveredAt     time.Time
}
