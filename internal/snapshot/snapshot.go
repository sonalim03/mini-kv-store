// Package snapshot periodically persists the full in-memory keyspace to
// disk, so recovery doesn't require replaying the WAL from the beginning of
// time — only from the most recent snapshot forward.
package snapshot

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type Entry struct {
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	ExpiresAt time.Time `json:"expires_at,omitempty"`
}

// Snapshotter is the minimal read surface needed to take a snapshot,
// deliberately narrow to avoid a circular dependency on the full storage
// engine interface.
type Snapshotter interface {
	AllEntries() []Entry
}

const snapshotFileName = "snapshot.json"
const tmpFileName = "snapshot.json.tmp"

// Save writes the current state atomically: write to a temp file, fsync,
// then rename over the real snapshot file. This ensures a crash mid-write
// never leaves a corrupted/partial snapshot — the rename is atomic at the
// filesystem level, so readers only ever see either the old complete
// snapshot or the new complete one, never a half-written one.
func Save(dir string, s Snapshotter) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	entries := s.AllEntries()
	data, err := json.Marshal(entries)
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}

	tmpPath := filepath.Join(dir, tmpFileName)
	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("create temp snapshot: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("write snapshot: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("fsync snapshot: %w", err)
	}
	if err := f.Close(); err != nil {
		return err
	}

	finalPath := filepath.Join(dir, snapshotFileName)
	return os.Rename(tmpPath, finalPath) // atomic on POSIX filesystems
}

// Load reads the most recent snapshot, if one exists. Returns (nil, nil) if
// no snapshot file is present (fresh node — recovery will rely on WAL alone).
func Load(dir string) ([]Entry, error) {
	path := filepath.Join(dir, snapshotFileName)
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read snapshot: %w", err)
	}
	var entries []Entry
	if err := json.Unmarshal(data, &entries); err != nil {
		// A corrupted snapshot is treated as "no usable snapshot" rather
		// than a fatal error — recovery falls back to WAL replay from
		// scratch, which is always safe, just slower.
		return nil, fmt.Errorf("corrupt snapshot, falling back to full WAL replay: %w", err)
	}
	return entries, nil
}

// RunPeriodic starts a ticker that calls Save every interval until stop is
// closed. Meant to run in its own goroutine.
func RunPeriodic(dir string, s Snapshotter, interval time.Duration, stop <-chan struct{}, onErr func(error)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			if err := Save(dir, s); err != nil && onErr != nil {
				onErr(err)
			}
		}
	}
}
