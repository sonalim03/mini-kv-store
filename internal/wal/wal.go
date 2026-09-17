// Package wal implements a write-ahead log: every mutating operation
// (SET/DELETE/EXPIRE) is durably appended here *before* the caller is told
// the write succeeded. On restart, replaying this log (after loading the
// latest snapshot) reconstructs in-memory state.
//
// Record format (length-prefixed + checksummed, one record per line-ish
// unit but binary, not text, so we can detect corruption reliably):
//
//	[4 bytes: record length][4 bytes: CRC32 checksum][payload bytes]
//
// The checksum is what makes corruption detection possible: on recovery, if
// a record's stored checksum doesn't match the checksum computed over its
// payload, we know a partial/corrupted write happened (e.g. crash mid-fsync)
// and we stop replaying at that point rather than trusting corrupted data.
package wal

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type OpType string

const (
	OpSet    OpType = "SET"
	OpDelete OpType = "DELETE"
	OpExpire OpType = "EXPIRE"
)

type Record struct {
	Op        OpType    `json:"op"`
	Key       string    `json:"key"`
	Value     string    `json:"value,omitempty"`
	TTLMillis int64     `json:"ttl_ms,omitempty"`
	Timestamp time.Time `json:"ts"`
}

const maxSegmentBytes = 64 * 1024 * 1024 // rotate after 64MB

type WAL struct {
	mu          sync.Mutex
	dir         string
	file        *os.File
	writer      *bufio.Writer
	currentSize int64
	segmentIdx  int
	// fsyncEveryWrite trades throughput for durability guarantees. true =
	// every write survives a hard power loss (fsync'd before returning);
	// false = writes are durable to the OS but a few could be lost on a
	// hard crash between the last fsync and the crash. Configurable
	// per the "fsync strategy" requirement rather than hardcoded.
	fsyncEveryWrite bool
}

func Open(dir string, fsyncEveryWrite bool) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create wal dir: %w", err)
	}
	w := &WAL{dir: dir, fsyncEveryWrite: fsyncEveryWrite}
	if err := w.openSegment(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *WAL) segmentPath(idx int) string {
	return filepath.Join(w.dir, fmt.Sprintf("segment-%06d.wal", idx))
}

func (w *WAL) openSegment() error {
	f, err := os.OpenFile(w.segmentPath(w.segmentIdx), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	info, _ := f.Stat()
	w.file = f
	w.writer = bufio.NewWriter(f)
	w.currentSize = info.Size()
	return nil
}

// Append durably records an operation. Returns only after the record is
// written (and fsync'd, if fsyncEveryWrite) — callers should not confirm
// the operation to clients until this returns nil.
func (w *WAL) Append(rec Record) error {
	rec.Timestamp = time.Now()
	payload, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal record: %w", err)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.currentSize > maxSegmentBytes {
		if err := w.rotate(); err != nil {
			return err
		}
	}

	checksum := crc32.ChecksumIEEE(payload)
	header := make([]byte, 8)
	binary.BigEndian.PutUint32(header[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[4:8], checksum)

	n1, err := w.writer.Write(header)
	if err != nil {
		return fmt.Errorf("write wal header: %w", err)
	}
	n2, err := w.writer.Write(payload)
	if err != nil {
		return fmt.Errorf("write wal payload: %w", err)
	}
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("flush wal: %w", err)
	}
	if w.fsyncEveryWrite {
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("fsync wal: %w", err)
		}
	}

	w.currentSize += int64(n1 + n2)
	return nil
}

// rotate closes the current segment and opens a fresh one. Called with the
// lock already held.
func (w *WAL) rotate() error {
	if err := w.writer.Flush(); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return err
	}
	w.segmentIdx++
	return w.openSegment()
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}

// Replay reads every segment in order and calls apply(rec) for each valid
// record. On hitting a corrupted or partially-written record (checksum
// mismatch, or a truncated header/payload from a crash mid-write), it stops
// replaying *that segment* and returns — this is the safe behavior: trust
// everything up to the corruption point, discard anything after it, since a
// partial write can't be trusted to represent a real committed operation.
func Replay(dir string, apply func(Record) error) error {
	segments, err := filepath.Glob(filepath.Join(dir, "segment-*.wal"))
	if err != nil {
		return err
	}
	for _, seg := range segments {
		if err := replaySegment(seg, apply); err != nil {
			return fmt.Errorf("replay %s: %w", seg, err)
		}
	}
	return nil
}

func replaySegment(path string, apply func(Record) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	for {
		header := make([]byte, 8)
		_, err := io.ReadFull(r, header)
		if err == io.EOF {
			return nil // clean end of segment
		}
		if err != nil {
			// Truncated header — a crash happened mid-write of this record.
			// Stop here; this record was never durably completed.
			return nil
		}

		length := binary.BigEndian.Uint32(header[0:4])
		wantChecksum := binary.BigEndian.Uint32(header[4:8])

		payload := make([]byte, length)
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil // truncated payload, same reasoning as above
		}

		gotChecksum := crc32.ChecksumIEEE(payload)
		if gotChecksum != wantChecksum {
			// Corruption detected: stop replaying rather than trusting a
			// record that might have been partially overwritten.
			return fmt.Errorf("checksum mismatch at offset, stopping replay (corruption detected)")
		}

		var rec Record
		if err := json.Unmarshal(payload, &rec); err != nil {
			return fmt.Errorf("corrupt record payload: %w", err)
		}
		if err := apply(rec); err != nil {
			return err
		}
	}
}
