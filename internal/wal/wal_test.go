package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAppendReplay_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, true)
	if err != nil {
		t.Fatal(err)
	}

	records := []Record{
		{Op: OpSet, Key: "a", Value: "1"},
		{Op: OpSet, Key: "b", Value: "2"},
		{Op: OpDelete, Key: "a"},
	}
	for _, r := range records {
		if err := w.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	var replayed []Record
	if err := Replay(dir, func(r Record) error {
		replayed = append(replayed, r)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if len(replayed) != len(records) {
		t.Fatalf("expected %d records replayed, got %d", len(records), len(replayed))
	}
	for i, r := range replayed {
		if r.Op != records[i].Op || r.Key != records[i].Key {
			t.Fatalf("record %d mismatch: got %+v want %+v", i, r, records[i])
		}
	}
}

// TestReplay_StopsAtCorruption simulates a crash mid-write: valid records
// followed by a truncated/corrupted final record. Replay should return the
// valid prefix and stop, not error out entirely or trust the garbage tail.
func TestReplay_StopsAtCorruption(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Record{Op: OpSet, Key: "good", Value: "1"}); err != nil {
		t.Fatal(err)
	}
	w.Close()

	// Corrupt: append garbage bytes that look like a header but have a
	// checksum that won't match any real payload.
	segPath := filepath.Join(dir, "segment-000000.wal")
	f, err := os.OpenFile(segPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{0, 0, 0, 5, 0xDE, 0xAD, 0xBE, 0xEF, 'x', 'y', 'z', 'z', 'y'})
	f.Close()

	var replayed []Record
	err = Replay(dir, func(r Record) error {
		replayed = append(replayed, r)
		return nil
	})

	if len(replayed) != 1 || replayed[0].Key != "good" {
		t.Fatalf("expected exactly the 1 valid record before corruption, got %+v", replayed)
	}
	if err == nil {
		t.Fatal("expected an error reporting corruption was detected")
	}
}

func TestRecovery_SetThenDeleteReconstructsState(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir, true)
	w.Append(Record{Op: OpSet, Key: "k", Value: "v1"})
	w.Append(Record{Op: OpSet, Key: "k", Value: "v2"})
	w.Close()

	state := map[string]string{}
	Replay(dir, func(r Record) error {
		if r.Op == OpSet {
			state[r.Key] = r.Value
		}
		return nil
	})

	if state["k"] != "v2" {
		t.Fatalf("expected last write to win on replay, got %q", state["k"])
	}
}
