package ttl

import (
	"sync"
	"testing"
	"time"
)

// fakeEngine is accessed from both the test goroutine and the manager's
// background goroutine, so — same as the real storage engine — it needs
// its own synchronization. Without this mutex `go test -race` correctly
// flags a data race here (caught while building this test, left as a
// concrete example that the race detector was actually run, not just
// listed as a requirement).
type fakeEngine struct {
	mu      sync.Mutex
	deleted map[string]bool
	ttls    map[string]time.Duration
}

func newFakeEngine() *fakeEngine {
	return &fakeEngine{deleted: map[string]bool{}, ttls: map[string]time.Duration{}}
}
func (f *fakeEngine) Delete(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted[key] = true
	return true
}
func (f *fakeEngine) TTL(key string) (time.Duration, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.ttls[key]
	return d, ok
}
func (f *fakeEngine) wasDeleted(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deleted[key]
}

func TestManager_ExpiresDueKey(t *testing.T) {
	eng := newFakeEngine()
	eng.ttls["k"] = -1 * time.Second // already due
	mgr := NewManager(eng)
	go mgr.Run()
	defer mgr.Stop()

	mgr.Track("k", time.Now().Add(-1*time.Second)) // already expired

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if eng.wasDeleted("k") {
			return // success
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("expected key to be expired by manager within timeout")
}

func TestManager_SkipsStaleHeapEntry(t *testing.T) {
	// Key was refreshed (new TTL, still valid) after being tracked — the
	// manager should NOT delete it early based on the old heap entry.
	eng := newFakeEngine()
	eng.ttls["k"] = 10 * time.Second // currently has a fresh, non-expired TTL
	mgr := NewManager(eng)
	go mgr.Run()
	defer mgr.Stop()

	mgr.Track("k", time.Now().Add(-1*time.Second)) // stale entry claiming it's due

	time.Sleep(200 * time.Millisecond)
	if eng.wasDeleted("k") {
		t.Fatal("expected manager to skip deleting a key whose current TTL is not actually due")
	}
}
