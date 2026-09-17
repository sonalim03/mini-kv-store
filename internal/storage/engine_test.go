package storage

import (
	"sync"
	"testing"
	"time"
)

func TestSetGetDelete(t *testing.T) {
	e := New()
	e.Set("k", "v", 0)
	if v, ok := e.Get("k"); !ok || v != "v" {
		t.Fatalf("expected v, got %q ok=%v", v, ok)
	}
	if !e.Delete("k") {
		t.Fatal("expected delete to report existed")
	}
	if _, ok := e.Get("k"); ok {
		t.Fatal("expected key gone after delete")
	}
}

func TestTTLExpiry_LazyOnRead(t *testing.T) {
	e := New()
	e.Set("k", "v", 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	if _, ok := e.Get("k"); ok {
		t.Fatal("expected expired key to be treated as missing")
	}
}

func TestExists(t *testing.T) {
	e := New()
	if e.Exists("missing") {
		t.Fatal("expected false for missing key")
	}
	e.Set("k", "v", 0)
	if !e.Exists("k") {
		t.Fatal("expected true for present key")
	}
}

// TestConcurrentAccess_NoRace hammers the engine from many goroutines across
// overlapping keys. Meaningful primarily under `go test -race`.
func TestConcurrentAccess_NoRace(t *testing.T) {
	e := New()
	var wg sync.WaitGroup
	const goroutines = 50
	const opsPerGoroutine = 500

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := "key" + string(rune('A'+i%26))
				e.Set(key, "v", 0)
				e.Get(key)
				e.Exists(key)
				if i%10 == 0 {
					e.Delete(key)
				}
			}
		}(g)
	}
	wg.Wait()
}
