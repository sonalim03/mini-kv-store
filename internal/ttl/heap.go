// Package ttl implements background key expiration using a min-heap keyed
// by expiry time, instead of periodically scanning the entire keyspace.
//
// Why a min-heap over alternatives:
//   - Full scan (walk every key, check expiry): O(n) per sweep regardless
//     of how many keys are actually due — wasteful at scale, the thing the
//     spec explicitly says to avoid.
//   - Timing wheel: O(1) insert/expire and arguably better at very high
//     throughput, but more complex to implement correctly (bucket sizing,
//     wheel advancement) for marginal benefit at this project's scale.
//   - Min-heap: O(log n) insert, and expiration work is naturally
//     proportional to how many keys are *actually* due right now (pop while
//     heap-top is expired) rather than the size of the whole keyspace.
//     Simple to reason about and to prove correct in a code review — chosen
//     for that clarity/complexity trade-off, documented explicitly per the
//     spec's requirement to justify the choice.
package ttl

import (
	"container/heap"
	"sync"
	"time"
)

// Expirer is the minimal surface the TTL manager needs from the storage
// engine — deliberately narrow so ttl doesn't depend on storage's full
// interface (and storage doesn't need to depend on ttl at all).
type Expirer interface {
	Delete(key string) bool
	TTL(key string) (time.Duration, bool)
}

type item struct {
	key       string
	expiresAt time.Time
	index     int // maintained by container/heap
}

type minHeap []*item

func (h minHeap) Len() int           { return len(h) }
func (h minHeap) Less(i, j int) bool { return h[i].expiresAt.Before(h[j].expiresAt) }
func (h minHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].index, h[j].index = i, j }
func (h *minHeap) Push(x interface{}) {
	it := x.(*item)
	it.index = len(*h)
	*h = append(*h, it)
}
func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	it := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return it
}

// Manager runs a background goroutine that wakes only when the
// soonest-expiring key is actually due (not on a fixed fast tick), pops it,
// and deletes it from the engine. This keeps idle CPU usage near zero when
// few keys have TTLs.
type Manager struct {
	mu     sync.Mutex
	h      minHeap
	engine Expirer
	wake   chan struct{}
	stop   chan struct{}
}

func NewManager(engine Expirer) *Manager {
	m := &Manager{
		h:      minHeap{},
		engine: engine,
		wake:   make(chan struct{}, 1),
		stop:   make(chan struct{}),
	}
	heap.Init(&m.h)
	return m
}

// Track registers a key's expiry with the manager. Called by the engine (or
// the layer that calls engine.Set/Expire) whenever a TTL is set.
func (m *Manager) Track(key string, expiresAt time.Time) {
	m.mu.Lock()
	heap.Push(&m.h, &item{key: key, expiresAt: expiresAt})
	m.mu.Unlock()

	select {
	case m.wake <- struct{}{}:
	default: // already a pending wake signal, no need to queue another
	}
}

// Run blocks, sleeping until the next key is due, until Stop is called.
// Intended to be started once with `go manager.Run()`.
func (m *Manager) Run() {
	for {
		m.mu.Lock()
		var wait time.Duration
		if len(m.h) == 0 {
			wait = 24 * time.Hour // idle: no keys tracked, sleep long, Track() will wake us
		} else {
			wait = time.Until(m.h[0].expiresAt)
			if wait < 0 {
				wait = 0
			}
		}
		m.mu.Unlock()

		timer := time.NewTimer(wait)
		select {
		case <-m.stop:
			timer.Stop()
			return
		case <-m.wake:
			timer.Stop()
			continue // a new, possibly sooner, key was tracked — recompute wait
		case <-timer.C:
			m.expireDue()
		}
	}
}

func (m *Manager) expireDue() {
	now := time.Now()
	for {
		m.mu.Lock()
		if len(m.h) == 0 || m.h[0].expiresAt.After(now) {
			m.mu.Unlock()
			return
		}
		it := heap.Pop(&m.h).(*item)
		m.mu.Unlock()

		// The key may have been overwritten with a new TTL (or deleted)
		// since this heap entry was pushed — checking the engine's current
		// TTL before deleting avoids expiring a key "early" based on a
		// stale heap entry. This is the min-heap equivalent of a
		// tombstone check.
		currentTTL, hasTTL := m.engine.TTL(it.key)
		if !hasTTL || currentTTL > 0 {
			continue // stale entry: key was refreshed or already gone
		}
		m.engine.Delete(it.key)
	}
}

func (m *Manager) Stop() { close(m.stop) }
