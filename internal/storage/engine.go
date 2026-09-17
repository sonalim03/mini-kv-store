// Package storage implements the in-memory KV engine. Deliberately NOT a
// single map + single mutex: that would serialize every operation across
// the whole keyspace regardless of which keys are touched. Instead the
// keyspace is split into N shards, each with its own RWMutex, so unrelated
// keys never contend with each other. This is the same technique Go's
// sync.Map and most real-world sharded caches use.
package storage

import (
	"hash/fnv"
	"sync"
	"time"

	"github.com/yourname/mini-kv-store/internal/snapshot"
)

const defaultShardCount = 256

// Engine is the public interface other layers (network, cluster) depend on.
// Keeping this as an interface — not the concrete *ShardedEngine type —
// means the storage layer can be swapped (e.g. for an LSM-tree-backed
// engine later) without touching the networking or protocol code.
type Engine interface {
	Set(key, value string, ttl time.Duration)
	Get(key string) (string, bool)
	Delete(key string) bool
	Exists(key string) bool
	TTL(key string) (time.Duration, bool) // ok=false if key missing or has no TTL
	Expire(key string, ttl time.Duration) bool
	Len() int
}

type entry struct {
	value     string
	expiresAt time.Time // zero value = no expiry
}

func (e *entry) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && now.After(e.expiresAt)
}

type shard struct {
	mu   sync.RWMutex
	data map[string]*entry
}

// ShardedEngine implements Engine with N independently-locked shards.
// OnExpire is called by the TTL manager (internal/ttl) when a key's timer
// fires, so the engine and the TTL heap stay in sync without the engine
// needing to import the ttl package (avoids an import cycle; the ttl
// manager imports storage, not the other way around).
type ShardedEngine struct {
	shards []*shard
}

func New() *ShardedEngine {
	shards := make([]*shard, defaultShardCount)
	for i := range shards {
		shards[i] = &shard{data: make(map[string]*entry)}
	}
	return &ShardedEngine{shards: shards}
}

func (e *ShardedEngine) shardFor(key string) *shard {
	h := fnv.New32a()
	h.Write([]byte(key))
	return e.shards[h.Sum32()%uint32(len(e.shards))]
}

func (e *ShardedEngine) Set(key, value string, ttl time.Duration) {
	s := e.shardFor(key)
	ent := &entry{value: value}
	if ttl > 0 {
		ent.expiresAt = time.Now().Add(ttl)
	}
	s.mu.Lock()
	s.data[key] = ent
	s.mu.Unlock()
}

func (e *ShardedEngine) Get(key string) (string, bool) {
	s := e.shardFor(key)
	s.mu.RLock()
	ent, ok := s.data[key]
	s.mu.RUnlock()
	if !ok {
		return "", false
	}
	if ent.expired(time.Now()) {
		// Lazy expiration on read, in addition to the background TTL
		// manager: guarantees a client never observes an expired key even
		// if the background sweep hasn't caught up yet.
		e.Delete(key)
		return "", false
	}
	return ent.value, true
}

func (e *ShardedEngine) Delete(key string) bool {
	s := e.shardFor(key)
	s.mu.Lock()
	_, existed := s.data[key]
	delete(s.data, key)
	s.mu.Unlock()
	return existed
}

func (e *ShardedEngine) Exists(key string) bool {
	_, ok := e.Get(key) // reuses lazy-expiry logic
	return ok
}

func (e *ShardedEngine) TTL(key string) (time.Duration, bool) {
	s := e.shardFor(key)
	s.mu.RLock()
	ent, ok := s.data[key]
	s.mu.RUnlock()
	if !ok || ent.expired(time.Now()) {
		return 0, false
	}
	if ent.expiresAt.IsZero() {
		return 0, false // key exists but has no TTL set
	}
	return time.Until(ent.expiresAt), true
}

func (e *ShardedEngine) Expire(key string, ttl time.Duration) bool {
	s := e.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	ent, ok := s.data[key]
	if !ok {
		return false
	}
	ent.expiresAt = time.Now().Add(ttl)
	return true
}

// AllEntries returns a point-in-time list of every live key/value/expiry,
// used by the snapshot manager (internal/snapshot) to persist state. Takes
// a read lock per shard sequentially rather than locking everything at
// once at the same instant — briefly inconsistent across shards under
// concurrent writes, an accepted trade-off for a periodic background
// snapshot (documented in README). Returns snapshot.Entry directly so this
// satisfies snapshot.Snapshotter without an adapter type.
func (e *ShardedEngine) AllEntries() []snapshot.Entry {
	var out []snapshot.Entry
	now := time.Now()
	for _, s := range e.shards {
		s.mu.RLock()
		for k, ent := range s.data {
			if ent.expired(now) {
				continue
			}
			out = append(out, snapshot.Entry{Key: k, Value: ent.value, ExpiresAt: ent.expiresAt})
		}
		s.mu.RUnlock()
	}
	return out
}

func (e *ShardedEngine) Len() int {
	total := 0
	for _, s := range e.shards {
		s.mu.RLock()
		total += len(s.data)
		s.mu.RUnlock()
	}
	return total
}
