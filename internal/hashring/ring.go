// Package hashring implements consistent hashing with virtual nodes for
// mapping keys to storage nodes.
//
// Why consistent hashing (not `hash(key) % nodeCount`): with modulo hashing,
// adding or removing a single node changes the target node for nearly every
// key (remainder changes for almost all keys when the divisor changes),
// forcing a near-total data reshuffle. Consistent hashing places both nodes
// and keys on a hash ring; adding/removing a node only remaps the keys
// between that node and its neighbor on the ring — roughly 1/N of the
// keyspace, not all of it.
//
// Why virtual nodes: a real node only occupies one point on the ring, so
// with few real nodes the keyspace split between them can be very uneven
// (one node might own 70% of the ring by chance). Giving each real node
// many virtual points spread around the ring averages this out, so load
// distributes roughly evenly even with a small number of real nodes.
package hashring

import (
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

const defaultVirtualNodes = 150 // higher = more even distribution, more memory/lookup cost

type Ring struct {
	mu           sync.RWMutex
	virtualNodes int
	sortedHashes []uint32          // sorted ring positions, for binary search
	hashToNode   map[uint32]string // ring position -> real node ID
	nodes        map[string]bool   // real node IDs currently in the ring
}

func New() *Ring {
	return &Ring{
		virtualNodes: defaultVirtualNodes,
		hashToNode:   make(map[uint32]string),
		nodes:        make(map[string]bool),
	}
}

func hashKey(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

// AddNode places virtualNodes points for nodeID around the ring. Only the
// keys that fall between this node's new points and their previous
// neighbors move — not the whole keyspace.
func (r *Ring) AddNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.nodes[nodeID] {
		return // already present
	}
	r.nodes[nodeID] = true

	for i := 0; i < r.virtualNodes; i++ {
		vKey := nodeID + "#" + strconv.Itoa(i)
		h := hashKey(vKey)
		r.hashToNode[h] = nodeID
		r.sortedHashes = append(r.sortedHashes, h)
	}
	sort.Slice(r.sortedHashes, func(i, j int) bool { return r.sortedHashes[i] < r.sortedHashes[j] })
}

// RemoveNode removes all of nodeID's virtual points. Keys that mapped to it
// fall through to the next node clockwise on the ring.
func (r *Ring) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.nodes[nodeID] {
		return
	}
	delete(r.nodes, nodeID)

	kept := r.sortedHashes[:0]
	for _, h := range r.sortedHashes {
		if r.hashToNode[h] == nodeID {
			delete(r.hashToNode, h)
			continue
		}
		kept = append(kept, h)
	}
	r.sortedHashes = kept
}

// GetNode returns the node responsible for key: walk clockwise from the
// key's hash position to the first node point (binary search since the
// ring is kept sorted).
func (r *Ring) GetNode(key string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.sortedHashes) == 0 {
		return "", false
	}

	h := hashKey(key)
	idx := sort.Search(len(r.sortedHashes), func(i int) bool { return r.sortedHashes[i] >= h })
	if idx == len(r.sortedHashes) {
		idx = 0 // wrap around the ring
	}
	return r.hashToNode[r.sortedHashes[idx]], true
}

// GetReplicaNodes returns the primary node plus the next (n-1) *distinct*
// real nodes clockwise on the ring — used for replication factor n.
func (r *Ring) GetReplicaNodes(key string, n int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.sortedHashes) == 0 {
		return nil
	}

	h := hashKey(key)
	start := sort.Search(len(r.sortedHashes), func(i int) bool { return r.sortedHashes[i] >= h })

	seen := make(map[string]bool)
	var result []string
	for i := 0; i < len(r.sortedHashes) && len(result) < n; i++ {
		idx := (start + i) % len(r.sortedHashes)
		node := r.hashToNode[r.sortedHashes[idx]]
		if !seen[node] {
			seen[node] = true
			result = append(result, node)
		}
	}
	return result
}

func (r *Ring) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.nodes))
	for n := range r.nodes {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}
