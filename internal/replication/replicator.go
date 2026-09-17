// Package replication propagates writes from a key's primary node to its
// replica nodes, asynchronously, after the primary has already durably
// written to its own WAL and ack'd the client. This is what makes the
// consistency model eventual, not strong: the client is told "OK" based on
// the primary's own durability, before replicas necessarily have the data.
//
// Delivery is best-effort with a short timeout per replica — a replica that's
// down or slow does NOT block or fail the client's write (the primary
// already committed). A replica that's unhealthy per membership.Registry is
// skipped up front rather than attempted and timed out, to avoid wasting
// time on a peer already known to be down.
package replication

import (
	"fmt"
	"log/slog"
	"net"
	"time"
)

const dialTimeout = 500 * time.Millisecond

type HealthChecker interface {
	IsHealthy(nodeID string) bool
}

type PeerAddrs interface {
	AddrOf(nodeID string) (string, bool)
}

type Replicator struct {
	logger *slog.Logger
	health HealthChecker
	addrs  PeerAddrs
}

func New(logger *slog.Logger, health HealthChecker, addrs PeerAddrs) *Replicator {
	return &Replicator{logger: logger, health: health, addrs: addrs}
}

// Replicate sends one write to each of the given replica node IDs,
// concurrently, fire-and-forget from the caller's perspective (it does not
// block the client response — call this in a goroutine from the write
// path). op is "SET", "DELETE", or "EXPIRE" (matches wal.OpType strings).
func (r *Replicator) Replicate(replicaNodeIDs []string, op, key, value string, ttlMillis int64) {
	for _, nodeID := range replicaNodeIDs {
		go r.sendOne(nodeID, op, key, value, ttlMillis)
	}
}

func (r *Replicator) sendOne(nodeID, op, key, value string, ttlMillis int64) {
	if r.health != nil && !r.health.IsHealthy(nodeID) {
		r.logger.Warn("skipping replication to unhealthy peer", "node_id", nodeID, "key", key)
		return
	}
	addr, ok := r.addrs.AddrOf(nodeID)
	if !ok {
		return
	}

	conn, err := net.DialTimeout("tcp", addr, dialTimeout)
	if err != nil {
		r.logger.Warn("replication dial failed", "node_id", nodeID, "error", err)
		return
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(dialTimeout))

	val := value
	if val == "" {
		val = "-"
	}
	line := fmt.Sprintf("REPL_WRITE %s %s %s %d\n", op, key, val, ttlMillis)
	if _, err := conn.Write([]byte(line)); err != nil {
		r.logger.Warn("replication write failed", "node_id", nodeID, "error", err)
		return
	}
	// Best-effort ack read; we don't retry on failure here (see package doc
	// — primary already committed, this is advisory). A production system
	// would track failed replications for later reconciliation/anti-entropy;
	// noted as a follow-up in README, not implemented here.
	buf := make([]byte, 64)
	conn.Read(buf)
}
