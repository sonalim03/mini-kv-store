// Package membership tracks the set of peer nodes in the cluster and their
// health, via periodic heartbeats over the same TCP protocol the client
// uses (a PING command). This is the failure detector: if a peer misses
// enough consecutive heartbeats, it's marked unhealthy and the cluster
// router (internal/cluster) stops routing writes to it as a replica target
// (it stays in the hash ring for key ownership math, since removing a node
// from the ring on every transient blip would cause needless data churn —
// see README for this trade-off spelled out).
package membership

import (
	"bufio"
	"log/slog"
	"net"
	"sync"
	"time"
)

const (
	heartbeatInterval     = 1 * time.Second
	heartbeatTimeout      = 500 * time.Millisecond
	missedBeforeUnhealthy = 3
)

type PeerStatus struct {
	Addr            string
	Healthy         bool
	ConsecutiveMiss int
	LastSeen        time.Time
}

type Registry struct {
	mu     sync.RWMutex
	peers  map[string]*PeerStatus // nodeID -> status
	logger *slog.Logger
}

func NewRegistry(logger *slog.Logger) *Registry {
	return &Registry{peers: make(map[string]*PeerStatus), logger: logger}
}

// AddPeer registers a peer node to be heartbeated. Called once at startup
// per entry in the PEERS env var.
func (r *Registry) AddPeer(nodeID, addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.peers[nodeID] = &PeerStatus{Addr: addr, Healthy: true, LastSeen: time.Now()}
}

func (r *Registry) IsHealthy(nodeID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.peers[nodeID]
	return ok && p.Healthy
}

func (r *Registry) Snapshot() map[string]PeerStatus {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]PeerStatus, len(r.peers))
	for id, p := range r.peers {
		out[id] = *p
	}
	return out
}

// Run starts the heartbeat loop, blocking until stop is closed. Meant to be
// started with `go registry.Run(stop)`.
func (r *Registry) Run(stop <-chan struct{}) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			r.pingAll()
		}
	}
}

func (r *Registry) pingAll() {
	r.mu.RLock()
	ids := make([]string, 0, len(r.peers))
	addrs := make(map[string]string, len(r.peers))
	for id, p := range r.peers {
		ids = append(ids, id)
		addrs[id] = p.Addr
	}
	r.mu.RUnlock()

	for _, id := range ids {
		ok := r.ping(addrs[id])
		r.recordResult(id, ok)
	}
}

func (r *Registry) ping(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, heartbeatTimeout)
	if err != nil {
		return false
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(heartbeatTimeout))

	if _, err := conn.Write([]byte("PING\n")); err != nil {
		return false
	}
	reply, err := bufio.NewReader(conn).ReadString('\n')
	return err == nil && len(reply) > 0
}

func (r *Registry) recordResult(nodeID string, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, exists := r.peers[nodeID]
	if !exists {
		return
	}

	if ok {
		if !p.Healthy {
			r.logger.Info("peer recovered", "node_id", nodeID)
		}
		p.Healthy = true
		p.ConsecutiveMiss = 0
		p.LastSeen = time.Now()
		return
	}

	p.ConsecutiveMiss++
	if p.ConsecutiveMiss >= missedBeforeUnhealthy && p.Healthy {
		p.Healthy = false
		r.logger.Warn("peer marked unhealthy", "node_id", nodeID, "missed_heartbeats", p.ConsecutiveMiss)
	}
}
