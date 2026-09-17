// Package cluster ties the hash ring, membership, and replication together:
// given a key, it knows which node is the primary, which nodes are
// replicas, and whether a request should be served locally or forwarded to
// another node over TCP.
package cluster

import (
	"bufio"
	"fmt"
	"net"
	"time"

	"github.com/yourname/mini-kv-store/internal/hashring"
)

const forwardTimeout = 2 * time.Second

type Router struct {
	selfID       string
	ring         *hashring.Ring
	addrs        map[string]string // nodeID -> addr, includes self
	replicationN int
}

func NewRouter(selfID string, ring *hashring.Ring, addrs map[string]string, replicationFactor int) *Router {
	return &Router{selfID: selfID, ring: ring, addrs: addrs, replicationN: replicationFactor}
}

func (rt *Router) AddrOf(nodeID string) (string, bool) {
	a, ok := rt.addrs[nodeID]
	return a, ok
}

// Owner returns the primary node ID for key and whether it's this node.
func (rt *Router) Owner(key string) (nodeID string, isLocal bool) {
	n, ok := rt.ring.GetNode(key)
	if !ok {
		return rt.selfID, true // no ring configured (single-node mode) — always local
	}
	return n, n == rt.selfID
}

// ReplicaTargets returns the OTHER nodes (excluding the primary itself)
// that should receive a copy of a write to key, per the configured
// replication factor.
func (rt *Router) ReplicaTargets(key string) []string {
	all := rt.ring.GetReplicaNodes(key, rt.replicationN)
	var targets []string
	for _, n := range all {
		if n != rt.selfID {
			targets = append(targets, n)
		}
	}
	return targets
}

// Forward proxies a raw client command line to the node that owns the key,
// and returns its raw reply. Used when this node receives a request for a
// key it doesn't own — the "partition router" behavior from the spec.
func Forward(addr, rawLine string) (string, error) {
	conn, err := net.DialTimeout("tcp", addr, forwardTimeout)
	if err != nil {
		return "", fmt.Errorf("forward dial: %w", err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(forwardTimeout))

	if _, err := conn.Write([]byte(rawLine)); err != nil {
		return "", fmt.Errorf("forward write: %w", err)
	}
	reply, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("forward read: %w", err)
	}
	return reply, nil
}
