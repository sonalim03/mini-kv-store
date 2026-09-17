// cmd/server wires together the storage engine, WAL, TTL manager, snapshot
// manager, TCP server, and (if PEERS is configured) the cluster layer —
// consistent hash ring, membership/heartbeats, and replication — into one
// runnable node process.
//
// Cluster mode is opt-in via env vars:
//
//	NODE_ID=node-a
//	PEERS=node-b@node-b:9000,node-c@node-c:9000   (comma-separated id@addr)
//	REPLICATION_FACTOR=2
//
// If PEERS is unset, the node runs exactly as before: single-node, every
// key local, no replication — this preserves the original behavior.
//
// Recovery order on startup: load latest snapshot -> replay WAL entries
// since -> start serving. This is the crash-recovery algorithm.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/yourname/mini-kv-store/internal/cluster"
	"github.com/yourname/mini-kv-store/internal/hashring"
	"github.com/yourname/mini-kv-store/internal/membership"
	"github.com/yourname/mini-kv-store/internal/network"
	"github.com/yourname/mini-kv-store/internal/replication"
	"github.com/yourname/mini-kv-store/internal/snapshot"
	"github.com/yourname/mini-kv-store/internal/storage"
	"github.com/yourname/mini-kv-store/internal/ttl"
	"github.com/yourname/mini-kv-store/internal/wal"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	addr := envOr("LISTEN_ADDR", ":9000")
	dataDir := envOr("DATA_DIR", "./data")
	fsyncEveryWrite := envOr("WAL_FSYNC_EVERY_WRITE", "true") == "true"

	engine := storage.New()

	// --- Crash recovery: snapshot first, then WAL replay on top of it ---
	if entries, err := snapshot.Load(dataDir); err != nil {
		logger.Warn("snapshot load failed, falling back to full WAL replay", "error", err)
	} else if entries != nil {
		for _, e := range entries {
			var remaining time.Duration
			if !e.ExpiresAt.IsZero() {
				remaining = time.Until(e.ExpiresAt)
				if remaining <= 0 {
					continue // already expired between snapshot and restart
				}
			}
			engine.Set(e.Key, e.Value, remaining)
		}
		logger.Info("loaded snapshot", "keys", len(entries))
	}

	walDir := dataDir + "/wal"
	replayed := 0
	if err := wal.Replay(walDir, func(rec wal.Record) error {
		switch rec.Op {
		case wal.OpSet:
			engine.Set(rec.Key, rec.Value, 0)
		case wal.OpDelete:
			engine.Delete(rec.Key)
		case wal.OpExpire:
			engine.Expire(rec.Key, time.Duration(rec.TTLMillis)*time.Millisecond)
		}
		replayed++
		return nil
	}); err != nil {
		logger.Error("WAL replay encountered an error", "error", err)
	}
	logger.Info("recovery complete", "wal_records_replayed", replayed)

	log, err := wal.Open(walDir, fsyncEveryWrite)
	if err != nil {
		logger.Error("failed to open WAL for writing", "error", err)
		os.Exit(1)
	}
	defer log.Close()

	ttlMgr := ttl.NewManager(engine)
	go ttlMgr.Run()
	defer ttlMgr.Stop()

	stopSnapshots := make(chan struct{})
	go snapshot.RunPeriodic(dataDir, engine, 5*time.Minute, stopSnapshots, func(err error) {
		logger.Error("periodic snapshot failed", "error", err)
	})
	defer close(stopSnapshots)

	srv := network.NewServer(addr, engine, log, logger, ttlMgr.Track)

	// --- Cluster mode: only activated if PEERS is set ---
	var stopMembership chan struct{}
	if peersEnv := os.Getenv("PEERS"); peersEnv != "" {
		nodeID := envOr("NODE_ID", "node-"+addr)
		replicationFactor, _ := strconv.Atoi(envOr("REPLICATION_FACTOR", "2"))

		ring := hashring.New()
		ring.AddNode(nodeID)
		addrs := map[string]string{nodeID: addr}

		members := membership.NewRegistry(logger)
		for _, entry := range strings.Split(peersEnv, ",") {
			parts := strings.SplitN(strings.TrimSpace(entry), "@", 2)
			if len(parts) != 2 {
				logger.Warn("skipping malformed PEERS entry", "entry", entry)
				continue
			}
			peerID, peerAddr := parts[0], parts[1]
			ring.AddNode(peerID)
			addrs[peerID] = peerAddr
			members.AddPeer(peerID, peerAddr)
		}

		router := cluster.NewRouter(nodeID, ring, addrs, replicationFactor)
		repl := replication.New(logger, members, router)

		stopMembership = make(chan struct{})
		go members.Run(stopMembership)

		srv = srv.WithCluster(router, repl)
		logger.Info("cluster mode enabled",
			"node_id", nodeID, "peers", len(addrs)-1, "replication_factor", replicationFactor)
	} else {
		logger.Info("running in single-node mode (no PEERS configured)")
	}

	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigCh
		logger.Info("shutdown signal received, taking final snapshot")
		if err := snapshot.Save(dataDir, engine); err != nil {
			logger.Error("final snapshot failed", "error", err)
		}
		if stopMembership != nil {
			close(stopMembership)
		}
		cancel()
	}()

	if err := srv.Run(ctx); err != nil {
		logger.Error("server stopped with error", "error", err)
		os.Exit(1)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
