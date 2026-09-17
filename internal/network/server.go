// Package network implements the TCP server: accepts concurrent client
// connections (one goroutine per connection — fine at the connection counts
// this project targets; a production system at massive scale might move to
// an epoll-based reactor, but goroutines-per-connection is the correct,
// idiomatic Go choice here and scales into the tens of thousands of
// connections without issue).
//
// This server is also the node-to-node endpoint: REPL_WRITE and PING
// commands (internal/protocol) arrive on the same port as client commands,
// distinguished by their command prefix — no separate internal port needed.
package network

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/yourname/mini-kv-store/internal/protocol"
	"github.com/yourname/mini-kv-store/internal/storage"
	"github.com/yourname/mini-kv-store/internal/wal"
)

const (
	readTimeout    = 30 * time.Second // idle connection timeout
	maxConnections = 10000            // simple connection-limit guard, see spec section 20
)

// ClusterRouter is the minimal surface the server needs from
// internal/cluster.Router, kept as an interface so network doesn't import
// cluster directly (cluster already imports hashring; network stays
// decoupled from cluster's internals, only needs routing decisions).
type ClusterRouter interface {
	Owner(key string) (nodeID string, isLocal bool)
	ReplicaTargets(key string) []string
	AddrOf(nodeID string) (string, bool)
}

// Replicator is the minimal surface needed from internal/replication.Replicator.
type Replicator interface {
	Replicate(replicaNodeIDs []string, op, key, value string, ttlMillis int64)
}

type Server struct {
	addr     string
	engine   storage.Engine
	log      *wal.WAL
	logger   *slog.Logger
	trackTTL func(key string, expiresAt time.Time) // wired to ttl.Manager.Track

	// router and repl are nil in single-node mode (no PEERS configured) —
	// every command is then always treated as local, matching the
	// project's original single-node behavior exactly.
	router ClusterRouter
	repl   Replicator

	connSem chan struct{} // bounded semaphore enforcing maxConnections
	wg      sync.WaitGroup
}

func NewServer(addr string, engine storage.Engine, log *wal.WAL, logger *slog.Logger, trackTTL func(string, time.Time)) *Server {
	return &Server{
		addr:     addr,
		engine:   engine,
		log:      log,
		logger:   logger,
		trackTTL: trackTTL,
		connSem:  make(chan struct{}, maxConnections),
	}
}

// WithCluster enables cluster-aware routing and replication. Call before
// Run. Left as a separate method (rather than a constructor param) so
// single-node usage and tests stay simple — see cmd/server/main.go for
// how it's wired up when PEERS is configured.
func (s *Server) WithCluster(router ClusterRouter, repl Replicator) *Server {
	s.router = router
	s.repl = repl
	return s
}

// Run blocks accepting connections until ctx is cancelled, then stops
// accepting new ones and waits for in-flight connections to finish
// (graceful shutdown).
func (s *Server) Run(ctx context.Context) error {
	lc := net.ListenConfig{}
	ln, err := lc.Listen(ctx, "tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	s.logger.Info("TCP server listening", "addr", s.addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				s.wg.Wait() // graceful: let in-flight connections finish
				return nil
			default:
				s.logger.Warn("accept error", "error", err)
				continue
			}
		}

		select {
		case s.connSem <- struct{}{}:
		default:
			s.logger.Warn("connection limit reached, rejecting", "remote", conn.RemoteAddr())
			conn.Close()
			continue
		}

		s.wg.Add(1)
		go s.handleConn(ctx, conn)
	}
}

func (s *Server) handleConn(ctx context.Context, conn net.Conn) {
	defer s.wg.Done()
	defer func() { <-s.connSem }()
	defer conn.Close()

	reader := bufio.NewReader(conn)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		conn.SetReadDeadline(time.Now().Add(readTimeout))
		line, err := reader.ReadString('\n')
		if err != nil {
			return // client disconnected or timed out
		}

		cmd, err := protocol.Parse(line)
		if err != nil {
			conn.Write([]byte("ERR " + err.Error() + "\n"))
			continue
		}

		resp := s.execute(cmd, line)
		conn.Write([]byte(resp + "\n"))
	}
}

// execute runs one command. rawLine is the original unparsed line, needed
// so a command for a key this node doesn't own can be forwarded verbatim
// to the owning node (the partition router behavior).
func (s *Server) execute(cmd *protocol.Command, rawLine string) string {
	switch cmd.Type {
	case protocol.CmdPing:
		return "PONG"

	case protocol.CmdReplicate:
		// Applying a replicated write: go through the WAL for local
		// durability, same as a client-originated write, but do NOT
		// re-replicate (this node is a replica for this key, not primary —
		// re-forwarding would risk infinite replication loops).
		return s.applyLocal(cmd.ReplOp, cmd.Key, cmd.Value, cmd.TTLMillis)

	case protocol.CmdSet, protocol.CmdDelete, protocol.CmdExpire:
		if owner, isLocal := s.owner(cmd.Key); !isLocal {
			return s.forward(owner, rawLine)
		}
		return s.executeWriteLocal(cmd)

	case protocol.CmdGet, protocol.CmdExists, protocol.CmdTTL:
		if owner, isLocal := s.owner(cmd.Key); !isLocal {
			return s.forward(owner, rawLine)
		}
		return s.executeReadLocal(cmd)

	default:
		return "ERR unknown command"
	}
}

// owner returns (nodeID, isLocal) for the key, or ("", true) in single-node
// mode (no router configured) so every command stays local — this is what
// makes cluster mode strictly additive over the original single-node path.
func (s *Server) owner(key string) (string, bool) {
	if s.router == nil {
		return "", true
	}
	return s.router.Owner(key)
}

func (s *Server) forward(ownerNodeID, rawLine string) string {
	addr, ok := s.router.AddrOf(ownerNodeID)
	if !ok {
		return "ERR unknown owning node " + ownerNodeID
	}
	// Forwarding dials the owning node directly, rather than importing
	// cluster.Forward here, to keep this package's only cluster dependency
	// the small ClusterRouter interface above (no import of internal/cluster
	// itself — avoids a needless coupling for one function call).
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return "ERR forward failed: " + err.Error()
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write([]byte(rawLine)); err != nil {
		return "ERR forward write failed: " + err.Error()
	}
	reply, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return "ERR forward read failed: " + err.Error()
	}
	// Strip the trailing newline the caller (handleConn) will re-add.
	if len(reply) > 0 && reply[len(reply)-1] == '\n' {
		reply = reply[:len(reply)-1]
	}
	return reply
}

// executeWriteLocal handles SET/DELETE/EXPIRE when this node IS the
// primary for the key: WAL first (durability before ack), apply to the
// engine, ack the client, THEN fire async replication to replica nodes —
// this ordering is exactly what makes the consistency model eventual: the
// client's "OK" does not wait for replicas.
func (s *Server) executeWriteLocal(cmd *protocol.Command) string {
	switch cmd.Type {
	case protocol.CmdSet:
		if err := s.log.Append(wal.Record{Op: wal.OpSet, Key: cmd.Key, Value: cmd.Value}); err != nil {
			return "ERR wal append failed: " + err.Error()
		}
		s.engine.Set(cmd.Key, cmd.Value, 0)
		s.replicateAsync(cmd.Key, "SET", cmd.Value, 0)
		return "OK"

	case protocol.CmdDelete:
		if err := s.log.Append(wal.Record{Op: wal.OpDelete, Key: cmd.Key}); err != nil {
			return "ERR wal append failed: " + err.Error()
		}
		existed := s.engine.Delete(cmd.Key)
		s.replicateAsync(cmd.Key, "DELETE", "", 0)
		if existed {
			return "OK"
		}
		return "NOT_FOUND"

	case protocol.CmdExpire:
		ttlDur := time.Duration(cmd.Sec) * time.Second
		if err := s.log.Append(wal.Record{Op: wal.OpExpire, Key: cmd.Key, TTLMillis: ttlDur.Milliseconds()}); err != nil {
			return "ERR wal append failed: " + err.Error()
		}
		if !s.engine.Expire(cmd.Key, ttlDur) {
			return "NOT_FOUND"
		}
		if s.trackTTL != nil {
			s.trackTTL(cmd.Key, time.Now().Add(ttlDur))
		}
		s.replicateAsync(cmd.Key, "EXPIRE", "", ttlDur.Milliseconds())
		return "OK"
	}
	return "ERR unknown write command"
}

func (s *Server) executeReadLocal(cmd *protocol.Command) string {
	switch cmd.Type {
	case protocol.CmdGet:
		v, ok := s.engine.Get(cmd.Key)
		if !ok {
			return "NOT_FOUND"
		}
		return v

	case protocol.CmdExists:
		if s.engine.Exists(cmd.Key) {
			return "1"
		}
		return "0"

	case protocol.CmdTTL:
		ttl, ok := s.engine.TTL(cmd.Key)
		if !ok {
			return "NOT_FOUND_OR_NO_TTL"
		}
		return strconv.Itoa(int(ttl.Seconds()))
	}
	return "ERR unknown read command"
}

// applyLocal handles an incoming REPL_WRITE from a primary node: same WAL
// + engine path as a local write, so a replica is independently crash-safe
// too, but with no further replication fan-out.
func (s *Server) applyLocal(op, key, value string, ttlMillis int64) string {
	switch op {
	case "SET":
		if err := s.log.Append(wal.Record{Op: wal.OpSet, Key: key, Value: value}); err != nil {
			return "ERR " + err.Error()
		}
		s.engine.Set(key, value, 0)
		return "OK"
	case "DELETE":
		if err := s.log.Append(wal.Record{Op: wal.OpDelete, Key: key}); err != nil {
			return "ERR " + err.Error()
		}
		s.engine.Delete(key)
		return "OK"
	case "EXPIRE":
		ttlDur := time.Duration(ttlMillis) * time.Millisecond
		if err := s.log.Append(wal.Record{Op: wal.OpExpire, Key: key, TTLMillis: ttlMillis}); err != nil {
			return "ERR " + err.Error()
		}
		s.engine.Expire(key, ttlDur)
		if s.trackTTL != nil {
			s.trackTTL(key, time.Now().Add(ttlDur))
		}
		return "OK"
	}
	return "ERR unknown replicated op"
}

func (s *Server) replicateAsync(key, op, value string, ttlMillis int64) {
	if s.router == nil || s.repl == nil {
		return // single-node mode, nothing to replicate to
	}
	targets := s.router.ReplicaTargets(key)
	if len(targets) == 0 {
		return
	}
	s.repl.Replicate(targets, op, key, value, ttlMillis)
}
