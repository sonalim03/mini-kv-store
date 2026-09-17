// Package protocol implements the simple text command protocol:
//
//	SET key value
//	GET key
//	DELETE key
//	EXISTS key
//	TTL key
//	EXPIRE key seconds
//
// Kept intentionally simple (not RESP-binary-compatible) since the spec
// calls for a "simple text protocol" — the interesting engineering is in
// the storage/WAL/distribution layers, not protocol bit-packing.
package protocol

import (
	"errors"
	"strconv"
	"strings"
)

type CommandType string

const (
	CmdSet    CommandType = "SET"
	CmdGet    CommandType = "GET"
	CmdDelete CommandType = "DELETE"
	CmdExists CommandType = "EXISTS"
	CmdTTL    CommandType = "TTL"
	CmdExpire CommandType = "EXPIRE"

	// Internal node-to-node commands. Prefixed distinctly (REPL_/PING) so a
	// client accidentally sending one gets a clean "unknown command" rather
	// than silently doing something internal-only.
	CmdReplicate CommandType = "REPL_WRITE" // REPL_WRITE op key value ttl_ms
	CmdPing      CommandType = "PING"       // heartbeat between nodes
)

var ErrMalformed = errors.New("malformed command")
var ErrUnknownCommand = errors.New("unknown command")

const maxCommandLength = 64 * 1024 // request size limit — see spec section 20 (security)

type Command struct {
	Type  CommandType
	Key   string
	Value string // SET, REPL_WRITE only
	Sec   int    // EXPIRE only

	// REPL_WRITE only: the underlying op being replicated ("SET"/"DELETE"/
	// "EXPIRE", matching wal.OpType strings) and its TTL in milliseconds.
	ReplOp    string
	TTLMillis int64
}

// Parse turns one line of client input into a Command. Values containing
// spaces are supported via a simple quoting convention: SET key "hello world".
func Parse(line string) (*Command, error) {
	if len(line) > maxCommandLength {
		return nil, errors.New("command exceeds max length")
	}
	line = strings.TrimSpace(line)
	if line == "" {
		return nil, ErrMalformed
	}

	parts := splitRespectingQuotes(line)
	if len(parts) == 0 {
		return nil, ErrMalformed
	}

	switch strings.ToUpper(parts[0]) {
	case string(CmdSet):
		if len(parts) != 3 {
			return nil, ErrMalformed
		}
		return &Command{Type: CmdSet, Key: parts[1], Value: parts[2]}, nil

	case string(CmdGet):
		if len(parts) != 2 {
			return nil, ErrMalformed
		}
		return &Command{Type: CmdGet, Key: parts[1]}, nil

	case string(CmdDelete):
		if len(parts) != 2 {
			return nil, ErrMalformed
		}
		return &Command{Type: CmdDelete, Key: parts[1]}, nil

	case string(CmdExists):
		if len(parts) != 2 {
			return nil, ErrMalformed
		}
		return &Command{Type: CmdExists, Key: parts[1]}, nil

	case string(CmdTTL):
		if len(parts) != 2 {
			return nil, ErrMalformed
		}
		return &Command{Type: CmdTTL, Key: parts[1]}, nil

	case string(CmdExpire):
		if len(parts) != 3 {
			return nil, ErrMalformed
		}
		sec, err := strconv.Atoi(parts[2])
		if err != nil {
			return nil, ErrMalformed
		}
		return &Command{Type: CmdExpire, Key: parts[1], Sec: sec}, nil

	case string(CmdReplicate):
		// REPL_WRITE op key value ttl_ms  (value/ttl_ms may be "-" if unused)
		if len(parts) != 5 {
			return nil, ErrMalformed
		}
		ttlMs, err := strconv.ParseInt(parts[4], 10, 64)
		if err != nil {
			return nil, ErrMalformed
		}
		val := parts[3]
		if val == "-" {
			val = ""
		}
		return &Command{Type: CmdReplicate, ReplOp: parts[1], Key: parts[2], Value: val, TTLMillis: ttlMs}, nil

	case string(CmdPing):
		return &Command{Type: CmdPing}, nil

	default:
		return nil, ErrUnknownCommand
	}
}

// splitRespectingQuotes splits on whitespace but keeps a "quoted string" as
// one token, so `SET greeting "hello world"` parses as 3 parts, not 4.
func splitRespectingQuotes(s string) []string {
	var parts []string
	var cur strings.Builder
	inQuotes := false

	for _, r := range s {
		switch {
		case r == '"':
			inQuotes = !inQuotes
		case r == ' ' && !inQuotes:
			if cur.Len() > 0 {
				parts = append(parts, cur.String())
				cur.Reset()
			}
		default:
			cur.WriteRune(r)
		}
	}
	if cur.Len() > 0 {
		parts = append(parts, cur.String())
	}
	return parts
}
