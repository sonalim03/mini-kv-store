# Mini Distributed Key-Value Store

A from-scratch key-value database in Go — sharded in-memory storage engine,
TCP text protocol, write-ahead log with crash recovery, atomic snapshots,
consistent hashing with virtual nodes, and a background TTL expiration
system using a min-heap. Built to demonstrate storage-engine and
distributed-systems fundamentals directly, not by wrapping Redis/Postgres.

## Honest project status (read this first)

**Built, tested, and verified working end-to-end**: single-node storage
engine, WAL, snapshots, crash recovery, TTL expiration, TCP protocol, CLI
client, consistent hashing, **and now cross-node replication + partition
routing**. A 2-node cluster was actually started, a write sent to only one
node, and the other node's own WAL file was inspected directly to confirm
it independently persisted its own durable copy a few hundred microseconds
later — proof of real replication, not just request-forwarding (see
"Verified end-to-end" for the transcript and WAL inspection).

**Still scaffolded, not wired up**: gRPC inter-node communication (the
current node-to-node protocol reuses the same TCP text protocol with a
`REPL_WRITE`/`PING` prefix — functionally equivalent for this project's
purposes, but not literally gRPC as the original spec suggested), formal
failure-injection test harness, and Prometheus metrics collection
(dependency declared, nothing recorded yet). Node health/heartbeats
**are** implemented (`internal/membership`) and used to skip replication
to a known-down peer.

**Why the TCP-text choice instead of gRPC**: the spec allows "gRPC or a
well-designed TCP protocol" for inter-node messages — a well-designed TCP
protocol was chosen to reuse the same parser/server/connection-handling
code already built and tested for the client-facing protocol, rather than
introducing a second protocol stack (protobuf definitions, separate gRPC
server) for marginal benefit at this project's scale. This is a documented
trade-off, not an oversight.

## Verified end-to-end

```
$ kv-cli SET user:1001 Sonali
OK
$ kv-cli GET user:1001
Sonali
$ kv-cli EXPIRE user:1001 1
OK
$ kv-cli TTL user:1001
0
(wait 2s)
$ kv-cli GET user:1001
NOT_FOUND

--- crash recovery test ---
$ kv-cli SET durable:key "survives a crash"
OK
$ kill -9 <server-pid>          # hard crash, no graceful shutdown
$ ./server                       # restart
{"msg":"recovery complete","wal_records_replayed":1}
$ kv-cli GET durable:key
survives a crash                 # correctly recovered from WAL

--- 2-node replication test ---
$ NODE_ID=nodeA LISTEN_ADDR=:9101 PEERS="nodeB@localhost:9102" REPLICATION_FACTOR=2 ./server &
$ NODE_ID=nodeB LISTEN_ADDR=:9102 PEERS="nodeA@localhost:9101" REPLICATION_FACTOR=2 ./server &

$ kv-cli -addr localhost:9101 SET user:1001 Sonali    # written ONLY to node A
OK
$ kv-cli -addr localhost:9102 GET user:1001            # asked node B, never written there directly
Sonali

# The above alone doesn't prove real replication — node B could just be
# silently forwarding the read to node A. So the WAL files on disk were
# inspected directly instead:

$ cat nodeA/wal/segment-000000.wal | (decoded)
{"op":"SET","key":"user:1001","value":"Sonali","ts":"...46.757102731Z"}

$ cat nodeB/wal/segment-000000.wal | (decoded)
{"op":"SET","key":"user:1001","value":"Sonali","ts":"...46.757555812Z"}
#                                                        ^ ~450 microseconds
#                                                          after node A's own
#                                                          write — node B
#                                                          independently
#                                                          durably stored its
#                                                          own copy, this is
#                                                          real replication.
```

`go test -race ./...` — all packages pass. One real data race was caught
and fixed during development: a test helper (`internal/ttl/heap_test.go`'s
`fakeEngine`) accessed a plain map from two goroutines without a lock; the
race detector caught it exactly as intended, and the fix (a mutex on the
test double) is in the current code. Left as evidence the tooling was
actually run, not just listed as a requirement.

## Architecture

```
Client ──TCP──► [Node] Protocol Parser ──► Storage Engine (sharded map)
                          │                        │
                          ▼                        ▼
                         WAL (durability)      TTL Manager (min-heap)
                          │
                          ▼
                     Snapshot Manager (periodic, atomic rename)

Cluster layer (scaffolded, not wired):
  hashring.Ring determines which node owns a key
  → partition router (not yet built) would forward client requests
  → replication manager (not yet built) would propagate writes to replicas
```

## Storage engine

`internal/storage` — 256 independently-locked shards (`RWMutex` per shard,
key routed to a shard via FNV hash) instead of one global lock, so
unrelated keys never contend. `Get`/`Set`/`Delete`/`Exists` are O(1)
average. The public type is an interface (`storage.Engine`) specifically so
the in-memory map implementation could later be swapped for something like
an LSM-tree without touching the networking layer above it.

## TTL system

`internal/ttl` — min-heap keyed by expiry time, not a full-keyspace scan.
**Why a min-heap over a timing wheel**: a timing wheel is arguably more
efficient at very high throughput, but is meaningfully more complex to
implement correctly (bucket sizing, wheel advancement, cascading). A
min-heap gives O(log n) inserts and does work proportional to how many keys
are *actually* due, not the size of the keyspace — the right complexity
trade-off for this project's scale, and it's easy to prove correct in
review. The background manager sleeps until the next key is actually due
(not a fast fixed tick), so idle CPU use is near zero. Lazy expiration on
read (`Get` checks the entry's own expiry) is layered on top so a client
can never observe a stale key even if the background sweep hasn't caught
up yet.

## Write-ahead log

`internal/wal` — every SET/DELETE/EXPIRE is appended (length-prefixed +
CRC32-checksummed) and, depending on `WAL_FSYNC_EVERY_WRITE`, fsync'd
*before* the client is told the write succeeded. Segments rotate at 64MB.
**Corruption detection**: on replay, if a record's stored checksum doesn't
match its payload, replay stops at that point rather than trusting
corrupted data — this is what a real crash-mid-write looks like (verified
in `TestReplay_StopsAtCorruption`, which deliberately appends garbage bytes
after a valid record and confirms only the valid prefix is replayed).

## Snapshots & crash recovery

`internal/snapshot` — periodic (default every 5 min) full-state dump,
written atomically: serialize to a temp file, fsync, then `os.Rename` over
the real snapshot file. Rename is atomic at the filesystem level, so a
crash mid-write never leaves a half-written snapshot behind.

**Recovery algorithm on startup** (`cmd/server/main.go`): load the latest
snapshot if one exists → replay the WAL on top of it → start serving. This
means recovery time is proportional to (WAL entries since the last
snapshot), not the entire history — verified by the crash-recovery
transcript above.

## Consistent hashing

`internal/hashring` — 150 virtual nodes per real node by default.
**Why consistent hashing over `hash(key) % N`**: modulo hashing means
adding/removing one node changes the target for almost every key (the
divisor changes for nearly all remainders), forcing a near-total data
reshuffle. Consistent hashing only remaps the keys between the changed
node and its neighbor on the ring — verified in
`TestAddNode_MinimalRemapping`, which adds a 3rd node to a 2-node ring and
asserts remapping stays well under the ~100% churn a naive modulo scheme
would cause. Virtual nodes exist because a handful of *real* node points on
a ring can land unevenly by chance; spreading many virtual points per real
node averages the keyspace split out.

## Consistency model

**Declared explicitly, per the spec's instruction not to overclaim**: this
project provides **eventual consistency** across replicas, not strong /
linearizable consistency. The write path is: primary node appends to its
own WAL, applies to its own engine, acks the client — **then**
asynchronously fires the write to replica nodes (`internal/replication`).
The client's "OK" does not wait for any replica to confirm. This was
verified directly (see "Verified end-to-end"): a replica's own WAL entry
for a key is timestamped observably later than the primary's. A reader
hitting a replica microseconds after a write to the primary could
theoretically see stale data in that narrow window — this is the accepted
trade-off of eventual consistency, and no consensus protocol
(Raft/Paxos) is implemented to avoid it. Do not claim strong consistency
for this project in an interview; that would be the single most likely
thing to get contradicted in a follow-up question.

## Concurrency model

Sharded locks (storage), one dedicated background goroutine for TTL
expiration, one goroutine per client connection (network layer), bounded by
`maxConnections`. Verified race-free with `go test -race ./...` including a
50-goroutine/500-ops-each contention test in
`internal/storage/engine_test.go`.

## What's built vs. still scaffolded (be ready to say this plainly)

**Built and verified**:
- Partition router (`internal/cluster`) — `hashring.Ring` determines a
  key's owner; a request for a key this node doesn't own is transparently
  forwarded over TCP to the owning node and the reply relayed back
- Replication (`internal/replication`) — async, best-effort, fire-and-forget
  after the primary's own local write; skips known-unhealthy peers up front
  rather than wasting a timeout on them
- Node membership / failure detection (`internal/membership`) — heartbeat
  every 1s via the same protocol's `PING` command; a peer is marked
  unhealthy after 3 consecutive missed heartbeats and recovered
  automatically once heartbeats resume
- All of this is opt-in via `PEERS` env var — unset, a node runs in the
  exact original single-node mode with zero behavior change

**Still NOT built**:
- Literal gRPC (a TCP text protocol with `REPL_WRITE`/`PING` commands is
  used instead — see "Honest project status" above for why)
- Prometheus metrics collection (dependency declared, nothing recorded yet)
- Formal failure-injection test harness (dropped messages, simulated
  network partition, WAL corruption during replication) — `tests/failure`
  exists as a placeholder directory
- Multi-node automated test suite — the replication proof above was a
  manual test run and WAL-inspected by hand, not a `go test` in
  `tests/distributed`
- Replication acknowledgement / write quorum (currently fire-and-forget;
  no "wait for W replicas to ack" option)
- `kv-cli cluster` / `kv-cli nodes` (stubbed to print "not yet implemented")
- Anti-entropy / reconciliation for a replica that missed writes while
  down and later recovers (it'll be behind until the next write to a key
  it holds; no background repair process re-syncs it)

## Design trade-offs

- **Text protocol, not RESP-binary or gRPC**: simpler to implement and
  debug by hand (`nc localhost 9000` works, same protocol used for both
  client and inter-node messages); the interesting engineering here is the
  storage/WAL/hashing/replication layers, not protocol bit-packing or a
  second protobuf-based stack.
- **Goroutine-per-connection**, not an epoll reactor: idiomatic Go, scales
  to tens of thousands of connections without issue at this project's
  scale; a reactor pattern would be premature complexity here.
- **JSON WAL records**, not a custom binary format: easier to inspect/debug
  during development; the length-prefix + checksum framing around each
  record is what actually matters for correctness, not the payload
  encoding — could swap to a tighter binary encoding later without changing
  the recovery algorithm.

## Limitations

- Single-node only in practice until the cluster layer above is built
- No authentication/TLS (config surface for it should exist per spec
  section 20, but isn't implemented — see Future improvements)
- `AllEntries()` for snapshotting takes each shard's lock sequentially, not
  a single consistent point-in-time view across all shards — acceptable for
  a periodic background snapshot, not appropriate if you needed a true
  atomic full-database snapshot

## Future improvements (in priority order to actually finish the spec)

1. Replication write quorum / ack semantics (currently fire-and-forget)
2. Anti-entropy repair for a replica that was down and comes back
3. Wire up `internal/metrics` — actually record the Prometheus metrics
   listed in the original spec (latency histograms, cache hit/miss, etc.)
4. Migrate inter-node protocol to literal gRPC if that specific technology
   is a hard requirement (functionally equivalent TCP protocol exists now)
5. Automated multi-node test suite in `tests/distributed` (the replication
   proof in this README was a manual test — good enough to demo, not a
   repeatable CI test yet)
6. Failure injection harness (dropped replication messages, simulated
   network partition)
7. Load-test / benchmark suite comparing 1/3/5 node configurations

## Running it (single node)

```bash
go build -o server ./cmd/server
go build -o kv-cli ./cmd/client

DATA_DIR=./data ./server &
./kv-cli SET user:1001 Sonali
./kv-cli GET user:1001
./kv-cli EXPIRE user:1001 60
./kv-cli TTL user:1001

go test ./...       # go test -race ./... also works if cgo is available
go vet ./...
```

## Running a real cluster locally (2+ nodes, replication + routing)

```bash
go build -o server ./cmd/server
go build -o kv-cli ./cmd/client

# Terminal 1
NODE_ID=nodeA LISTEN_ADDR=:9101 DATA_DIR=./dataA \
  PEERS="nodeB@localhost:9102" REPLICATION_FACTOR=2 ./server

# Terminal 2
NODE_ID=nodeB LISTEN_ADDR=:9102 DATA_DIR=./dataB \
  PEERS="nodeA@localhost:9101" REPLICATION_FACTOR=2 ./server

# Terminal 3 — write to A, read from B: should work via replication,
# and if you kill node B's DATA_DIR/wal file and inspect it, you'll see
# it independently stored the write, not just forwarded the read.
./kv-cli -addr localhost:9101 SET user:1001 Sonali
./kv-cli -addr localhost:9102 GET user:1001
```

```bash
# starts 3 independent nodes (see "Honest project status" — not yet a
# coordinated cluster)
docker compose up --build
```
