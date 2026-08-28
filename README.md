# sqlraft — SQLite over Raft

[![CI](https://github.com/kanonon3ko/sqlite-raft/actions/workflows/ci.yml/badge.svg)](https://github.com/kanonon3ko/sqlite-raft/actions)
[![Go](https://img.shields.io/badge/Go-1.25-blue)](https://go.dev/dl/)

Repository: [github.com/kanonon3ko/sqlite-raft](https://github.com/kanonon3ko/sqlite-raft)

sqlraft turns SQLite into a strongly consistent distributed database. SQL statements are replicated and ordered by a Raft consensus layer; every node executes the same commands in the same order against its own local SQLite state machine, so every node converges to the same data.

What makes this project different from existing systems such as rqlite:

- **PostgreSQL wire protocol as a first-class citizen** — connect with `psql`, JDBC, or ORMs directly instead of a custom HTTP/gRPC API only;
- **Deterministic rewriting on the leader** — non-deterministic functions such as `NOW()` / `RANDOM()` are replaced with literals before they enter the log, so the rewritten SQL is auditable and unit-testable;
- **Pure Go, no CGO** — SQLite runs on `modernc.org/sqlite`, so cross-compilation and deployment are simple;
- **Verifiable by construction** — a linearizability checker and chaos tests validate leader uniqueness, state-machine convergence, and operation linearizability under node crashes and network faults.

## Architecture

```
                     +---------------------------+
Client --- gRPC ---> | SQL service layer          |
Client --- PG wire ->|  (deterministic rewriting  |
   (psql/JDBC/ORM)   |   and statement routing)   |
                     | Raft consensus core (WAL)  |
                     | SQLite state machine       |
                     +---------------------------+
```

Write path: `Execute` → the leader rewrites the SQL to eliminate non-determinism → appends to the Raft log (persisted to WAL) → a majority commits → every node applies the entry in order via its `applyLoop` (recording `applied_index` in the same transaction) → the leader returns the result.

Read path: `strong=false` reads the local replica; `strong=true` requires a linearizable read via ReadIndex (M3+).

## Deterministic commands (core design)

Every replicated log entry carries a `DeterministicCommand` (see [proto/log.proto](proto/log.proto)). Before writing to the log, the leader must eliminate SQL non-determinism:

1. `NOW()` / `CURRENT_TIMESTAMP` / `CURRENT_DATE` / `CURRENT_TIME` → literals derived from the command's unified timestamp `now_micros`;
2. `RANDOM()` / `RANDOMBLOB(n)` → precomputed values carried as literals in the log;
3. `AUTOINCREMENT` → the leader pre-allocates explicit IDs based on committed state (the `sequence` field), avoiding dependence on SQLite version-specific rowid allocation;
4. Multi-statement transactions → replicated as a single log entry, applied atomically in one SQLite transaction with `applied_index` advancement.

Implemented in `internal/rewrite`. Known boundaries: non-determinism inside triggers, defaults, or time-string arguments such as `strftime('%s','now')` is out of scope (documented; unsupported until M4).

## PostgreSQL wire compatibility (M2)

Start with `sqlraftd -pg-addr 127.0.0.1:5432`, then connect existing PG tooling directly:

```bash
psql -h 127.0.0.1 -p 5432 -U sqlraft -d sqlraft -c "CREATE TABLE t (id SERIAL PRIMARY KEY, v TEXT)"
psql -h 127.0.0.1 -p 5432 -U sqlraft -d sqlraft -c "INSERT INTO t (v) VALUES ('x') RETURNING id"
```

Implemented in [internal/pgwire](internal/pgwire):

- Startup handshake (trust or SCRAM-SHA-256), parameter status, backend key, cancel requests;
- Simple query and extended query protocols (Parse/Bind/Describe/Execute/Sync) with text-format parameter binding; `$N` is mapped to SQLite numbered parameters;
- PG dialect translation: `$N`, `expr::type`, `ILIKE`, and DDL type names (`SERIAL`→`INTEGER PRIMARY KEY`, `VARCHAR/CHAR`→`TEXT`, `BOOLEAN`→`INTEGER`, `BYTEA`→`BLOB`, `TIMESTAMP`→`TEXT`, ...);
- Result type mapping (INTEGER→INT8, REAL→FLOAT8, TEXT→TEXT, BLOB→BYTEA, ...) and SQLSTATE error codes (unique violation 23505, missing table 42P01, syntax error 42601, not leader 40001, ...);
- `RETURNING` clauses: rows are captured during Raft-replicated apply;
- Metadata shims: `SET`/`SHOW`, `SELECT version()`, `current_database()`, `pg_catalog.set_config(...)`, etc.;
- **SCRAM-SHA-256 authentication** via `-pg-users "alice=s3cret,bob=..."` (RFC 5802/7677; the server stores only salt/StoredKey/ServerKey, never plaintext passwords; wrong passwords return SQLSTATE 28000);
- **pg_catalog metadata queries**: `\dt` / `\dv` / `\di` / `\dn` / `\l` / `\du` / `\df` / `\d t` / `\d+ t` all work — an in-memory catalog database (pg_class/pg_namespace/pg_attribute/pg_type/pg_index/...) is synced from the state machine, and PG-specific syntax (`OPERATOR(~)`, `!~` regex, `::` casts, `= any(...)`, `array(select...)`, ...) is rewritten to executable SQLite.

Known boundaries: psql server-side `PREPARE/EXECUTE` is not supported (the extended query protocol used by JDBC/ORMs is).

## Atomic transactions and linearizable reads (M3)

- **Atomic multi-statement transactions**: writes after `BEGIN` are buffered per session and committed as a **single Raft log entry** on `COMMIT` — if any statement fails, the whole transaction rolls back; `ROLLBACK` discards the buffer; a dropped connection discards it too. Parameterized statements are safely inlined as literals (string escaping, NULL, numbers, BLOB).
- **ReadIndex linearizable reads**: gRPC `Query{strong:true}` records `commitIndex`, confirms its term with a majority of followers, then waits until the state machine has applied that index before reading locally.

Transaction semantics boundaries (documented): buffered writes return placeholder responses (`INSERT 0 0`), actual row counts and `RETURNING` results are unavailable until `COMMIT`; reads inside a transaction see committed state only (read-your-writes / snapshot isolation are future work); `SAVEPOINT` is not supported.

## Snapshot catch-up and membership changes (M3)

- **InstallSnapshot**: the leader compacts its log independently (no longer requiring every node to have replicated the prefix), generates SQLite state-machine snapshots via `VACUUM INTO`, and serves them to lagging followers through the `InstallSnapshot` RPC so long-offline or newly joined nodes can catch up.
- **Dynamic membership**: `ConfChange` entries are replicated as ordinary log entries (one node at a time); the configuration is persisted to WAL (`peers.json`) and the majority size follows the configuration dynamically. A node being removed is excluded from majority computation so a dead node cannot wedge the cluster.

Management CLI ([cmd/sqlraftctl](cmd/sqlraftctl)):

```bash
sqlraftctl -addr 127.0.0.1:50051 status
sqlraftctl -addr 127.0.0.1:50051 add-peer 3=127.0.0.1:50053
sqlraftctl -addr 127.0.0.1:50051 remove-peer 3
```

## Persistence and compaction (M1)

- `-data`: SQLite data file (WAL mode). The `sqlraft_meta` table records `applied_index`, committed in the same transaction as the business write, so a crash never re-applies entries.
- `-raft-dir`: Raft log and state directory. Log entries are length-prefixed protobuf appended with fsync; term/vote/snapshot positions are updated atomically; restarts recover from disk.
- Log compaction: the leader drops applied-and-fully-replicated prefixes once the log exceeds a threshold (conservative strategy; `InstallSnapshot` covers lagging nodes).

## Directory layout

```
proto/                 # API definitions (protobuf)
  sqlraft.proto        # client API: Execute / Query / Admin
  log.proto            # deterministic log entries
  raft.proto           # node-to-node consensus RPC
gen/                   # generated Go code (make gen regenerates)
internal/raft/         # consensus core (election/heartbeat/replication/commit/compaction)
internal/raftwal/      # Raft log and state persistence (crash recovery)
internal/rewrite/      # deterministic SQL rewriter
internal/store/        # SQLite state machine (applied_index metadata)
internal/server/       # gRPC service (Execute/Query/Admin)
internal/pgwire/       # PostgreSQL wire protocol compatibility
internal/lincheck/     # linearizability checker
cmd/sqlraftd/          # daemon
cmd/sqlraftctl/        # cluster management CLI
```

## Getting started

```bash
make gen       # regenerate protobuf code (requires protoc + protoc-gen-go)
make build
make test

# single node (in-memory)
bin/sqlraftd -id 0 -addr 127.0.0.1:50051

# single node, persistent (SQLite file + Raft WAL directory)
bin/sqlraftd -id 0 -addr 127.0.0.1:50051 -data /tmp/node0.db -raft-dir /tmp/node0.raft

# also enable the PostgreSQL wire layer (psql / JDBC direct connection)
bin/sqlraftd -id 0 -addr 127.0.0.1:50051 -pg-addr 127.0.0.1:5432

# three nodes (one terminal each)
bin/sqlraftd -id 0 -addr 127.0.0.1:50051 -peers 1=127.0.0.1:50052,2=127.0.0.1:50053
bin/sqlraftd -id 1 -addr 127.0.0.1:50052 -peers 0=127.0.0.1:50051,2=127.0.0.1:50053
bin/sqlraftd -id 2 -addr 127.0.0.1:50053 -peers 0=127.0.0.1:50051,1=127.0.0.1:50052
```

Or run the one-shot demo:

```bash
./scripts/demo.sh
```

## Verification and performance (M4)

- **Linearizability checker** ([internal/lincheck](internal/lincheck)): verifies operation histories (single-key register semantics) by searching for a valid linearization order, constrained by time windows, per-client call order, and read-value equality.
- **Chaos tests** ([internal/server/chaos_test.go](internal/server/chaos_test.go)): a 3-node cluster runs concurrent read/write workloads under random node crash/restart and network faults (packet loss, latency, partition via a gRPC interceptor). Invariants checked: at most one leader at any time, state-machine convergence, and history linearizability. These tests caught and fixed three real Raft bugs.
- **Performance** (`go test -bench=. -benchtime=3s ./internal/server/`):

  | Scenario | Result |
  |---|---|
  | Single-node write throughput (no WAL) | ~7,400 ops/s (135µs/op) |
  | Three-node write latency | p50 ≈ 255µs, p99 ≈ 505µs |
  | Three-node concurrent UPDATE (group commit) | ~20,700 ops/s |

  Key optimizations: immediate replication push after append (instead of waiting for the next heartbeat) and group commit (concurrent writes are batched into one fsync and one replication round; only INSERT statements are serialized server-side for AUTOINCREMENT correctness).

## Roadmap

- **M0–M3 (done)**: consensus core, deterministic rewriting, persistence & snapshots, membership changes, atomic transactions, ReadIndex, PG wire compatibility, SCRAM, psql meta-commands.
- **M4 (in progress)**: linearizability verification and chaos testing are complete; remaining items are a deterministic simulator (mock clock + message injection) and further performance work.

## License

MIT
