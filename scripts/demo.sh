#!/usr/bin/env bash
# sqlraft quick demo: build, start a single node, write via psql,
# and inspect cluster status.
#
# Dependencies: Go; psql is optional (PG part is skipped when absent).
set -euo pipefail

cd "$(dirname "$0")/.."

GRPC_PORT="${GRPC_PORT:-50051}"
PG_PORT="${PG_PORT:-5432}"
DATA_DIR="$(mktemp -d)"

cleanup() {
  if [ -n "${PID:-}" ]; then
    kill "$PID" 2>/dev/null || true
  fi
  rm -rf "$DATA_DIR"
}
trap cleanup EXIT

echo "== 1. build =="
make build
make -s buildctl

echo "== 2. start single node (gRPC :$GRPC_PORT / PG wire :$PG_PORT) =="
bin/sqlraftd -id 0 -addr "127.0.0.1:$GRPC_PORT" \
  -pg-addr "127.0.0.1:$PG_PORT" \
  -data "$DATA_DIR/node0.db" -raft-dir "$DATA_DIR/node0.raft" &
PID=$!

echo "== 3. wait for leader =="
for i in $(seq 1 30); do
  if bin/sqlraftctl -addr "127.0.0.1:$GRPC_PORT" status | grep -q "leader:     0"; then
    break
  fi
  sleep 0.2
done
bin/sqlraftctl -addr "127.0.0.1:$GRPC_PORT" status

PSQL="$(command -v psql 2>/dev/null || true)"
if [ -z "$PSQL" ] && [ -x /opt/homebrew/opt/libpq/bin/psql ]; then
  PSQL=/opt/homebrew/opt/libpq/bin/psql
fi
if [ -n "$PSQL" ]; then
  echo "== 4. write via psql =="
  "$PSQL" -h 127.0.0.1 -p "$PG_PORT" -U sqlraft -d sqlraft -X <<'SQL'
CREATE TABLE IF NOT EXISTS kv (id SERIAL PRIMARY KEY, v TEXT, created TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
INSERT INTO kv (v) VALUES ('hello'), ('world') RETURNING id, v, created;
SELECT * FROM kv ORDER BY id;
SQL
else
  echo "== 4. psql not found; skip SQL demo (use bin/sqlraftctl or a gRPC client) =="
fi

echo "== 5. done (data dir $DATA_DIR removed on exit) =="
