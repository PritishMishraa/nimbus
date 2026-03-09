#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEMO_DIR="$(mktemp -d "${TMPDIR:-/tmp}/nimbus-demo-XXXXXX")"
ADDR="${NIMBUS_DEMO_ADDR:-127.0.0.1:9010}"
COORDINATOR_LOG="$DEMO_DIR/coordinator.log"
OUT_DIR="$DEMO_DIR/out"
PID=""

cleanup() {
	if [[ -n "$PID" ]] && kill -0 "$PID" 2>/dev/null; then
		kill "$PID" 2>/dev/null || true
		wait "$PID" 2>/dev/null || true
	fi
}

trap cleanup EXIT

mkdir -p "$OUT_DIR"

cat > "$DEMO_DIR/demo-a.txt" <<'EOF'
alpha
match here
omega
EOF

cat > "$DEMO_DIR/demo-b.txt" <<'EOF'
skip
another match
EOF

echo "demo workspace: $DEMO_DIR"
echo "starting coordinator on $ADDR"

go run "$ROOT_DIR"/cmd/coordinator \
	-addr "$ADDR" \
	-inputs "$DEMO_DIR/demo-a.txt,$DEMO_DIR/demo-b.txt" \
	>"$COORDINATOR_LOG" 2>&1 &
PID=$!

sleep 1

echo
echo "coordinator output"
cat "$COORDINATOR_LOG"

echo
echo "first worker run"
go run "$ROOT_DIR"/cmd/worker \
	-addr "$ADDR" \
	-needle "match" \
	-dir "$OUT_DIR"

echo
echo "second worker run"
go run "$ROOT_DIR"/cmd/worker \
	-addr "$ADDR" \
	-needle "match" \
	-dir "$OUT_DIR"

echo
echo "intermediate artifacts"
ls -1 "$OUT_DIR"

for file in "$OUT_DIR"/*.jsonl; do
	echo
	echo "--- $(basename "$file") ---"
	cat "$file"
done
