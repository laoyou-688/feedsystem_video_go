#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)"
PERF_DIR="$ROOT_DIR/scripts/perf"

HOST="${HOST:-http://127.0.0.1:8080}"
CONNECTIONS="${CONNECTIONS:-50}"
THREADS="${THREADS:-4}"
DURATION="${DURATION:-15s}"
WRK_BIN="${WRK_BIN:-wrk}"
LIMIT="${LIMIT:-10}"

if ! command -v "$WRK_BIN" >/dev/null 2>&1; then
  echo "missing wrk binary: $WRK_BIN" >&2
  exit 1
fi

echo "== prewarm listLatest auto path (limit=$LIMIT) =="
LIMIT="$LIMIT" FEED_SOURCE_MODE=auto FEED_ENTITY_CACHE_MODE=auto "$WRK_BIN" -t1 -c1 -d2s -s "$PERF_DIR/feed_list_latest.lua" "$HOST/feed/listLatest" >/dev/null

echo
echo "== timeline + auto entity cache path (limit=$LIMIT) =="
LIMIT="$LIMIT" FEED_SOURCE_MODE=auto FEED_ENTITY_CACHE_MODE=auto "$WRK_BIN" -t"$THREADS" -c"$CONNECTIONS" -d"$DURATION" -s "$PERF_DIR/feed_list_latest.lua" "$HOST/feed/listLatest"

echo
echo "== timeline + mysql entity path (limit=$LIMIT) =="
LIMIT="$LIMIT" FEED_SOURCE_MODE=timeline FEED_ENTITY_CACHE_MODE=mysql "$WRK_BIN" -t"$THREADS" -c"$CONNECTIONS" -d"$DURATION" -s "$PERF_DIR/feed_list_latest.lua" "$HOST/feed/listLatest"

echo
echo "== mysql + mysql path (limit=$LIMIT) =="
LIMIT="$LIMIT" FEED_SOURCE_MODE=mysql FEED_ENTITY_CACHE_MODE=mysql "$WRK_BIN" -t"$THREADS" -c"$CONNECTIONS" -d"$DURATION" -s "$PERF_DIR/feed_list_latest.lua" "$HOST/feed/listLatest"
