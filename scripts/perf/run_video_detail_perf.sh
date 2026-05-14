#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)"
PERF_DIR="$ROOT_DIR/scripts/perf"

HOST="${HOST:-http://127.0.0.1:8080}"
VIDEO_ID="${VIDEO_ID:-}"
CONNECTIONS="${CONNECTIONS:-50}"
THREADS="${THREADS:-4}"
DURATION="${DURATION:-15s}"
WRK_BIN="${WRK_BIN:-wrk}"

if ! command -v "$WRK_BIN" >/dev/null 2>&1; then
  echo "missing wrk binary: $WRK_BIN" >&2
  exit 1
fi

if [ -z "$VIDEO_ID" ]; then
  echo "VIDEO_ID is required. Run scripts/perf/prepare_perf_data.sh first." >&2
  exit 1
fi

TMP_LUA="$(mktemp)"
trap 'rm -f "$TMP_LUA"' EXIT
sed "s/__VIDEO_ID__/$VIDEO_ID/g" "$PERF_DIR/video_detail.lua" > "$TMP_LUA"

echo "== prewarm auto path =="
CACHE_MODE=auto "$WRK_BIN" -t1 -c1 -d2s -s "$TMP_LUA" "$HOST/video/getDetail" >/dev/null

echo
echo "== local cache path =="
CACHE_MODE=local "$WRK_BIN" -t"$THREADS" -c"$CONNECTIONS" -d"$DURATION" -s "$TMP_LUA" "$HOST/video/getDetail"

echo
echo "== redis cache path =="
CACHE_MODE=redis "$WRK_BIN" -t"$THREADS" -c"$CONNECTIONS" -d"$DURATION" -s "$TMP_LUA" "$HOST/video/getDetail"

echo
echo "== mysql direct path =="
CACHE_MODE=mysql "$WRK_BIN" -t"$THREADS" -c"$CONNECTIONS" -d"$DURATION" -s "$TMP_LUA" "$HOST/video/getDetail"
