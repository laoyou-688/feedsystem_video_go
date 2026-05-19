#!/usr/bin/env bash
set -euo pipefail

HOST="${HOST:-http://127.0.0.1:8080}"
USERNAME="${USERNAME:-feed_perf_user_$(date +%s)}"
PASSWORD="${PASSWORD:-pass123456}"
VIDEO_COUNT="${VIDEO_COUNT:-30}"
PLAY_URL="${PLAY_URL:-http://example.com/perf.mp4}"
COVER_URL="${COVER_URL:-http://example.com/perf.jpg}"

register_body=$(printf '{"username":"%s","password":"%s"}' "$USERNAME" "$PASSWORD")
curl -sS -X POST "$HOST/account/register" \
  -H 'Content-Type: application/json' \
  -d "$register_body" >/dev/null || true

login_resp=$(curl -sS -X POST "$HOST/account/login" \
  -H 'Content-Type: application/json' \
  -d "$register_body")

token=$(printf '%s' "$login_resp" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
if [ -z "$token" ]; then
  echo "failed to login and extract token" >&2
  echo "$login_resp" >&2
  exit 1
fi

for i in $(seq 1 "$VIDEO_COUNT"); do
  publish_body=$(printf '{"title":"feed perf video %s","description":"feed perf generated video %s","play_url":"%s","cover_url":"%s"}' "$i" "$i" "$PLAY_URL" "$COVER_URL")
  curl -sS -X POST "$HOST/video/publish" \
    -H 'Content-Type: application/json' \
    -H "Authorization: Bearer $token" \
    -d "$publish_body" >/dev/null
done

echo "HOST=$HOST"
echo "USERNAME=$USERNAME"
echo "TOKEN=$token"
echo "VIDEO_COUNT=$VIDEO_COUNT"
echo "STATUS=feed_perf_seed_ready"
