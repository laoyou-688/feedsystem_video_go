#!/usr/bin/env bash
set -euo pipefail

HOST="${HOST:-http://127.0.0.1:8080}"
USERNAME="${USERNAME:-perf_user_$(date +%s)}"
PASSWORD="${PASSWORD:-pass123456}"
TITLE="${TITLE:-perf video}"
DESC="${DESC:-perf generated video}"
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

publish_body=$(printf '{"title":"%s","description":"%s","play_url":"%s","cover_url":"%s"}' "$TITLE" "$DESC" "$PLAY_URL" "$COVER_URL")
publish_resp=$(curl -sS -X POST "$HOST/video/publish" \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $token" \
  -d "$publish_body")

video_id=$(printf '%s' "$publish_resp" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
if [ -z "$video_id" ]; then
  echo "failed to publish video and extract video id" >&2
  echo "$publish_resp" >&2
  exit 1
fi

echo "HOST=$HOST"
echo "USERNAME=$USERNAME"
echo "TOKEN=$token"
echo "VIDEO_ID=$video_id"
