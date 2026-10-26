#!/usr/bin/env bash
# Verify that an every-minute cron schedule creates exactly one job per minute while two
# scheduler replicas run side by side.
#
#   ./scripts/verify_cron.sh [minutes]   (default 3)
set -euo pipefail
cd "$(dirname "$0")/.."

MINUTES=${1:-3}
API=${API:-localhost:${DJOS_API_HOST_PORT:-58000}}
psql() { docker compose exec -T postgres psql -U djos -d djos -tAc "$1"; }

# Keep however many workers are running; compose resets unnamed services to scale 1.
WORKERS=$(docker compose ps -q worker 2>/dev/null | wc -l | tr -d ' ')
WORKERS=${WORKERS:-1}; [ "$WORKERS" -ge 1 ] || WORKERS=1

echo "== starting stack with 2 schedulers"
docker compose up -d --build --wait --scale scheduler=2 --scale worker="$WORKERS" >/dev/null 2>&1
echo "   schedulers running: $(docker compose ps -q scheduler | wc -l | tr -d ' ')"

name="verify-cron-$(date +%s)"
id=$(curl -sf -X POST "$API/schedules" -H 'Content-Type: application/json' \
  -d "{\"name\": \"$name\", \"cron\": \"* * * * *\", \"job\": {\"type\": \"sleep\", \"payload\": {\"seconds\": 0}}}" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
echo "== created schedule $name ($id); waiting ${MINUTES}m + 20s"
sleep $(( MINUTES * 60 + 20 ))
curl -sf -X DELETE "$API/schedules/$id" >/dev/null

echo "== jobs created by the schedule (fire time -> status):"
psql "select '   ' || split_part(idempotency_key, ':', 3) || ':' || split_part(idempotency_key, ':', 4) || ' -> ' || status
      from jobs where idempotency_key like 'cron:$id:%' order by idempotency_key"
total=$(psql "select count(*) from jobs where idempotency_key like 'cron:$id:%'")
distinct=$(psql "select count(distinct date_trunc('minute', created_at)) from jobs where idempotency_key like 'cron:$id:%'")
echo "== $total jobs over $distinct distinct minutes"
echo "== which scheduler fired each minute:"
docker compose logs scheduler 2>/dev/null | grep "schedule $name fired" | sed -E 's/^([a-z-]+-[0-9]+).*fired for ([0-9:-]+ [0-9:]+).*/   \2  by \1/'

if [ "$total" -ge "$MINUTES" ] && [ "$total" -eq "$distinct" ]; then
  echo "PASS: one job per minute, no duplicates"
else
  echo "FAIL"; exit 1
fi
docker compose up -d --wait --scale scheduler=1 --scale worker="$WORKERS" >/dev/null 2>&1
