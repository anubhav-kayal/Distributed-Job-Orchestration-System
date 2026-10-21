#!/usr/bin/env bash
# Demo: freeze a worker mid-job (docker pause, like a long GC pause), let its job be
# reclaimed and finished by another worker, then unfreeze it and show that its stale
# result is rejected: the job ends up with exactly one effect.
#
# The freeze has to land in a ~1s window and `docker pause` itself takes a moment, so
# occasionally it lands too late; the script detects that and asks you to rerun.
set -euo pipefail
cd "$(dirname "$0")/.."

API=${API:-localhost:${DJOS_API_HOST_PORT:-58000}}
export DJOS_LEASE_TTL_SECONDS=4 DJOS_HEARTBEAT_INTERVAL_SECONDS=2 DJOS_MONITOR_INTERVAL_SECONDS=1

echo "== starting stack: 2 workers, 4s lease TTL"
docker compose up -d --build --wait --scale worker=2 >/dev/null 2>&1

job() { curl -sf "$API/jobs/$1"; }
field() { python3 -c "import json,sys; print(json.load(sys.stdin).get('$1') or '')"; }
summary() {
  python3 -c 'import json,sys; j=json.load(sys.stdin); print("status=%s attempts=%s token=%s worker=%s" % (j["status"], j["attempts"], j["lease_token"], j["locked_by"]))'
}
psql() { docker compose exec -T postgres psql -U djos -d djos -tAc "$1"; }

id=$(curl -sf -X POST "$API/jobs" -H 'Content-Type: application/json' \
  -d '{"type": "sleep", "payload": {"seconds": 5}}' | field id)
echo "== submitted job $id (sleeps 5s)"

for _ in $(seq 1 20); do
  [ "$(job "$id" | field status)" = running ] && break
  sleep 0.2
done
victim=$(job "$id" | field locked_by)
container=${victim%-*}
# Heartbeats fire at t=2s and t=4s and the job ends at t=5s. Freezing inside that last
# second means that on waking, the job's timer is due before the next heartbeat, so the
# worker finishes the job *without* noticing it lost its lease: the case only fencing
# catches. (Freeze earlier and the heartbeat notices first, and the worker abandons.)
# Time it from the job's real start (containers share the host clock), not from when we
# happened to notice it running.
job "$id" | python3 -c '
import json, sys, time
from datetime import datetime
started = datetime.fromisoformat(json.load(sys.stdin)["started_at"]).timestamp()
time.sleep(max(0.0, started + 4.2 - time.time()))'
echo "== freezing worker $victim (docker pause) just before its job finishes"
docker pause "$container" >/dev/null
if [ "$(job "$id" | field status)" != running ]; then
  docker unpause "$container" >/dev/null
  echo "!! the job finished before the freeze took effect; run the demo again"; exit 1
fi

for _ in $(seq 1 40); do
  [ "$(job "$id" | field status)" = succeeded ] && break
  sleep 0.5
done
echo "== while it was frozen: $(job "$id" | summary)"

echo "== unfreezing $victim"
docker unpause "$container" >/dev/null
sleep 2

echo "== what the woken worker did:"
docker logs "$container" 2>&1 | grep "$id" | grep -E "fenced|abandoning" | sed 's/^/  /' || echo "  (nothing logged)"
echo "== effects recorded for the job (must be exactly 1):"
psql "select '  token=' || lease_token || ' worker=' || worker_id from effects where job_id = '$id'"

echo "== restoring the stack's default timings"
unset DJOS_LEASE_TTL_SECONDS DJOS_HEARTBEAT_INTERVAL_SECONDS DJOS_MONITOR_INTERVAL_SECONDS
docker compose up -d --wait --scale worker=2 >/dev/null 2>&1
