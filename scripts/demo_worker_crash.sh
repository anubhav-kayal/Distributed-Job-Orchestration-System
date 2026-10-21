#!/usr/bin/env bash
# Demo: SIGKILL the worker running a job and watch the monitor hand it to another worker.
#
# Starts the stack with short timings (6s lease, 1s monitor sweeps) and two workers,
# submits a 8-second job, kills the container running it mid-job, then polls the job.
set -euo pipefail
cd "$(dirname "$0")/.."

API=${API:-localhost:${DJOS_API_HOST_PORT:-58000}}
export DJOS_LEASE_TTL_SECONDS=6 DJOS_HEARTBEAT_INTERVAL_SECONDS=2 DJOS_MONITOR_INTERVAL_SECONDS=1

echo "== starting stack: 2 workers, 6s lease TTL"
docker compose up -d --build --wait --scale worker=2 >/dev/null 2>&1

job() { curl -sf "$API/jobs/$1"; }
field() { python3 -c "import json,sys; print(json.load(sys.stdin).get('$1') or '')"; }

id=$(curl -sf -X POST "$API/jobs" -H 'Content-Type: application/json' \
  -d '{"type": "sleep", "payload": {"seconds": 8}}' | field id)
echo "== submitted job $id (sleeps 8s)"

for _ in $(seq 1 20); do
  [ "$(job "$id" | field status)" = running ] && break
  sleep 0.5
done
victim=$(job "$id" | field locked_by)
container=${victim%-*}  # locked_by is "<container hostname>-<pid>"
echo "== job is running on worker $victim; sending SIGKILL to container $container"
docker kill "$container" >/dev/null

start=$(date +%s)
last=""
while :; do
  body=$(job "$id")
  line="status=$(echo "$body" | field status) attempts=$(echo "$body" | field attempts) worker=$(echo "$body" | field locked_by)"
  if [ "$line" != "$last" ]; then echo "  +$(( $(date +%s) - start ))s  $line"; last=$line; fi
  case $(echo "$body" | field status) in succeeded|failed) break ;; esac
  if [ $(( $(date +%s) - start )) -gt 60 ]; then echo "timed out"; exit 1; fi
  sleep 1
done
echo "== monitor log:"
docker compose logs monitor --no-log-prefix 2>/dev/null | grep "$id" | sed 's/^/  /'
echo "== restoring the stack's default timings"
unset DJOS_LEASE_TTL_SECONDS DJOS_HEARTBEAT_INTERVAL_SECONDS DJOS_MONITOR_INTERVAL_SECONDS
docker compose up -d --wait --scale worker=2 >/dev/null 2>&1
