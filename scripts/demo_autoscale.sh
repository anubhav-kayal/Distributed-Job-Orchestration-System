#!/usr/bin/env bash
# Demo: the autoscaler takes 2 workers up to 8 under a burst, then back down to 2 once
# the backlog is gone, and every job still runs exactly once (scale-down drains workers
# gracefully: each finishes its current job before stopping).
set -euo pipefail
cd "$(dirname "$0")/.."

LOG=$(mktemp -t djos-autoscaler)
echo "== stack with 2 workers; autoscaler: 2..8 workers, 20 jobs per worker, 20s scale-down window"
docker compose up -d --build --wait --scale worker=2 >/dev/null 2>&1
uv run python -m djos.autoscaler --min 2 --max 8 --target 20 --window 20 --interval 2 >"$LOG" 2>&1 &
AUTOSCALER=$!
trap 'kill $AUTOSCALER 2>/dev/null || true' EXIT

echo "== submitting 1500 mixed jobs (0.05–0.5s sleeps and hashes) and waiting for all of them"
uv run python scripts/verify_exactly_once.py --jobs 1500 --workers 2 \
  | grep -E "^(elapsed|statuses|attempts|effects|lost|PASS|FAIL)"

echo "== waiting for the scale-down window to pass"
for _ in $(seq 1 30); do
  [ "$(docker compose ps -q --status running worker | wc -l | tr -d ' ')" -le 2 ] && break
  sleep 2
done
echo "== workers now: $(docker compose ps -q --status running worker | wc -l | tr -d ' ')"
echo "== autoscaler decisions:"
grep "scaling workers" "$LOG" | sed -E 's/^.*backlog/   backlog/'
