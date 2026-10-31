#!/usr/bin/env bash
# Demo: tenant isolation. A "noisy" tenant (max 1 running job) floods the system; a
# "quiet" tenant's jobs submitted afterwards still finish promptly. Then the noisy
# tenant hits its submission rate limit.
set -euo pipefail
cd "$(dirname "$0")/.."

API=${API:-localhost:${DJOS_API_HOST_PORT:-58000}}
export DJOS_ADMIN_TOKEN=${DJOS_ADMIN_TOKEN:-demo-admin-token-0123456789}
ADMIN="Authorization: Bearer $DJOS_ADMIN_TOKEN"
WORKERS=$(docker compose ps -q worker 2>/dev/null | wc -l | tr -d ' '); [ "$WORKERS" -ge 1 ] || WORKERS=3

echo "== starting stack ($WORKERS workers) with the admin API enabled"
docker compose up -d --build --wait --scale worker="$WORKERS" >/dev/null 2>&1

suffix=$(date +%s)
key() { python3 -c 'import json,sys; print(json.load(sys.stdin)["api_key"])'; }
noisy=$(curl -sf -X POST "$API/admin/tenants" -H "$ADMIN" -H 'Content-Type: application/json' \
  -d "{\"name\": \"noisy-$suffix\", \"max_concurrency\": 1, \"rate_limit_per_second\": 2, \"burst\": 40}" | key)
quiet=$(curl -sf -X POST "$API/admin/tenants" -H "$ADMIN" -H 'Content-Type: application/json' \
  -d "{\"name\": \"quiet-$suffix\"}" | key)
echo "== created tenants noisy-$suffix (max 1 running, 2/s burst 40) and quiet-$suffix"

submit() { curl -s -o /dev/null -w '%{http_code}\n' -X POST "$API/jobs" -H "Authorization: Bearer $1" \
  -H 'Content-Type: application/json' -d '{"type": "sleep", "payload": {"seconds": 0.5}}'; }

echo "== noisy submits 30 half-second jobs; then quiet submits 5"
for _ in $(seq 1 30); do submit "$noisy" >/dev/null; done
start=$(python3 -c 'import time; print(time.time())')
for _ in $(seq 1 5); do submit "$quiet" >/dev/null; done

count() { curl -sf "$API/jobs?status=succeeded&limit=100" -H "Authorization: Bearer $1" \
  | python3 -c 'import json,sys; print(len(json.load(sys.stdin)["items"]))'; }
while [ "$(count "$quiet")" -lt 5 ]; do sleep 0.2; done
elapsed=$(python3 -c "import time; print(f'{time.time() - $start:.1f}')")
echo "   quiet's 5 jobs all finished in ${elapsed}s; noisy had finished $(count "$noisy") of 30 by then"
echo "   (without the cap, quiet would wait behind ~30 x 0.5s / $WORKERS workers ≈ $(( 30 / 2 / WORKERS ))s of noisy work)"

echo "== noisy bursts 50 more submissions against a burst of 40:"
for _ in $(seq 1 50); do submit "$noisy"; done | sort | uniq -c | sed 's/^/   /'
echo "== tenants can't see each other's jobs: quiet sees $(curl -sf "$API/jobs?limit=100" -H "Authorization: Bearer $quiet" | python3 -c 'import json,sys; print(len(json.load(sys.stdin)["items"]))') jobs (its own 5)"
