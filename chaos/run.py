"""Chaos run: submit a mixed workload, inject faults until it settles, check invariants.

    uv run python -m chaos.run --jobs 1000 --workflows 40 \
        --faults kill,pause,restart-control,redis-kill

The workload (all under a fresh tenant, so the checker sees only this run):
  - independent jobs: mostly short sleeps and hashes, some `flaky` (fail once, then
    succeed: exercises retries) and a few `fail` (permanent: must end dead)
  - diamond workflows A → (B, C) → D; in some, B is a `fail` job, so D must end
    upstream_failed and C must still succeed

Exit status 0 only if every invariant in chaos/checker.py holds.
"""

import argparse
import asyncio
import json
import os
import random
import sys
import time
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import text

from chaos.checker import check
from chaos.faults import WEIGHTS, Faults, compose, pick
from djos.core import db
from djos.core.config import get_settings

ADMIN_TOKEN = "chaos-admin-token-0123456789"
FAST = {
    "DJOS_LEASE_TTL_SECONDS": "4",
    "DJOS_HEARTBEAT_INTERVAL_SECONDS": "1",
    "DJOS_MONITOR_INTERVAL_SECONDS": "1",
    "DJOS_STRANDED_AFTER_SECONDS": "5",
    "DJOS_RETRY_BASE_SECONDS": "0.2",
    "DJOS_ADMIN_TOKEN": ADMIN_TOKEN,
}


def post(api: str, path: str, body: dict, token: str) -> dict:
    req = urllib.request.Request(
        f"http://{api}{path}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def job(rng: random.Random) -> dict:
    roll = rng.random()
    # Kills and pauses use up attempts, so give healthy jobs plenty of them.
    if roll < 0.55:
        return {
            "type": "sleep",
            "payload": {"seconds": round(rng.uniform(0.05, 0.6), 2)},
            "max_attempts": 10,
        }
    if roll < 0.80:
        return {"type": "hash", "payload": {"data": "x", "iterations": 30_000}, "max_attempts": 10}
    if roll < 0.95:
        return {"type": "flaky", "payload": {"fail_times": 1}, "max_attempts": 10}
    return {"type": "fail", "payload": {"permanent": True, "message": "broken on purpose"}}


def diamond(rng: random.Random, broken: bool) -> dict:
    ok = {"type": "sleep", "payload": {"seconds": 0.2}, "max_attempts": 10}
    b = {"type": "fail", "payload": {"permanent": True}} if broken else ok
    return {
        "jobs": [
            {"key": "a", **ok},
            {"key": "b", **b, "depends_on": ["a"]},
            {"key": "c", **ok, "depends_on": ["a"]},
            {"key": "d", **ok, "depends_on": ["b", "c"]},
        ]
    }


async def unfinished(engine, tenant_id) -> int:
    async with engine.connect() as conn:
        return await conn.scalar(
            text(
                "SELECT count(*) FROM jobs WHERE tenant_id = :t AND status IN "
                "('blocked', 'scheduled', 'queued', 'running')"
            ),
            {"t": tenant_id},
        )


def prometheus_total(metric: str) -> float | None:
    url = f"http://localhost:59090/api/v1/query?query=sum({metric})"
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            result = json.load(resp)["data"]["result"]
        return float(result[0]["value"][1]) if result else 0.0
    except OSError:
        return None


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--jobs", type=int, default=1000)
    parser.add_argument("--workflows", type=int, default=40)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--faults", default=",".join(WEIGHTS))
    parser.add_argument("--fault-every", type=float, default=4.0, help="seconds between faults")
    parser.add_argument("--timeout", type=float, default=900)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--api", default="localhost:58000")
    args = parser.parse_args()
    enabled = [f for f in args.faults.split(",") if f]
    rng = random.Random(args.seed)

    print(f"== starting stack: {args.workers} workers, 4s leases, faults={enabled}")
    os.environ.update(FAST)
    compose("up", "-d", "--build", "--wait", "--scale", f"worker={args.workers}")
    faults = Faults(workers=args.workers, lease_ttl=float(FAST["DJOS_LEASE_TTL_SECONDS"]))

    run = uuid.uuid4().hex[:8]
    tenant = post(args.api, "/admin/tenants", {"name": f"chaos-{run}"}, ADMIN_TOKEN)
    tenant_id, key = uuid.UUID(tenant["id"]), tenant["api_key"]
    broken_workflows = max(1, args.workflows // 4)

    start = time.monotonic()
    fenced_before = prometheus_total("djos_fenced_writes_total")
    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(lambda _: post(args.api, "/jobs", job(rng), key), range(args.jobs)))
        list(
            pool.map(
                lambda i: post(args.api, "/workflows", diamond(rng, i < broken_workflows), key),
                range(args.workflows),
            )
        )
    total = args.jobs + 4 * args.workflows
    took = time.monotonic() - start
    print(f"== run {run}: submitted {total} jobs ({args.workflows} workflows) in {took:.1f}s")

    engine = db.create_engine(get_settings().database_url)
    events: list[str] = []
    try:
        next_fault = time.monotonic() + args.fault_every
        while (left := await unfinished(engine, tenant_id)) > 0:
            elapsed = time.monotonic() - start
            if elapsed > args.timeout:
                print(f"!! timeout with {left} jobs unfinished")
                break
            if enabled and time.monotonic() >= next_fault:
                event = await asyncio.to_thread(
                    getattr(
                        faults,
                        {
                            "kill": "kill_worker",
                            "pause": "pause_worker",
                            "restart-control": "restart_control_plane",
                            "redis-kill": "kill_redis",
                        }[pick(enabled)],
                    )
                )
                events.append(event)
                print(f"  t={elapsed:6.1f}s  {left:5d} unfinished  ← {event}", flush=True)
                next_fault = time.monotonic() + args.fault_every
            await asyncio.sleep(1)
        faults.unpause_all()
        report = await check(engine, tenant_id)
    finally:
        await engine.dispose()
        for k in FAST:
            os.environ.pop(k, None)
        compose("up", "-d", "--wait", "--scale", f"worker={args.workers}")

    fenced_after = prometheus_total("djos_fenced_writes_total")
    fault_counts = {
        f: sum(e.startswith(p) for e in events)
        for f, p in [
            ("worker kills", "SIGKILL worker"),
            ("worker pauses", "paused"),
            ("control-plane restarts", "restarted"),
            ("redis kills", "SIGKILLed Redis"),
        ]
    }
    print(f"\n== {report.jobs} jobs, {time.monotonic() - start:.0f}s, faults: {fault_counts}")
    print(f"   final statuses:   {dict(report.statuses)}")
    print(f"   attempts per job: {dict(sorted(report.attempts.items()))}")
    if fenced_before is not None and fenced_after is not None:
        print(
            f"   fenced writes during the run (from Prometheus): {fenced_after - fenced_before:.0f}"
        )
    if report.violations:
        print(f"FAIL: {len(report.violations)} invariant violations")
        for v in report.violations[:20]:
            print(f"   - {v}")
        return 1
    print("PASS: every job settled; exactly one effect per success, fenced by its final claim;")
    print("      only broken jobs died; dependencies respected")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
