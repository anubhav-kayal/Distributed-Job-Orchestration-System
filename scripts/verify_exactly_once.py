"""Submit N jobs to the running stack, optionally SIGKILL random workers while they run,
then verify from Postgres that every job succeeded with exactly one effect.

    DJOS_LEASE_TTL_SECONDS=4 DJOS_HEARTBEAT_INTERVAL_SECONDS=1 DJOS_MONITOR_INTERVAL_SECONDS=1 \\
        docker compose up -d --build --wait
    uv run python scripts/verify_exactly_once.py --jobs 1000 --workers 3 --kill-every 3

Exits non-zero if any job is lost (not succeeded) or has any number of effects but one.
"""

import argparse
import asyncio
import json
import random
import subprocess
import sys
import time
import urllib.request
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import text

from djos.core import db
from djos.core.config import get_settings

TERMINAL = ("succeeded", "dead", "cancelled")


def compose(*args: str) -> str:
    return subprocess.run(
        ["docker", "compose", *args], check=True, capture_output=True, text=True
    ).stdout


def kill_random_worker(workers: int) -> bool:
    """SIGKILL one worker container, then start a replacement to keep `workers` running."""
    victims = compose("ps", "-q", "worker").split()
    if not victims:
        return False
    subprocess.run(["docker", "kill", random.choice(victims)], check=True, capture_output=True)
    compose("up", "-d", "--no-recreate", "--scale", f"worker={workers}")
    return True


def submit(api: str, key: str) -> None:
    # A mix of short I/O-bound and CPU-bound jobs.
    if random.random() < 0.7:
        body = {"type": "sleep", "payload": {"seconds": round(random.uniform(0.05, 0.5), 2)}}
    else:
        body = {"type": "hash", "payload": {"data": key, "iterations": 20_000}}
    req = urllib.request.Request(
        f"http://{api}/jobs",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "Idempotency-Key": key},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        assert resp.status in (200, 201), resp.status


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--jobs", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--kill-every", type=float, default=0, help="seconds; 0 = no kills")
    parser.add_argument("--timeout", type=float, default=300)
    parser.add_argument("--api", default="localhost:58000")
    args = parser.parse_args()

    run = f"verify-{uuid.uuid4().hex[:8]}"
    compose("up", "-d", "--no-recreate", "--wait", "--scale", f"worker={args.workers}")
    print(f"run {run}: {args.jobs} jobs, {args.workers} workers, kill every {args.kill_every}s")

    start = time.monotonic()
    with ThreadPoolExecutor(max_workers=20) as pool:
        list(pool.map(lambda i: submit(args.api, f"{run}-{i}"), range(args.jobs)))
    print(f"submitted in {time.monotonic() - start:.1f}s")

    engine = db.create_engine(get_settings().database_url)
    where = "j.idempotency_key LIKE :prefix"
    params = {"prefix": f"{run}-%"}
    kills, last_kill = 0, time.monotonic()
    try:
        while True:
            async with engine.connect() as conn:
                statuses = dict(
                    (
                        await conn.execute(
                            text(f"SELECT status, count(*) FROM jobs j WHERE {where} GROUP BY 1"),
                            params,
                        )
                    ).all()
                )
            done = sum(n for s, n in statuses.items() if s in TERMINAL)
            print(f"  t={time.monotonic() - start:5.1f}s {statuses} kills={kills}", flush=True)
            if done == args.jobs:
                break
            if time.monotonic() - start > args.timeout:
                print("TIMEOUT")
                break
            if args.kill_every and time.monotonic() - last_kill >= args.kill_every:
                if await asyncio.to_thread(kill_random_worker, args.workers):
                    kills += 1
                last_kill = time.monotonic()
            await asyncio.sleep(1)

        async with engine.connect() as conn:
            rows = (
                await conn.execute(
                    text(
                        f"""SELECT j.status, j.attempts, j.locked_by,
                                   (SELECT count(*) FROM effects e WHERE e.job_id = j.id)
                            FROM jobs j WHERE {where}"""
                    ),
                    params,
                )
            ).all()
    finally:
        await engine.dispose()

    statuses = Counter(r[0] for r in rows)
    attempts = Counter(r[1] for r in rows)
    effects = Counter(r[3] for r in rows)
    per_worker = Counter(r[2] for r in rows)
    lost = args.jobs - statuses["succeeded"]
    duplicated = sum(n for count, n in effects.items() if count > 1)
    missing = effects[0]

    print(f"\nelapsed:            {time.monotonic() - start:.1f}s, {kills} workers killed")
    print(f"statuses:           {dict(statuses)}")
    print(f"attempts per job:   {dict(sorted(attempts.items()))}")
    print(f"effects per job:    {dict(sorted(effects.items()))}")
    print(f"jobs per worker:    {len(per_worker)} workers, busiest ran {max(per_worker.values())}")
    print(f"lost: {lost}   duplicated effects: {duplicated}   missing effects: {missing}")
    ok = lost == 0 and duplicated == 0 and missing == 0
    print("PASS: every job succeeded with exactly one effect" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
