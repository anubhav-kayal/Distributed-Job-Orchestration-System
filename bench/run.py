"""Benchmarks against the Docker Compose stack. Results go to docs/benchmarks/.

    uv run python -m bench.run [--backend redis|postgres] [--quick]

1. Throughput vs workers: with workers stopped, pre-load N jobs, then start W workers
   and time the drain (first claim → last finish). Pre-loading isolates worker
   throughput from how fast the API accepts jobs.
2. Latency under steady load: an open-loop submitter sends jobs at a fixed rate,
   *without waiting* for earlier ones, and we measure created → finished. A closed loop
   ("send, wait, send") slows down exactly when the system does, and so hides its worst
   latencies: the "coordinated omission" trap.
3. Recovery: SIGKILL the worker running a job; time until another worker runs it.
"""

import argparse
import asyncio
import json
import os
import statistics
import subprocess
import sys
import time
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from sqlalchemy import text

from djos.core import db
from djos.core.config import get_settings

OUT = Path(__file__).parent.parent / "docs" / "benchmarks"


def compose(*args: str) -> str:
    return subprocess.run(
        ["docker", "compose", *args], check=True, capture_output=True, text=True
    ).stdout


def submit(api: str, body: dict, key: str) -> None:
    req = urllib.request.Request(
        f"http://{api}/jobs",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "Idempotency-Key": key},
        method="POST",
    )
    urllib.request.urlopen(req, timeout=30).read()


def open_loop(api: str, body: dict, prefix: str, n: int, rate: float) -> None:
    """Send n jobs at `rate`/s on a fixed schedule, never waiting for a response before
    sending the next: the submitter doesn't slow down when the system does."""
    with ThreadPoolExecutor(64) as pool:
        start = time.monotonic()
        for i in range(n):
            delay = start + i / rate - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            pool.submit(submit, api, body, f"{prefix}{i}")


def compose_kill(container: str) -> None:
    subprocess.run(["docker", "kill", container], check=True, capture_output=True)


def percentile(values: list[float], p: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, round(p / 100 * (len(ordered) - 1)))]


class Bench:
    def __init__(self, api: str):
        self.api = api
        self.engine = db.create_engine(get_settings().database_url)

    async def rows(self, prefix: str) -> list:
        async with self.engine.connect() as conn:
            return (
                await conn.execute(
                    text(
                        "SELECT status, created_at, started_at, finished_at FROM jobs "
                        "WHERE idempotency_key LIKE :p"
                    ),
                    {"p": f"{prefix}%"},
                )
            ).all()

    async def wait_done(self, prefix: str, n: int, within: float = 600) -> list:
        deadline = time.monotonic() + within
        while time.monotonic() < deadline:
            rows = await self.rows(prefix)
            if len(rows) == n and all(r.status == "succeeded" for r in rows):
                return rows
            await asyncio.sleep(0.5)
        raise TimeoutError(f"{prefix}: not all {n} jobs succeeded")

    async def throughput(self, workers: int, jobs: int, seconds: float) -> dict:
        compose("up", "-d", "--no-deps", "--scale", "worker=0", "worker")
        prefix = f"bench-tp-{uuid.uuid4().hex[:6]}-"
        body = {"type": "sleep", "payload": {"seconds": seconds}}
        with ThreadPoolExecutor(32) as pool:
            list(pool.map(lambda i: submit(self.api, body, f"{prefix}{i}"), range(jobs)))
        compose("up", "-d", "--no-deps", "--scale", f"worker={workers}", "worker")
        rows = await self.wait_done(prefix, jobs)
        span = (max(r.finished_at for r in rows) - min(r.started_at for r in rows)).total_seconds()
        return {
            "workers": workers,
            "job_seconds": seconds,
            "jobs": jobs,
            "seconds": round(span, 2),
            "jobs_per_second": round(jobs / span, 1),
        }

    async def latency(self, workers: int, rate: float, duration: float, seconds: float) -> dict:
        compose("up", "-d", "--no-deps", "--wait", "--scale", f"worker={workers}", "worker")
        await asyncio.sleep(3)  # let freshly started workers connect before measuring
        prefix = f"bench-lat-{uuid.uuid4().hex[:6]}-"
        body = {"type": "sleep", "payload": {"seconds": seconds}}
        n = int(rate * duration)
        await asyncio.to_thread(open_loop, self.api, body, prefix, n, rate)
        rows = await self.wait_done(prefix, n)
        e2e = [(r.finished_at - r.created_at).total_seconds() * 1000 for r in rows]
        wait = [(r.started_at - r.created_at).total_seconds() * 1000 for r in rows]
        return {
            "workers": workers,
            "rate_per_second": rate,
            "jobs": n,
            "job_seconds": seconds,
            "end_to_end_ms": {p: round(percentile(e2e, p), 1) for p in (50, 95, 99)},
            "queue_wait_ms": {p: round(percentile(wait, p), 1) for p in (50, 95, 99)},
        }

    async def recovery(self, trials: int, lease_ttl: float) -> dict:
        """Kill the worker running a 3s job; time until another worker has started it."""
        compose("up", "-d", "--no-deps", "--wait", "--scale", "worker=3", "worker")
        samples = []
        for _ in range(trials):
            prefix = f"bench-rec-{uuid.uuid4().hex[:6]}-"
            submit(self.api, {"type": "sleep", "payload": {"seconds": 3}}, f"{prefix}0")
            locked_by = None
            for _ in range(100):
                async with self.engine.connect() as conn:
                    row = (
                        await conn.execute(
                            text("SELECT status, locked_by FROM jobs WHERE idempotency_key = :k"),
                            {"k": f"{prefix}0"},
                        )
                    ).one()
                if row.status == "running":
                    locked_by = row.locked_by
                    break
                await asyncio.sleep(0.05)
            killed_at = time.time()
            await asyncio.to_thread(compose_kill, locked_by.rsplit("-", 1)[0])
            while True:
                async with self.engine.connect() as conn:
                    row = (
                        await conn.execute(
                            text(
                                "SELECT status, locked_by, started_at FROM jobs "
                                "WHERE idempotency_key = :k"
                            ),
                            {"k": f"{prefix}0"},
                        )
                    ).one()
                if row.status in ("running", "succeeded") and row.locked_by != locked_by:
                    samples.append(row.started_at.timestamp() - killed_at)
                    break
                await asyncio.sleep(0.05)
            compose(
                "up", "-d", "--no-deps", "--no-recreate", "--wait", "--scale", "worker=3", "worker"
            )
        return {
            "lease_ttl_seconds": lease_ttl,
            "trials": trials,
            "kill_to_rerun_seconds": {
                "median": round(statistics.median(samples), 2),
                "max": round(max(samples), 2),
            },
        }


def plot(results: dict, path: Path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(7, 4))
    by_seconds: dict[float, list] = {}
    for r in results["throughput"]:
        by_seconds.setdefault(r["job_seconds"], []).append(r)
    for seconds, rs in by_seconds.items():
        xs = [r["workers"] for r in rs]
        ax.plot(
            xs,
            [r["jobs_per_second"] for r in rs],
            marker="o",
            label=f"{seconds * 1000:.0f} ms jobs",
        )
        if seconds:
            ax.plot(
                xs,
                [w / seconds for w in xs],
                linestyle=":",
                color="grey",
                label="ideal (no overhead)",
            )
    ax.set_xlabel("workers")
    ax.set_ylabel("jobs / second")
    ax.set_title(f"Throughput vs workers ({results['backend']} queue)")
    ax.set_xticks(sorted({r["workers"] for r in results["throughput"]}))
    ax.grid(alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(path, dpi=120)


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--backend", default="redis", choices=["redis", "postgres"])
    parser.add_argument("--quick", action="store_true", help="smaller runs")
    parser.add_argument("--api", default="localhost:58000")
    args = parser.parse_args()

    os.environ.update(
        {
            "DJOS_QUEUE_BACKEND": args.backend,
            "DJOS_LEASE_TTL_SECONDS": "4",
            "DJOS_HEARTBEAT_INTERVAL_SECONDS": "1",
            "DJOS_MONITOR_INTERVAL_SECONDS": "1",
        }
    )
    compose("up", "-d", "--build", "--wait", "--scale", "worker=1")
    bench = Bench(args.api)
    jobs = 300 if args.quick else 1000
    results: dict = {"backend": args.backend, "throughput": [], "latency": [], "recovery": None}
    try:
        # Unrecorded warm-up: the first run after a rebuild pays one-off costs (cold
        # database caches, connection pools, image start) that would skew it low.
        await bench.throughput(2, 200, 0.0)
        for seconds in (0.0, 0.05):
            for workers in (1, 2, 4, 8):
                r = await bench.throughput(workers, jobs, seconds)
                print(f"throughput  {r}", flush=True)
                results["throughput"].append(r)
        for rate in (20, 60):
            r = await bench.latency(4, rate, 15 if args.quick else 30, 0.05)
            print(f"latency     {r}", flush=True)
            results["latency"].append(r)
        results["recovery"] = await bench.recovery(3 if args.quick else 5, lease_ttl=4)
        print(f"recovery    {results['recovery']}", flush=True)
    finally:
        await bench.engine.dispose()
        for k in (
            "DJOS_QUEUE_BACKEND",
            "DJOS_LEASE_TTL_SECONDS",
            "DJOS_HEARTBEAT_INTERVAL_SECONDS",
            "DJOS_MONITOR_INTERVAL_SECONDS",
        ):
            os.environ.pop(k, None)
        compose("up", "-d", "--wait", "--scale", "worker=3")

    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / f"{args.backend}.json").write_text(json.dumps(results, indent=2) + "\n")
    plot(results, OUT / f"throughput-{args.backend}.png")
    print(f"wrote {OUT}/{args.backend}.json and throughput-{args.backend}.png")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
