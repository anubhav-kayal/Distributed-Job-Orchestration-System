"""Autoscaler: `python -m djos.autoscaler [--min 2 --max 8 --target 20]`, run on the host.

Every few seconds: measure the backlog (job IDs waiting in Redis + jobs running in
Postgres), ask the policy for a worker count, and scale the Compose `worker` service.
Scaling down stops workers with SIGTERM; each finishes its current job before exiting
(stop_grace_period gives it 30s), so scaling never loses or interrupts work.

On Kubernetes the same policy maps onto a Deployment's replica count, which is what
KEDA does with a Redis list-length trigger, with HPA behaviour (a scale-down
stabilization window) for the hysteresis.
"""

import argparse
import asyncio
import logging
import subprocess
import time

from sqlalchemy import func, select

from djos.autoscaler import ScalingPolicy
from djos.core import db, redis
from djos.core.config import get_settings
from djos.core.logging import setup_logging
from djos.models import Job, JobStatus
from djos.queue import make_queue

log = logging.getLogger("djos.autoscaler")


def compose(*args: str) -> str:
    return subprocess.run(
        ["docker", "compose", *args], check=True, capture_output=True, text=True
    ).stdout


class ComposeScaler:
    def current(self) -> int:
        return len(compose("ps", "-q", "--status", "running", "worker").split())

    def scale(self, n: int) -> None:
        # --no-recreate leaves running workers alone; --no-deps leaves other services alone.
        compose("up", "-d", "--no-recreate", "--no-deps", "--scale", f"worker={n}", "worker")


async def main() -> None:
    parser = argparse.ArgumentParser(description="Scale workers to the job backlog")
    parser.add_argument("--min", type=int, default=2)
    parser.add_argument("--max", type=int, default=8)
    parser.add_argument("--target", type=int, default=20, help="backlog per worker")
    parser.add_argument("--window", type=float, default=30, help="scale-down window (s)")
    parser.add_argument("--interval", type=float, default=3, help="seconds between checks")
    args = parser.parse_args()

    settings = get_settings()
    setup_logging(settings.log_level, "text")
    engine = db.create_engine(settings.database_url)
    redis_client = redis.create_redis(
        settings.redis_url, socket_timeout=settings.redis_socket_timeout_seconds
    )
    sessionmaker = db.create_sessionmaker(engine)
    queue = make_queue(settings, redis_client, sessionmaker)
    policy = ScalingPolicy(args.min, args.max, args.target, args.window)
    scaler = ComposeScaler()
    log.info("autoscaler started: %s", policy)

    try:
        while True:
            try:
                waiting = sum((await queue.depth()).values())
                async with sessionmaker() as session:
                    running = await session.scalar(
                        select(func.count()).where(Job.status == JobStatus.RUNNING)
                    )
                backlog = waiting + running
                current = await asyncio.to_thread(scaler.current)
                desired = policy.desired(current, backlog, time.monotonic())
                if desired != current:
                    log.info(
                        "backlog %d (waiting %d + running %d): scaling workers %d → %d",
                        backlog,
                        waiting,
                        running,
                        current,
                        desired,
                    )
                    await asyncio.to_thread(scaler.scale, desired)
            except Exception:
                log.exception("autoscaler check failed; will retry")
            await asyncio.sleep(args.interval)
    finally:
        await redis_client.aclose()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
