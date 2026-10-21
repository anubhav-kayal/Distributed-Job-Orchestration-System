"""Entry point: `python -m djos.worker`."""

import asyncio
import logging
import signal

from djos.coordination import Leases
from djos.core import db, redis
from djos.core.config import get_settings
from djos.queue import RedisQueue
from djos.worker import Worker


async def main() -> None:
    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    engine = db.create_engine(settings.database_url)
    redis_client = redis.create_redis(settings.redis_url)
    worker = Worker(
        db.create_sessionmaker(engine),
        RedisQueue(redis_client),
        Leases(redis_client, ttl_seconds=settings.lease_ttl_seconds),
        block_seconds=settings.worker_block_seconds,
        heartbeat_seconds=settings.heartbeat_interval_seconds,
    )

    # SIGTERM is what `docker stop` sends: finish the current job, then exit cleanly.
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    try:
        await worker.run(stop)
    finally:
        await redis_client.aclose()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
