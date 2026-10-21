"""Entry point: `python -m djos.monitor`."""

import asyncio
import logging
import signal

from djos.coordination import Leases
from djos.core import db, redis
from djos.core.config import get_settings
from djos.monitor import Monitor
from djos.queue import RedisQueue


async def main() -> None:
    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    engine = db.create_engine(settings.database_url)
    redis_client = redis.create_redis(settings.redis_url)
    monitor = Monitor(
        db.create_sessionmaker(engine),
        RedisQueue(redis_client),
        Leases(redis_client, ttl_seconds=settings.lease_ttl_seconds),
        interval_seconds=settings.monitor_interval_seconds,
        stranded_after_seconds=settings.stranded_after_seconds,
        batch_size=settings.monitor_batch_size,
    )

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    try:
        await monitor.run(stop)
    finally:
        await redis_client.aclose()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
