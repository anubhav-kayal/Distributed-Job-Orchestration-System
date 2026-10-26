"""Entry point: `python -m djos.scheduler`."""

import asyncio
import logging
import signal

from djos.core import db, redis
from djos.core.config import get_settings
from djos.queue import RedisQueue
from djos.scheduler import Scheduler


async def main() -> None:
    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    engine = db.create_engine(settings.database_url)
    redis_client = redis.create_redis(
        settings.redis_url, socket_timeout=settings.redis_socket_timeout_seconds
    )
    scheduler = Scheduler(
        db.create_sessionmaker(engine),
        RedisQueue(redis_client),
        interval_seconds=settings.scheduler_interval_seconds,
        batch_size=settings.scheduler_batch_size,
    )

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    try:
        await scheduler.run(stop)
    finally:
        await redis_client.aclose()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
