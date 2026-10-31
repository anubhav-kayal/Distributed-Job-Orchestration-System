"""Entry point: `python -m djos.scheduler`."""

import asyncio
import signal

from djos.core import db, metrics, redis
from djos.core.config import get_settings
from djos.core.logging import setup_logging
from djos.queue import RedisQueue
from djos.scheduler import Scheduler


async def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level, settings.log_format)
    metrics.serve(settings.metrics_port)

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
