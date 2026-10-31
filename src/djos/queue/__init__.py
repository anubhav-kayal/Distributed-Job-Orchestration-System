from djos.queue.fairness import DEFAULT_WEIGHTS, WeightedRotation
from djos.queue.postgres_queue import PostgresQueue
from djos.queue.redis_queue import RedisQueue

Queue = RedisQueue | PostgresQueue


def make_queue(settings, redis, sessionmaker) -> Queue:
    """The configured queue backend (DJOS_QUEUE_BACKEND=redis|postgres)."""
    if settings.queue_backend == "postgres":
        return PostgresQueue(sessionmaker, poll_seconds=settings.postgres_poll_seconds)
    return RedisQueue(redis)


__all__ = [
    "DEFAULT_WEIGHTS",
    "PostgresQueue",
    "Queue",
    "RedisQueue",
    "WeightedRotation",
    "make_queue",
]
