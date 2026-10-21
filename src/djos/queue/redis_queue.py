"""Priority queues of job IDs in Redis lists.

One list per priority tier. Producers LPUSH onto the left; consumers pop from the right,
so each tier is FIFO. BLMPOP checks the keys in the order given and pops from the first
non-empty one, which gives strict priority: `high` before `normal` before `low`.

Redis only carries job IDs, never job state. Postgres decides whether a popped ID is still
runnable, so a duplicate or stale ID in a queue is harmless (the worker skips it).
"""

import uuid

from redis.asyncio import Redis

from djos.models import Priority

# Highest priority first: this order is what BLMPOP uses.
PRIORITY_ORDER = (Priority.HIGH, Priority.NORMAL, Priority.LOW)


def queue_key(priority: Priority) -> str:
    return f"q:{priority.value}"


QUEUE_KEYS = tuple(queue_key(p) for p in PRIORITY_ORDER)


class RedisQueue:
    def __init__(self, redis: Redis):
        self.redis = redis

    async def enqueue(self, job_id: uuid.UUID, priority: Priority) -> None:
        await self.redis.lpush(queue_key(priority), str(job_id))

    async def dequeue(self, block_seconds: float) -> uuid.UUID | None:
        """Block up to `block_seconds` (server-side) for the next job ID; None if none arrives."""
        popped = await self.redis.blmpop(
            block_seconds, len(QUEUE_KEYS), *QUEUE_KEYS, direction="RIGHT"
        )
        if popped is None:
            return None
        _key, [job_id] = popped
        return uuid.UUID(job_id)

    async def contains(self, job_id: uuid.UUID, priority: Priority) -> bool:
        """Whether the ID is waiting in its queue. LPOS is a linear scan of the list,
        so callers should only check a bounded number of IDs."""
        return await self.redis.lpos(queue_key(priority), str(job_id)) is not None

    async def depth(self) -> dict[Priority, int]:
        async with self.redis.pipeline(transaction=False) as pipe:
            for key in QUEUE_KEYS:
                pipe.llen(key)
            lengths = await pipe.execute()
        return dict(zip(PRIORITY_ORDER, lengths, strict=True))
