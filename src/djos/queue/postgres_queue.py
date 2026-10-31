"""A queue backend that uses the jobs table itself as the queue.

The classic Postgres-as-a-queue pattern: a worker takes the oldest queued job with

    SELECT … WHERE status = 'queued' AND priority = :p
    ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED

SKIP LOCKED makes concurrent workers pass over rows another worker has just locked
instead of queueing behind them, so each takes a different job. Compared with Redis:

+ No dual write. The committed row *is* the queue entry, so there's nothing to push and
  nothing to lose between the commit and the push (enqueue is a no-op).
+ The pick and the claim happen in one transaction (see Worker._claim_from_postgres).
- No blocking pop. An idle worker must poll, which adds latency (up to the poll interval)
  and load on the database. LISTEN/NOTIFY could wake workers instead; polling keeps the
  comparison simple, and the benchmark shows what it costs.
- Every dequeue is a database round trip, and the database is already the system's
  bottleneck for claims and finishes.

A partial index on (priority, created_at) WHERE status = 'queued' keeps each pick
O(log n) however large the backlog is.
"""

import uuid

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from djos.models import Job, JobStatus, Priority

PRIORITY_ORDER = (Priority.HIGH, Priority.NORMAL, Priority.LOW)


class PostgresQueue:
    claims_in_postgres = True

    def __init__(self, sessionmaker: async_sessionmaker, poll_seconds: float = 0.05):
        self.sessionmaker = sessionmaker
        self.poll_seconds = poll_seconds

    async def enqueue(self, job_id: uuid.UUID, priority: Priority) -> None:
        """Nothing to do: a committed `queued` row is already in the queue."""

    async def contains(self, job_id: uuid.UUID, priority: Priority) -> bool:
        return True  # a queued row can't be "missing from the queue"

    async def depth(self) -> dict[Priority, int]:
        async with self.sessionmaker() as session:
            counts = dict(
                (
                    await session.execute(
                        select(Job.priority, func.count())
                        .where(Job.status == JobStatus.QUEUED)
                        .group_by(Job.priority)
                    )
                ).all()
            )
        return {p: counts.get(p, 0) for p in PRIORITY_ORDER}

    @staticmethod
    def order(prefer: Priority | None) -> tuple[Priority, ...]:
        if prefer is None:
            return PRIORITY_ORDER
        return (prefer, *(p for p in PRIORITY_ORDER if p is not prefer))

    @staticmethod
    def next_in_tier(priority: Priority, skip: set[uuid.UUID]):
        query = (
            select(Job)
            .where(Job.status == JobStatus.QUEUED, Job.priority == priority)
            .order_by(Job.created_at)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if skip:
            query = query.where(Job.id.not_in(skip))
        return query
