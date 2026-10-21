"""Leases: time-limited locks in Redis that say "this worker is running this job".

A lease is the key `lease:{job_id}` holding an owner string, with a TTL:

- acquire: `SET key owner NX PX ttl`. NX makes it succeed only if nobody holds the lease;
  PX makes it expire on its own, so a worker that dies can't hold it forever.
- renew (heartbeat) and release must only touch the lease if *we* still own it. Doing
  that as GET-then-PEXPIRE/DEL from the client is a race: between our GET and our write,
  the lease can expire and another worker can acquire it, and we'd extend or delete
  *their* lease. A Lua script runs atomically inside Redis, so check-and-write is one step.

A lease is advisory: it detects dead workers, but it can't stop a worker that is paused
(GC, swapped out, network partition) from carrying on after its lease expired. Fencing
tokens in Postgres handle that case.
"""

import uuid
from collections.abc import Sequence

from redis.asyncio import Redis

# KEYS[1] = lease key, ARGV[1] = owner, ARGV[2] = ttl in ms
_RENEW = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
"""

# KEYS[1] = lease key, ARGV[1] = owner
_RELEASE = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""


def lease_key(job_id: uuid.UUID) -> str:
    return f"lease:{job_id}"


class Leases:
    def __init__(self, redis: Redis, ttl_seconds: float):
        self.redis = redis
        self.ttl_ms = int(ttl_seconds * 1000)
        self._renew = redis.register_script(_RENEW)
        self._release = redis.register_script(_RELEASE)

    async def acquire(self, job_id: uuid.UUID, owner: str) -> bool:
        return bool(await self.redis.set(lease_key(job_id), owner, nx=True, px=self.ttl_ms))

    async def renew(self, job_id: uuid.UUID, owner: str) -> bool:
        """Extend the lease by a full TTL. False means we no longer own it."""
        return bool(await self._renew(keys=[lease_key(job_id)], args=[owner, self.ttl_ms]))

    async def release(self, job_id: uuid.UUID, owner: str) -> bool:
        return bool(await self._release(keys=[lease_key(job_id)], args=[owner]))

    async def holder(self, job_id: uuid.UUID) -> str | None:
        return await self.redis.get(lease_key(job_id))

    async def alive(self, job_ids: Sequence[uuid.UUID]) -> list[bool]:
        """For each job, whether any worker currently holds its lease."""
        if not job_ids:
            return []
        async with self.redis.pipeline(transaction=False) as pipe:
            for job_id in job_ids:
                pipe.exists(lease_key(job_id))
            return [bool(n) for n in await pipe.execute()]
