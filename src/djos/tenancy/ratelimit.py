"""Per-tenant submission rate limiting with a token bucket in Redis.

A bucket holds up to `burst` tokens and refills at `rate` tokens per second. Each
submission spends tokens (one per job); with too few tokens the request is refused with
how long until enough have refilled. So a tenant can burst briefly, but its sustained
rate is capped at `rate`.

The whole check-refill-spend runs as one Lua script, so concurrent API replicas can't
both spend the last token, and it uses Redis's own clock (TIME), so replicas with
skewed clocks agree on how much has refilled.

If Redis is unreachable the limiter fails *open* (allows the request). That's a choice:
availability of job submission matters more than strict limiting during an outage, and
Postgres still protects correctness.
"""

import logging
import math
import uuid
from dataclasses import dataclass

from redis.asyncio import Redis
from redis.exceptions import RedisError

log = logging.getLogger(__name__)

# KEYS[1] = bucket; ARGV = rate (tokens/s), burst (capacity), cost
_TAKE = """
local rate, burst, cost = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
local state = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1]) or burst
local ts = tonumber(state[2]) or now
tokens = math.min(burst, tokens + math.max(0, now - ts) * rate)
local allowed, retry_after = 0, (cost - tokens) / rate
if tokens >= cost then
    tokens = tokens - cost
    allowed, retry_after = 1, 0
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], math.ceil(burst / rate * 1000) + 1000)
return {allowed, tostring(retry_after)}
"""


@dataclass(frozen=True)
class Decision:
    allowed: bool
    retry_after: float = 0.0  # seconds until enough tokens will have refilled


class RateLimiter:
    def __init__(self, redis: Redis):
        self.redis = redis
        self._take = redis.register_script(_TAKE)

    async def take(self, tenant_id: uuid.UUID, rate: float, burst: int, cost: int = 1) -> Decision:
        if cost > burst:
            return Decision(False, math.inf)  # can never fit, however long you wait
        try:
            allowed, retry_after = await self._take(
                keys=[f"rl:{tenant_id}"], args=[rate, burst, cost]
            )
        except RedisError as exc:
            log.warning("rate limiter unavailable (%s); allowing request", exc)
            return Decision(True)
        return Decision(bool(allowed), float(retry_after))
