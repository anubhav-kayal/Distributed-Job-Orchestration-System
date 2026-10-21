from redis.asyncio import Redis


def create_redis(redis_url: str, *, socket_timeout: float) -> Redis:
    # Both timeouts matter. Without a connect timeout an unreachable Redis hangs the
    # caller; without a read timeout a half-open connection (Redis gone, TCP still up)
    # hangs every call forever, so a heartbeat could never notice it lost its lease.
    # The read timeout must exceed the longest blocking command we issue (worker BLMPOP).
    return Redis.from_url(
        redis_url,
        decode_responses=True,
        socket_connect_timeout=2,
        socket_timeout=socket_timeout,
        health_check_interval=30,
    )


async def ping(client: Redis) -> None:
    await client.ping()
