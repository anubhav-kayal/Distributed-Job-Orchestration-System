from redis.asyncio import Redis


def create_redis(redis_url: str) -> Redis:
    # Fail fast when Redis is unreachable rather than hanging the caller. There's no
    # read timeout because workers deliberately block on BLMPOP.
    return Redis.from_url(
        redis_url, decode_responses=True, socket_connect_timeout=2, health_check_interval=30
    )


async def ping(client: Redis) -> None:
    await client.ping()
