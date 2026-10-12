from redis.asyncio import Redis


def create_redis(redis_url: str) -> Redis:
    return Redis.from_url(redis_url, decode_responses=True)


async def ping(client: Redis) -> None:
    await client.ping()
