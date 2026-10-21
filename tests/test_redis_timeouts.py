import asyncio
import time

import pytest
from redis.exceptions import TimeoutError as RedisTimeoutError

from djos.core.config import Settings
from djos.core.redis import create_redis


async def test_a_redis_that_never_answers_times_out_instead_of_hanging():
    # A "black hole": accepts the TCP connection, never replies. This is what a
    # half-open connection looks like after a partition.
    async def swallow(reader, writer):
        await reader.read()  # read until the client gives up

    server = await asyncio.start_server(swallow, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    client = create_redis(f"redis://127.0.0.1:{port}/0", socket_timeout=0.3)
    try:
        start = time.monotonic()
        with pytest.raises(RedisTimeoutError):
            await client.ping()
        assert time.monotonic() - start < 2
    finally:
        await client.aclose()
        server.close()


@pytest.mark.parametrize(
    "overrides",
    [
        {"worker_block_seconds": 5, "redis_socket_timeout_seconds": 5},
        {"lease_ttl_seconds": 10, "heartbeat_interval_seconds": 6},
    ],
)
def test_inconsistent_timings_are_rejected(overrides):
    with pytest.raises(ValueError):
        Settings(**overrides)
