import asyncio
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
import redis as sync_redis
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from redis.asyncio import Redis
from sqlalchemy import make_url, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from djos.api.main import create_app
from djos.core import db
from djos.core.config import Settings
from djos.queue import RedisQueue

ROOT = Path(__file__).resolve().parent.parent
TEST_DB_NAME = "djos_test"
# Tests use their own Redis logical database so flushing it never touches dev data (db 0).
TEST_REDIS_DB = 15


async def _recreate_database(admin_url: str, name: str) -> None:
    engine = create_async_engine(admin_url, isolation_level="AUTOCOMMIT")
    async with engine.connect() as conn:
        await conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        await conn.execute(text(f'CREATE DATABASE "{name}"'))
    await engine.dispose()


async def _truncate_all(url: str) -> None:
    engine = create_async_engine(url)
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE jobs, cron_schedules CASCADE"))
    await engine.dispose()


@pytest.fixture(scope="session")
def test_settings() -> Settings:
    """Settings pointing at a dedicated, freshly migrated test database and Redis db."""
    base = Settings()
    url = make_url(base.database_url)
    test_url = url.set(database=TEST_DB_NAME).render_as_string(hide_password=False)
    redis_url = base.redis_url.rsplit("/", 1)[0] + f"/{TEST_REDIS_DB}"

    asyncio.run(_recreate_database(url.render_as_string(hide_password=False), TEST_DB_NAME))

    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(ROOT / "migrations"))
    cfg.set_main_option("sqlalchemy.url", test_url)
    command.upgrade(cfg, "head")

    return base.model_copy(update={"database_url": test_url, "redis_url": redis_url})


@pytest.fixture
def clean_state(test_settings: Settings) -> None:
    asyncio.run(_truncate_all(test_settings.database_url))
    with sync_redis.Redis.from_url(test_settings.redis_url) as r:
        r.flushdb()


@pytest.fixture
def client(test_settings: Settings, clean_state: None) -> Iterator[TestClient]:
    with TestClient(create_app(test_settings)) as c:
        yield c


@pytest.fixture
def redis_sync(test_settings: Settings, clean_state: None) -> Iterator[sync_redis.Redis]:
    with sync_redis.Redis.from_url(test_settings.redis_url, decode_responses=True) as r:
        yield r


@pytest.fixture
async def redis(test_settings: Settings, clean_state: None) -> AsyncIterator[Redis]:
    client = Redis.from_url(test_settings.redis_url, decode_responses=True)
    yield client
    await client.aclose()


@pytest.fixture
def queue(redis: Redis) -> RedisQueue:
    return RedisQueue(redis)


@pytest.fixture
async def sessionmaker(
    test_settings: Settings, clean_state: None
) -> AsyncIterator[async_sessionmaker]:
    engine = db.create_engine(test_settings.database_url)
    yield db.create_sessionmaker(engine)
    await engine.dispose()
