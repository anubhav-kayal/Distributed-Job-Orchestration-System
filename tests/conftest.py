import asyncio
from collections.abc import Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from sqlalchemy import make_url, text
from sqlalchemy.ext.asyncio import create_async_engine

from djos.api.main import create_app
from djos.core.config import Settings

ROOT = Path(__file__).resolve().parent.parent
TEST_DB_NAME = "djos_test"


async def _recreate_database(admin_url: str, name: str) -> None:
    engine = create_async_engine(admin_url, isolation_level="AUTOCOMMIT")
    async with engine.connect() as conn:
        await conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        await conn.execute(text(f'CREATE DATABASE "{name}"'))
    await engine.dispose()


async def _truncate_all(url: str) -> None:
    engine = create_async_engine(url)
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE jobs"))
    await engine.dispose()


@pytest.fixture(scope="session")
def test_settings() -> Settings:
    """Settings pointing at a dedicated, freshly migrated test database."""
    base = Settings()
    url = make_url(base.database_url)
    test_url = url.set(database=TEST_DB_NAME).render_as_string(hide_password=False)

    asyncio.run(_recreate_database(url.render_as_string(hide_password=False), TEST_DB_NAME))

    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(ROOT / "migrations"))
    cfg.set_main_option("sqlalchemy.url", test_url)
    command.upgrade(cfg, "head")

    return base.model_copy(update={"database_url": test_url})


@pytest.fixture
def client(test_settings: Settings) -> Iterator[TestClient]:
    asyncio.run(_truncate_all(test_settings.database_url))
    with TestClient(create_app(test_settings)) as c:
        yield c
