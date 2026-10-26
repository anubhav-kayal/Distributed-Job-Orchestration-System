from datetime import datetime

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine


def create_engine(database_url: str) -> AsyncEngine:
    # pool_pre_ping drops dead connections (e.g. after a Postgres restart) instead of
    # handing them to a request.
    return create_async_engine(database_url, pool_pre_ping=True)


def create_sessionmaker(engine: AsyncEngine) -> async_sessionmaker:
    return async_sessionmaker(engine, expire_on_commit=False)


async def ping(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))


async def db_now(session) -> datetime:
    """The database's clock. Processes on different machines disagree about the time;
    using Postgres's `now()` as the one clock means every scheduler replica agrees on
    what is due."""
    return await session.scalar(select(func.now()))
