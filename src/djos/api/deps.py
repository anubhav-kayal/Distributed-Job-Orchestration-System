from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from djos.queue import RedisQueue


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    async with request.app.state.sessionmaker() as session:
        yield session


SessionDep = Annotated[AsyncSession, Depends(get_session)]


def get_queue(request: Request) -> RedisQueue:
    return request.app.state.queue


QueueDep = Annotated[RedisQueue, Depends(get_queue)]
