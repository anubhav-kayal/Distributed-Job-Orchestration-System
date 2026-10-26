from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from djos.api import dlq, health, jobs, schedules
from djos.core import db, redis
from djos.core.config import Settings, get_settings
from djos.queue import RedisQueue


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # Clients are created once per process and shared by all requests.
        app.state.settings = settings
        app.state.engine = db.create_engine(settings.database_url)
        app.state.sessionmaker = db.create_sessionmaker(app.state.engine)
        app.state.redis = redis.create_redis(
            settings.redis_url, socket_timeout=settings.redis_socket_timeout_seconds
        )
        app.state.queue = RedisQueue(app.state.redis)
        try:
            yield
        finally:
            await app.state.redis.aclose()
            await app.state.engine.dispose()

    app = FastAPI(title="Distributed Job Orchestration System", lifespan=lifespan)
    app.include_router(health.router)
    app.include_router(jobs.router)
    app.include_router(dlq.router)
    app.include_router(schedules.router)
    return app


app = create_app()
