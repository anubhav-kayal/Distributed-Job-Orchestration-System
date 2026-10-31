from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from djos.api import admin, dlq, health, jobs, observability, schedules, workflows
from djos.core import db, redis
from djos.core.config import Settings, get_settings
from djos.core.logging import setup_logging
from djos.queue import RedisQueue
from djos.tenancy.ratelimit import RateLimiter


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # Clients are created once per process and shared by all requests.
        setup_logging(settings.log_level, settings.log_format)
        app.state.settings = settings
        app.state.engine = db.create_engine(settings.database_url)
        app.state.sessionmaker = db.create_sessionmaker(app.state.engine)
        app.state.redis = redis.create_redis(
            settings.redis_url, socket_timeout=settings.redis_socket_timeout_seconds
        )
        app.state.queue = RedisQueue(app.state.redis)
        app.state.limiter = RateLimiter(app.state.redis)
        try:
            yield
        finally:
            await app.state.redis.aclose()
            await app.state.engine.dispose()

    app = FastAPI(title="Distributed Job Orchestration System", lifespan=lifespan)
    observability.instrument(app)
    app.include_router(health.router)
    app.include_router(observability.router)
    app.include_router(jobs.router)
    app.include_router(dlq.router)
    app.include_router(schedules.router)
    app.include_router(workflows.router)
    app.include_router(admin.router)
    return app


app = create_app()
