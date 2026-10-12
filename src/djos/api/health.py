import asyncio

from fastapi import APIRouter, Request, Response, status

from djos.core import db, redis

router = APIRouter(tags=["health"])


@router.get("/healthz")
async def healthz() -> dict[str, str]:
    """Liveness: the process is up and serving requests. Never checks dependencies,
    so a Postgres outage doesn't make the orchestrator restart healthy API pods."""
    return {"status": "ok"}


@router.get("/readyz")
async def readyz(request: Request, response: Response) -> dict:
    """Readiness: can this instance do useful work right now? Checks every dependency."""
    state = request.app.state
    timeout = state.settings.dependency_timeout_seconds
    checks = {
        "postgres": db.ping(state.engine),
        "redis": redis.ping(state.redis),
    }

    results: dict[str, str] = {}
    for name, check in checks.items():
        try:
            await asyncio.wait_for(check, timeout=timeout)
            results[name] = "ok"
        except Exception as exc:  # any failure means "not ready"
            results[name] = f"error: {type(exc).__name__}"

    ready = all(v == "ok" for v in results.values())
    if not ready:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return {"status": "ready" if ready else "unavailable", "checks": results}
