import secrets
from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, Header, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from djos.models import DEFAULT_TENANT_ID, Tenant
from djos.queue import RedisQueue
from djos.tenancy.keys import hash_api_key
from djos.tenancy.ratelimit import RateLimiter


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    async with request.app.state.sessionmaker() as session:
        yield session


SessionDep = Annotated[AsyncSession, Depends(get_session)]


def get_queue(request: Request) -> RedisQueue:
    return request.app.state.queue


QueueDep = Annotated[RedisQueue, Depends(get_queue)]


def _bearer(authorization: str | None) -> str | None:
    if authorization is None:
        return None
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            "expected 'Authorization: Bearer <api key>'",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return token


async def get_tenant(
    request: Request,
    session: SessionDep,
    authorization: Annotated[str | None, Header()] = None,
) -> Tenant:
    """The tenant making the request, identified by its API key.

    A wrong key is always a 401. No key at all means the default tenant, unless the
    deployment requires keys.
    """
    key = _bearer(authorization)
    if key is None:
        if request.app.state.settings.require_api_key:
            raise HTTPException(
                status.HTTP_401_UNAUTHORIZED,
                "an API key is required",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return await session.get(Tenant, DEFAULT_TENANT_ID)
    tenant = (
        await session.scalars(select(Tenant).where(Tenant.api_key_hash == hash_api_key(key)))
    ).first()
    if tenant is None:
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED, "invalid API key", headers={"WWW-Authenticate": "Bearer"}
        )
    return tenant


TenantDep = Annotated[Tenant, Depends(get_tenant)]


async def enforce_rate_limit(request: Request, tenant: Tenant, cost: int = 1) -> None:
    """Spend `cost` tokens from the tenant's bucket, or refuse with 429 + Retry-After."""
    if tenant.rate_limit_per_second is None or tenant.burst is None:
        return
    limiter: RateLimiter = request.app.state.limiter
    decision = await limiter.take(tenant.id, tenant.rate_limit_per_second, tenant.burst, cost)
    if decision.allowed:
        return
    if decision.retry_after == float("inf"):
        raise HTTPException(
            status.HTTP_429_TOO_MANY_REQUESTS,
            f"this request needs {cost} submissions but the tenant's burst limit is {tenant.burst}",
        )
    raise HTTPException(
        status.HTTP_429_TOO_MANY_REQUESTS,
        f"rate limit of {tenant.rate_limit_per_second}/s exceeded",
        headers={"Retry-After": str(max(1, int(decision.retry_after + 0.999)))},
    )


def require_admin(request: Request, authorization: Annotated[str | None, Header()] = None) -> None:
    configured = request.app.state.settings.admin_token
    if configured is None:
        # The admin API doesn't exist unless an admin token is configured.
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Not Found")
    token = _bearer(authorization)
    if not token or not secrets.compare_digest(token, configured.get_secret_value()):
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            "invalid admin token",
            headers={"WWW-Authenticate": "Bearer"},
        )
