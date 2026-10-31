import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from djos.api.deps import SessionDep, require_admin
from djos.models import Tenant
from djos.tenancy.keys import generate_api_key, hash_api_key
from djos.tenancy.schemas import TenantCreate, TenantLimits, TenantList, TenantOut, TenantWithKey

router = APIRouter(prefix="/admin", tags=["admin"], dependencies=[Depends(require_admin)])


def _with_key(tenant: Tenant, key: str) -> TenantWithKey:
    return TenantWithKey(**TenantOut.model_validate(tenant).model_dump(), api_key=key)


def _set_key(tenant: Tenant) -> str:
    key = generate_api_key()
    tenant.api_key_hash = hash_api_key(key)
    tenant.api_key_prefix = key[:12]
    return key


@router.post("/tenants", response_model=TenantWithKey, status_code=status.HTTP_201_CREATED)
async def create_tenant(body: TenantCreate, session: SessionDep) -> TenantWithKey:
    """Create a tenant and its API key. The key is returned once and never again."""
    tenant = Tenant(**body.model_dump())
    key = _set_key(tenant)
    session.add(tenant)
    try:
        await session.commit()
    except IntegrityError:
        raise HTTPException(status.HTTP_409_CONFLICT, f"tenant {body.name!r} exists") from None
    return _with_key(tenant, key)


@router.get("/tenants", response_model=TenantList)
async def list_tenants(session: SessionDep) -> TenantList:
    tenants = await session.scalars(select(Tenant).order_by(Tenant.name))
    return TenantList(items=[TenantOut.model_validate(t) for t in tenants])


async def _get(session, tenant_id: uuid.UUID) -> Tenant:
    tenant = await session.get(Tenant, tenant_id)
    if tenant is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "tenant not found")
    return tenant


@router.put("/tenants/{tenant_id}/limits", response_model=TenantOut)
async def set_limits(tenant_id: uuid.UUID, body: TenantLimits, session: SessionDep) -> TenantOut:
    """Replace the tenant's limits; omitted limits become unlimited."""
    tenant = await _get(session, tenant_id)
    for field, value in body.model_dump().items():
        setattr(tenant, field, value)
    await session.commit()
    return TenantOut.model_validate(tenant)


@router.post("/tenants/{tenant_id}/rotate-key", response_model=TenantWithKey)
async def rotate_key(tenant_id: uuid.UUID, session: SessionDep) -> TenantWithKey:
    """Issue a new API key; the old one stops working immediately."""
    tenant = await _get(session, tenant_id)
    key = _set_key(tenant)
    await session.commit()
    return _with_key(tenant, key)
