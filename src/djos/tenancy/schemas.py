import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, model_validator


class TenantLimits(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rate_limit_per_second: float | None = Field(default=None, gt=0, le=100_000)
    burst: int | None = Field(default=None, ge=1, le=1_000_000)
    max_concurrency: int | None = Field(default=None, ge=1, le=100_000)

    @model_validator(mode="after")
    def rate_and_burst_together(self) -> "TenantLimits":
        if (self.rate_limit_per_second is None) != (self.burst is None):
            raise ValueError("set rate_limit_per_second and burst together (or neither)")
        return self


class TenantCreate(TenantLimits):
    name: str = Field(pattern=r"^[a-z0-9][a-z0-9_.-]{0,99}$", examples=["acme"])


class TenantOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    api_key_prefix: str | None
    rate_limit_per_second: float | None
    burst: int | None
    max_concurrency: int | None
    created_at: datetime


class TenantWithKey(TenantOut):
    api_key: str = Field(description="Shown once. Only its hash is stored.")


class TenantList(BaseModel):
    items: list[TenantOut]
