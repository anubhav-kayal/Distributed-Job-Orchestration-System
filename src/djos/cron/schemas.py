import uuid
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from djos.cron.expr import next_fire_after
from djos.jobs.schemas import JobSpec


def _check_cron(value: str) -> str:
    next_fire_after(value, datetime.now(UTC))  # raises InvalidCron (a ValueError) → 422
    return value


class ScheduleCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(pattern=r"^[a-z0-9][a-z0-9_.-]{0,99}$", examples=["nightly-report"])
    cron: str = Field(max_length=100, examples=["0 2 * * *"], description="5-field cron, UTC")
    job: JobSpec
    enabled: bool = True

    _valid_cron = field_validator("cron")(_check_cron)


class ScheduleUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cron: str | None = Field(default=None, max_length=100)
    job: JobSpec | None = None
    enabled: bool | None = None

    @field_validator("cron")
    @classmethod
    def _valid_cron(cls, value: str | None) -> str | None:
        return None if value is None else _check_cron(value)


class ScheduleOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    cron: str = Field(validation_alias="cron_expr")
    job: dict[str, Any]
    enabled: bool
    next_fire_at: datetime
    last_fired_at: datetime | None
    created_at: datetime
    updated_at: datetime


class ScheduleList(BaseModel):
    items: list[ScheduleOut]
