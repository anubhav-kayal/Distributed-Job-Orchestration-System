import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from djos.models import JobStatus, Priority


class JobCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: str = Field(pattern=r"^[a-z][a-z0-9_.-]{0,99}$", examples=["sleep"])
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: Priority = Priority.NORMAL
    max_attempts: int = Field(default=3, ge=1, le=25)


class JobOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    type: str
    payload: dict[str, Any]
    priority: Priority
    status: JobStatus
    attempts: int
    max_attempts: int
    idempotency_key: str | None
    result: dict[str, Any] | None
    last_error: str | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


class JobList(BaseModel):
    items: list[JobOut]
    limit: int
    offset: int
