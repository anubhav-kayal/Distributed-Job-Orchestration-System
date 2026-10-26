import uuid
from datetime import datetime, timedelta
from typing import Any

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    model_validator,
)

from djos.handlers import UnknownJobType, get_handler, registered_types
from djos.models import JobStatus, Priority

MAX_DELAY_SECONDS = 365 * 24 * 3600


class JobSpec(BaseModel):
    """What to run: shared by direct submissions and cron schedule templates."""

    model_config = ConfigDict(extra="forbid")

    type: str = Field(pattern=r"^[a-z][a-z0-9_.-]{0,99}$", examples=["sleep"])
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: Priority = Priority.NORMAL
    max_attempts: int = Field(default=3, ge=1, le=25)

    @model_validator(mode="after")
    def validate_payload_for_type(self) -> "JobSpec":
        try:
            handler = get_handler(self.type)
        except UnknownJobType:
            raise ValueError(
                f"unknown job type {self.type!r}; expected one of {registered_types()}"
            ) from None
        try:
            validated = handler.payload_model.model_validate(self.payload)
        except ValidationError as exc:
            problems = "; ".join(
                f"payload.{'.'.join(map(str, e['loc']))}: {e['msg']}" for e in exc.errors()
            )
            raise ValueError(f"invalid payload for {self.type!r}: {problems}") from None
        # Store the normalized dump: defaults are filled in and the worker sees exactly
        # what was validated.
        self.payload = validated.model_dump(mode="json")
        return self


class JobCreate(JobSpec):
    # Run later: an absolute time (must include a timezone) or a delay. At most one.
    run_at: AwareDatetime | None = None
    delay_seconds: float | None = Field(default=None, ge=0, le=MAX_DELAY_SECONDS)

    def resolve_run_at(self, now: datetime) -> datetime | None:
        """When the job should first run, or None for "as soon as possible"."""
        if self.delay_seconds is not None:
            return now + timedelta(seconds=self.delay_seconds)
        return self.run_at

    @model_validator(mode="after")
    def check_schedule(self) -> "JobCreate":
        if self.run_at is not None and self.delay_seconds is not None:
            raise ValueError("give either run_at or delay_seconds, not both")
        return self


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
    locked_by: str | None
    lease_token: int
    run_at: datetime | None
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
