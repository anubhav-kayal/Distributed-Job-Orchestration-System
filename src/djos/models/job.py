import uuid
from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Enum,
    Index,
    String,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from djos.models.base import Base


class JobStatus(StrEnum):
    SCHEDULED = "scheduled"  # waiting until run_at (a retry backoff, or submitted for later)
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    DEAD = "dead"  # out of attempts, or failed permanently: parked in the dead-letter queue
    CANCELLED = "cancelled"


class Priority(StrEnum):
    HIGH = "high"
    NORMAL = "normal"
    LOW = "low"


def _string_enum(enum_cls: type[StrEnum], length: int) -> Enum:
    # Stored as plain VARCHAR; the allowed values are enforced by the named CHECK
    # constraints below, which are easy to replace in a migration when values are added.
    return Enum(
        enum_cls,
        native_enum=False,
        create_constraint=False,
        length=length,
        values_callable=lambda e: [m.value for m in e],
    )


def _in_check(column: str, enum_cls: type[StrEnum]) -> str:
    values = ", ".join(f"'{m.value}'" for m in enum_cls)
    return f"{column} IN ({values})"


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    type: Mapped[str] = mapped_column(String(100))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, server_default=text("'{}'::jsonb"))
    priority: Mapped[Priority] = mapped_column(
        _string_enum(Priority, 10), server_default=Priority.NORMAL.value
    )
    status: Mapped[JobStatus] = mapped_column(
        _string_enum(JobStatus, 20), server_default=JobStatus.QUEUED.value
    )

    # When a `scheduled` job becomes runnable; the scheduler promotes it to `queued` then.
    run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    attempts: Mapped[int] = mapped_column(server_default=text("0"))
    max_attempts: Mapped[int] = mapped_column(server_default=text("3"))

    # Clients send an Idempotency-Key header; request_hash detects a key reused with a
    # different request body.
    idempotency_key: Mapped[str | None] = mapped_column(String(255), unique=True)
    request_hash: Mapped[str | None] = mapped_column(String(64))

    # Worker that most recently claimed the job (kept after it finishes, for debugging).
    locked_by: Mapped[str | None] = mapped_column(String(255))
    # Fencing token: incremented on every claim and never reset. A worker remembers the
    # token it claimed with, and its writes only apply while the job still carries it.
    lease_token: Mapped[int] = mapped_column(BigInteger, server_default=text("0"))

    result: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    last_error: Mapped[str | None] = mapped_column(Text)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        CheckConstraint(_in_check("status", JobStatus), name="status"),
        CheckConstraint(_in_check("priority", Priority), name="priority"),
        CheckConstraint("max_attempts >= 1", name="max_attempts_positive"),
        CheckConstraint("attempts >= 0", name="attempts_non_negative"),
        Index("ix_jobs_status_created_at", "status", "created_at"),
        # The scheduler's query: scheduled jobs whose run_at has passed.
        Index("ix_jobs_status_run_at", "status", "run_at"),
    )
    # Fetch DB-generated values (updated_at = now()) via RETURNING on INSERT/UPDATE.
    # Without this they'd be lazily reloaded on access, which async sessions can't do.
    __mapper_args__ = {"eager_defaults": True}
