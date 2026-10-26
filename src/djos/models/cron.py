import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, Index, String, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from djos.models.base import Base


class CronSchedule(Base):
    """A recurring job: every time `cron_expr` (UTC) fires, a job is created from `job`."""

    __tablename__ = "cron_schedules"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(100), unique=True)
    cron_expr: Mapped[str] = mapped_column(String(100))
    # A validated JobSpec (type, payload, priority, max_attempts) for every fired job.
    job: Mapped[dict[str, Any]] = mapped_column(JSONB)
    enabled: Mapped[bool] = mapped_column(server_default=text("true"))
    # The next fire time. The scheduler fires the schedule once this has passed and
    # advances it in the same transaction that creates the job.
    next_fire_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_fired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    __table_args__ = (
        # The scheduler's query: enabled schedules whose next fire time has passed.
        Index("ix_cron_schedules_enabled_next_fire_at", "enabled", "next_fire_at"),
    )
    __mapper_args__ = {"eager_defaults": True}
