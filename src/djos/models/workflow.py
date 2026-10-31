import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Index, String, func
from sqlalchemy.orm import Mapped, mapped_column

from djos.models.base import Base


class Workflow(Base):
    """A DAG of jobs. Its status isn't stored: it's derived from its jobs when read, so it
    can never drift from them and finishing a job doesn't need an extra write."""

    __tablename__ = "workflows"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    name: Mapped[str | None] = mapped_column(String(200))
    idempotency_key: Mapped[str | None] = mapped_column(String(255), unique=True)
    request_hash: Mapped[str | None] = mapped_column(String(64))
    # Set when an operator cancels the whole workflow.
    cancelled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __mapper_args__ = {"eager_defaults": True}


class JobDependency(Base):
    """Edge: `job_id` can't start until `depends_on_job_id` has succeeded."""

    __tablename__ = "job_dependencies"

    job_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("jobs.id", ondelete="CASCADE"), primary_key=True
    )
    depends_on_job_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("jobs.id", ondelete="CASCADE"), primary_key=True
    )

    __table_args__ = (
        # "Who is waiting on this job?": the lookup made every time a job finishes.
        Index("ix_job_dependencies_depends_on_job_id", "depends_on_job_id"),
    )
