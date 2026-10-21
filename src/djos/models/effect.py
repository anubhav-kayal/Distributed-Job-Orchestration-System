import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, DateTime, ForeignKey, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from djos.models.base import Base


class Effect(Base):
    """The externally visible outcome of a job (think "the email was sent"), written in
    the same transaction that marks the job succeeded.

    Deliberately no unique constraint on job_id: the chaos checker counts rows per job
    to prove fencing works. A constraint would hide a duplicate instead of exposing it.
    """

    __tablename__ = "effects"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"), index=True)
    lease_token: Mapped[int] = mapped_column(BigInteger)
    worker_id: Mapped[str] = mapped_column(String(255))
    data: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
