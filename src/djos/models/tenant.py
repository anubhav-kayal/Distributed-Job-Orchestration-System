import uuid
from datetime import datetime

from sqlalchemy import DateTime, String, func
from sqlalchemy.orm import Mapped, mapped_column

from djos.models.base import Base

# Requests without an API key (when keys aren't required) belong to this tenant. It's a
# real row, so every job, workflow and schedule has a non-null tenant_id.
DEFAULT_TENANT_ID = uuid.UUID(int=0)


class Tenant(Base):
    __tablename__ = "tenants"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(100), unique=True)
    # SHA-256 of the API key. Keys are long random strings, so a fast hash is enough:
    # slow hashes (bcrypt) exist to protect low-entropy human passwords.
    api_key_hash: Mapped[str | None] = mapped_column(String(64), unique=True)
    api_key_prefix: Mapped[str | None] = mapped_column(String(16))  # for identifying keys
    # Submission rate limit (token bucket): sustained rate and burst size. None = unlimited.
    rate_limit_per_second: Mapped[float | None]
    burst: Mapped[int | None]
    # Max jobs running at once across all workers. None = unlimited.
    max_concurrency: Mapped[int | None]
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __mapper_args__ = {"eager_defaults": True}
