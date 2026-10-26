"""add scheduled status and run_at

Revision ID: fad54083674b
Revises: 966639b7104f
Create Date: 2026-10-26 21:04:54.659624

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "fad54083674b"
down_revision: str | Sequence[str] | None = "966639b7104f"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# Autogenerate can't see changes inside a CHECK constraint, so the status constraint is
# swapped by hand. It's addressed by name thanks to the naming convention in models/base.py.
OLD_STATUSES = ("queued", "running", "succeeded", "failed", "cancelled")
NEW_STATUSES = ("scheduled", *OLD_STATUSES)


def _status_check(statuses: tuple[str, ...]) -> str:
    return "status IN (" + ", ".join(f"'{s}'" for s in statuses) + ")"


def upgrade() -> None:
    op.add_column("jobs", sa.Column("run_at", sa.DateTime(timezone=True), nullable=True))
    op.create_index("ix_jobs_status_run_at", "jobs", ["status", "run_at"], unique=False)
    op.drop_constraint(op.f("ck_jobs_status"), "jobs", type_="check")
    op.create_check_constraint("status", "jobs", _status_check(NEW_STATUSES))


def downgrade() -> None:
    # Scheduled jobs have no equivalent in the old schema; make them runnable now.
    op.execute("UPDATE jobs SET status = 'queued' WHERE status = 'scheduled'")
    op.drop_constraint(op.f("ck_jobs_status"), "jobs", type_="check")
    op.create_check_constraint("status", "jobs", _status_check(OLD_STATUSES))
    op.drop_index("ix_jobs_status_run_at", table_name="jobs")
    op.drop_column("jobs", "run_at")
