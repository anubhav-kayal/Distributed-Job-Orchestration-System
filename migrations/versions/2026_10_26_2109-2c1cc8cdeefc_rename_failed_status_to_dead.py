"""rename failed status to dead

Revision ID: 2c1cc8cdeefc
Revises: fad54083674b
Create Date: 2026-10-26 21:09:37.307640

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2c1cc8cdeefc"
down_revision: str | Sequence[str] | None = "fad54083674b"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# A data migration as well as a schema one: existing `failed` rows become `dead`. The
# constraint is dropped first, rows converted, then the new constraint added, so neither
# the old nor the new constraint ever sees a row it would reject.
BEFORE = ("scheduled", "queued", "running", "succeeded", "failed", "cancelled")
AFTER = ("scheduled", "queued", "running", "succeeded", "dead", "cancelled")


def _status_check(statuses: tuple[str, ...]) -> str:
    return "status IN (" + ", ".join(f"'{s}'" for s in statuses) + ")"


def _swap(old_value: str, new_value: str, statuses: tuple[str, ...]) -> None:
    op.drop_constraint(op.f("ck_jobs_status"), "jobs", type_="check")
    op.execute(f"UPDATE jobs SET status = '{new_value}' WHERE status = '{old_value}'")
    op.create_check_constraint("status", "jobs", _status_check(statuses))


def upgrade() -> None:
    _swap("failed", "dead", AFTER)


def downgrade() -> None:
    _swap("dead", "failed", BEFORE)
