"""Invariants checked against Postgres after a chaos run.

Safety:   every succeeded job has exactly one effect, carrying its final claim's fencing
          token, and nothing else has an effect at all.
Liveness: every job reached a final state.
Outcomes: only the intentionally broken jobs died; only their descendants were
          upstream_failed; everything else succeeded.
Ordering: no workflow job started before every dependency had succeeded.
"""

import uuid
from collections import Counter
from dataclasses import dataclass, field

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

TERMINAL = {"succeeded", "dead", "cancelled", "upstream_failed"}


@dataclass
class Report:
    jobs: int = 0
    statuses: Counter = field(default_factory=Counter)
    attempts: Counter = field(default_factory=Counter)
    violations: list[str] = field(default_factory=list)

    def fail(self, message: str) -> None:
        self.violations.append(message)


async def check(engine: AsyncEngine, tenant_id: uuid.UUID) -> Report:
    report = Report()
    async with engine.connect() as conn:
        jobs = (
            await conn.execute(
                text(
                    """
                    SELECT j.id, j.status, j.attempts, j.lease_token, j.type, j.payload,
                           j.started_at, j.finished_at,
                           count(e.id) AS effects,
                           min(e.lease_token) AS effect_token_min,
                           max(e.lease_token) AS effect_token_max
                    FROM jobs j LEFT JOIN effects e ON e.job_id = j.id
                    WHERE j.tenant_id = :tenant
                    GROUP BY j.id
                    """
                ),
                {"tenant": tenant_id},
            )
        ).all()
        edges = (
            await conn.execute(
                text(
                    """
                    SELECT c.id, c.status, c.started_at, p.id, p.status, p.finished_at
                    FROM job_dependencies d
                    JOIN jobs c ON c.id = d.job_id
                    JOIN jobs p ON p.id = d.depends_on_job_id
                    WHERE c.tenant_id = :tenant
                    """
                ),
                {"tenant": tenant_id},
            )
        ).all()

    report.jobs = len(jobs)
    for j in jobs:
        report.statuses[j.status] += 1
        report.attempts[j.attempts] += 1
        if j.status not in TERMINAL:
            report.fail(f"liveness: job {j.id} is still {j.status}")
        if j.status == "succeeded":
            if j.effects != 1:
                report.fail(f"safety: succeeded job {j.id} has {j.effects} effects")
            elif j.effect_token_min != j.lease_token:
                report.fail(
                    f"fencing: job {j.id} effect has token {j.effect_token_min}, "
                    f"final claim was {j.lease_token}"
                )
        elif j.effects:
            report.fail(f"safety: {j.status} job {j.id} has {j.effects} effects")

        broken = j.type == "fail"
        if broken and j.status != "dead":
            report.fail(f"outcome: broken job {j.id} ended {j.status}, expected dead")
        if not broken and j.status == "dead":
            report.fail(f"outcome: healthy job {j.id} died (attempts={j.attempts})")

    for child, child_status, child_started, parent, parent_status, parent_finished in edges:
        if child_started is not None:
            if parent_status != "succeeded":
                report.fail(
                    f"order: job {child} ran although dependency {parent} is {parent_status}"
                )
            elif child_started < parent_finished:
                report.fail(f"order: job {child} started before dependency {parent} finished")
        if child_status == "upstream_failed" and parent_status == "succeeded":
            # Fine only if *another* dependency failed.
            siblings = [e for e in edges if e[0] == child and e[4] != "succeeded"]
            if not siblings:
                report.fail(f"outcome: job {child} upstream_failed but all dependencies succeeded")
    return report
