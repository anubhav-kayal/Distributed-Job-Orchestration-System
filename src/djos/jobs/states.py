"""The job lifecycle as an explicit state machine.

Every status change in the system goes through `transition()`, so the allowed lifecycle
lives in exactly one place:

    scheduled ──► queued ──► running ──► succeeded
        │  ▲          │ ▲        │  └────► dead        (out of attempts / permanent error)
        │  │          │ │        ├───────► queued      (lease expired: reclaimed)
        │  └──────────┼─┼────────┘                     (failed, attempts left: retry later)
        │             │ └─────────────── dead          (manual retry from the DLQ)
        └─────────────┴──────────────────► cancelled

`dead` is final for the system: only an operator's DLQ retry moves a job out of it.

Workflow jobs start `blocked` until every dependency has succeeded, then become `queued`.
If a dependency dies or is cancelled they become `upstream_failed` without running, and
go back to `blocked` if that dependency is retried from the DLQ:

    blocked ──► queued ──► …
       │ ▲
       ▼ │
    upstream_failed
"""

from datetime import UTC, datetime

from djos.models import Job, JobStatus

ALLOWED_TRANSITIONS: dict[JobStatus, frozenset[JobStatus]] = {
    JobStatus.BLOCKED: frozenset(
        {JobStatus.QUEUED, JobStatus.CANCELLED, JobStatus.UPSTREAM_FAILED}
    ),
    # Back to waiting if the dependency that failed is retried from the DLQ.
    JobStatus.UPSTREAM_FAILED: frozenset({JobStatus.BLOCKED}),
    JobStatus.SCHEDULED: frozenset({JobStatus.QUEUED, JobStatus.CANCELLED}),
    # queued → scheduled: deferred because its tenant is at its concurrency limit.
    JobStatus.QUEUED: frozenset({JobStatus.RUNNING, JobStatus.CANCELLED, JobStatus.SCHEDULED}),
    JobStatus.RUNNING: frozenset(
        {JobStatus.SUCCEEDED, JobStatus.DEAD, JobStatus.QUEUED, JobStatus.SCHEDULED}
    ),
    JobStatus.SUCCEEDED: frozenset(),
    JobStatus.DEAD: frozenset({JobStatus.QUEUED}),
    JobStatus.CANCELLED: frozenset(),
}

# Finished from the system's point of view (finished_at is set). `dead` and
# `upstream_failed` are included even though a DLQ retry can revive them.
TERMINAL_STATUSES = frozenset(
    {JobStatus.SUCCEEDED, JobStatus.DEAD, JobStatus.CANCELLED, JobStatus.UPSTREAM_FAILED}
)


class InvalidTransition(Exception):
    def __init__(self, current: JobStatus, target: JobStatus):
        super().__init__(f"cannot move job from {current} to {target}")
        self.current = current
        self.target = target


def can_transition(current: JobStatus, target: JobStatus) -> bool:
    return target in ALLOWED_TRANSITIONS[current]


def transition(job: Job, target: JobStatus, *, now: datetime | None = None) -> None:
    """Move `job` to `target`, updating the bookkeeping fields that go with it."""
    if not can_transition(job.status, target):
        raise InvalidTransition(job.status, target)

    now = now or datetime.now(UTC)
    if target is JobStatus.RUNNING:
        # Each claim is one attempt; retries and reclaims count towards max_attempts.
        job.attempts += 1
        # A new fencing token per claim. Unlike attempts it is never reset, so it only
        # ever grows and an older claim's token can never become current again.
        job.lease_token += 1
        job.started_at = now
    if target in TERMINAL_STATUSES:
        job.finished_at = now
    elif job.status in TERMINAL_STATUSES:
        job.finished_at = None  # leaving the DLQ: it isn't finished any more
    job.status = target
