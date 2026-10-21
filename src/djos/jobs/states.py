"""The job lifecycle as an explicit state machine.

Every status change in the system goes through `transition()`, so the allowed lifecycle
lives in exactly one place:

    queued ──► running ──► succeeded
      │          │  └────► failed
      │          └───────► queued      (lease expired / retry: run it again)
      └──────────────────► cancelled
"""

from datetime import UTC, datetime

from djos.models import Job, JobStatus

ALLOWED_TRANSITIONS: dict[JobStatus, frozenset[JobStatus]] = {
    JobStatus.QUEUED: frozenset({JobStatus.RUNNING, JobStatus.CANCELLED}),
    JobStatus.RUNNING: frozenset({JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.QUEUED}),
    JobStatus.SUCCEEDED: frozenset(),
    JobStatus.FAILED: frozenset(),
    JobStatus.CANCELLED: frozenset(),
}

TERMINAL_STATUSES = frozenset(s for s, targets in ALLOWED_TRANSITIONS.items() if not targets)


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
    job.status = target
