import itertools

import pytest

from djos.jobs.states import (
    ALLOWED_TRANSITIONS,
    TERMINAL_STATUSES,
    InvalidTransition,
    can_transition,
    transition,
)
from djos.models import Job, JobStatus


def make_job(status: JobStatus = JobStatus.QUEUED) -> Job:
    return Job(type="sleep", payload={}, status=status, attempts=0, max_attempts=3, lease_token=0)


def test_every_status_has_transition_rules():
    assert set(ALLOWED_TRANSITIONS) == set(JobStatus)


def test_terminal_statuses():
    assert TERMINAL_STATUSES == {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED}


def test_happy_path_sets_attempts_and_timestamps():
    job = make_job()
    transition(job, JobStatus.RUNNING)
    assert job.status is JobStatus.RUNNING
    assert job.attempts == 1
    assert job.started_at is not None
    assert job.finished_at is None

    transition(job, JobStatus.SUCCEEDED)
    assert job.status is JobStatus.SUCCEEDED
    assert job.finished_at is not None


def test_requeue_and_rerun_counts_each_attempt():
    job = make_job()
    transition(job, JobStatus.RUNNING)
    transition(job, JobStatus.QUEUED)
    transition(job, JobStatus.RUNNING)
    assert job.attempts == 2


def test_every_claim_gets_a_new_fencing_token():
    job = make_job()
    transition(job, JobStatus.RUNNING)
    assert job.lease_token == 1
    transition(job, JobStatus.QUEUED)
    assert job.lease_token == 1  # requeueing doesn't change it
    transition(job, JobStatus.RUNNING)
    assert job.lease_token == 2


@pytest.mark.parametrize(
    ("current", "target"),
    [(c, t) for c, t in itertools.product(JobStatus, JobStatus) if not can_transition(c, t)],
)
def test_disallowed_transitions_raise_and_leave_job_untouched(current, target):
    job = make_job(current)
    with pytest.raises(InvalidTransition):
        transition(job, target)
    assert job.status is current
    assert job.attempts == 0


@pytest.mark.parametrize("status", sorted(TERMINAL_STATUSES))
def test_terminal_statuses_cannot_be_left(status):
    for target in JobStatus:
        assert not can_transition(status, target)
