import hashlib
import json
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from djos.jobs.schemas import JobCreate
from djos.jobs.service import request_fingerprint


def test_fingerprint_of_a_plain_request_is_unchanged_by_the_new_optional_fields():
    old_style = {
        "type": "sleep",
        "payload": {"seconds": 1.0},
        "priority": "normal",
        "max_attempts": 3,
    }
    expected = hashlib.sha256(
        json.dumps(old_style, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    assert request_fingerprint(JobCreate(type="sleep")) == expected


def test_schedule_fields_do_count_when_set():
    assert request_fingerprint(JobCreate(type="sleep", delay_seconds=5)) != request_fingerprint(
        JobCreate(type="sleep")
    )


def test_run_at_and_delay_are_mutually_exclusive():
    with pytest.raises(ValidationError, match="not both"):
        JobCreate(type="sleep", run_at=datetime.now(UTC), delay_seconds=5)


def test_run_at_must_have_a_timezone():
    with pytest.raises(ValidationError):
        JobCreate(type="sleep", run_at=datetime(2030, 1, 1, 12, 0))


def test_resolve_run_at():
    now = datetime(2030, 1, 1, tzinfo=UTC)
    assert JobCreate(type="sleep").resolve_run_at(now) is None
    assert JobCreate(type="sleep", delay_seconds=90).resolve_run_at(now) == now + timedelta(
        seconds=90
    )


@pytest.mark.integration
def test_delayed_submit_is_scheduled_and_not_queued_yet(client, redis_sync):
    before = datetime.now(UTC)
    job = client.post("/jobs", json={"type": "sleep", "delay_seconds": 60}).json()

    assert job["status"] == "scheduled"
    run_at = datetime.fromisoformat(job["run_at"])
    assert before + timedelta(seconds=59) <= run_at <= datetime.now(UTC) + timedelta(seconds=60)
    assert redis_sync.llen("q:normal") == 0


@pytest.mark.integration
def test_run_at_in_the_past_runs_right_away(client, redis_sync):
    past = (datetime.now(UTC) - timedelta(minutes=5)).isoformat()
    job = client.post("/jobs", json={"type": "sleep", "run_at": past}).json()

    assert job["status"] == "queued"
    assert redis_sync.lrange("q:normal", 0, -1) == [job["id"]]


@pytest.mark.integration
def test_scheduled_job_can_be_cancelled_before_it_runs(client):
    job = client.post("/jobs", json={"type": "sleep", "delay_seconds": 60}).json()
    resp = client.post(f"/jobs/{job['id']}/cancel")
    assert resp.json()["status"] == "cancelled"
