import uuid

import pytest
from pydantic import ValidationError

from djos.handlers import JobContext, UnknownJobType, get_handler, handler, registered_types
from djos.handlers.builtin import SleepPayload
from djos.jobs.schemas import JobCreate


def ctx(attempt: int = 1) -> JobContext:
    return JobContext(job_id=uuid.uuid4(), attempt=attempt, worker_id="test")


def test_builtin_handlers_are_registered():
    assert {"sleep", "hash", "fake_email", "flaky"} <= set(registered_types())


def test_unknown_type_raises():
    with pytest.raises(UnknownJobType):
        get_handler("nope")


def test_duplicate_registration_is_rejected():
    with pytest.raises(ValueError, match="already registered"):
        handler("sleep", SleepPayload)(lambda p, c: None)


async def run(type_: str, payload: dict, attempt: int = 1):
    h = get_handler(type_)
    return await h.fn(h.payload_model.model_validate(payload), ctx(attempt))


async def test_sleep():
    assert await run("sleep", {"seconds": 0}) == {"slept": 0}


async def test_hash_is_deterministic():
    a = await run("hash", {"data": "x", "iterations": 10})
    b = await run("hash", {"data": "x", "iterations": 10})
    assert a == b
    assert len(a["sha256"]) == 64


async def test_fake_email_returns_message_id():
    result = await run("fake_email", {"to": "a@b.co", "subject": "hi"})
    assert result["to"] == "a@b.co"
    uuid.UUID(result["message_id"])


async def test_flaky_fails_then_succeeds():
    with pytest.raises(RuntimeError, match="attempt 1"):
        await run("flaky", {"fail_times": 2}, attempt=1)
    with pytest.raises(RuntimeError, match="attempt 2"):
        await run("flaky", {"fail_times": 2}, attempt=2)
    assert await run("flaky", {"fail_times": 2}, attempt=3) == {"succeeded_on_attempt": 3}


def test_job_create_normalizes_payload_with_defaults():
    job = JobCreate(type="sleep")
    assert job.payload == {"seconds": 1.0}


@pytest.mark.parametrize(
    "body",
    [
        {"type": "nope"},
        {"type": "sleep", "payload": {"seconds": -1}},
        {"type": "sleep", "payload": {"unknown_field": 1}},
        {"type": "hash", "payload": {}},  # data is required
        {"type": "fake_email", "payload": {"to": "not-an-email", "subject": "x"}},
    ],
)
def test_job_create_rejects_bad_type_or_payload(body):
    with pytest.raises(ValidationError):
        JobCreate(**body)
