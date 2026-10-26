import asyncio
import uuid

import pytest
from sqlalchemy import func, update
from sqlalchemy.ext.asyncio import create_async_engine

from djos.models import Job, JobStatus

pytestmark = pytest.mark.integration


def submit(client, **body) -> str:
    body.setdefault("type", "sleep")
    return client.post("/jobs", json=body).json()["id"]


def kill(test_settings, job_id: str, *, attempts: int = 3, token: int = 3) -> None:
    """Put a job straight into the DLQ, as if it had used up its attempts."""

    async def go():
        engine = create_async_engine(test_settings.database_url)
        async with engine.begin() as conn:
            await conn.execute(
                update(Job)
                .where(Job.id == uuid.UUID(job_id))
                .values(
                    status=JobStatus.DEAD,
                    attempts=attempts,
                    lease_token=token,
                    last_error="RuntimeError: boom",
                    finished_at=func.now(),
                )
            )
        await engine.dispose()

    asyncio.run(go())


def test_dlq_lists_only_dead_jobs(client, test_settings):
    dead = submit(client)
    submit(client)  # stays queued
    kill(test_settings, dead)

    items = client.get("/dlq").json()["items"]

    assert [j["id"] for j in items] == [dead]
    assert items[0]["last_error"] == "RuntimeError: boom"


def test_dlq_filters_by_type(client, test_settings):
    a = submit(client, type="sleep")
    b = submit(client, type="fail")
    kill(test_settings, a)
    kill(test_settings, b)

    assert [j["id"] for j in client.get("/dlq", params={"type": "fail"}).json()["items"]] == [b]


def test_retry_requeues_with_fresh_attempts_but_keeps_the_fencing_token(
    client, test_settings, redis_sync
):
    job_id = submit(client)
    redis_sync.delete("q:normal")
    kill(test_settings, job_id, attempts=3, token=7)

    resp = client.post(f"/dlq/{job_id}/retry")

    assert resp.status_code == 200
    job = resp.json()
    assert (job["status"], job["attempts"], job["lease_token"]) == ("queued", 0, 7)
    assert job["finished_at"] is None
    assert job["last_error"] == "RuntimeError: boom"  # kept until the next run
    assert redis_sync.lrange("q:normal", 0, -1) == [job_id]
    assert client.get("/dlq").json()["items"] == []


def test_retrying_a_job_that_is_not_dead_is_409(client):
    job_id = submit(client)
    resp = client.post(f"/dlq/{job_id}/retry")
    assert resp.status_code == 409
    assert "only dead jobs" in resp.json()["detail"]


def test_retrying_an_unknown_job_is_404(client):
    assert client.post(f"/dlq/{uuid.uuid4()}/retry").status_code == 404
