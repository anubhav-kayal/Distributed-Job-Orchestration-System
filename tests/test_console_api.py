import asyncio

import pytest
from sqlalchemy import update
from sqlalchemy.ext.asyncio import create_async_engine

from djos.models import Job, JobStatus

pytestmark = pytest.mark.integration


def set_status(test_settings, job_id: str, **values) -> None:
    async def go():
        engine = create_async_engine(test_settings.database_url)
        async with engine.begin() as conn:
            await conn.execute(update(Job).where(Job.id == job_id).values(**values))
        await engine.dispose()

    asyncio.run(go())


def test_stats_counts_queue_and_running_jobs(client, test_settings):
    high = client.post("/jobs", json={"type": "sleep", "priority": "high"}).json()
    client.post("/jobs", json={"type": "sleep"})
    running = client.post("/jobs", json={"type": "hash", "payload": {"data": "x"}}).json()
    set_status(
        test_settings,
        running["id"],
        status=JobStatus.RUNNING,
        locked_by="w1",
        attempts=1,
        lease_token=1,
        started_at=Job.created_at,
    )

    stats = client.get("/stats").json()

    assert stats["counts"]["queued"] == 2
    assert stats["counts"]["running"] == 1
    assert stats["counts"]["dead"] == 0  # every status present, even at zero
    assert stats["queued_by_priority"] == {"high": 1, "normal": 1, "low": 0}
    [strip] = stats["running"]
    assert (strip["id"], strip["worker"], strip["lease_token"]) == (running["id"], "w1", 1)
    assert high["id"] not in str(stats["running"])


def test_stats_throughput_series_covers_five_minutes(client, test_settings):
    job = client.post("/jobs", json={"type": "sleep"}).json()
    set_status(test_settings, job["id"], status=JobStatus.SUCCEEDED, finished_at=Job.created_at)

    series = client.get("/stats").json()["throughput"]

    assert len(series) == 20  # 15-second buckets
    assert sum(b["succeeded"] for b in series) == 1
    assert series == sorted(series, key=lambda b: b["at"])


def test_stats_lists_recent_failures(client, test_settings):
    job = client.post("/jobs", json={"type": "sleep"}).json()
    set_status(
        test_settings,
        job["id"],
        status=JobStatus.DEAD,
        last_error="RuntimeError: boom",
        finished_at=Job.created_at,
    )

    [failure] = client.get("/stats").json()["recent_failures"]

    assert (failure["id"], failure["last_error"]) == (job["id"], "RuntimeError: boom")


def test_job_types_describe_their_payloads(client):
    types = {t["type"]: t for t in client.get("/job-types").json()}

    assert {"sleep", "hash", "fake_email", "flaky", "fail"} <= set(types)
    sleep = types["sleep"]
    assert sleep["description"] == "I/O-bound work: waiting on something external."
    assert sleep["payload_schema"]["properties"]["seconds"]["default"] == 1.0
    assert "data" in types["hash"]["payload_schema"]["required"]
