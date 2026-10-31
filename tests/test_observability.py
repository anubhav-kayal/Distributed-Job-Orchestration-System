import json
import logging
import uuid

import pytest
from prometheus_client import REGISTRY

from djos.coordination import Leases, lease_key
from djos.core.logging import JsonFormatter, TextFormatter, bind
from djos.jobs import service
from djos.jobs.schemas import JobCreate
from djos.monitor import Monitor
from djos.worker import Worker
from djos.worker.worker import Outcome


def sample(name: str, **labels) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


def record(msg: str = "hello") -> logging.LogRecord:
    return logging.LogRecord("djos.test", logging.INFO, __file__, 1, msg, None, None)


def test_json_log_lines_carry_the_bound_job_context():
    job_id = uuid.uuid4()
    with bind(job_id=job_id, worker_id="w1"):
        with bind(lease_token=3, tenant_id=None):  # None values are left out
            line = json.loads(JsonFormatter().format(record()))
    assert line["msg"] == "hello"
    assert (line["job_id"], line["worker_id"], line["lease_token"]) == (str(job_id), "w1", "3")
    assert "tenant_id" not in line
    assert "job_id" not in json.loads(JsonFormatter().format(record()))  # context is scoped


def test_text_logs_show_the_context_too():
    with bind(job_id="abc"):
        assert TextFormatter().format(record()).endswith("[job_id=abc]")


@pytest.mark.integration
def test_metrics_endpoint_labels_requests_by_route_template(client):
    job = client.post("/jobs", json={"type": "sleep"}).json()
    client.get(f"/jobs/{job['id']}")
    client.get(f"/jobs/{uuid.uuid4()}")

    body = client.get("/metrics").text

    assert 'djos_http_requests_total{method="GET",route="/jobs/{job_id}",status="200"}' in body
    assert 'route="/jobs/{job_id}",status="404"' in body
    assert job["id"] not in body  # never a raw id as a label value
    assert 'djos_jobs_submitted_total{priority="normal",type="sleep"}' in body


@pytest.fixture
def leases(redis) -> Leases:
    return Leases(redis, ttl_seconds=5)


async def submit(sessionmaker, queue, **body) -> uuid.UUID:
    body.setdefault("type", "sleep")
    async with sessionmaker() as session:
        job, _ = await service.submit_job(session, queue, JobCreate(**body))
        return job.id


@pytest.mark.integration
async def test_worker_counts_outcomes_and_times_jobs(sessionmaker, queue, leases):
    worker = Worker(sessionmaker, queue, leases, block_seconds=0.1, heartbeat_seconds=1)
    ok_before = sample("djos_jobs_finished_total", type="sleep", outcome="succeeded")
    dead_before = sample("djos_jobs_finished_total", type="fail", outcome="dead")
    timed_before = sample("djos_job_duration_seconds_count", type="sleep")

    await submit(sessionmaker, queue, payload={"seconds": 0})
    await submit(sessionmaker, queue, type="fail", payload={"permanent": True})
    await worker.process_next()
    await worker.process_next()

    assert sample("djos_jobs_finished_total", type="sleep", outcome="succeeded") == ok_before + 1
    assert sample("djos_jobs_finished_total", type="fail", outcome="dead") == dead_before + 1
    assert sample("djos_job_duration_seconds_count", type="sleep") == timed_before + 1
    assert sample("djos_worker_busy") == 0


@pytest.mark.integration
async def test_fenced_writes_and_reclaims_are_counted(sessionmaker, queue, leases, redis):
    fenced_before = sample("djos_fenced_writes_total")
    reclaims_before = sample("djos_reclaims_total", outcome="requeued")
    job_id = await submit(sessionmaker, queue, payload={"seconds": 0})
    stale = Worker(sessionmaker, queue, leases, worker_id="stale", heartbeat_seconds=1)
    await queue.dequeue(block_seconds=0.1)
    claimed = await stale._claim(job_id)
    await redis.delete(lease_key(job_id))
    await Monitor(sessionmaker, queue, leases).sweep()
    await Worker(sessionmaker, queue, leases, heartbeat_seconds=1).process_next()

    await stale._finish(job_id, claimed.lease_token, Outcome(True, result={}))

    assert sample("djos_fenced_writes_total") == fenced_before + 1
    assert sample("djos_reclaims_total", outcome="requeued") == reclaims_before + 1


@pytest.mark.integration
async def test_monitor_publishes_queue_depth_and_status_counts(sessionmaker, queue, leases):
    for _ in range(3):
        await submit(sessionmaker, queue, priority="high")

    await Monitor(sessionmaker, queue, leases).sweep()

    assert sample("djos_queue_depth", priority="high") == 3
    assert sample("djos_jobs", status="queued") == 3
    assert sample("djos_jobs", status="dead") == 0  # present even when zero
