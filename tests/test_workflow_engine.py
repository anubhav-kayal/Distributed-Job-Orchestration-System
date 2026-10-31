import asyncio
import uuid

import pytest
from sqlalchemy import func, select, update

from djos.coordination import Leases, lease_key
from djos.jobs import service as job_service
from djos.models import Effect, Job, JobStatus, Priority
from djos.monitor import Monitor
from djos.worker import Worker
from djos.worker.worker import Outcome
from djos.workflows import service
from djos.workflows.schemas import WorkflowCreate

pytestmark = pytest.mark.integration

OK = {"type": "sleep", "payload": {"seconds": 0}}
DIAMOND = [
    {"key": "a", **OK},
    {"key": "b", **OK, "depends_on": ["a"]},
    {"key": "c", **OK, "depends_on": ["a"]},
    {"key": "d", **OK, "depends_on": ["b", "c"]},
]


@pytest.fixture
def leases(redis) -> Leases:
    return Leases(redis, ttl_seconds=5)


def make_worker(sessionmaker, queue, leases, name="w") -> Worker:
    return Worker(
        sessionmaker, queue, leases, worker_id=name, block_seconds=0.05, heartbeat_seconds=1
    )


async def submit(sessionmaker, queue, jobs) -> uuid.UUID:
    async with sessionmaker() as session:
        workflow, _ = await service.submit_workflow(session, queue, WorkflowCreate(jobs=jobs))
        return workflow.id


async def nodes(sessionmaker, workflow_id) -> dict[str, Job]:
    async with sessionmaker() as session:
        jobs = await session.scalars(select(Job).where(Job.workflow_id == workflow_id))
        return {j.workflow_key: j for j in jobs}


async def workflow_status(sessionmaker, workflow_id) -> str:
    async with sessionmaker() as session:
        return (await service.get_workflow(session, workflow_id)).status


async def drain(*workers: Worker) -> None:
    """Run until every worker finds the queue empty."""
    while any(await asyncio.gather(*(w.process_next() for w in workers))):
        pass


async def test_roots_are_queued_and_the_rest_wait(sessionmaker, queue):
    wf = await submit(sessionmaker, queue, DIAMOND)
    n = await nodes(sessionmaker, wf)

    assert n["a"].status is JobStatus.QUEUED
    assert [(n[k].status, n[k].pending_deps) for k in "bcd"] == [
        (JobStatus.BLOCKED, 1),
        (JobStatus.BLOCKED, 1),
        (JobStatus.BLOCKED, 2),
    ]
    assert await queue.depth() == {Priority.HIGH: 0, Priority.NORMAL: 1, Priority.LOW: 0}
    assert await workflow_status(sessionmaker, wf) == "running"


async def test_diamond_runs_in_dependency_order(sessionmaker, queue, leases):
    wf = await submit(sessionmaker, queue, DIAMOND)

    await drain(*(make_worker(sessionmaker, queue, leases, f"w{i}") for i in range(3)))

    n = await nodes(sessionmaker, wf)
    assert all(j.status is JobStatus.SUCCEEDED for j in n.values())
    for child, parents in {"b": "a", "c": "a", "d": "bc"}.items():
        for parent in parents:
            assert n[child].started_at >= n[parent].finished_at, f"{child} before {parent}"
    assert await workflow_status(sessionmaker, wf) == "succeeded"
    async with sessionmaker() as session:
        assert await session.scalar(select(func.count()).select_from(Effect)) == 4


async def test_a_dead_dependency_fails_only_its_descendants(sessionmaker, queue, leases):
    jobs = [dict(j) for j in DIAMOND]
    jobs[1] = {"key": "b", "type": "fail", "payload": {"permanent": True}, "depends_on": ["a"]}
    wf = await submit(sessionmaker, queue, jobs)

    await drain(make_worker(sessionmaker, queue, leases))

    n = await nodes(sessionmaker, wf)
    assert n["b"].status is JobStatus.DEAD
    assert n["c"].status is JobStatus.SUCCEEDED  # a sibling branch is unaffected
    assert n["d"].status is JobStatus.UPSTREAM_FAILED
    assert n["d"].attempts == 0  # never ran
    assert n["d"].last_error == "upstream job 'b' is dead"
    assert await workflow_status(sessionmaker, wf) == "failed"


async def test_dlq_retry_of_the_failed_job_resumes_the_workflow(sessionmaker, queue, leases):
    jobs = [dict(j) for j in DIAMOND]
    jobs[1] = {
        "key": "b",
        "type": "flaky",
        "payload": {"fail_times": 1},
        "max_attempts": 1,
        "depends_on": ["a"],
    }
    wf = await submit(sessionmaker, queue, jobs)
    worker = make_worker(sessionmaker, queue, leases)
    await drain(worker)
    n = await nodes(sessionmaker, wf)
    assert (n["b"].status, n["d"].status) == (JobStatus.DEAD, JobStatus.UPSTREAM_FAILED)

    async with sessionmaker() as session:  # "fix the bug", then retry from the DLQ
        await session.execute(
            update(Job).where(Job.id == n["b"].id).values(payload={"fail_times": 0})
        )
        await session.commit()
        await job_service.retry_dead_job(session, queue, n["b"].id)

    n = await nodes(sessionmaker, wf)
    assert (n["b"].status, n["d"].status, n["d"].pending_deps) == (
        JobStatus.QUEUED,
        JobStatus.BLOCKED,
        1,  # c already succeeded; only b is outstanding
    )
    await drain(worker)
    assert await workflow_status(sessionmaker, wf) == "succeeded"


async def test_revive_keeps_jobs_that_have_another_failed_dependency(sessionmaker, queue, leases):
    dead = {"type": "fail", "payload": {"permanent": True}}
    wf = await submit(
        sessionmaker,
        queue,
        [{"key": "b", **dead}, {"key": "c", **dead}, {"key": "d", **OK, "depends_on": ["b", "c"]}],
    )
    await drain(make_worker(sessionmaker, queue, leases))
    n = await nodes(sessionmaker, wf)

    async with sessionmaker() as session:
        await job_service.retry_dead_job(session, queue, n["b"].id)
    assert (await nodes(sessionmaker, wf))["d"].status is JobStatus.UPSTREAM_FAILED  # c still dead

    async with sessionmaker() as session:
        await job_service.retry_dead_job(session, queue, n["c"].id)
    assert (await nodes(sessionmaker, wf))["d"].status is JobStatus.BLOCKED


async def test_parents_finishing_at_the_same_instant_unblock_the_child_once(
    sessionmaker, queue, leases, redis
):
    for _ in range(10):
        await redis.flushdb()
        wf = await submit(
            sessionmaker,
            queue,
            [{"key": "b", **OK}, {"key": "c", **OK}, {"key": "d", **OK, "depends_on": ["b", "c"]}],
        )
        n = await nodes(sessionmaker, wf)
        wb, wc = (
            make_worker(sessionmaker, queue, leases, "wb"),
            make_worker(sessionmaker, queue, leases, "wc"),
        )
        b, c = await wb._claim(n["b"].id), await wc._claim(n["c"].id)
        await redis.delete("q:normal")

        results = await asyncio.gather(
            wb._finish(b.id, b.lease_token, Outcome(True, result={})),
            wc._finish(c.id, c.lease_token, Outcome(True, result={})),
        )

        assert results == [True, True]
        assert (await nodes(sessionmaker, wf))["d"].status is JobStatus.QUEUED
        assert await redis.lrange("q:normal", 0, -1) == [str(n["d"].id)]  # pushed exactly once


async def test_a_fenced_stale_worker_cannot_unblock_children_again(
    sessionmaker, queue, leases, redis
):
    wf = await submit(
        sessionmaker, queue, [{"key": "a", **OK}, {"key": "b", **OK, "depends_on": ["a"]}]
    )
    n = await nodes(sessionmaker, wf)
    stale = make_worker(sessionmaker, queue, leases, "stale")
    await queue.dequeue(block_seconds=0.1)
    claimed = await stale._claim(n["a"].id)
    await redis.delete(lease_key(n["a"].id))
    await Monitor(sessionmaker, queue, leases).sweep()
    await make_worker(sessionmaker, queue, leases, "fresh").process_next()  # a succeeds; b queued

    assert await stale._finish(n["a"].id, claimed.lease_token, Outcome(True, result={})) is False
    assert await redis.lrange("q:normal", 0, -1) == [str(n["b"].id)]


async def test_cancelling_one_job_fails_its_descendants(sessionmaker, queue):
    wf = await submit(
        sessionmaker, queue, [{"key": "a", **OK}, {"key": "b", **OK, "depends_on": ["a"]}]
    )
    n = await nodes(sessionmaker, wf)
    async with sessionmaker() as session:
        await job_service.cancel_job(session, n["a"].id)

    n = await nodes(sessionmaker, wf)
    assert (n["a"].status, n["b"].status) == (JobStatus.CANCELLED, JobStatus.UPSTREAM_FAILED)
    assert n["b"].last_error == "upstream job 'a' is cancelled"


async def test_monitor_giving_up_on_a_job_fails_its_descendants(sessionmaker, queue, leases, redis):
    wf = await submit(
        sessionmaker,
        queue,
        [{"key": "a", **OK, "max_attempts": 1}, {"key": "b", **OK, "depends_on": ["a"]}],
    )
    n = await nodes(sessionmaker, wf)
    await queue.dequeue(block_seconds=0.1)
    await make_worker(sessionmaker, queue, leases)._claim(n["a"].id)  # ...and the worker dies

    await Monitor(sessionmaker, queue, leases).sweep()

    n = await nodes(sessionmaker, wf)
    assert (n["a"].status, n["b"].status) == (JobStatus.DEAD, JobStatus.UPSTREAM_FAILED)


async def test_cancel_workflow_stops_everything_not_yet_running(sessionmaker, queue, leases):
    wf = await submit(sessionmaker, queue, DIAMOND)
    n = await nodes(sessionmaker, wf)
    worker = make_worker(sessionmaker, queue, leases)
    await queue.dequeue(block_seconds=0.1)
    a = await worker._claim(n["a"].id)  # a is running when the cancel arrives

    async with sessionmaker() as session:
        await service.cancel_workflow(session, wf)
    await worker._finish(a.id, a.lease_token, Outcome(True, result={}))  # a still finishes

    n = await nodes(sessionmaker, wf)
    assert n["a"].status is JobStatus.SUCCEEDED
    assert {n[k].status for k in "bcd"} == {JobStatus.CANCELLED}  # nothing new started
    assert await workflow_status(sessionmaker, wf) == "cancelled"
    assert await queue.depth() == {Priority.HIGH: 0, Priority.NORMAL: 0, Priority.LOW: 0}


async def test_a_job_in_a_cancelled_workflow_cannot_be_retried(sessionmaker, queue, leases):
    wf = await submit(
        sessionmaker, queue, [{"key": "a", "type": "fail", "payload": {"permanent": True}}]
    )
    await drain(make_worker(sessionmaker, queue, leases))
    async with sessionmaker() as session:
        await service.cancel_workflow(session, wf)
        with pytest.raises(job_service.WorkflowCancelled):
            await job_service.retry_dead_job(
                session, queue, (await nodes(sessionmaker, wf))["a"].id
            )
