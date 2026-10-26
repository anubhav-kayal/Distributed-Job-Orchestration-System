# Distributed-Job-Orchestration-System

A distributed, fault-tolerant job orchestration system. Clients submit long-running async jobs; independent worker processes execute them with **exactly-once effects**, even when workers crash or stall mid-job.

## Status

🚧 Building in public over 3 weeks ([plan](PLAN.md)). **Week 1 is complete:** the core pipeline, multi-worker coordination with leases and heartbeats, orphan reclaim, and fencing tokens. It's verified by killing workers under load (see [Proof](#proof)). **Week 2 in progress:** retries with backoff, a dead-letter queue, fair priority scheduling, and delayed and cron jobs are done. Notes: [week 1](docs/notes/week-1.md), [week 2](docs/notes/week-2.md).

## The Problem

Background job systems are easy until a worker dies mid-task. Naive designs either:
- **Lose the job** (nobody notices the worker died), or
- **Duplicate it** (a slow-but-alive worker gets mistaken for a dead one and another worker re-runs the same job).

This project avoids both with leases, heartbeats, a monitor that reclaims orphaned jobs, and fencing tokens that reject stale workers.

## Architecture

```
                 commit, then push ID               BLMPOP (priority order)
Client ──► API ─────────────────────► Redis q:high/normal/low ───────────► Workers ×N
            │                          lease:{job_id} (SET NX PX + heartbeat) ◄──┤
            │                                                                    │
            ▼               claim: SELECT … FOR UPDATE, lease_token += 1         │
        Postgres ◄──────────────────────────────────────────────────────────────┘
   (source of truth)        finish: only if lease_token still matches (fenced)
            ▲
            │  running + no lease ──► requeue (or fail when out of attempts)
         Monitor
               queued + ID missing from Redis ──► push again
            ▲
            │  scheduled + run_at passed ──► queued + push      (retries, delayed jobs)
        Scheduler
               cron schedule due ──► insert job + advance next_fire_at (one transaction)
```

**Postgres is the source of truth.** Redis only carries job IDs and leases, so anything Redis loses can be rebuilt from Postgres. Coming next: DAG workflows, multi-tenancy with rate limits, Prometheus/Grafana, an autoscaler, and a full chaos suite.

## Core Guarantees

True exactly-once *execution* is impossible in a distributed system: a worker can stall (GC pause, frozen VM, network partition), lose its lease, and wake up still believing it owns the job. So the guarantee here is:

> **At-least-once execution + fencing tokens = exactly-once effects.**

| Mechanism | What it prevents | Status |
|---|---|---|
| **Idempotent submission** (`Idempotency-Key`, `INSERT … ON CONFLICT`) | a retried request creating a second job | ✅ |
| **Commit-then-enqueue**, Postgres as source of truth | a worker popping a job that doesn't exist yet | ✅ |
| **Claim through Postgres** (`SELECT … FOR UPDATE`, only if `queued`) | duplicate or stale queue entries running a job twice | ✅ |
| **Leases** (`SET NX PX`, Lua compare-and-renew/release) | two live workers owning one job; a dead worker holding it forever | ✅ |
| **Heartbeats** (renew every TTL/3; abandon the job on lease loss) | a long job being mistaken for a dead one | ✅ |
| **Monitor** (orphan reclaim, stranded re-enqueue) | jobs lost to crashes or failed Redis pushes | ✅ |
| **Fencing tokens** (`lease_token` bumped per claim, fenced final write) | a stalled worker overwriting a re-run's result | ✅ |
| **Retries** (exponential backoff, full jitter; `PermanentError` skips them) | transient failures being terminal; synchronized retry storms | ✅ |
| **Dead-letter queue** (`dead` status, `GET /dlq`, `POST /dlq/{id}/retry`) | failed jobs vanishing, or looping forever | ✅ |
| **Weighted priority rotation** (6/3/1 by default) | low priority starving under a flood of high | ✅ |
| **Cron firing** (`SKIP LOCKED`, job + `next_fire_at` in one transaction, `cron:{id}:{time}` key) | duplicate or lost runs with several schedulers | ✅ |
| Full chaos suite (kills, pauses, Redis restarts) + checker | untested failure modes | Week 3 |

### How a job runs

1. **Submit:** the API validates the payload against the job type's schema, inserts the job (`queued`), commits, and then pushes its ID onto the priority's Redis list. If the push fails, the job is still safe in Postgres, and the monitor re-pushes it later.
2. **Lease:** a worker pops the highest-priority ID (`BLMPOP`) and takes `lease:{job_id}` with `SET NX PX`. If someone else holds the lease, the worker skips the ID.
3. **Claim:** in Postgres, under `SELECT … FOR UPDATE`, the worker moves the job `queued → running`, incrementing `attempts` and the fencing token `lease_token`. If the job isn't `queued` (a duplicate ID or a cancelled job), the worker skips it.
4. **Run + heartbeat:** the handler runs while a heartbeat renews the lease. If the lease is lost, the worker cancels the handler and records nothing.
5. **Finish (fenced):** under a row lock, the outcome is written **only if the job is still `running` with the worker's token**. On success, the effect row is written in the same transaction. A failure with attempts left becomes `scheduled` with `run_at = now + jittered backoff`. A permanent failure, or the last attempt failing, makes the job `dead` (the DLQ). Then the lease is released.
6. **Monitor:** a `running` job with no lease means its worker died. The monitor requeues it, or marks it dead once `max_attempts` is used up. A job that has been `queued` for a while but whose ID is missing from Redis gets its ID pushed again.
7. **Scheduler:** moves `scheduled` jobs whose `run_at` has passed to `queued` (retries and delayed submissions), and fires due cron schedules. It reads Postgres with `FOR UPDATE SKIP LOCKED` and the database clock, so any number of replicas can run.

## Proof

Each scenario is reproducible with one command against the Docker Compose stack:

| Scenario | Command | Result |
|---|---|---|
| 1,000 jobs on 3 workers | `make verify` | 1,000 succeeded, every job `attempts=1`, **exactly 1 effect each** |
| 1,000 jobs, SIGKILL a random worker every 4s | `make chaos` | 12 workers killed, 12 jobs re-run, **0 lost, 0 duplicated effects** |
| SIGKILL the worker running a job | `make demo-crash` | monitor reclaims it after the lease TTL; another worker finishes it (attempt 2) |
| Freeze a worker mid-job (`docker pause`), let the job be re-run, unfreeze | `make demo-pause` | woken worker's write rejected: `fenced: our token 1 … is stale (job is succeeded with token 2)`; 1 effect |
| `flaky` job failing twice | (see the Jobs API) | retried after 0.64s and 1.43s (full jitter within 1s and 2s windows), succeeded on attempt 3 |
| Always-failing job, then a DLQ retry | (see the DLQ API) | dead after 3 attempts; the retry ran 3 fresh attempts (token kept growing: 3 → 6); a permanent error died after 1 |
| Every-minute cron with **2 scheduler replicas** | `make verify-cron` | 4 minutes → exactly 4 jobs, one per minute; the fires were split between the replicas (2 each), 0 duplicates |

The test suite (178 tests, run in CI against real Postgres and Redis) covers the same scenarios deterministically, including a stale worker being fenced while the re-run is still in progress.

## Tech Stack

- **API:** FastAPI (Python 3.12, asyncio), Pydantic
- **Queue & coordination:** Redis 7 (lists + `BLMPOP`, `SET NX PX` leases, Lua scripts)
- **Durable state:** PostgreSQL 16 (SQLAlchemy 2 async, asyncpg, Alembic)
- **Scheduling:** cron expressions via [cronsim](https://github.com/cuu508/cronsim)
- **Infra:** Docker Compose (API, workers, monitor, scheduler, one-shot migrations), GitHub Actions CI, uv, ruff, pytest

## Getting Started

Requires Docker and [uv](https://docs.astral.sh/uv/).

```bash
make up              # API, 3 workers, monitor, scheduler, Postgres, Redis (migrations run automatically)
make test            # full test suite (needs the stack, or just: docker compose up -d postgres redis)
make lint

make demo-crash      # see the Proof table
make demo-pause
make verify
make chaos
make verify-cron     # every-minute cron, 2 schedulers, ~3.5 minutes
```

The API is at http://localhost:58000 (interactive docs at `/docs`). Host ports default to `58000` (API), `55432` (Postgres) and `56379` (Redis) to avoid clashing with locally installed services. Override them with `DJOS_API_HOST_PORT`, `DJOS_PG_HOST_PORT` and `DJOS_REDIS_HOST_PORT`. All app settings are environment variables prefixed `DJOS_` (see [`core/config.py`](src/djos/core/config.py)).

### Jobs API

```bash
# Submit (201). Retrying with the same Idempotency-Key returns the same job (200,
# Idempotent-Replayed: true); reusing the key with a different body is a 422.
curl -X POST localhost:58000/jobs \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: order-42' \
  -d '{"type": "sleep", "payload": {"seconds": 2}, "priority": "high"}'

curl localhost:58000/jobs/<id>                       # fetch one (status, attempts, lease_token, locked_by, result…)
curl 'localhost:58000/jobs?status=queued&limit=20'   # list / filter / paginate
curl -X POST localhost:58000/jobs -H 'Content-Type: application/json' \
  -d '{"type": "sleep", "delay_seconds": 300}'      # run in 5 minutes (or "run_at": "<ISO time with tz>")
curl -X POST localhost:58000/jobs/<id>/cancel        # only while queued or scheduled, else 409
curl localhost:58000/healthz                         # liveness
curl localhost:58000/readyz                          # readiness (checks Postgres + Redis)
```

Job types (each with a validated payload schema, see [`handlers/builtin.py`](src/djos/handlers/builtin.py)):

| Type | Payload | What it simulates |
|---|---|---|
| `sleep` | `{"seconds": 1.0}` | I/O-bound work |
| `hash` | `{"data": "...", "iterations": 100000}` | CPU-bound work (runs in a thread) |
| `fake_email` | `{"to": "a@b.co", "subject": "..."}` | an external API call |
| `flaky` | `{"fail_times": 1}` | fails on its first N attempts |
| `fail` | `{"message": "...", "permanent": false}` | always fails (permanently: no retries) |

A handler signals "retrying won't help" by raising `PermanentError`. Any other exception is retried with backoff until `max_attempts` is used up.

### Dead-letter queue

```bash
curl 'localhost:58000/dlq?type=fail'                 # dead jobs, newest first
curl -X POST localhost:58000/dlq/<id>/retry          # re-queue with a fresh set of attempts
```

### Cron schedules

```bash
curl -X POST localhost:58000/schedules -H 'Content-Type: application/json' -d '{
  "name": "nightly-digest", "cron": "0 2 * * *",
  "job": {"type": "fake_email", "payload": {"to": "team@example.com", "subject": "digest"}}}'

curl localhost:58000/schedules                                        # list
curl -X PATCH localhost:58000/schedules/<id> -d '{"enabled": false}' \
  -H 'Content-Type: application/json'                                 # pause (or change cron / job)
curl -X DELETE localhost:58000/schedules/<id>
```

Cron expressions are 5-field and evaluated in UTC. If the scheduler was down across several fire times, the schedule fires **once** on recovery and skips ahead rather than replaying every missed run.

Job lifecycle (enforced in [`jobs/states.py`](src/djos/jobs/states.py)):

```
scheduled ──► queued ──► running ──► succeeded
    │  ▲          │ ▲        │  └────► dead        (out of attempts / permanent error)
    │  │          │ │        ├───────► queued      (lease expired: reclaimed)
    │  └──────────┼─┼────────┘                     (failed, attempts left: retry later)
    │             │ └─────────────── dead          (manual retry from the DLQ)
    └─────────────┴──────────────────► cancelled
```

## Known Limitations

- **Redis is a single point of failure for coordination.** A production system would use Redis Sentinel/Cluster or a consensus-based coordinator like etcd. Because Postgres is the source of truth, a Redis loss delays jobs but doesn't lose them.
- **Fencing protects writes this system controls.** It covers the job's status and its effect row. A handler calling an external service (payments, email) should also pass `job_id` as that service's idempotency key, because the stale run may already have made the call.
- **The stranded-job check uses `LPOS`**, a linear scan of the queue, bounded per sweep. A very large backlog would want a reliable-queue pattern (`BLMOVE` into per-worker processing lists) instead.
- **Each worker runs one job at a time.** You scale by adding workers.

## License

MIT
