# Distributed-Job-Orchestration-System

A distributed, fault-tolerant job orchestration system. Clients submit long-running async jobs (or whole DAG workflows); independent worker processes execute them with **exactly-once effects**, even when workers crash or stall mid-job.

## Status
🚧 Work in progress — building in public. See the [3-week plan](PLAN.md).

## The Problem

Background job systems are easy until a worker dies mid-task. Naive designs either:
- **Lose the job** (nobody notices the worker died), or
- **Duplicate it** (a slow-but-alive worker gets mistaken for a dead one and another worker re-runs the same job).

This project solves both with lease-based distributed locks, heartbeats, fencing tokens, and a monitor process that reclaims orphaned jobs safely.

## Architecture (planned)

```
Client ─► API (FastAPI, API keys, rate limit) ─► Postgres (source of truth)
                     │                                ▲ fenced writes
                     ▼                                │
                  Redis (priority queues, leases, delayed jobs, rate limits)
                     │
                 Workers ×N ◄── Autoscaler (queue depth)
Scheduler (retries / delayed / cron) · Monitor (lease reclaim, Postgres ↔ Redis reconcile)
Prometheus + Grafana scrape everything
```

## Core Guarantees

True exactly-once *execution* is impossible in a distributed system: a worker can stall (GC pause, network partition), lose its lease, and wake up still believing it owns the job. So the guarantee here is:

> **At-least-once execution + fencing tokens + idempotent effects = exactly-once effects.**

- **Lease-based claiming:** Redis `SET NX PX` ensures only one worker holds a job's lease at a time
- **Failure detection:** workers heartbeat by renewing their lease; a dead worker's lease expires and the job is reclaimed
- **Fencing tokens:** every claim gets a monotonically increasing token; Postgres rejects writes from a stale worker holding an old token
- **Durable state:** Postgres is the source of truth; Redis can be rebuilt from it
- **Idempotent submission:** retried client requests (via idempotency key) never create duplicate jobs
- **Retry + dead-letter queue:** failed jobs retry with exponential backoff and jitter, then land in a DLQ for inspection
- **Verified by chaos testing:** workers are randomly killed and paused under load, and a checker asserts zero lost and zero duplicated effects

## Tech Stack

- **API:** FastAPI (Python 3.12, asyncio)
- **Queue & Coordination:** Redis 7 (priority queues, leases, Lua scripts)
- **Durable State:** PostgreSQL 16 (SQLAlchemy 2 async, Alembic)
- **Observability:** Prometheus, Grafana, structured JSON logs
- **Orchestration:** Docker Compose (multi-worker), GitHub Actions CI

## Roadmap

- [ ] Week 1: core pipeline, multi-worker coordination, heartbeats, fencing tokens
- [ ] Week 2: retries + DLQ, priorities, scheduled/cron jobs, DAG workflows, multi-tenancy & rate limiting
- [ ] Week 3: metrics + Grafana, chaos testing, autoscaling, benchmarks, Redis vs. Postgres `SKIP LOCKED` comparison

Day-by-day details are in [PLAN.md](PLAN.md).

## Known Limitations

Redis is a single point of failure for coordination in this design (a production system would use Redis Sentinel/Cluster or a consensus-based coordinator like etcd). Because Postgres is the source of truth, a Redis loss delays jobs but does not lose them. This project focuses on correct multi-worker coordination logic, not full node-level fault tolerance.

## Getting Started

Requires Docker and [uv](https://docs.astral.sh/uv/).

```bash
docker compose up -d --build --wait     # API, Postgres, Redis

curl localhost:58000/healthz            # liveness  → {"status":"ok"}
curl localhost:58000/readyz             # readiness → checks Postgres + Redis

uv sync                                 # local dev environment
uv run pytest                           # tests (integration tests need the stack running)
uv run pytest -m "not integration"      # tests that need no services
uv run ruff check . && uv run ruff format --check .
```

`docker compose up` runs database migrations automatically (the one-shot `migrate` service) before the API starts. Interactive API docs: http://localhost:58000/docs

### Jobs API

```bash
# Submit (201). Retrying with the same Idempotency-Key returns the same job (200,
# Idempotent-Replayed: true); reusing the key with a different body is a 422.
curl -X POST localhost:58000/jobs \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: order-42' \
  -d '{"type": "sleep", "payload": {"seconds": 2}, "priority": "high"}'

curl localhost:58000/jobs/<id>                       # fetch one
curl 'localhost:58000/jobs?status=queued&limit=20'   # list / filter / paginate
curl -X POST localhost:58000/jobs/<id>/cancel        # only while queued, else 409
```

Job lifecycle (enforced in [`jobs/states.py`](src/djos/jobs/states.py)):

```
queued ──► running ──► succeeded
  │          │  └────► failed
  │          └───────► queued      (lease expired / retry)
  └──────────────────► cancelled
```

Host ports default to `58000` (API), `55432` (Postgres) and `56379` (Redis) to avoid clashing with locally installed services. Override them with `DJOS_API_HOST_PORT`, `DJOS_PG_HOST_PORT` and `DJOS_REDIS_HOST_PORT`.

## License

MIT
