# 3-Week Build & Learn Plan

Every day has three parts:
- **Learn:** the concept to understand *before* coding (articles, docs, or a short explanation).
- **Build:** what to implement.
- **Done when:** a concrete check that proves the day is finished.

Assumed pace: ~3–4 focused hours/day. Days 7, 14 and 21 are buffer/consolidation days. If you fall behind, see [What to cut if behind](#what-to-cut-if-behind).

---

## Final Goal

A fault-tolerant job orchestrator that:
- runs async jobs on N independent workers, with priorities, retries, a DLQ, scheduling and cron
- runs **DAG workflows** (job C runs only after A and B succeed)
- isolates **tenants** with rate limits and concurrency caps
- **autoscales** workers from queue depth
- is **observable** (Prometheus + Grafana)
- is **proven correct under chaos** (random kills and pauses: zero lost jobs, zero duplicate effects)
- has **published benchmarks** and a comparison of a Redis backend vs. a Postgres `SKIP LOCKED` backend

## Correctness Guarantee (fix #1: stated honestly)

"Exactly-once execution" is impossible in a distributed system. A worker can stall (GC pause, network partition), lose its lease, and wake up still believing it owns the job. This project guarantees:

> **At-least-once execution + fencing tokens + idempotent effects = exactly-once *effects*.**

How it works:
1. Each time a job is claimed, the claim gets a monotonically increasing **fencing token** (`jobs.lease_token`).
2. Every write a worker makes (result, status, side-effect rows) is conditional: `... WHERE id = :id AND lease_token = :my_token`.
3. A stale worker that wakes up has an old token. Its writes affect 0 rows, so it detects it was fenced and aborts.
4. The chaos tests check this directly: they `docker pause` a worker past its lease, let another worker take over, then unpause the first one.

## Name (fix #2)

The project is named **Distributed-Job-Orchestration-System** (short form: `djos`) everywhere. The stray "forge-queue" reference is removed from the README.

---

## Architecture (final)

```
                 ┌──────────── Prometheus ──── Grafana
                 │ (scrapes all services)
Client ──► API (FastAPI) ──► Postgres  (source of truth: jobs, DAG edges, tenants, cron, effects)
  │ API key      │  rate limit       ▲
  │              ▼                   │ fenced writes
  │           Redis  ── priority queues, leases, delayed zset, rate-limit buckets
  │              │  ▲
  │              ▼  │
  │          Workers ×N  ◄── Autoscaler (queue depth → scale up/down)
  │
  └── Scheduler: delayed/retry jobs, cron fires
      Monitor:   reclaims expired leases, reconciles Postgres ↔ Redis
```

**Key principle:** Postgres is the source of truth and Redis is a fast dispatch and coordination layer. If Redis loses data, the monitor rebuilds the queues from Postgres.

## Tech Stack

| Concern | Choice |
|---|---|
| Language | Python 3.12, asyncio |
| API | FastAPI + Pydantic |
| DB | PostgreSQL 16, SQLAlchemy 2.0 (async) + asyncpg, Alembic migrations |
| Queue/coordination | Redis 7 (`redis.asyncio`, Lua scripts) |
| Cron parsing | `croniter` |
| Metrics | `prometheus_client`, Prometheus, Grafana (provisioned dashboards) |
| Logs | structured JSON logs (`structlog`) with `job_id` / `tenant_id` |
| Tests | pytest, pytest-asyncio, testcontainers (or the compose services) |
| Load/bench | custom async load generator + matplotlib for graphs |
| Infra | Docker Compose, GitHub Actions CI, `uv` for deps, `ruff` for lint |

## Repo Layout (target)

```
app/
  api/          # FastAPI routes: jobs, workflows, cron, dlq, tenants
  core/         # config, db session, redis client, logging, metrics
  models/       # SQLAlchemy models
  queue/        # backend interface + redis_backend.py + postgres_backend.py
  worker/       # worker loop, heartbeat, handler registry, fencing
  scheduler/    # delayed jobs, retries, cron
  monitor/      # lease reclaim, reconciliation
  autoscaler/   # queue-depth control loop
  handlers/     # demo job types (sleep, hash, fake_email, flaky)
migrations/
chaos/          # chaos runner + correctness checker
bench/          # load generator + plots
deploy/         # prometheus.yml, grafana dashboards
tests/
docker-compose.yml
```

## Data Model (sketch)

- **tenants**(id, name, api_key_hash, rate_limit_per_sec, max_concurrency)
- **jobs**(id, tenant_id, type, payload jsonb, priority, status, attempts, max_attempts, run_at, lease_token, locked_by, idempotency_key, result jsonb, last_error, workflow_id, pending_deps, created_at, started_at, finished_at)
  - unique(tenant_id, idempotency_key)
  - status ∈ `blocked | scheduled | queued | running | succeeded | failed | dead | cancelled`
- **job_dependencies**(job_id, depends_on_job_id)
- **workflows**(id, tenant_id, status, created_at)
- **cron_schedules**(id, tenant_id, cron_expr, job_template jsonb, next_fire_at, enabled)
- **effects**(job_id, lease_token, data): the demo "side effect" table, unique(job_id); the chaos checker counts these rows

## Redis Keys (sketch)

| Key | Type | Purpose |
|---|---|---|
| `q:{priority}` | list | ready queue per tier (high/normal/low); worker uses `BLMPOP` in priority order |
| `lease:{job_id}` | string | `SET NX PX` → value `worker_id:token`, renewed by heartbeat |
| `delayed` | zset | score = run_at; holds scheduled jobs and retry backoffs |
| `rl:{tenant}` | hash | token bucket state (Lua) |
| `running:{tenant}` | counter | per-tenant concurrency |
| `workers:{id}` | string w/ TTL | worker liveness (for autoscaler/dashboard) |

---

## Week 1: Core Pipeline & Multi-Worker Coordination

### Day 1: Project skeleton
- **Learn:** why async job systems exist; Docker Compose basics; the 12-factor app config.
- **Build:** repo layout, `uv` project, ruff, pytest, `docker-compose.yml` (api, postgres, redis), FastAPI `/healthz`, GitHub Actions CI (lint + tests).
- **Done when:** `docker compose up` gives a healthy API, and CI is green on a PR.

### Day 2: Job model & submission API
- **Learn:** state machines for job lifecycles; idempotency keys (read Stripe's "Designing robust and predictable APIs with idempotency").
- **Build:** SQLAlchemy models + Alembic migration for `jobs`; `POST /jobs` (with `Idempotency-Key` header), `GET /jobs/{id}`, `GET /jobs?status=`. An explicit, tested state-transition function.
- **Done when:** submitting twice with the same key returns the same job, and invalid transitions raise errors in tests.

### Day 3: Queue + single worker (happy path)
- **Learn:** Redis lists, `BLMPOP`; the producer/consumer pattern; the transactional-outbox problem (what if the DB commit succeeds but the Redis push fails?).
- **Build:** enqueue on submit (commit to DB first, then push to Redis); worker loop; handler registry with demo handlers (`sleep`, `hash`, `fake_email`, `flaky`); status updates to Postgres.
- **Done when:** submitted jobs move `queued → running → succeeded` with results stored.

### Day 4: Distributed locks & multiple workers
- **Learn:** `SET NX PX`; why release must be compare-and-delete (Lua); read Martin Kleppmann, *"How to do distributed locking"*.
- **Build:** lease acquire/release via Lua; scale to 3 workers with `docker compose up --scale worker=3`; worker IDs.
- **Done when:** 1,000 jobs across 3 workers, each claimed exactly once (checked in the logs/DB).

### Day 5: Heartbeats & orphan reclaim
- **Learn:** leases vs. locks; failure detectors; choosing timeouts (lease TTL vs. heartbeat interval).
- **Build:** heartbeat task renewing the lease (Lua compare-and-pexpire); a **monitor** process that finds `running` jobs with no live lease and requeues them (incrementing `attempts`); reconciliation that requeues `queued` jobs missing from Redis.
- **Done when:** `kill -9` on a worker mid-job causes the job to be reclaimed and finished by another worker.

### Day 6: Fencing tokens (fix #1 implemented)
- **Learn:** the paused-process problem; fencing tokens (Kleppmann's article, part 2).
- **Build:** `lease_token` incremented atomically on every claim; all worker writes conditional on the token; `effects` table written within the same fenced transaction; a fenced worker logs and aborts.
- **Done when:** a test pauses worker A past its lease, lets B finish, resumes A, and A's writes are rejected, leaving exactly one `effects` row.

### Day 7: Buffer + consolidate
- Write tests for anything skipped, clean up, update the README, and write a short "what I learned this week" note (raw material for the final write-up).

---

## Week 2: Reliability, Scheduling, Orchestration, Multi-Tenancy

### Day 8: Retries with exponential backoff + jitter
- **Learn:** AWS Architecture Blog, *"Exponential Backoff and Jitter"*; retryable vs. non-retryable errors.
- **Build:** a **scheduler** process; failed jobs go to the `delayed` zset with `run_at = now + base·2^attempt ± jitter`; the scheduler moves due jobs to the ready queues atomically (Lua).
- **Done when:** a `flaky` job (fails 2 times, then succeeds) completes on attempt 3 with the backoff visible in timestamps.

### Day 9: Dead-letter queue & priority tiers
- **Learn:** poison messages; starvation in priority queues.
- **Build:** after `max_attempts` the status becomes `dead`; `GET /dlq`, `POST /dlq/{id}/retry`; 3 priority tiers with `BLMPOP` order; an optional anti-starvation rule (e.g. every Nth pop takes from low).
- **Done when:** an always-failing job lands in the DLQ and can be retried via the API, and high-priority jobs overtake a backlog of low ones.

### Day 10: Scheduled jobs & cron
- **Learn:** cron semantics; avoiding duplicate fires when multiple schedulers run; clock skew.
- **Build:** `run_at` on submit (→ `scheduled` status + delayed zset); `cron_schedules` table + API; the scheduler fires due crons with idempotency key `cron:{id}:{fire_time}` so a double-fire can't create two jobs.
- **Done when:** a `*/1 * * * *` cron creates exactly one job per minute even with 2 scheduler replicas running.

### Days 11–12: DAG workflows
- **Learn:** DAGs, topological sort, Kahn's algorithm, cycle detection; how Airflow/Temporal model dependencies.
- **Build:**
  - `POST /workflows` with a list of jobs + dependency edges; reject cycles (400).
  - Jobs with dependencies start as `blocked` with `pending_deps = count`.
  - On success, in **one transaction**: decrement children's `pending_deps`; any child reaching 0 becomes `queued` and is enqueued.
  - On a permanent failure, downstream jobs become `cancelled` (upstream failed).
  - `GET /workflows/{id}` returns the graph with per-node status.
- **Done when:** a diamond DAG (A → B, C → D) runs in the correct order, and failing B cancels D but C still runs.

### Day 13: Multi-tenancy & rate limiting
- **Learn:** token bucket vs. leaky bucket vs. sliding window; why Lua makes Redis operations atomic; noisy-neighbor problems.
- **Build:** `tenants` table, API-key auth (hashed keys); token-bucket rate limit on submission (`429` + `Retry-After`); a per-tenant **max concurrent running jobs** cap enforced at claim time (over-cap jobs go back to the delayed set briefly).
- **Done when:** tenant A flooding the system can't push tenant B's latency up, verified with a quick load test.

### Day 14: Buffer + consolidate
- Integration tests for retries/DLQ/DAG/cron, refactor, README update, weekly learning note.

---

## Week 3: Observe It, Break It, Scale It, Measure It

### Day 15: Metrics & structured logs
- **Learn:** RED (Rate, Errors, Duration) and USE methods; Prometheus counters/gauges/histograms; label cardinality.
- **Build:** `/metrics` on API and workers: jobs submitted/succeeded/failed/retried/dead, queue depth per tier, job duration histogram, lease reclaims, fenced writes rejected, rate-limit rejections. JSON logs with `job_id`, `tenant_id`, `worker_id`, `lease_token`.
- **Done when:** Prometheus scrapes all services and the metrics show up in queries.

### Day 16: Grafana dashboard
- **Learn:** percentiles from histograms (`histogram_quantile`); basic alerting rules.
- **Build:** a provisioned Grafana dashboard (it ships in `deploy/`, so it appears on `docker compose up`): throughput, p50/p95/p99 latency, queue depth, workers alive, reclaims, DLQ size. Two alert rules (DLQ growing, queue depth high).
- **Done when:** the dashboard comes up automatically and visibly reacts when you kill a worker.

### Day 17: Chaos testing + correctness checker
- **Learn:** chaos engineering principles; how Jepsen-style checkers verify invariants.
- **Build:** a `chaos/` runner that submits 10k jobs (including DAGs) while randomly running `kill -9` on workers, `docker pause`-ing workers past their lease (tests fencing), restarting the monitor/scheduler, and briefly restarting Redis (tests reconciliation). A **checker** asserts that every job is terminal, that no job has more than 1 `effects` row, and that DAG order was respected.
- **Done when:** `make chaos` passes repeatedly and prints a summary (jobs, kills, pauses, reclaims, fenced writes, violations = 0).

### Day 18: Autoscaling + graceful shutdown
- **Learn:** control loops; hysteresis and cooldowns (avoiding flapping); SIGTERM handling.
- **Build:** workers drain on SIGTERM (finish the current job, stop claiming new ones); an **autoscaler** service that reads queue depth from Redis and scales workers via the Docker SDK between min and max, with a cooldown. (Optional: a note on how the same logic maps to Kubernetes HPA/KEDA.)
- **Done when:** a load spike scales workers from 2 up to 8 and back down with no jobs lost (chaos checker still passes).

### Day 19: Benchmarks
- **Learn:** throughput vs. latency, Little's Law, coordinated omission.
- **Build:** a `bench/` load generator; measure throughput vs. worker count (1, 2, 4, 8, 16), p50/p99 end-to-end latency, and **recovery time** after a worker kill; matplotlib graphs saved to `docs/`.
- **Done when:** the graphs and a results table are committed.

### Day 20: Postgres `SKIP LOCKED` backend (stretch comparison)
- **Learn:** row-level locks, `SELECT ... FOR UPDATE SKIP LOCKED`; why many teams use Postgres as a queue.
- **Build:** a `QueueBackend` interface with `RedisBackend` and `PostgresBackend` behind it (config flag); rerun the benchmarks and the chaos suite on both.
- **Done when:** there's a comparison table (throughput, latency, failure behavior, operational complexity) with a written conclusion.

### Day 21: Write-up & polish
- README: architecture diagram, guarantees (the honest version), design decisions and tradeoffs, benchmark graphs, chaos results, known limitations (single Redis node; Sentinel/Cluster/etcd as next steps), and a 1-command demo.
- Record a demo GIF/video: submit a DAG, kill workers, watch Grafana recover.
- Draft resume bullets with real numbers from your benchmarks.

---

## What to cut if behind

Cut in this order, so the core story survives:
1. Day 20 (Postgres backend comparison): keep it as a "next steps" section.
2. Grafana alert rules (keep the dashboard).
3. Anti-starvation for priorities.
4. Autoscaler: fall back to a documented manual `--scale` experiment.

**Never cut:** fencing tokens, the chaos checker, or benchmarks. They make up most of the project's resume value.

## Target Resume Bullets (fill in real numbers)

- Built a distributed job orchestrator (FastAPI, Redis, PostgreSQL) supporting DAG workflows, cron scheduling, priority queues, retries with exponential backoff, and a dead-letter queue.
- Guaranteed exactly-once effects under failures using lease-based distributed locks with heartbeats and **fencing tokens**; verified with a chaos suite injecting worker kills and pauses across **N k jobs with 0 lost and 0 duplicated**.
- Added multi-tenant isolation (token-bucket rate limiting, per-tenant concurrency caps) and a queue-depth autoscaler; sustained **X jobs/s** with p99 latency of **Y ms** at 8 workers.
- Instrumented with Prometheus/Grafana; benchmarked Redis vs. Postgres `SKIP LOCKED` queue backends.
