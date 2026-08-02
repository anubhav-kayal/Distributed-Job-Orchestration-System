# Distributed-Job-Orchestration-System

A distributed, fault-tolerant job processing system. Clients submit long-running async jobs; independent worker processes execute them with guaranteed exactly-once processing — even when workers crash mid-job.

## Status
🚧 Work in progress — building in public. See [Roadmap](#roadmap) below.

## The Problem

Background job systems are easy until a worker dies mid-task. Naive designs either:
- **Lose the job** (nobody notices the worker died), or
- **Duplicate it** (a slow-but-alive worker gets mistaken for a dead one and another worker re-runs the same job).

forge-queue solves this with distributed locks, heartbeats, and a monitor process that reclaims orphaned jobs safely.

## Architecture (planned)

```
Client → API (FastAPI) → Redis (priority queues + distributed locks)
                              ↓
                    Worker containers (N, independent)
                              ↓
                        PostgreSQL (durable job state)
                              ↑
                     Monitor (detects & reclaims orphaned jobs)
```

## Core Guarantees

- **Exactly-once processing** — Redis `SET NX EX` locks ensure only one worker ever claims a job
- **Failure detection** — workers heartbeat by renewing their lock; a dead worker's lock expires and the job is reclaimed
- **Durable state** — job history lives in Postgres, not just Redis
- **Idempotent submission** — retried client requests (via idempotency key) never create duplicate jobs
- **Retry + dead-letter queue** — failed jobs retry with exponential backoff, then land in a DLQ for inspection

## Tech Stack

- **API:** FastAPI
- **Queue & Coordination:** Redis (priority queues, distributed locks)
- **Durable State:** PostgreSQL
- **Orchestration:** Docker Compose (multi-worker)

## Roadmap

- [ ] Week 1 — Core pipeline: API, single worker, happy-path job execution
- [ ] Week 2 — Multi-worker coordination: distributed locks, heartbeats, reclaim logic
- [ ] Week 3 — Reliability: retries with backoff, dead-letter queue, priority tiers, idempotency
- [ ] Week 4 — Chaos testing: scripted worker kills, correctness verification, results write-up

## Known Limitations

Redis is a single point of failure for coordination in this design (a production system would use Redis Sentinel/Cluster or a consensus-based coordinator like etcd). This project focuses on correct multi-worker coordination logic, not full node-level fault tolerance.

## Getting Started

```bash
# coming soon
docker compose up
```

## License

MIT