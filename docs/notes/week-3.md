# Week 3 notes: observe it, break it, scale it, measure it

## Concepts

| Day | Concept | The one thing to remember |
|---|---|---|
| 15 | RED metrics | Rate, Errors, Duration for every service, plus the events specific to *this* system: reclaims, fenced writes, lost leases, deferrals. |
| 15 | Label cardinality | Each distinct label value is a separate time series. `tenant` or `job_id` labels grow without bound and take Prometheus down. Per-job detail belongs in logs. |
| 15 | Histograms, not averages | Averages hide the tail. Histograms let you compute p50/p95/p99 across all instances with `histogram_quantile`. |
| 15 | Structured logs + context | Bind job_id, tenant_id, worker_id and lease_token once (contextvars); every log line in that scope carries them. One search reconstructs a job's history across workers and attempts. |
| 16 | Service discovery | Prometheus resolves the Compose service name (one A record per replica) to scrape every worker. Scaling needs no config change. |
| 16 | Alert on symptoms and on "should never happen" | DLQ growing and backlog high are symptoms. `FencedWritesSeen` is different: correctness held, but it means something is stalling workers past their lease. |
| 17 | Chaos engineering | Inject the failures you claim to survive (SIGKILL, freeze, control-plane restart, losing Redis entirely) while the system is under load, then *check invariants*, not just "did it finish". |
| 17 | Jepsen-style checking | Assert safety (exactly one effect per success, carrying its final claim's token; none otherwise) and liveness (everything settles) from the source of truth. |
| 18 | Control loops and hysteresis | Scale up immediately, scale down only after a stabilization window (HPA-style); otherwise spiky load flaps. |
| 18 | Graceful shutdown | SIGTERM means stop taking work, finish the current job, exit. Scale-down then never interrupts a job. |
| 19 | Open-loop load | A benchmark that waits for each response before sending the next slows down exactly when the system does, and hides its worst latencies ("coordinated omission"). |
| 19 | Benchmark hygiene | Warm up first, and distrust results that are too good: the first run measured *super-linear* scaling, which turned out to be cold caches. |
| 20 | Postgres as a queue | `SELECT … FOR UPDATE SKIP LOCKED` plus a partial index on queued rows. No dual write, and the pick and claim happen in one transaction. The cost: no blocking pop, so idle workers poll. |

## Design decisions I can defend

- **Redis loss is a recoverable event, not a disaster.** The chaos suite SIGKILLs Redis (no save), losing every queue entry and lease. The monitor reclaims running jobs that no longer have leases and re-pushes queued IDs missing from Redis, and fencing guarantees no double effects from workers that were mid-job. Every invariant held.
- **Fencing as the backstop, heartbeats as the first line.** In the chaos runs, stalled workers usually discovered their lost lease via the heartbeat and abandoned the job, so fenced writes were rare (0 in the long runs). The Day 6 demo shows fencing catching the case the heartbeat can't: waking up just as the job finishes.
- **The autoscaler runs outside the cluster's critical path.** If it dies, the current worker count stays. It's a Compose-specific prototype of what KEDA does on Kubernetes (Redis list-length trigger plus HPA behaviour).
- **The dashboard is generated code.** `build_dashboard.py` emits the JSON, and `check_dashboard.py` runs every panel query against Prometheus, so a renamed metric can't silently blank a panel.

## Proof

- Chaos suite (Redis backend): 1,740 jobs including 60 workflows; 4 worker kills, 7 worker freezes past the lease, 1 control-plane restart, 2 Redis kills. PASS: every invariant held.
- Chaos suite (Postgres backend): 920 jobs; 6 kills, 2 freezes, 1 Redis kill. PASS.
- Autoscaler: a 1,077-job backlog scaled workers 2 → 8 immediately, then 8 → 7 → 3 → 2 as the window aged out. 1,500 jobs ran with `attempts=1` each: no scale-down interrupted a job.
- Alerting: with workers scaled to 0, `NoWorkers` went pending, then firing after its 1-minute hold.
- Benchmarks ([details](../benchmarks/README.md)): about 620 jobs/s of pure coordination at 4 workers, with Postgres commits as the ceiling. 50 ms jobs scale linearly (14.8 → 119.7 jobs/s from 1 → 8 workers). p99 end-to-end latency was 71 ms at 20 jobs/s on the Redis queue. A killed worker's job runs again in ≈4.2 s with a 4 s lease.
- A benchmark result I didn't trust: 1 → 2 workers looked super-linear. The cause was the WAL flush (group commit), proven by re-measuring with `synchronous_commit = off`: one worker went 201 → 607 jobs/s, and scaling became normal.

## Interview questions this answers

- *How would you know your exactly-once claim holds in production?* Use metrics for the mechanisms (reclaims, fenced writes, lost leases), alerts on the anomalies, and a chaos suite that checks invariants from the source of truth after injecting the failures you claim to survive.
- *Why not label metrics by tenant?* Cardinality. Use logs, or a per-tenant query against Postgres, for per-tenant drill-down.
- *How do you autoscale queue workers without flapping?* Size workers to the backlog, scale up fast, and scale down only to the highest recommendation within a stabilization window. Shut down gracefully.
- *Redis or Postgres for a job queue?* Measured here: identical throughput (Postgres commits are the ceiling either way), Redis about 15 ms lower median queue wait thanks to blocking pops, and Postgres-only removes a dual write. Pick Postgres for simplicity, and Redis when latency matters, or when queue throughput (not commits) is the bottleneck.
