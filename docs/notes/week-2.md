# Week 2 notes: reliability, scheduling, orchestration

## Days 8–10: retries, the dead-letter queue, fairness, and scheduled and cron jobs

### Concepts

| Day | Concept | The one thing to remember |
|---|---|---|
| 8 | Exponential backoff | Each failure doubles the wait ceiling, so a struggling dependency gets more room to recover. |
| 8 | Jitter | Without randomness, every job that failed together retries together, as a synchronized wave. Full jitter (`uniform(0, ceiling)`) spreads them out (AWS, "Exponential Backoff and Jitter"). |
| 8 | Retryable vs. permanent | Retrying a bad input or an unknown job type just burns attempts. Handlers raise `PermanentError`, and the worker treats an unknown type or invalid payload the same way. |
| 9 | Dead-letter queue | A job that can't succeed must not vanish (data loss) or loop forever (resource leak). It's parked as `dead` for a human to inspect and retry. |
| 9 | Poison messages | A job that crashes its worker every time. Reclaims count as attempts, so it ends in the DLQ instead of killing workers forever. |
| 9 | Starvation | Strict priority starves `low` while `high` keeps arriving. A weighted rotation guarantees every tier a share under contention and changes nothing without contention. |
| 10 | Scheduled jobs | `run_at` / `delay_seconds` make a job start as `scheduled`; the scheduler promotes it to `queued` when due. Retries use the same path. |
| 10 | Cron and exactly-once firing | Lock due schedules with `SKIP LOCKED`, create the job and advance `next_fire_at` in **one transaction**, and add an idempotency key per fire time as a second guard. |
| 10 | One clock | Replicas on different machines disagree about the time. Use the database's `now()` so every scheduler agrees on what's due. |

### Design decisions I can defend

- **The scheduler reads Postgres, not a Redis sorted set.** The classic design (Sidekiq's retry set) keeps due times in a Redis ZSET. That would be a second copy of "when does this run", which is another dual write with its own failure cases. Querying Postgres via a `(status, run_at)` index keeps one source of truth, and `FOR UPDATE SKIP LOCKED` makes any number of scheduler replicas safe without leader election.
- **`failed` became `dead`, with a data migration.** The migration drops the CHECK constraint, converts the rows, then adds the new constraint, so neither constraint ever sees a row it would reject. Autogenerate can't see changes inside a CHECK constraint, so it's written by hand. Verified upgrade → downgrade → upgrade on real dev data.
- **A DLQ retry resets `attempts` but never `lease_token`.** This is the payoff of keeping the fencing token separate from the attempt counter. If the token were `attempts`, a retry would send it backwards, and a stale worker from before the job died could become "current" again.
- **Adding optional fields must not change idempotency fingerprints.** The fingerprint hashes the request body. Adding `run_at` and `delay_seconds` (default null) would have changed every request's hash, so replaying an old `Idempotency-Key` with the same body would suddenly be a 422. Unset new fields are left out of the hash, and a test pins the old fingerprint.
- **Missed cron fires run once, then skip ahead.** After an outage, replaying every missed run (60 jobs for an hour of an every-minute schedule) is usually wrong and can overload downstream systems. Kubernetes CronJobs make the same default choice.
- **A broken schedule doesn't block the others.** If a schedule's stored template stops validating (its job type removed in a deploy), that fire is skipped and logged. Otherwise one bad row would roll back the whole batch on every tick.
- **Fairness uses smooth weighted round-robin** (nginx's algorithm), so tiers interleave (H N H H N H L H N H) rather than burst. Each worker keeps its own rotation, so the global shares are approximate.

### Proof

- `flaky` with `fail_times=2` on real containers: retried after 0.64s, then 1.43s (each within its 1s and 2s window), and succeeded on attempt 3.
- An always-failing job died after 3 attempts. A DLQ retry ran 3 more (6 total; `lease_token` went 3 → 6). A permanent error died after exactly 1 attempt.
- Every-minute cron with two schedulers ticking every 10s in simulated time (often at the same instant): exactly 5 jobs in 5 minutes. On real containers, with two scheduler replicas for 4 minutes: exactly 4 jobs, one per minute, fired 2 by each replica, 0 duplicates.

### Interview questions this answers

- *How do you retry safely?* Use exponential backoff with jitter, cap the ceiling, cap the attempts, separate retryable from permanent errors, and park exhausted jobs in a DLQ. Retries are only safe because effects are idempotent or fenced.
- *Why jitter?* To de-correlate clients that failed for the same reason, so the recovering dependency doesn't get hit by synchronized waves.
- *How do you run cron with high availability without double-firing?* Run several schedulers. Each locks due schedules with `SKIP LOCKED`, then inserts the job and advances the schedule atomically. An idempotency key per (schedule, fire time) makes a duplicate impossible even if that logic were bypassed.
- *How do you add a field to an API that uses idempotency keys?* Make sure old requests still hash the same. Exclude unset new fields from the fingerprint, and pin the old fingerprint in a test.
