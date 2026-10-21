# Week 1 notes: core pipeline and coordination

What I built and the reasoning behind each decision, written so I can explain it in an interview.

## The one-paragraph version

A client submits a job, and the API stores it in Postgres, then pushes its ID onto a Redis list. A worker pops the ID, takes a **lease** in Redis (`SET NX PX`), and **claims** the job in Postgres under a row lock, which bumps a **fencing token**. It runs the handler while a **heartbeat** keeps the lease alive, and records the result **only if its token is still current**. A **monitor** reclaims jobs whose lease vanished (dead worker) and re-pushes jobs whose ID never made it into Redis. The guarantee is *at-least-once execution + fencing = exactly-once effects*, verified by killing workers under load.

## Concepts, day by day

| Day | Concept | The one thing to remember |
|---|---|---|
| 1 | Liveness vs. readiness | Liveness failing means *restart me*, so it must never check dependencies, or a DB outage causes a restart storm. |
| 1 | 12-factor config | Same image everywhere; only environment variables change (`postgres:5432` in Compose, `localhost:55432` on the host). |
| 2 | Idempotency keys | Never check-then-insert. Let the unique index decide atomically: `INSERT … ON CONFLICT DO NOTHING`. |
| 2 | State machine | Every status change goes through one function, and the allowed transitions are a table you can test exhaustively. |
| 2 | `SELECT … FOR UPDATE` | Locks the row so "read status, decide, write" can't interleave with another transaction. |
| 3 | Dual writes | Two systems can't share a transaction. Write the durable one first (Postgres), then the derived one (Redis), and make the derived one rebuildable. |
| 3 | "Redis carries IDs, Postgres decides" | Queue delivery is at-least-once. Claiming through Postgres makes duplicate deliveries harmless. |
| 4 | Leases | A lock with a TTL, so a dead holder can't hold it forever. Renew and release must be compare-and-set (Lua), never GET-then-DEL. |
| 5 | Heartbeats | A long job renews its lease every TTL/3. Losing the lease means stop working, because someone else is about to take over. |
| 5 | Failure detection | You can't tell "dead" from "slow". A missing lease means *presumed* dead, which is exactly why fencing is needed. |
| 6 | Fencing tokens | A monotonically increasing number per claim. The storage layer rejects writes carrying an old number. This is the only defense against a worker that doesn't *know* it was paused. |

## Design decisions I can defend

- **Postgres is the source of truth; Redis is disposable.** Every Redis structure (queues, leases) can be rebuilt or recovered from Postgres by the monitor. A Redis outage delays jobs but can't lose them.
- **Lease first, then claim.** Since a `running` job always had a lease at claim time, "running with no lease" reliably means a dead worker.
- **The token lives in Postgres, not Redis.** If Redis restarts empty, a Redis `INCR` counter would restart at 1 and every new claim would look stale. Postgres's `lease_token` only ever grows.
- **The effect is written in the same transaction as the fenced status change.** The effect exists if and only if that claim's success was recorded.
- **No unique constraint on `effects.job_id`.** The chaos checker must be able to *see* a duplicate; a constraint would hide the bug by turning it into an error.
- **Reclaimed jobs count as attempts.** A job that keeps killing its worker ("poison job") ends as `failed` instead of looping forever.

## Proof

- **1,000 jobs on 3 workers:** every job `attempts=1` with exactly one effect.
- **1,000 jobs with a worker SIGKILLed every 4s:** 0 lost and 0 duplicated effects. The jobs caught mid-flight were re-run (attempts 2–3) and still had exactly one effect.
- **`docker pause` near the end of a job:** the woken worker finished its handler unaware it had lost the lease. Its write was rejected (`fenced: our token 1 … is stale (job is succeeded with token 2)`).
- **Mutation check:** with the token comparison removed, the "stale worker while the re-run is in progress" test fails. A status check alone isn't enough, because the job looks `running` to both workers.

## Bugs found by an end-of-week review (and fixed)

1. **Same-tick race.** If the handler finished in the same event-loop tick that the heartbeat detected lease loss, awaiting the heartbeat re-raised `LeaseLost` and the result was thrown away, forcing a needless re-run. Fix: always attempt the fenced write once the handler has finished, since fencing decides whether it counts.
2. **No Redis read timeout.** A half-open connection (TCP alive, Redis gone) would hang heartbeats and monitor sweeps forever. Fix: a 5s read timeout, validated at startup to exceed the BLMPOP block time.
3. **One error aborted a whole monitor sweep.** Fix: per-job error isolation.
4. **Monitor starvation.** Only the oldest 100 candidates were ever checked. Fix: page through all running jobs, and use a cursor for the stranded check.

Lesson: the happy path and even the chaos test passed with all four bugs present. Timing and partial-failure bugs need targeted tests (a black-hole Redis server, forced same-tick completion), not just load.

## Interview questions this week answers

- *How do you guarantee a job runs exactly once?* You can't guarantee exactly-once execution; you get at-least-once plus fencing and idempotency, which gives exactly-once effects. Then explain why, with the paused-worker example.
- *What's wrong with Redis-based distributed locks?* A lock holder can pause past its TTL and not know it. Locks give liveness, not safety. Safety comes from fencing at the resource (Kleppmann's argument against relying on Redlock alone).
- *How do you detect a dead worker?* Leases with a TTL renewed by heartbeats. Pick the heartbeat interval at no more than TTL/2 (we use TTL/3), and accept that detection takes up to one TTL.
- *What happens if Redis goes down?* Submissions still succeed (the job is stored in Postgres). Workers back off and retry. When Redis returns, the monitor re-pushes stranded jobs. Nothing is lost; jobs are only delayed.
- *Why not just use the database as the queue?* You can (`SKIP LOCKED`), and that's the Week 3 comparison. Redis gives cheap blocking pops and TTL-based leases; Postgres gives one system to operate.
