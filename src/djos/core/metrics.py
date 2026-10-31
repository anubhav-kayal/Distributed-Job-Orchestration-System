"""Prometheus metrics, defined once and shared by every process.

Naming follows Prometheus conventions (`_total` counters, `_seconds` histograms) and the
RED method: Rate (submitted/finished), Errors (outcome="dead", fenced writes, reclaims),
Duration (job and queue-wait histograms).

Labels are deliberately low-cardinality: job type, priority and outcome have a handful of
values each. tenant and job id are *not* labels: every distinct label value is a separate
time series, so per-tenant or per-job labels would grow without bound. Per-job detail
belongs in logs (which carry job_id); per-tenant numbers come from Postgres.
"""

from prometheus_client import Counter, Gauge, Histogram, start_http_server

# Buckets in seconds, from 5ms to 5 minutes: covers quick jobs and long ones.
_DURATION_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300)

# --- API ---
HTTP_REQUESTS = Counter(
    "djos_http_requests_total", "HTTP requests handled", ["method", "route", "status"]
)
HTTP_LATENCY = Histogram(
    "djos_http_request_duration_seconds", "HTTP request latency", ["method", "route"]
)
JOBS_SUBMITTED = Counter(
    "djos_jobs_submitted_total", "Jobs accepted via the API", ["type", "priority"]
)
RATE_LIMITED = Counter("djos_rate_limited_total", "Submissions refused by a tenant rate limit")

# --- Workers ---
JOBS_FINISHED = Counter(
    "djos_jobs_finished_total",
    "Job runs that ended, by what happened next",
    ["type", "outcome"],  # outcome: succeeded | retry | dead
)
JOB_DURATION = Histogram(
    "djos_job_duration_seconds", "Handler run time", ["type"], buckets=_DURATION_BUCKETS
)
QUEUE_WAIT = Histogram(
    "djos_job_queue_wait_seconds",
    "Time from runnable (created or due) to claimed",
    ["priority"],
    buckets=_DURATION_BUCKETS,
)
FENCED_WRITES = Counter(
    "djos_fenced_writes_total", "Results rejected because the worker's token was stale"
)
LEASES_LOST = Counter("djos_leases_lost_total", "Jobs a worker abandoned after losing the lease")
JOBS_DEFERRED = Counter(
    "djos_jobs_deferred_total", "Claims put off because the tenant was at its concurrency cap"
)
WORKER_BUSY = Gauge("djos_worker_busy", "1 while this worker is running a job")

# --- Monitor ---
RECLAIMS = Counter("djos_reclaims_total", "Orphaned running jobs taken back", ["outcome"])
STRANDED_REENQUEUED = Counter(
    "djos_stranded_reenqueued_total", "Queued jobs re-pushed because their ID was missing"
)
QUEUE_DEPTH = Gauge("djos_queue_depth", "Job IDs waiting in Redis", ["priority"])
JOBS_BY_STATUS = Gauge("djos_jobs", "Jobs in Postgres by status", ["status"])

# --- Scheduler ---
JOBS_PROMOTED = Counter("djos_jobs_promoted_total", "Scheduled jobs moved to the queue when due")
CRON_FIRES = Counter("djos_cron_fires_total", "Jobs created by cron schedules")


def serve(port: int) -> None:
    """Expose /metrics for a non-HTTP process (worker, monitor, scheduler)."""
    start_http_server(port)
