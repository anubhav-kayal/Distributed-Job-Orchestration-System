"""Generates dashboards/djos.json. Edit this file, not the JSON:

uv run python deploy/grafana/build_dashboard.py
"""

import json
from pathlib import Path

DS = {"type": "prometheus", "uid": "prometheus"}
JOBS_PER_S = "suffix: jobs/s"  # Grafana's "reqps" would label job rates "req/s"
_ids = iter(range(1, 1000))


def target(expr: str, legend: str = "", ref: str = "A") -> dict:
    return {"datasource": DS, "expr": expr, "legendFormat": legend, "refId": ref}


def panel(kind: str, title: str, targets: list[dict], x: int, y: int, w: int, h: int, **extra):
    return {
        "id": next(_ids),
        "type": kind,
        "title": title,
        "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [{**t, "refId": chr(ord("A") + i)} for i, t in enumerate(targets)],
        **extra,
    }


def stat(title, expr, x, unit="short", decimals=0, description=""):
    return panel(
        "stat",
        title,
        [target(expr)],
        x,
        0,
        4,
        4,
        description=description,
        fieldConfig={"defaults": {"unit": unit, "decimals": decimals}, "overrides": []},
        options={"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "value"},
    )


def series(title, targets, x, y, w=12, h=8, unit="short", stack=False, description=""):
    custom = {
        "fillOpacity": 20 if stack else 0,
        "stacking": {"mode": "normal" if stack else "none"},
    }
    return panel(
        "timeseries",
        title,
        targets,
        x,
        y,
        w,
        h,
        description=description,
        fieldConfig={"defaults": {"unit": unit, "custom": custom}, "overrides": []},
        options={"legend": {"displayMode": "list", "placement": "bottom"}},
    )


def quantiles(metric: str, by: str = "") -> list[dict]:
    group = f"le{', ' + by if by else ''}"
    suffix = f" {{{{{by}}}}}" if by else ""
    return [
        target(
            f"histogram_quantile({q}, sum by ({group}) (rate({metric}_bucket[1m])))",
            f"p{int(q * 100)}{suffix}",
        )
        for q in (0.5, 0.95, 0.99)
    ]


PANELS = [
    stat(
        "Throughput",
        'sum(rate(djos_jobs_finished_total{outcome="succeeded"}[1m]))',
        0,
        unit=JOBS_PER_S,
        decimals=1,
        description="Jobs succeeding per second",
    ),
    stat("Waiting in queue", "sum(djos_queue_depth)", 4),
    stat("Running", 'djos_jobs{status="running"}', 8),
    stat("Dead-letter queue", 'djos_jobs{status="dead"}', 12),
    stat("Workers up", 'sum(up{job="worker"})', 16),
    stat(
        "Fenced writes (1h)",
        "sum(increase(djos_fenced_writes_total[1h]))",
        20,
        description="Stale workers whose results were rejected",
    ),
    series(
        "Job outcomes / s",
        [target("sum by (outcome) (rate(djos_jobs_finished_total[1m]))", "{{outcome}}")],
        0,
        4,
        unit=JOBS_PER_S,
        stack=True,
    ),
    series(
        "Queue depth by priority",
        [target("sum by (priority) (djos_queue_depth)", "{{priority}}")],
        12,
        4,
        stack=True,
    ),
    series("Job run time", quantiles("djos_job_duration_seconds"), 0, 12, unit="s"),
    series(
        "Queue wait (runnable → claimed)",
        quantiles("djos_job_queue_wait_seconds", "priority"),
        12,
        12,
        unit="s",
        description="How long jobs wait for a worker; rises when workers can't keep up",
    ),
    series(
        "Jobs by status",
        [target('djos_jobs{status!="succeeded"}', "{{status}}")],
        0,
        20,
        description="Everything except succeeded (which only grows)",
    ),
    series(
        "Failure handling / s",
        [
            target("sum by (outcome) (rate(djos_reclaims_total[1m]))", "reclaimed: {{outcome}}"),
            target("sum(rate(djos_fenced_writes_total[1m]))", "fenced writes"),
            target("sum(rate(djos_leases_lost_total[1m]))", "leases lost"),
            target("sum(rate(djos_jobs_deferred_total[1m]))", "deferred (tenant cap)"),
            target("sum(rate(djos_rate_limited_total[1m]))", "rate limited (429)"),
        ],
        12,
        20,
        unit="suffix: /s",
    ),
    series(
        "API requests / s",
        [target("sum by (status) (rate(djos_http_requests_total[1m]))", "{{status}}")],
        0,
        28,
        unit="reqps",
        stack=True,
    ),
    series(
        "API latency p99 by route",
        [
            target(
                "histogram_quantile(0.99, sum by (le, route) "
                "(rate(djos_http_request_duration_seconds_bucket[1m])))",
                "{{route}}",
            )
        ],
        12,
        28,
        unit="s",
    ),
]

DASHBOARD = {
    "uid": "djos",
    "title": "Distributed Job Orchestration",
    "tags": ["djos"],
    "timezone": "browser",
    "schemaVersion": 39,
    "version": 1,
    "refresh": "5s",
    "time": {"from": "now-15m", "to": "now"},
    "panels": PANELS,
}

if __name__ == "__main__":
    out = Path(__file__).parent / "dashboards" / "djos.json"
    out.write_text(json.dumps(DASHBOARD, indent=2) + "\n")
    print(f"wrote {out} ({len(PANELS)} panels)")
