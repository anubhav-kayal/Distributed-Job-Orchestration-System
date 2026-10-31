import time

from fastapi import APIRouter, FastAPI, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from djos.core import metrics

router = APIRouter(tags=["health"])


@router.get("/metrics", include_in_schema=False)
def prometheus_metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


def instrument(app: FastAPI) -> None:
    """Count and time every request, labelled by route *template* (`/jobs/{job_id}`),
    never the raw path: one label value per job id would be unbounded cardinality."""

    @app.middleware("http")
    async def record(request: Request, call_next):
        started = time.perf_counter()
        response = await call_next(request)
        route = request.scope.get("route")
        template = getattr(route, "path", "unmatched")
        if template != "/metrics":
            metrics.HTTP_REQUESTS.labels(request.method, template, response.status_code).inc()
            metrics.HTTP_LATENCY.labels(request.method, template).observe(
                time.perf_counter() - started
            )
        return response
