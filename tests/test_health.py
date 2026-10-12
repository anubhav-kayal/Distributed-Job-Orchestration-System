import pytest
from fastapi.testclient import TestClient

from djos.api.main import create_app
from djos.core.config import Settings


def make_client(**overrides) -> TestClient:
    return TestClient(create_app(Settings(**overrides)))


def test_healthz_is_ok_without_dependencies():
    # Liveness must not depend on Postgres/Redis, so point both at dead ports.
    with make_client(
        database_url="postgresql+asyncpg://x:x@127.0.0.1:1/x",
        redis_url="redis://127.0.0.1:1/0",
    ) as client:
        resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_readyz_is_503_when_dependencies_are_down():
    with make_client(
        database_url="postgresql+asyncpg://x:x@127.0.0.1:1/x",
        redis_url="redis://127.0.0.1:1/0",
        dependency_timeout_seconds=0.5,
    ) as client:
        resp = client.get("/readyz")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "unavailable"
    assert body["checks"]["postgres"].startswith("error")
    assert body["checks"]["redis"].startswith("error")


@pytest.mark.integration
def test_readyz_is_ok_with_real_dependencies():
    with make_client() as client:
        resp = client.get("/readyz")
    assert resp.status_code == 200, resp.json()
    assert resp.json() == {"status": "ready", "checks": {"postgres": "ok", "redis": "ok"}}
