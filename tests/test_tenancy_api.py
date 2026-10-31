import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from djos.api.main import create_app
from djos.core.config import Settings

pytestmark = pytest.mark.integration

ADMIN_TOKEN = "admin-secret-0123456789"
ADMIN = {"Authorization": f"Bearer {ADMIN_TOKEN}"}
JOB = {"type": "sleep", "payload": {"seconds": 0}}


@pytest.fixture
def make_client(test_settings, clean_state):
    clients = []

    def make(**overrides) -> TestClient:
        overrides = {"admin_token": SecretStr(ADMIN_TOKEN), **overrides}
        settings = test_settings.model_copy(update=overrides)
        client = TestClient(create_app(settings)).__enter__()
        clients.append(client)
        return client

    yield make
    for c in clients:
        c.__exit__(None, None, None)


@pytest.fixture
def api(make_client) -> TestClient:
    return make_client()


def create_tenant(api, name, **limits) -> tuple[dict, dict]:
    resp = api.post("/admin/tenants", json={"name": name, **limits}, headers=ADMIN)
    assert resp.status_code == 201, resp.json()
    tenant = resp.json()
    return tenant, {"Authorization": f"Bearer {tenant['api_key']}"}


def test_admin_api_is_hidden_without_an_admin_token(make_client):
    api = make_client(admin_token=None)
    assert api.post("/admin/tenants", json={"name": "x"}, headers=ADMIN).status_code == 404


def test_admin_token_is_checked(api):
    resp = api.post("/admin/tenants", json={"name": "x"}, headers={"Authorization": "Bearer no"})
    assert resp.status_code == 401
    assert api.get("/admin/tenants").status_code == 401  # no credentials at all


@pytest.mark.parametrize("value", ["", "   "])
def test_an_empty_admin_token_disables_the_admin_api(value, monkeypatch):
    # Regression: an empty token used to enable the admin API with "" as the password,
    # so a request with no Authorization header matched it.
    monkeypatch.setenv("DJOS_ADMIN_TOKEN", value)
    assert Settings().admin_token is None


def test_a_short_admin_token_is_rejected():
    with pytest.raises(ValueError, match="at least 16"):
        Settings(admin_token="short")


def test_create_tenant_returns_the_key_once(api):
    tenant, _ = create_tenant(api, "acme")
    assert tenant["api_key"].startswith("djos_")
    assert tenant["api_key_prefix"] == tenant["api_key"][:12]
    listed = api.get("/admin/tenants", headers=ADMIN).json()["items"]
    assert {t["name"] for t in listed} == {"acme", "default"}
    assert all("api_key" not in t for t in listed)


def test_duplicate_tenant_and_half_specified_limits_are_rejected(api):
    create_tenant(api, "acme")
    assert api.post("/admin/tenants", json={"name": "acme"}, headers=ADMIN).status_code == 409
    resp = api.post("/admin/tenants", json={"name": "b", "burst": 5}, headers=ADMIN)
    assert resp.status_code == 422


def test_tenants_only_see_their_own_jobs(api):
    _, a = create_tenant(api, "a")
    _, b = create_tenant(api, "b")
    job = api.post("/jobs", json=JOB, headers=a).json()

    assert api.get(f"/jobs/{job['id']}", headers=a).status_code == 200
    assert api.get(f"/jobs/{job['id']}", headers=b).status_code == 404
    assert api.get(f"/jobs/{job['id']}").status_code == 404  # the default tenant either
    assert api.post(f"/jobs/{job['id']}/cancel", headers=b).status_code == 404
    assert [j["id"] for j in api.get("/jobs", headers=a).json()["items"]] == [job["id"]]
    assert api.get("/jobs", headers=b).json()["items"] == []


def test_idempotency_keys_and_schedule_names_are_per_tenant(api):
    _, a = create_tenant(api, "a")
    _, b = create_tenant(api, "b")
    ja = api.post("/jobs", json=JOB, headers={**a, "Idempotency-Key": "k"}).json()
    jb = api.post("/jobs", json=JOB, headers={**b, "Idempotency-Key": "k"}).json()
    assert ja["id"] != jb["id"]

    schedule = {"name": "nightly", "cron": "0 2 * * *", "job": JOB}
    assert api.post("/schedules", json=schedule, headers=a).status_code == 201
    assert api.post("/schedules", json=schedule, headers=b).status_code == 201
    assert api.post("/schedules", json=schedule, headers=a).status_code == 409


def test_workflows_and_dlq_are_scoped_too(api):
    _, a = create_tenant(api, "a")
    _, b = create_tenant(api, "b")
    wf = api.post("/workflows", json={"jobs": [{"key": "x", **JOB}]}, headers=a).json()
    assert api.get(f"/workflows/{wf['id']}", headers=b).status_code == 404
    assert api.get("/workflows", headers=b).json()["items"] == []
    assert api.get("/dlq", headers=b).json()["items"] == []


@pytest.mark.parametrize("header", ["Bearer djos_wrong", "Basic abc", "Bearer"])
def test_bad_credentials_are_401(api, header):
    resp = api.post("/jobs", json=JOB, headers={"Authorization": header})
    assert resp.status_code == 401
    assert resp.headers["WWW-Authenticate"] == "Bearer"


def test_keys_can_be_required(make_client):
    api = make_client(require_api_key=True)
    _, a = create_tenant(api, "a")
    assert api.post("/jobs", json=JOB).status_code == 401
    assert api.post("/jobs", json=JOB, headers=a).status_code == 201


def test_rotating_a_key_revokes_the_old_one(api):
    tenant, old = create_tenant(api, "a")
    new_key = api.post(f"/admin/tenants/{tenant['id']}/rotate-key", headers=ADMIN).json()["api_key"]
    assert api.get("/jobs", headers=old).status_code == 401
    assert api.get("/jobs", headers={"Authorization": f"Bearer {new_key}"}).status_code == 200


def test_rate_limit_returns_429_with_retry_after(api):
    _, a = create_tenant(api, "a", rate_limit_per_second=0.5, burst=2)
    assert [api.post("/jobs", json=JOB, headers=a).status_code for _ in range(2)] == [201, 201]

    resp = api.post("/jobs", json=JOB, headers=a)
    assert resp.status_code == 429
    assert resp.headers["Retry-After"] == "2"  # one token refills in 1 / 0.5 s

    _, b = create_tenant(api, "b")  # another tenant is unaffected
    assert api.post("/jobs", json=JOB, headers=b).status_code == 201


def test_workflow_costs_one_token_per_job(api):
    _, a = create_tenant(api, "a", rate_limit_per_second=1, burst=3)
    three = {"jobs": [{"key": k, **JOB} for k in "xyz"]}
    four = {"jobs": [{"key": k, **JOB} for k in "wxyz"]}
    assert api.post("/workflows", json=four, headers=a).status_code == 429  # > burst: never fits
    assert api.post("/workflows", json=three, headers=a).status_code == 201
    assert api.post("/jobs", json=JOB, headers=a).status_code == 429  # bucket is empty


def test_limits_can_be_changed(api):
    tenant, a = create_tenant(api, "a", rate_limit_per_second=1, burst=1)
    api.post("/jobs", json=JOB, headers=a)
    assert api.post("/jobs", json=JOB, headers=a).status_code == 429
    resp = api.put(f"/admin/tenants/{tenant['id']}/limits", json={}, headers=ADMIN)
    assert resp.json()["rate_limit_per_second"] is None
    assert api.post("/jobs", json=JOB, headers=a).status_code == 201
