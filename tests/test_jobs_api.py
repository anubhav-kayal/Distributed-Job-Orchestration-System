import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from djos.api.main import create_app

pytestmark = pytest.mark.integration


def submit(client, body=None, key=None):
    headers = {"Idempotency-Key": key} if key else {}
    return client.post("/jobs", json=body or {"type": "sleep"}, headers=headers)


def test_submit_creates_queued_job_with_defaults(client):
    resp = submit(client, {"type": "sleep", "payload": {"seconds": 1}})
    assert resp.status_code == 201
    job = resp.json()
    assert job["status"] == "queued"
    assert job["priority"] == "normal"
    assert job["attempts"] == 0
    assert job["max_attempts"] == 3
    assert job["payload"] == {"seconds": 1}
    assert job["started_at"] is None


def test_get_job(client):
    job_id = submit(client).json()["id"]
    resp = client.get(f"/jobs/{job_id}")
    assert resp.status_code == 200
    assert resp.json()["id"] == job_id


def test_get_unknown_job_is_404(client):
    assert client.get(f"/jobs/{uuid.uuid4()}").status_code == 404


@pytest.mark.parametrize(
    "body",
    [
        {},  # missing type
        {"type": "Not Valid!"},
        {"type": "sleep", "priority": "urgent"},
        {"type": "sleep", "max_attempts": 0},
        {"type": "sleep", "unexpected": True},
    ],
)
def test_invalid_bodies_are_rejected(client, body):
    assert client.post("/jobs", json=body).status_code == 422


def test_same_idempotency_key_returns_same_job(client):
    first = submit(client, key="order-42")
    second = submit(client, key="order-42")
    assert first.status_code == 201
    assert second.status_code == 200
    assert second.headers["Idempotent-Replayed"] == "true"
    assert second.json()["id"] == first.json()["id"]
    assert len(client.get("/jobs").json()["items"]) == 1


def test_idempotency_key_reused_with_different_body_is_422(client):
    submit(client, {"type": "sleep"}, key="k1")
    resp = submit(client, {"type": "sleep", "payload": {"seconds": 5}}, key="k1")
    assert resp.status_code == 422
    assert "different request body" in resp.json()["detail"]


def test_without_key_every_submit_creates_a_job(client):
    submit(client)
    submit(client)
    assert len(client.get("/jobs").json()["items"]) == 2


def test_concurrent_submits_with_same_key_create_exactly_one_job(client):
    with ThreadPoolExecutor(max_workers=10) as pool:
        responses = list(pool.map(lambda _: submit(client, key="race"), range(10)))

    assert sorted(r.status_code for r in responses) == [200] * 9 + [201]
    assert len({r.json()["id"] for r in responses}) == 1
    assert len(client.get("/jobs").json()["items"]) == 1


def test_list_filters_by_status_and_paginates(client):
    ids = [submit(client).json()["id"] for _ in range(3)]
    client.post(f"/jobs/{ids[0]}/cancel")

    queued = client.get("/jobs", params={"status": "queued"}).json()["items"]
    assert {j["id"] for j in queued} == set(ids[1:])

    page = client.get("/jobs", params={"limit": 2, "offset": 0}).json()
    assert len(page["items"]) == 2
    rest = client.get("/jobs", params={"limit": 2, "offset": 2}).json()
    assert len(rest["items"]) == 1


def test_cancel_queued_job(client):
    job_id = submit(client).json()["id"]
    resp = client.post(f"/jobs/{job_id}/cancel")
    assert resp.status_code == 200
    assert resp.json()["status"] == "cancelled"
    assert resp.json()["finished_at"] is not None


def test_cancel_twice_is_409(client):
    job_id = submit(client).json()["id"]
    client.post(f"/jobs/{job_id}/cancel")
    resp = client.post(f"/jobs/{job_id}/cancel")
    assert resp.status_code == 409
    assert "cancelled" in resp.json()["detail"]


def test_cancel_unknown_job_is_404(client):
    assert client.post(f"/jobs/{uuid.uuid4()}/cancel").status_code == 404


def test_submit_pushes_job_id_onto_its_priority_queue(client, redis_sync):
    job = submit(client, {"type": "sleep", "priority": "high"}).json()
    assert redis_sync.lrange("q:high", 0, -1) == [job["id"]]
    assert redis_sync.llen("q:normal") == 0


def test_idempotent_replay_of_queued_job_pushes_again(client, redis_sync):
    # Re-pushing heals a push that failed the first time; the duplicate is harmless
    # because workers only run jobs that are still `queued` in Postgres.
    job = submit(client, key="k").json()
    submit(client, key="k")
    assert redis_sync.lrange("q:normal", 0, -1) == [job["id"], job["id"]]


def test_replay_of_cancelled_job_does_not_push(client, redis_sync):
    job = submit(client, key="k").json()
    client.post(f"/jobs/{job['id']}/cancel")
    redis_sync.delete("q:normal")
    submit(client, key="k")
    assert redis_sync.llen("q:normal") == 0


def test_submit_succeeds_when_redis_is_down(test_settings, clean_state):
    # Postgres is the source of truth: the job is stored and the push is retried later.
    settings = test_settings.model_copy(update={"redis_url": "redis://127.0.0.1:1/0"})
    with TestClient(create_app(settings)) as c:
        resp = submit(c)
        assert resp.status_code == 201
        assert c.get(f"/jobs/{resp.json()['id']}").json()["status"] == "queued"
