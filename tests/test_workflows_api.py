import uuid

import pytest

pytestmark = pytest.mark.integration

OK = {"type": "sleep", "payload": {"seconds": 0}}
DIAMOND = {
    "name": "etl",
    "jobs": [
        {"key": "extract", **OK},
        {"key": "clean", **OK, "depends_on": ["extract"]},
        {"key": "enrich", **OK, "depends_on": ["extract"]},
        {"key": "load", **OK, "depends_on": ["clean", "enrich"]},
    ],
}


def test_submit_returns_the_graph(client, redis_sync):
    resp = client.post("/workflows", json=DIAMOND)

    assert resp.status_code == 201
    wf = resp.json()
    assert (wf["name"], wf["status"]) == ("etl", "running")
    assert wf["counts"] == {"queued": 1, "blocked": 3}
    by_key = {j["key"]: j for j in wf["jobs"]}
    assert by_key["load"]["depends_on"] == ["clean", "enrich"]
    assert by_key["load"]["pending_deps"] == 2
    assert redis_sync.lrange("q:normal", 0, -1) == [by_key["extract"]["job_id"]]


@pytest.mark.parametrize(
    ("jobs", "message"),
    [
        (
            [{"key": "a", **OK, "depends_on": ["b"]}, {"key": "b", **OK, "depends_on": ["a"]}],
            "dependency cycle among jobs ['a', 'b']",
        ),
        ([{"key": "a", **OK, "depends_on": ["ghost"]}], "unknown job 'ghost'"),
        ([{"key": "a", **OK}, {"key": "a", **OK}], "duplicate job keys ['a']"),
        ([{"key": "a", "type": "sleep", "payload": {"seconds": -1}}], "invalid payload"),
    ],
)
def test_invalid_workflows_are_rejected(client, jobs, message):
    resp = client.post("/workflows", json={"jobs": jobs})
    assert resp.status_code == 422
    assert message in str(resp.json())


def test_empty_workflow_is_rejected(client):
    assert client.post("/workflows", json={"jobs": []}).status_code == 422


def test_idempotent_submit(client):
    first = client.post("/workflows", json=DIAMOND, headers={"Idempotency-Key": "etl-1"})
    second = client.post("/workflows", json=DIAMOND, headers={"Idempotency-Key": "etl-1"})
    assert (first.status_code, second.status_code) == (201, 200)
    assert second.json()["id"] == first.json()["id"]
    assert len(client.get("/workflows").json()["items"]) == 1

    other = {**DIAMOND, "name": "different"}
    assert (
        client.post("/workflows", json=other, headers={"Idempotency-Key": "etl-1"}).status_code
        == 422
    )


def test_get_list_and_404(client):
    wf = client.post("/workflows", json=DIAMOND).json()
    assert client.get(f"/workflows/{wf['id']}").json()["id"] == wf["id"]
    [summary] = client.get("/workflows").json()["items"]
    assert (summary["status"], summary["counts"]) == ("running", {"queued": 1, "blocked": 3})
    assert client.get(f"/workflows/{uuid.uuid4()}").status_code == 404


def test_cancel(client):
    wf = client.post("/workflows", json=DIAMOND).json()
    resp = client.post(f"/workflows/{wf['id']}/cancel")
    assert resp.status_code == 200
    assert resp.json()["status"] == "cancelled"
    assert resp.json()["counts"] == {"cancelled": 4}
    assert client.post(f"/workflows/{uuid.uuid4()}/cancel").status_code == 404
