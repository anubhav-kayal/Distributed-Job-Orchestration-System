import uuid
from datetime import UTC, datetime

import pytest

pytestmark = pytest.mark.integration

JOB = {"type": "sleep", "payload": {"seconds": 0}}


def create(client, **overrides):
    body = {"name": "every-minute", "cron": "* * * * *", "job": JOB} | overrides
    return client.post("/schedules", json=body)


def test_create_computes_the_next_fire_time(client):
    resp = create(client, cron="*/5 * * * *")
    assert resp.status_code == 201
    schedule = resp.json()
    next_fire = datetime.fromisoformat(schedule["next_fire_at"])
    assert next_fire > datetime.now(UTC)
    assert next_fire.minute % 5 == 0 and next_fire.second == 0
    assert schedule["enabled"] is True
    assert schedule["last_fired_at"] is None
    assert schedule["job"]["payload"] == {"seconds": 0.0}  # validated + normalized


@pytest.mark.parametrize(
    "overrides",
    [
        {"cron": "61 * * * *"},
        {"cron": "every minute"},
        {"job": {"type": "nope"}},
        {"job": {"type": "sleep", "payload": {"seconds": -1}}},
        {"name": "Bad Name!"},
    ],
)
def test_invalid_schedules_are_rejected(client, overrides):
    assert create(client, **overrides).status_code == 422


def test_duplicate_name_is_409(client):
    create(client)
    assert create(client).status_code == 409


def test_list_and_get(client):
    a = create(client, name="b-second").json()
    create(client, name="a-first")
    names = [s["name"] for s in client.get("/schedules").json()["items"]]
    assert names == ["a-first", "b-second"]
    assert client.get(f"/schedules/{a['id']}").json()["name"] == "b-second"
    assert client.get(f"/schedules/{uuid.uuid4()}").status_code == 404


def test_pause_and_resume(client):
    schedule = create(client).json()
    paused = client.patch(f"/schedules/{schedule['id']}", json={"enabled": False}).json()
    assert paused["enabled"] is False
    resumed = client.patch(f"/schedules/{schedule['id']}", json={"enabled": True}).json()
    assert resumed["enabled"] is True
    assert datetime.fromisoformat(resumed["next_fire_at"]) > datetime.now(UTC)


def test_changing_the_cron_recomputes_the_next_fire(client):
    schedule = create(client).json()
    updated = client.patch(f"/schedules/{schedule['id']}", json={"cron": "0 3 * * *"}).json()
    next_fire = datetime.fromisoformat(updated["next_fire_at"])
    assert (updated["cron"], next_fire.hour, next_fire.minute) == ("0 3 * * *", 3, 0)


def test_invalid_update_is_422(client):
    schedule = create(client).json()
    assert client.patch(f"/schedules/{schedule['id']}", json={"cron": "nope"}).status_code == 422


def test_delete(client):
    schedule = create(client).json()
    assert client.delete(f"/schedules/{schedule['id']}").status_code == 204
    assert client.get(f"/schedules/{schedule['id']}").status_code == 404
    assert client.delete(f"/schedules/{schedule['id']}").status_code == 404
