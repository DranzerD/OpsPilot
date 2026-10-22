import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from sim.app import create_app
from sim.db import DATASETS, init_platform_db


@pytest.fixture(scope="module")
def client():
    init_platform_db(Path(os.environ["PLATFORM_DB_PATH"]), reset=True)
    with TestClient(create_app("metadata", export_telemetry=False)) as c:
        yield c


def test_lists_seeded_datasets(client):
    names = [d["name"] for d in client.get("/datasets").json()]
    assert names == sorted(DATASETS)


def test_unknown_dataset_is_404(client):
    assert client.get("/datasets/nope").status_code == 404


def test_get_dataset_returns_schema(client):
    body = client.get("/datasets/orders").json()
    assert body["schema"]["columns"][0] == "order_id"


def test_register_partition_then_list(client):
    resp = client.post(
        "/datasets/orders/partitions", json={"partition_key": "29991231T000000", "row_count": 7}
    )
    assert resp.status_code == 201
    latest = client.get("/datasets/orders/partitions", params={"limit": 1}).json()[0]
    assert latest == {**latest, "partition_key": "29991231T000000", "row_count": 7}


def test_register_partition_validates_input(client):
    resp = client.post("/datasets/orders/partitions", json={"partition_key": "x", "row_count": -1})
    assert resp.status_code == 422
