import logging
import os
import re
import time
import traceback
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from sim.app import create_app
from sim.chaos import FaultSpec, FaultState, FaultType, affected_datasets, faults
from sim.db import DATASETS, init_platform_db
from sim.services.stream_processor import RecordValidationError, validate_records


@pytest.fixture(scope="module")
def metadata():
    init_platform_db(Path(os.environ["PLATFORM_DB_PATH"]))
    with TestClient(create_app("metadata", export_telemetry=False)) as c:
        yield c
        c.delete("/chaos")


def test_fault_expires():
    state = FaultState()
    state.activate(FaultSpec(fault_type="dependency_latency", target="metadata", intensity=0.5, duration_s=1))
    assert state.get(FaultType.DEPENDENCY_LATENCY) is not None
    time.sleep(1.05)
    assert state.get(FaultType.DEPENDENCY_LATENCY) is None


def test_affected_datasets_is_stable_and_sized_by_intensity():
    spec = FaultSpec(
        fault_type="bad_input_job_failure", target="stream-processor", intensity=0.5, params={"seed": 1}
    )
    first = affected_datasets(spec, list(DATASETS))
    assert first == affected_datasets(spec, list(reversed(DATASETS)))
    assert len(first) == len(DATASETS) // 2
    explicit = spec.model_copy(update={"params": {"datasets": ["orders"]}})
    assert affected_datasets(explicit, list(DATASETS)) == {"orders"}


def test_chaos_rejects_other_targets_and_control_plane_faults(metadata):
    other = {"fault_type": "dependency_latency", "target": "gateway", "intensity": 0.5}
    assert metadata.post("/chaos", json=other).status_code == 400
    deploy = {"fault_type": "bad_deploy_errors", "target": "metadata", "intensity": 0.5}
    assert metadata.post("/chaos", json=deploy).status_code == 400


def test_latency_fault_slows_requests_until_cleared(metadata):
    spec = {
        "fault_type": "dependency_latency",
        "target": "metadata",
        "intensity": 0.1,
        "params": {"added_ms": 200, "jitter_ms": 0},
    }
    assert metadata.post("/chaos", json=spec).status_code == 204
    assert metadata.get("/chaos").json()[0]["fault_type"] == "dependency_latency"
    start = time.perf_counter()
    metadata.get("/datasets/orders")
    assert time.perf_counter() - start >= 0.19
    assert metadata.delete("/chaos").status_code == 204
    assert faults.active() == []
    start = time.perf_counter()
    metadata.get("/datasets/orders")
    assert time.perf_counter() - start < 0.15


def test_pool_leak_drains_connections(metadata):
    spec = {
        "fault_type": "db_pool_exhaustion",
        "target": "metadata",
        "intensity": 1.0,
        "params": {"leak_interval_s": 0.05, "max_leaked": 3},
    }
    assert metadata.post("/chaos", json=spec).status_code == 204
    time.sleep(0.5)
    app_db = next(
        r.endpoint for r in metadata.app.routes if getattr(r, "path", "") == "/datasets"
    ).__self__.db
    assert app_db.engine.pool.checkedout() == 3
    metadata.delete("/chaos")
    assert app_db.engine.pool.checkedout() == 0


def test_record_validation_rejects_malformed_values():
    good = {"dataset": "orders", "partition_key": "p", "ts": "t", "category": "a", "value": 1.0}
    validate_records([good], "orders", "p", 0)
    with pytest.raises(RecordValidationError) as exc:
        validate_records([good, {**good, "value": "N/A"}], "orders", "p", 100)
    assert (exc.value.offset, exc.value.column) == (101, "value")


def test_active_fault_never_appears_in_error_tracebacks(metadata, caplog):
    """Regression: a latency middleware in sim/chaos.py once put a 'chaos' frame in every logged
    traceback, leaking the fault into any error log the agent could read."""

    @metadata.app.get("/boom")
    def boom() -> None:
        raise ValueError("bad state")

    spec = {
        "fault_type": "dependency_latency",
        "target": "metadata",
        "intensity": 0.1,
        "params": {"added_ms": 1, "jitter_ms": 0},
    }
    metadata.post("/chaos", json=spec)
    try:
        with caplog.at_level(logging.ERROR):
            resp = TestClient(metadata.app, raise_server_exceptions=False).get("/boom")
    finally:
        metadata.delete("/chaos")
    assert resp.status_code == 500
    (record,) = [r for r in caplog.records if r.getMessage().startswith("Unhandled error in GET /boom")]
    trace = "".join(traceback.format_exception(*record.exc_info))
    assert "boom" in trace
    assert not re.search(r"sim[\/]chaos\.py", trace), trace
