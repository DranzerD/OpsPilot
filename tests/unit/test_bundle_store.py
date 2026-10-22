import json
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pytest

from opspilot.store.bundle import BundleStore, load_ground_truth
from scenarios.check import check_bundle
from scenarios.model import ScenarioDef
from scenarios.record import write_bundle

T0 = datetime(2026, 10, 22, 12, 0, tzinfo=UTC)
FAULT_START = T0 + timedelta(minutes=3)
FAULT_END = T0 + timedelta(minutes=8)
END = T0 + timedelta(minutes=10)

SCENARIO = ScenarioDef.model_validate(
    {
        "id": "synthetic-latency",
        "fault": {"fault_type": "dependency_latency", "target": "metadata", "intensity": 0.5},
        "timeline": {"baseline_s": 180, "fault_s": 300, "recovery_s": 120},
        "label": {
            "root_cause": {"service": "metadata", "fault_type": "dependency_latency"},
            "key_evidence": [
                {"kind": "metric", "service": "metadata", "signal": "latency_p95", "direction": "up"},
                {"kind": "log", "service": "query-engine", "pattern": "exceeded deadline"},
                {"kind": "change", "service": "metadata", "change_kind": "deploy"},
            ],
        },
    }
)


def _metrics() -> pa.Table:
    rows = {"ts": [], "metric": [], "labels": [], "value": []}
    for i in range(0, 600, 5):
        ts = T0 + timedelta(seconds=i)
        in_fault = FAULT_START <= ts <= FAULT_END
        for service, value in (("metadata", 0.5 if in_fault else 0.005), ("gateway", 0.02)):
            rows["ts"].append(ts)
            rows["metric"].append("service:latency_p95:1m")
            rows["labels"].append(json.dumps({"service_name": service}))
            rows["value"].append(value)
        rows["ts"].append(ts)
        rows["metric"].append("job:duration_p95:5m")
        rows["labels"].append(json.dumps({"job_name": "ingest"}))
        rows["value"].append(float("nan") if i == 0 else 1.0)
    return pa.table(
        rows,
        schema=pa.schema(
            [
                ("ts", pa.timestamp("ms", tz="UTC")),
                ("metric", pa.string()),
                ("labels", pa.string()),
                ("value", pa.float64()),
            ]
        ),
    )


def _logs() -> pa.Table:
    msgs = [
        (T0 + timedelta(minutes=1), "query-engine", "INFO", "Started"),
        (
            FAULT_START + timedelta(seconds=30),
            "query-engine",
            "ERROR",
            "Upstream metadata call /x exceeded deadline (2.0s)",
        ),
    ]
    return pa.table(
        {
            "ts": [m[0] for m in msgs],
            "service": [m[1] for m in msgs],
            "severity": [m[2] for m in msgs],
            "message": [m[3] for m in msgs],
            "trace_id": [None, "t1"],
            "span_id": [None, "s1"],
            "attrs": ["{}", "{}"],
        }
    )


def _spans() -> pa.Table:
    return pa.table(
        {
            "trace_id": ["t1"],
            "span_id": ["s1"],
            "parent_span_id": [""],
            "service": ["metadata"],
            "name": ["GET /x"],
            "kind": ["SERVER"],
            "start_ts": [FAULT_START],
            "end_ts": [FAULT_START + timedelta(seconds=1)],
            "duration_ms": [1000.0],
            "is_error": [False],
            "peer_service": [None],
            "attrs": ["{}"],
        }
    )


def _changes() -> pa.Table:
    return pa.table(
        {
            "id": ["c1"],
            "ts": [FAULT_START],
            "service": ["metadata"],
            "kind": ["deploy"],
            "summary": ["Deployed metadata 0.1.1 (was 0.1.0)"],
            "details": [json.dumps({"to_version": "0.1.1"})],
        }
    )


@pytest.fixture
def bundle(tmp_path):
    out = tmp_path / SCENARIO.id
    window = {"start": T0, "fault_start": FAULT_START, "fault_end": FAULT_END, "end": END}
    tables = {"metrics": _metrics(), "spans": _spans(), "logs": _logs(), "changes": _changes()}
    write_bundle(out, SCENARIO, window, tables)
    return out


def test_manifest_records_files_and_window(bundle):
    manifest = json.loads((bundle / "manifest.json").read_text())
    assert manifest["files"]["metrics.parquet"]["rows"] == 360
    gt = load_ground_truth(bundle)
    assert (gt.scenario_id, gt.fault_start) == ("synthetic-latency", FAULT_START)


def test_metric_by_signal_and_service(bundle):
    with BundleStore(bundle) as store:
        assert store.time_range() == (T0, END)
        (series,) = store.metric("latency_p95", "metadata")
        assert series.labels == {"service_name": "metadata"}
        assert max(v for _, v in series.points) == 0.5
        assert len(store.metric("latency_p95")) == 2


def test_job_signals_belong_to_scheduler_and_drop_nan(bundle):
    with BundleStore(bundle) as store:
        assert store.metric("job_duration_p95", "gateway") == []
        (series,) = store.metric("job_duration_p95", "scheduler", labels={"job_name": "ingest"})
        assert len(series.points) == 119  # the NaN sample is dropped


def test_rejects_unsafe_label_names(bundle):
    with BundleStore(bundle) as store, pytest.raises(ValueError):
        store.metric("latency_p95", labels={"x') OR 1=1 --": "y"})


def test_logs_changes_spans(bundle):
    with BundleStore(bundle) as store:
        (log,) = store.logs(service="query-engine", pattern="deadline")
        assert log["trace_id"] == "t1"
        assert store.count_logs(pattern="Started") == 1
        (change,) = store.changes(service="metadata")
        assert change["details"]["to_version"] == "0.1.1"
        assert store.spans(trace_id="t1")[0]["duration_ms"] == 1000.0


def test_check_finds_key_evidence(bundle):
    assert [r.ok for r in check_bundle(bundle)] == [True, True, True]
