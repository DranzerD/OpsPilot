"""Record scenarios against the live stack and export them as bundles (spec §6.3).

    python -m scenarios.record scenarios/defs/f1-*.yaml [--force] [--cooldown 180]

Requires a running stack (`python -m sim.run up`). Per scenario: reset to a clean baseline,
cool down, then baseline -> apply fault -> revert -> recovery, then export the window.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import shutil
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from scenarios import faults
from scenarios.model import BUNDLES_DIR, ScenarioDef
from sim.config import CHANGES_PATH, PROMETHEUS_URL, ROOT, TELEMETRY_DIR, load_topology
from sim.control import ChangeLog, ControlClient
from sim.otlp import iter_logs, iter_spans

BUNDLE_FORMAT_VERSION = 1
STEP_S = 5
# Exporters batch for up to 5 s, the Collector for 2 s, Prometheus scrapes and evaluates every 5 s.
FLUSH_WAIT_S = 20
# Infrastructure series that say nothing about the platform.
SKIP_METRIC_PREFIXES = ("prometheus_", "go_", "process_", "otelcol_", "net_", "promhttp_", "scrape_")
SKIP_METRICS = {"up", "target_info"}
DROP_LABELS = {
    "__name__",
    "job",  # duplicate of service_name
    "service_instance_id",  # duplicate of instance
    "otel_scope_name",
    "otel_scope_version",
    "otel_scope_schema_url",
    "deployment_environment",
    "network_protocol_version",
    "url_scheme",
}


def _now() -> datetime:
    return datetime.now(UTC)


def _sleep_until(deadline: float, label: str) -> None:
    remaining = deadline - time.monotonic()
    if remaining > 0:
        print(f"    {label}: {remaining:.0f}s", flush=True)
        time.sleep(remaining)


def _git_sha() -> str:
    try:
        sha = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True, check=True
        ).stdout.strip()
        dirty = subprocess.run(
            ["git", "status", "--porcelain"], check=False, cwd=ROOT, capture_output=True, text=True
        ).stdout
        return sha + ("-dirty" if dirty.strip() else "")
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


# --- Export ----------------------------------------------------------------------------------


def _metric_names(start: datetime, end: datetime) -> list[str]:
    resp = httpx.get(
        f"{PROMETHEUS_URL}/api/v1/label/__name__/values",
        params={"start": start.timestamp(), "end": end.timestamp()},
        timeout=30,
    )
    resp.raise_for_status()
    return [
        n for n in resp.json()["data"] if not n.startswith(SKIP_METRIC_PREFIXES) and n not in SKIP_METRICS
    ]


def export_metrics(start: datetime, end: datetime) -> pa.Table:
    rows: dict[str, list[Any]] = {"ts": [], "metric": [], "labels": [], "value": []}
    with httpx.Client(base_url=PROMETHEUS_URL, timeout=60) as client:
        for name in _metric_names(start, end):
            resp = client.get(
                "/api/v1/query_range",
                params={
                    "query": f'{{__name__="{name}"}}',
                    "start": start.timestamp(),
                    "end": end.timestamp(),
                    "step": STEP_S,
                },
            )
            resp.raise_for_status()
            for series in resp.json()["data"]["result"]:
                labels = json.dumps(
                    {k: v for k, v in sorted(series["metric"].items()) if k not in DROP_LABELS},
                    separators=(",", ":"),
                )
                for ts, value in series["values"]:
                    rows["ts"].append(datetime.fromtimestamp(float(ts), UTC))
                    rows["metric"].append(name)
                    rows["labels"].append(labels)
                    rows["value"].append(float(value))
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


def _telemetry_files(prefix: str, start: datetime) -> list[Path]:
    # Current file plus rotated backups, skipping backups that ended before the window.
    cutoff = (start - timedelta(minutes=1)).timestamp()
    return sorted(p for p in TELEMETRY_DIR.glob(f"{prefix}*.jsonl") if p.stat().st_mtime >= cutoff)


def export_spans(start: datetime, end: datetime) -> pa.Table:
    lo, hi = int(start.timestamp() * 1e9), int(end.timestamp() * 1e9)
    cols: dict[str, list[Any]] = {
        k: []
        for k in (
            "trace_id",
            "span_id",
            "parent_span_id",
            "service",
            "name",
            "kind",
            "start_ts",
            "end_ts",
            "duration_ms",
            "is_error",
            "peer_service",
            "attrs",
        )
    }
    for path in _telemetry_files("traces", start):
        for s in iter_spans(path):
            if not lo <= s.start_ns <= hi:
                continue
            cols["trace_id"].append(s.trace_id)
            cols["span_id"].append(s.span_id)
            cols["parent_span_id"].append(s.parent_span_id)
            cols["service"].append(s.service)
            cols["name"].append(s.name)
            cols["kind"].append(s.kind)
            cols["start_ts"].append(datetime.fromtimestamp(s.start_ns / 1e9, UTC))
            cols["end_ts"].append(datetime.fromtimestamp(s.end_ns / 1e9, UTC))
            cols["duration_ms"].append(s.duration_ms)
            cols["is_error"].append(s.is_error)
            cols["peer_service"].append(s.attrs.get("peer.service"))
            cols["attrs"].append(json.dumps(s.attrs, default=str))
    ts = pa.timestamp("us", tz="UTC")
    return pa.table(
        cols,
        schema=pa.schema(
            [
                ("trace_id", pa.string()),
                ("span_id", pa.string()),
                ("parent_span_id", pa.string()),
                ("service", pa.string()),
                ("name", pa.string()),
                ("kind", pa.string()),
                ("start_ts", ts),
                ("end_ts", ts),
                ("duration_ms", pa.float64()),
                ("is_error", pa.bool_()),
                ("peer_service", pa.string()),
                ("attrs", pa.string()),
            ]
        ),
    )


def export_logs(start: datetime, end: datetime) -> pa.Table:
    lo, hi = int(start.timestamp() * 1e9), int(end.timestamp() * 1e9)
    cols: dict[str, list[Any]] = {
        k: [] for k in ("ts", "service", "severity", "message", "trace_id", "span_id", "attrs")
    }
    for path in _telemetry_files("logs", start):
        for r in iter_logs(path):
            if not lo <= r.time_ns <= hi:
                continue
            cols["ts"].append(datetime.fromtimestamp(r.time_ns / 1e9, UTC))
            cols["service"].append(r.service)
            cols["severity"].append(r.severity)
            cols["message"].append(r.body)
            cols["trace_id"].append(r.trace_id or None)
            cols["span_id"].append(r.span_id or None)
            cols["attrs"].append(json.dumps(r.attrs, default=str))
    return pa.table(
        cols,
        schema=pa.schema(
            [
                ("ts", pa.timestamp("us", tz="UTC")),
                ("service", pa.string()),
                ("severity", pa.string()),
                ("message", pa.string()),
                ("trace_id", pa.string()),
                ("span_id", pa.string()),
                ("attrs", pa.string()),
            ]
        ),
    )


def export_changes(start: datetime, end: datetime) -> pa.Table:
    events = [e for e in ChangeLog(CHANGES_PATH).read() if start <= datetime.fromisoformat(e["ts"]) <= end]
    return pa.table(
        {
            "id": [e["id"] for e in events],
            "ts": [datetime.fromisoformat(e["ts"]) for e in events],
            "service": [e["service"] for e in events],
            "kind": [e["kind"] for e in events],
            "summary": [e["summary"] for e in events],
            "details": [json.dumps(e["details"]) for e in events],
        },
        schema=pa.schema(
            [
                ("id", pa.string()),
                ("ts", pa.timestamp("ms", tz="UTC")),
                ("service", pa.string()),
                ("kind", pa.string()),
                ("summary", pa.string()),
                ("details", pa.string()),
            ]
        ),
    )


def _topology() -> dict[str, Any]:
    topo = load_topology()
    return {
        "services": {
            name: {
                "tier": svc.tier,
                "description": svc.description,
                "calls": {d.name: {"timeout_s": d.timeout_s} for d in svc.calls},
            }
            for name, svc in topo.services.items()
        },
        "databases": {
            name: {"engine": spec.get("engine"), "description": spec.get("description", "")}
            for name, spec in topo.databases.items()
        },
    }


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_bundle(
    out: Path, scenario: ScenarioDef, window: dict[str, datetime], tables: dict[str, pa.Table]
) -> dict[str, Any]:
    tmp = out.with_name(out.name + ".tmp")
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(parents=True)
    files: dict[str, Any] = {}
    for name, table in tables.items():
        path = tmp / f"{name}.parquet"
        pq.write_table(table, path, compression="zstd")
        files[path.name] = {"rows": table.num_rows, "sha256": _sha256(path)}
    topo_path = tmp / "topology.json"
    topo_path.write_text(json.dumps(_topology(), indent=2))
    files[topo_path.name] = {"sha256": _sha256(topo_path)}
    manifest = {
        "format_version": BUNDLE_FORMAT_VERSION,
        "scenario": scenario.model_dump(mode="json"),
        "window": {k: v.isoformat() for k, v in window.items()},
        "step_s": STEP_S,
        "recorded_at": _now().isoformat(timespec="seconds"),
        "git_sha": _git_sha(),
        "platform": {"python": sys.version.split()[0], "os": platform.platform()},
        "files": files,
    }
    (tmp / "manifest.json").write_text(json.dumps(manifest, indent=2))
    shutil.rmtree(out, ignore_errors=True)
    tmp.rename(out)
    return manifest


# --- Recording -------------------------------------------------------------------------------


def _check_stack(control: ControlClient) -> None:
    try:
        control.services()
        httpx.get(f"{PROMETHEUS_URL}/-/ready", timeout=2).raise_for_status()
    except (httpx.HTTPError, RuntimeError) as exc:
        sys.exit(f"stack not reachable ({exc}); start it with `python -m sim.run up`")


def record(scenario: ScenarioDef, control: ControlClient, out_dir: Path, cooldown_s: int) -> Path:
    tl = scenario.timeline
    print(
        f"[{scenario.id}] {scenario.fault.fault_type} on {scenario.fault.target}, ~{(cooldown_s + tl.total_s) // 60 + 1} min",
        flush=True,
    )
    faults.reset(control)
    _sleep_until(time.monotonic() + cooldown_s, "cooldown")

    t0 = _now()
    mark = time.monotonic()
    _sleep_until(mark + tl.baseline_s, "baseline")
    fault_start = _now()
    revert = faults.apply(scenario.fault, control)
    try:
        _sleep_until(mark + tl.baseline_s + tl.fault_s, "fault")
    finally:
        fault_end = _now()
        revert()
    _sleep_until(mark + tl.total_s, "recovery")
    t_end = _now()
    time.sleep(FLUSH_WAIT_S)

    window = {"start": t0, "fault_start": fault_start, "fault_end": fault_end, "end": t_end}
    tables = {
        "metrics": export_metrics(t0, t_end),
        "spans": export_spans(t0, t_end),
        "logs": export_logs(t0, t_end),
        "changes": export_changes(t0, t_end),
    }
    out = out_dir / scenario.id
    manifest = write_bundle(out, scenario, window, tables)
    counts = ", ".join(f"{k.split('.')[0]}={v['rows']}" for k, v in manifest["files"].items() if "rows" in v)
    print(f"[{scenario.id}] bundle written to {out} ({counts})", flush=True)
    return out


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("defs", nargs="+", type=Path)
    parser.add_argument("--out", type=Path, default=BUNDLES_DIR)
    parser.add_argument("--force", action="store_true", help="re-record scenarios that already have a bundle")
    parser.add_argument(
        "--cooldown", type=int, default=180, help="unrecorded settle time before each scenario"
    )
    parser.add_argument(
        "--timeline",
        help="override baseline,fault,recovery seconds (for tuning only; bundles are marked as such)",
    )
    args = parser.parse_args(argv)

    scenarios = [ScenarioDef.load(p) for p in args.defs]
    control = ControlClient()
    _check_stack(control)
    args.out.mkdir(parents=True, exist_ok=True)
    for scenario in scenarios:
        if args.timeline:
            b, f, r = (int(x) for x in args.timeline.split(","))
            scenario = scenario.model_copy(
                update={
                    "id": f"{scenario.id}-tuning",
                    "timeline": scenario.timeline.model_copy(
                        update={"baseline_s": b, "fault_s": f, "recovery_s": r}
                    ),
                }
            )
        if (args.out / scenario.id).exists() and not args.force:
            print(f"[{scenario.id}] bundle exists, skipping (use --force)")
            continue
        try:
            record(scenario, control, args.out, args.cooldown)
        except Exception as exc:  # noqa: BLE001 - one bad scenario shouldn't sink an unattended batch
            print(f"[{scenario.id}] FAILED: {type(exc).__name__}: {exc}", flush=True)
    faults.reset(control)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
