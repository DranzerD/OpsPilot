"""M1 acceptance check against a running stack: `python -m sim.verify`.

1. every service is healthy and Prometheus is ready;
2. Prometheus has RED series for every service that serves traffic, job metrics for the
   scheduler, and every recording rule evaluates without error;
3. a single interactive trace spans gateway -> query-engine -> metadata -> platform-db;
4. a pipeline trace spans scheduler -> stream-processor -> metadata/platform-db;
5. application logs carry trace IDs that resolve to exported traces.
"""

from __future__ import annotations

import sys
from collections.abc import Callable

import httpx

from sim.config import PROMETHEUS_URL, VAR, load_topology
from sim.otlp import Span, group_traces, iter_logs, iter_spans, render_tree

TELEMETRY = VAR / "telemetry"
TAIL_BYTES = 30 * 1024 * 1024
# Services whose RED metrics come from inbound HTTP traffic (the scheduler's work is jobs).
HTTP_SERVICES = {"gateway", "query-engine", "metadata", "stream-processor"}


class Check:
    def __init__(self) -> None:
        self.failed = 0

    def __call__(self, name: str, ok: bool, detail: str = "") -> bool:
        print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f": {detail}" if detail else ""))
        self.failed += not ok
        return ok


def _prom(query: str) -> list[dict]:
    resp = httpx.get(f"{PROMETHEUS_URL}/api/v1/query", params={"query": query}, timeout=5)
    resp.raise_for_status()
    return resp.json()["data"]["result"]


def _server(spans: list[Span], service: str) -> bool:
    return any(s.service == service and s.kind == "SERVER" for s in spans)


def _db_call_from(spans: list[Span], service: str) -> bool:
    return any(
        s.service == service and s.kind == "CLIENT" and s.attrs.get("peer.service") == "platform-db"
        for s in spans
    )


def _find_trace(traces: dict[str, list[Span]], pred: Callable[[list[Span]], bool]) -> list[Span] | None:
    for spans in traces.values():
        if pred(spans):
            return spans
    return None


def main() -> int:
    check = Check()
    topo = load_topology()

    for svc in topo.services.values():
        try:
            ok = httpx.get(f"{svc.base_url}/healthz", timeout=2).status_code == 200
        except httpx.HTTPError:
            ok = False
        check(f"{svc.name} healthy", ok)
    try:
        prom_ready = httpx.get(f"{PROMETHEUS_URL}/-/ready", timeout=2).status_code == 200
    except httpx.HTTPError:
        prom_ready = False
    if not check("prometheus ready", prom_ready):
        return 1

    rate = {r["metric"].get("service_name") for r in _prom("service:request_rate:1m")}
    check("RED request rate per service", HTTP_SERVICES <= rate, f"have {sorted(rate)}")
    p95 = {r["metric"].get("service_name") for r in _prom("service:latency_p95:1m")}
    check("RED p95 latency per service", HTTP_SERVICES <= p95, f"have {sorted(p95)}")
    errors = {r["metric"].get("service_name") for r in _prom("service:error_ratio:1m")}
    check("RED error ratio per service", HTTP_SERVICES <= errors, f"have {sorted(errors)}")
    jobs = {r["metric"].get("job_name") for r in _prom("job:duration_p95:5m")}
    check("scheduler job duration", {"nightly_pipeline", "ingest"} <= jobs, f"have {sorted(jobs)}")

    rules = httpx.get(f"{PROMETHEUS_URL}/api/v1/rules", timeout=5).json()["data"]["groups"]
    bad = [r["name"] for g in rules for r in g["rules"] if r["health"] != "ok"]
    check(
        "recording rules healthy", not bad, ", ".join(bad) or f"{sum(len(g['rules']) for g in rules)} rules"
    )

    traces = group_traces(iter_spans(TELEMETRY / "traces.jsonl", TAIL_BYTES))
    check("traces exported", bool(traces), f"{len(traces)} traces in the last {TAIL_BYTES // 2**20} MB")

    interactive = _find_trace(
        traces,
        lambda t: (
            all(_server(t, s) for s in ("gateway", "query-engine", "metadata"))
            and _db_call_from(t, "metadata")
        ),
    )
    if check("trace gateway -> query-engine -> metadata -> platform-db", interactive is not None):
        print(render_tree(interactive))

    pipeline = _find_trace(
        traces,
        lambda t: (
            any(s.service == "scheduler" and s.name.startswith("pipeline") for s in t)
            and _server(t, "stream-processor")
            and _db_call_from(t, "stream-processor")
        ),
    )
    if check("trace scheduler -> stream-processor -> platform-db", pipeline is not None) and pipeline:
        print("\n".join(render_tree(pipeline).splitlines()[:12]) + "\n  ...")

    app_logs = [r for r in iter_logs(TELEMETRY / "logs.jsonl", TAIL_BYTES) if r.trace_id]
    linked = [r for r in app_logs if r.trace_id in traces]
    check(
        "logs carry trace IDs that resolve to traces",
        bool(linked),
        f"{len(linked)}/{len(app_logs)} logs with a trace ID resolve to an exported trace",
    )

    print(f"\n{'M1 acceptance: PASS' if not check.failed else f'{check.failed} check(s) failed'}")
    return 1 if check.failed else 0


if __name__ == "__main__":
    sys.exit(main())
