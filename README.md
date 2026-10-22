# OpsPilot

A simulated multi-service data platform with real telemetry, a fault-injection harness, an incident
pipeline and an evidence-grounded AI incident investigator, evaluated against labelled incidents
and a rules-based baseline.

The full design is in [docs/SPEC.md](docs/SPEC.md).

**Status:** M1 (simulated platform emits telemetry) is complete. Fault injection (M2) is next.

## Quickstart

Requires Python 3.11+ on Windows. No Docker: everything runs as native processes.

```bash
python -m venv .venv && .venv/Scripts/python -m pip install -e ".[dev]"
```

```bash
.venv/Scripts/python scripts/fetch_tools.py
```

```bash
.venv/Scripts/python -m sim.run up
```

`fetch_tools.py` downloads the official Prometheus and OpenTelemetry Collector Windows binaries into
`tools/` and verifies their release checksums. `sim.run up` starts the Collector, Prometheus, the
five services and the workload generator. Ctrl+C stops everything.

Once it has run for a minute or two, check the M1 acceptance criteria from another terminal:

```bash
.venv/Scripts/python -m sim.verify
```

Prometheus is at http://127.0.0.1:9090. Try `service:latency_p95:1m` or `job:duration_p95:5m`.

## The simulated platform

```
workload ──► gateway ──┬──► query-engine ──┬──► metadata ──► platform-db (SQLite)
                       └──► metadata       └──► platform-db

scheduler ──┬──► stream-processor ──┬──► metadata
            └──► metadata           └──► platform-db
```

| Service | Port | Role |
|---------|------|------|
| gateway | 8001 | Public query API; checks the catalog, forwards to query-engine |
| query-engine | 8002 | Runs queries over a dataset's latest partitions |
| metadata | 8003 | Dataset catalog and partitions, with a TTL cache |
| scheduler | 8004 | Runs the "nightly" ingest pipeline every 120 s (compressed clock), with retries |
| stream-processor | 8005 | Ingest jobs: writes partitions, registers them with metadata, applies retention |

Topology, ports, timeouts and pool sizes live in [sim/topology.yaml](sim/topology.yaml).

**Telemetry:** every service uses the OpenTelemetry SDK and sends OTLP/HTTP to the Collector.
Metrics go to Prometheus (5 s scrape) with RED/saturation recording rules in
[configs/prometheus/rules/red.yml](configs/prometheus/rules/red.yml). Traces and logs are written as
OTLP JSON lines to `var/telemetry/` until the OpsPilot ingest API exists (M3). Logs carry trace
and span IDs, and the database appears in traces as `peer.service=platform-db` client spans.

## Layout

```
sim/        simulated services, telemetry setup, process supervisor (run.py), M1 checks (verify.py)
configs/    Collector and Prometheus configuration
scripts/    fetch_tools.py
tests/      unit tests (pytest)
docs/       SPEC.md
tools/, var/  fetched binaries and runtime state (gitignored)
```

## Tests

```bash
.venv/Scripts/python -m pytest
```
