# OpsPilot

A simulated multi-service data platform with real telemetry, a fault-injection harness, an incident
pipeline and an evidence-grounded AI incident investigator, evaluated against labelled incidents
and a rules-based baseline.

The full design is in [docs/SPEC.md](docs/SPEC.md).

**Status:** M1 (telemetry) and M2 (fault injection, scenario recorder, bundles) are complete. Incidents and the rules-based baseline (M3) are next.

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

## Fault scenarios

Scenario definitions in [scenarios/defs/](scenarios/defs/) pair a fault with its ground-truth label and
the evidence it should leave behind. With the stack running:

```bash
.venv/Scripts/python -m scenarios.record scenarios/defs/*.yaml
```

```bash
.venv/Scripts/python -m scenarios.check
```

Each recording resets the platform, waits out a 3-minute cooldown, then records 3 min of baseline,
5 min of fault and 2 min of recovery, and exports the window to `scenarios/bundles/<id>/` as Parquet
(metrics, logs, spans, change events) plus a manifest. `scenarios.check` confirms every bundle shows the
key evidence its label claims. The leak test (`tests/test_leaks.py`) confirms no recorded string reveals
that a fault was injected.

| Fault | How it's injected | What it looks like |
|-------|-------------------|--------------------|
| F1 pool exhaustion | `/chaos`: leaks one DB connection every N seconds | Pool usage climbs stepwise, then requests queue and time out |
| F2 latency | `/chaos`: adds latency inside the server span | Slow service, slow callers, longer pipeline jobs |
| F3 timeout misconfig | Control plane config change (ms written as s) | Config change event, caller timeouts, healthy callee |
| F4 malformed input | `/chaos`: bad values in some datasets' source data | Schema-validation failures, job retries, partial pipeline runs |
| F5 bad deploy | Control plane deploy of a release with a buggy feature-flagged path | Deploy event, then 500s on one code path |

The control plane (`127.0.0.1:8090`, served by `sim.run`) performs deploys, config changes and
restarts, and logs each one as a change event in `var/telemetry/changes.jsonl`.

## Layout

```
sim/        simulated services, telemetry, faults (chaos.py), control plane, supervisor (run.py), M1 checks
scenarios/  scenario definitions, recorder, bundle checks
opspilot/   OpsPilot itself; so far the telemetry store over bundles (store/)
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
