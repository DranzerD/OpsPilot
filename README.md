# OpsPilot

An AI incident investigator for a distributed data platform, measured on labelled failures.

A five-service data platform runs natively with OpenTelemetry, Prometheus and an OTel Collector.
A fault-injection harness breaks it in realistic ways and records each incident with its true root
cause. An LLM agent (gpt-oss-20b via Groq) and a rules-based baseline then diagnose every incident
from the telemetry, and an evaluation harness scores them.

The original design is in [docs/SPEC.md](docs/SPEC.md). What's built is a subset (see *Scope* below).

## Results

24 recorded incidents: 21 faults across 5 fault types and 4 services, plus 3 no-fault runs.
Full per-scenario output is in [results/final/](results/final/).

| System | Top-1 root cause | Top-3 | No-fault runs cleared | Citations valid | Evidence relevance | Median latency | p95 latency | Tokens / run |
|---|---|---|---|---|---|---|---|---|
| Rules baseline | **100%** (21/21) | 100% | 3/3 | 100% | 96% | 0.1 s | 0.1 s | 0 |
| Agent (gpt-oss-20b + tools) | **86%** (18/21) | 86% | 3/3 | 100% (92/92) | 81% | 13.7 s | 29.4 s | 2.5k |
| Agent, briefing only (no tools) | 76% (16/21) | 81% | 3/3 | 100% (118/118) | 73% | 9.5 s | 14.8 s | 1.5k |

Top-1 by fault type:

| Fault type | Rules | Agent | Agent, no tools |
|---|---|---|---|
| Bad deploy | 4/4 | 4/4 | 4/4 |
| Malformed job input | 3/3 | 3/3 | 3/3 |
| DB connection-pool exhaustion | 5/5 | 5/5 | 5/5 |
| Timeout misconfiguration | 4/4 | 4/4 | 4/4 |
| Service latency | 5/5 | **2/5** | 0/5 |
| No fault (correctly cleared) | 3/3 | 3/3 | 3/3 |

What the numbers say:

- **Drill-down tools matter:** they lift the agent from 76% to 86% top-1, all of it on latency
  faults (0/5 to 2/5). Without them the model guesses "pool exhaustion" or "slow query" for a
  slow service.
- **Latency faults are the agent's weak spot.** Every failure is a latency fault where it blamed a
  neighbouring service or the wrong mechanism. Separating a slow service from slow callers needs
  span-level reasoning that gpt-oss-20b only sometimes does.
- **Citations are always grounded:** every evidence id the agent cited (92/92) exists in the
  evidence it actually retrieved. Hallucinated evidence is checked mechanically, not trusted.
- **The rules baseline is an upper bound, not a fair fight.** Its signature rules were written
  for these five fault types and checked against these same scenarios. The agent has only a
  general prompt. The interesting comparison is agent vs. agent-without-tools.

Caveats: 21 faulted scenarios is a small sample (one miss moves top-1 by ~5 points). Prompts and
the triage noise threshold were adjusted during development on this same scenario set; there is
no held-out set yet. 3 of 24 bundles show weaker evidence than labelled (`scenarios.check`) and are
kept.

## How the investigator works

1. **Triage** ([opspilot/analysis.py](opspilot/analysis.py)) compares every signal against the
   first minute of the window and builds a briefing: anomalous signals with onset times and
   severity (small blips are marked *minor*: the size of normal background noise), new error-log
   patterns, deploys/config changes, and span latency/error shifts. Every item gets a stable
   evidence id.
2. **The agent** ([opspilot/agent.py](opspilot/agent.py)) reads the briefing and may call
   read-only tools (`query_metric`, `search_logs`, `get_service_spans`) before calling
   `submit_diagnosis` with up to three ranked hypotheses, each citing evidence ids. Every id a tool
   returns enters an evidence ledger; citations are checked against it.
3. **Token budget:** the triage-first design keeps an investigation to ~2.5k tokens, so the whole
   evaluation fits in Groq's free tier.

Try it on a recorded incident (needs `GROQ_API_KEY`; `--rules` needs nothing):

```bash
.venv/Scripts/python -m opspilot.investigate scenarios/bundles/f5-deploy-metadata-a --show-answer
```

Re-run the evaluation:

```bash
.venv/Scripts/python -m opspilot.evaluate --run final --model openai/gpt-oss-20b
```

## Scope

Built: simulated platform and telemetry, fault injection and recording, triage, rules baseline,
LLM agent with tools and citation checking, evaluation harness. From the original spec, not built:
alerting/incident grouping, the web console, Slack integration, remediation approvals, and the
robustness experiments (missing evidence, confounders, tool failures).

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

Each recording resets the platform, waits out a cooldown, then records 90 s of baseline and 150 s
of fault (definitions are generated by `python -m scenarios.generate`), and exports the window to `scenarios/bundles/<id>/` as Parquet
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
opspilot/   triage, rules baseline, agent, evaluation, demo CLI; store/ reads bundles
results/    evaluation output
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
