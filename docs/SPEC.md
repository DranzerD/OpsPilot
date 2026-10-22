# OpsPilot — Technical Specification

**Status:** Draft v0.3 · 2026-10-22 (v0.2: native processes, no Docker; SQLite platform-db. v0.3: fault injection and bundles as built in M2)
**Owner:** Sarana Gnanojval
**One-line summary:** A simulated multi-service data platform with real telemetry, a fault-injection harness, an incident pipeline, and an evidence-grounded AI investigation agent — measured against labelled incidents and simpler baselines.

---

## 0. How to read this document

This spec exists to stop OpsPilot from becoming a pile of features with no result. Every component listed here exists for one of two reasons:

1. **It produces or carries the evidence** the agent investigates (simulator, telemetry, incidents), or
2. **It measures something** (evaluation harness, baselines, perturbations).

The Slack bot and the web console are interfaces to (1) and (2). They're kept small on purpose.

**The research question the project answers:**

> How accurately does an LLM incident investigator identify root causes in a distributed data platform, and how does that accuracy degrade when evidence is missing, misleading, or the tools themselves fail — compared with a rules-based diagnoser?

If a feature does not help answer that question or demo the answer, it is out of scope (§13).

---

## 1. Key design decisions

These are the decisions that most shape the build. Each one has a short justification you should be able to defend in an interview.

| # | Decision | Why |
|---|----------|-----|
| D1 | **Record-and-replay evaluation.** Fault scenarios are run once against the live simulator, and the resulting telemetry is frozen into a *scenario bundle*. All evaluation runs against bundles, not the live stack. | Live runs aren't reproducible: timing, scheduling and sampling change on every run. Bundles make every experiment deterministic on the data side, make ablations cheap (they're transforms on a bundle), and let the eval suite run in CI without the live stack. |
| D2 | **One `TelemetryStore` interface, two backends:** `LiveStore` (Prometheus HTTP API + ops-db) and `BundleStore` (DuckDB over Parquet files). Agent tools only see the interface. | The agent code is identical in the demo and in evaluation. Without this, you end up evaluating a different system from the one you demo. |
| D3 | **Logs and traces go to ops-db, not Loki/Tempo.** Metrics go to Prometheus. | Prometheus is the industry standard for metrics and alert rules, so it's worth using. Loki and Tempo would add two more services for data volumes a single database handles easily. It's the trade-off to state explicitly in the README. |
| D4 | **Retrieval lives in ops-db, not Chroma** (pgvector if ops-db is Postgres; brute-force cosine over a few hundred chunks if it's SQLite). | Keeps all state in one database and drops one service. Retrieval quality is not the research question here; Meeting Synth already covers that. |
| D5 | **Build the evaluation harness and the rules-based baseline *before* the agent.** | Agent development is measured from its first commit, and the baseline sets the bar the agent has to beat. Putting evaluation in weeks 8–9 (as the original plan did) means tuning the agent with no feedback. |
| D6 | **Every piece of evidence gets a stable ID, and reports may only cite IDs that are in the investigation's evidence ledger.** | Makes "evidence grounding" mechanically checkable instead of something you eyeball. Hallucinated citations become a measurable failure mode rather than an invisible one. |
| D7 | **The agent is read-only. Remediation is a *proposal* that needs human approval, and approved actions run only against the simulator's control API.** | Safe by construction, and it still lets you demo the full detect → diagnose → approve → recover loop (Experiment E5). |
| D8 | **Root causes are labelled from a closed taxonomy (`service × fault_type`).** The agent can answer `other`, but only taxonomy answers can score as correct. | Makes top-1/top-3 accuracy well-defined and automatically scorable. |
| D9 | **Slack via Socket Mode.** | Works from a laptop with no public URL or tunnel. |
| D10 | **No containers. Everything runs as native processes** under a small Python supervisor (`python -m sim.run up`). Prometheus and the OTel Collector are the official standalone Windows binaries, fetched and checksum-verified by `scripts/fetch_tools.py`. | Runs on the dev laptop with nothing to install beyond Python. The engineering signal is in telemetry, correlation and evaluation, not in container orchestration. |
| D11 | **platform-db is SQLite** (WAL mode), accessed through bounded SQLAlchemy connection pools. **ops-db engine is decided at M3:** SQLite (FTS5 for log search) unless something concrete needs Postgres. | No database server to install. Pool exhaustion, slow queries and write failures still happen in the services' own code paths, which is where the evidence is generated. |

---

## 2. System architecture

```
┌──────────────────────────── Simulated data platform (native processes) ─────────────────────────┐
│                                                                                                  │
│  workload-gen ──► gateway ──► query-engine ──► metadata ──► platform-db (SQLite)                 │
│       │                            │               ▲                ▲                            │
│       └─────► scheduler ──► stream-processor ──────┘────────────────┘                            │
│                                                                                                  │
│  Each service: FastAPI + OpenTelemetry SDK + /chaos control endpoint                             │
└───────────────┬──────────────────────────────────────────────────────────────────────────────────┘
                │ OTLP (metrics, logs, traces)
                ▼
        OpenTelemetry Collector ──metrics──► Prometheus ──alerts──► Alertmanager
                │                                │                      │ webhook
                │ OTLP/HTTP (logs, traces)        │ PromQL               ▼
                ▼                                ▼            ┌────────────────────────┐
        ┌───────────────────────────────────────────────────►│      OpsPilot API      │
        │                 ingest                              │  (FastAPI)             │
        │                                                     │  - incident grouping   │
        │                                                     │  - investigation agent │
        │                                                     │  - approvals, audit    │
        │                                                     └───┬──────────┬─────────┘
        │                                                         │          │
        │   ops-db (decided at M3, D11)◄─────────────────────────┘          │
        │                                                                     │
        │                                    Web console (React) ◄────────────┤
        │                                    Slack (Socket Mode) ◄────────────┘
        │
        └── scenario recorder (CLI): runs a scenario, exports a bundle ──► scenarios/bundles/*
                                                                              │
                                     eval harness (CLI, no live stack)    ◄───┘
```

### 2.1 Components

| Component | Tech | Responsibility |
|-----------|------|----------------|
| `sim/` services ×5 | Python, FastAPI, OTel SDK | Simulated platform. One codebase; `python -m sim.serve <name>` runs one service. |
| `platform-db` | SQLite (WAL) | The simulated platform's real database (the 6th "service"). Separate from `ops-db`. |
| `workload-gen` | Python | Generates interactive query traffic. The scheduler owns the "nightly" batch clock. |
| OTel Collector | `otelcol-contrib.exe` | Receives OTLP; exports metrics to Prometheus. Logs/traces go to OTLP JSON files until the OpsPilot ingest endpoint exists (M3). |
| Prometheus + Alertmanager | Official Windows binaries | Metrics storage, alert rules, alert routing to a webhook. |
| `opspilot/api` | FastAPI, Pydantic v2, SQLAlchemy 2 | Ingest, incidents, investigations, approvals, audit, SSE streaming. |
| `opspilot/agent` | Python, LLM SDK | Tool-calling investigation loop, evidence ledger, report validation. |
| `opspilot/correlate` | Python | Incident grouping, anomaly detection, rules-based diagnoser (baseline B0, also usable as a tool). |
| `ops-db` | Decided at M3 (D11) | OpsPilot state (§4). |
| `console/` | React, TypeScript, Vite, Plotly | Service health, incidents, investigation view, approvals, eval results. |
| `slackbot/` | `slack_bolt` (Socket Mode) | Notifications, commands, approval buttons. |
| Control plane (`sim/control.py`) | Python, served by the supervisor on :8090 | Deploys (restart a service at a version with feature flags), config changes, restarts. Every operation is appended to `var/telemetry/changes.jsonl` as a change event. |
| `scenarios/` + `eval/` | Python CLI, DuckDB, Parquet | Scenario definitions, recorder, bundles, perturbations, eval runner, reports. |

### 2.2 Simulated platform

Five FastAPI services plus a SQLite database.

| Service | Role | Calls |
|---------|------|-------|
| `gateway` | Public API for interactive queries | `query-engine`, `metadata` (auth/catalog lookups) |
| `query-engine` | Plans and executes queries over datasets | `metadata`, `platform-db` |
| `metadata` | Dataset catalog, schemas, partitions; has an in-process cache | `platform-db` |
| `scheduler` | Triggers batch jobs ("nightly pipeline") and retries failed ones | `stream-processor`, `metadata` |
| `stream-processor` | Runs ingestion/transform jobs; uses a DB connection pool | `metadata`, `platform-db` |
| `platform-db` | SQLite | — |

The dependency graph is declared once in `sim/topology.yaml` and loaded into `ops-db.service_dependencies`. The agent can query the declared graph, and the correlation engine can also derive the observed graph from spans (§5.3). A mismatch between the two is itself a useful signal.

**Clock compression.** The "nightly" pipeline runs every 120 s of wall time. A scenario is 3 min baseline + 5 min fault + 2 min recovery = **10 min of wall time**. That's short enough to record ~60 base scenarios in an unattended overnight batch.

**Telemetry resolution.** Prometheus scrape interval is 5 s. Alert rules use `for: 30s`. Without high resolution, short scenarios won't produce realistic alert sequences.

### 2.3 Fault injection

Faults reach the platform two ways, matching how real incidents start:

- **In-process faults** (F1 pool exhaustion, F2 latency, F4 malformed input, later F6–F11): each service exposes `POST/GET/DELETE /chaos` (bound to 127.0.0.1, excluded from instrumentation, silent by design).
- **Operational faults** go through the control plane, so they leave exactly the change events a real change would. F3 is a config change on the caller (a timeout written in ms where seconds were meant). F5 is a deploy of the next release with a release feature flag rolled out to `intensity` of requests. Each service's next release ships one new code path with a real bug (an unsupported SQL function, a missing column, a `KeyError`), so a bad deploy fails through ordinary application code and error handling.

`FaultSpec`:

```python
class FaultSpec(BaseModel):
    fault_type: FaultType  # enum, §6.1
    target: str  # service name, or "service->dependency" for edge faults
    intensity: float = Field(ge=0, le=1)
    params: dict[str, Any] = {}
    duration_s: int | None = None  # None = until DELETE /chaos
```

Faults are implemented inside the service code paths they affect: a connection leak that drains the pool, latency inside the server span, malformed records in a job's source data, a misconfigured timeout, a buggy release. This is not network-level chaos.

**Hard rule: no label leakage.** Chaos hooks must never write log lines, span attributes, metric labels or HTTP headers that reveal a fault was injected (no `"chaos"`, `"injected"`, `"fault"`). The symptoms must be indistinguishable from a real failure. A CI test greps every recorded bundle for a denylist of leak tokens (§11.2). Without this test, every accuracy number in the project is suspect.

---

## 3. Telemetry contract

All services use the OpenTelemetry Python SDK with resource attributes `service.name`, `service.version` and `deployment.environment=sim`.

### 3.1 Metrics

Names follow OTel semantic conventions where one exists. Prometheus names are what the Collector exports.

| Signal | Prometheus name | Type | Labels |
|--------|-----------------|------|--------|
| Inbound request duration | `http_server_request_duration_seconds` | histogram | `service`, `http_route`, `http_response_status_code` |
| Outbound dependency call duration | `http_client_request_duration_seconds` | histogram | `service`, `peer_service`, `status` |
| DB pool connections in use | `db_client_connection_count` | gauge | `service`, `state` (used/idle) |
| DB pool pending requests | `db_client_connection_pending_requests` | gauge | `service` |
| DB pool wait time | `db_client_connection_wait_time_seconds` | histogram | `service` |
| DB query duration | `db_client_operation_duration_seconds` | histogram | `service`, `db_operation` |
| Job duration | `job_duration_seconds` | histogram | `job_name`, `outcome` |
| Job retries | `job_retries_total` | counter | `job_name` |
| Records processed / lag | `stream_records_processed_total`, `stream_consumer_lag` | counter / gauge | `service` |
| Cache hits/misses | `cache_requests_total` | counter | `service`, `result` |
| Process memory (M2, for F7) | `process_memory_usage_bytes` | gauge | `service` |

Derived series (Prometheus recording rules, `configs/prometheus/rules/red.yml`):
`service:request_rate:1m`, `service:error_ratio:1m`, `service:latency_p95:1m`, `service:dependency_latency_p95:1m`, `job:duration_p95:5m`, `service:availability:5m`.

### 3.2 Logs

Structured JSON through the OTel log bridge. Required fields: `timestamp`, `severity`, `service`, `message`, `trace_id`, `span_id`. Optional `attrs` dict (for example `job_id`, `dataset`, `pool_size`, `exception.type`).

Log messages should look like real application logs: `"Timed out acquiring connection from pool after 5000ms"`, `"Upstream metadata call exceeded deadline (2s)"`. They are the main evidence the agent reads, so they need to be realistic and varied. Each fault type should have 3–5 phrasings chosen by seed, so the agent can't memorise one string.

### 3.3 Traces

Head sampling at 100% in sim (volumes are small). Every inbound request and every job run is a root span. Dependency calls and DB queries are child spans. Errors set `status=ERROR` and record the exception event.

### 3.4 Change events

Deploys and config changes are first-class evidence, because in real systems a large share of incidents follow a change. The control plane writes them to `var/telemetry/changes.jsonl` whenever anything is deployed, reconfigured or restarted (from M3 they are also posted to OpsPilot's `POST /api/v1/changes`). Change events record what changed, never why. **Decoy** changes that have nothing to do with the fault (§7.2, E3) are added to scenario definitions in M5.

---

## 4. Data model (`ops-db`)

Engine decided at M3 (D11). The schema below is written in Postgres dialect; on SQLite, `jsonb` → JSON text, `text[]` → JSON arrays, `vector` → a BLOB column, and log full-text search uses FTS5. Timestamps are stored in UTC.

```sql
-- Topology
services(name text pk, tier text, owner_team text, description text)
service_dependencies(caller text fk, callee text fk, kind text /* http|db|queue */, pk(caller, callee))

-- Telemetry (logs & traces; metrics live in Prometheus)
log_records(id bigserial pk, ts timestamptz, service text, severity text, message text,
            trace_id text, span_id text, attrs jsonb)
  -- index (service, ts); GIN on to_tsvector('english', message); index (trace_id)
spans(trace_id text, span_id text, parent_span_id text, service text, name text,
      start_ts timestamptz, end_ts timestamptz, duration_ms double precision,
      status text, peer_service text, attrs jsonb, pk(trace_id, span_id))
  -- index (service, start_ts); index (status, start_ts)
change_events(id uuid pk, ts timestamptz, service text, kind text /* deploy|config */,
              summary text, details jsonb)

-- Alerting & incidents
alerts(id uuid pk, fingerprint text, rule_name text, service text, severity text,
       status text /* firing|resolved */, starts_at timestamptz, ends_at timestamptz,
       labels jsonb, annotations jsonb, incident_id uuid fk null)
  -- unique (fingerprint, starts_at)  ← dedup of Alertmanager re-sends
incidents(id uuid pk, short_id text unique /* INC-0042 */, title text,
          status text /* open|investigating|mitigated|resolved */, severity text,
          opened_at timestamptz, mitigated_at timestamptz, resolved_at timestamptz,
          affected_services text[], scenario_id text null /* set only in sim; never shown to agent */)
incident_events(id bigserial pk, incident_id uuid fk, ts timestamptz, kind text, payload jsonb)

-- Investigations
investigations(id uuid pk, incident_id uuid fk, mode text /* live|bundle */,
               agent_config jsonb, agent_config_hash text, status text /* running|completed|failed|budget_exceeded */,
               started_at timestamptz, finished_at timestamptz, report jsonb,
               input_tokens int, output_tokens int, cost_usd numeric(10,5), error text)
investigation_steps(id bigserial pk, investigation_id uuid fk, seq int, kind text /* llm|tool */,
                    tool_name text, input jsonb, output_summary text, evidence_ids text[],
                    status text /* ok|validation_error|timeout|tool_error */, latency_ms int)
evidence(id text, investigation_id uuid fk, kind text /* metric|log|trace|change|alert|runbook|incident */,
         selector jsonb, summary text, payload jsonb, pk(investigation_id, id))

-- Knowledge
documents(id uuid pk, kind text /* runbook|postmortem */, title text, source_path text, version text)
chunks(id uuid pk, document_id uuid fk, ord int, text text, embedding vector(384))
  -- HNSW index on embedding

-- Actions, access, audit
pending_actions(id uuid pk, investigation_id uuid fk, action text, params jsonb,
                rationale text, evidence_ids text[], status text /* pending|approved|rejected|executed|failed */,
                requested_at timestamptz, decided_by text, decided_at timestamptz, result jsonb)
principals(id text pk /* slack:U123 | web:alice */, display_name text, role text /* viewer|responder|approver */)
audit_log(id bigserial pk, ts timestamptz, actor text, channel text /* web|slack|system|agent */,
          action text, target text, details jsonb)
processed_events(source text, event_id text, received_at timestamptz, pk(source, event_id))

-- Evaluation
eval_runs(id uuid pk, started_at timestamptz, git_sha text, system text /* B0|B1|B2|agent... */,
          agent_config jsonb, split text, perturbation text, repeats int, notes text)
eval_results(id bigserial pk, run_id uuid fk, scenario_id text, repeat int, report jsonb,
             top1 bool, top3 bool, service_top1 bool, abstained bool, expected_abstain bool,
             citation_validity double precision, evidence_precision double precision,
             evidence_recall double precision, tool_calls int, tool_failures int,
             latency_ms int, input_tokens int, output_tokens int, cost_usd numeric(10,5))
```

The evaluation tables mirror the bundle-based eval outputs. The eval CLI writes JSONL/Parquet first, and the DB write is optional, so the eval suite runs without the database.

---

## 5. Incident pipeline

### 5.1 Alert rules (`configs/prometheus/rules/alerts.yml`)

A deliberately small, realistic rule set. Symptom-based alerts page, cause-ish alerts are informational:

| Rule | Expression (sketch) | Severity |
|------|---------------------|----------|
| `HighErrorRate` | `service:error_ratio:1m > 0.05` for 30s | page |
| `HighLatencyP95` | `service:latency_p95:1m > 2 × baseline` for 30s | page |
| `JobSlow` | `job:duration_p95:5m > 1.5 × baseline` | page |
| `JobFailing` | `increase(job_runs_total{outcome="failed"}[2m]) > 0` | page |
| `DBPoolSaturated` | `db_client_connection_pending_requests > 0` for 30s | info |
| `ConsumerLagHigh` | `stream_consumer_lag > N` for 60s | info |
| `ServiceDown` | `up == 0` for 15s | page |

Baselines come from recording rules over a trailing window (`avg_over_time(...[10m] offset 10m)`), which keeps rules static and readable.

### 5.2 Ingest and dedup

`POST /api/v1/alerts/webhook` accepts the standard Alertmanager webhook payload. An alert instance is keyed on `(fingerprint, startsAt)`. Re-sends update `status`/`ends_at` idempotently. The endpoint must return 2xx for duplicates, or Alertmanager will retry.

### 5.3 Grouping alerts into incidents

An incoming firing alert joins an existing **open** incident when:

1. its `starts_at` is within **W = 5 min** of the incident's latest alert, **and**
2. its service is within **k = 2 hops** (undirected) of any service in the incident's `affected_services` in the dependency graph.

Otherwise it opens a new incident. An incident moves to `mitigated` when all its alerts resolve, and to `resolved` after 2 min with no re-fire.

Grouping quality is evaluated too: each recorded scenario should produce exactly one incident containing all fault-related alerts. Report **grouping purity** and **completeness** over the scenario set.

### 5.4 Anomaly detection and the rules-based diagnoser (B0)

`correlate.anomalies(window, baseline_window)` scores each `(service, signal)` series with a robust z-score: `(x − median) / (1.4826 × MAD)` against the baseline. It returns anomalies with an **onset time** (first point where |z| > 4 for ≥ 3 consecutive samples).

**B0 — rules-based root-cause ranking:**

1. Collect anomalous services.
2. Candidate roots = anomalous services that have **no anomalous downstream dependency**, or whose onset precedes all of their anomalous dependencies' onsets.
3. Rank candidates by onset time (earliest first), with ties broken by anomaly magnitude.
4. Map each candidate's anomaly signature to a `fault_type` using a signature table (e.g. pool pending ↑ + pool wait ↑ + DB query duration flat → `db_pool_exhaustion`; query duration ↑ → `slow_query`).
5. Boost a candidate if a change event on that service precedes the onset by < 10 min.

B0 is the first baseline, and it's also exposed to the agent as an optional tool (`get_rule_based_ranking`). Whether the agent does better *with* or *without* that tool is one of the ablations.

---

## 6. Fault catalog and labels

### 6.1 Fault taxonomy

| ID | `fault_type` | Typical target | Primary symptoms | Split |
|----|--------------|----------------|------------------|-------|
| F1 | `db_pool_exhaustion` | stream-processor, query-engine | pool pending ↑, pool wait ↑, upstream latency ↑, timeouts in logs | dev+test |
| F2 | `dependency_latency` | metadata | metadata p95 ↑, callers' dependency latency ↑, job duration ↑ | dev+test |
| F3 | `upstream_timeout_misconfig` | query-engine→metadata | config change event, timeouts at caller, callee healthy | dev+test |
| F4 | `bad_input_job_failure` | stream-processor | job failures, schema/parse exceptions, retries ↑ | dev+test |
| F5 | `bad_deploy_errors` | any app service | deploy event, error ratio ↑ on that service only | dev+test |
| F6 | `slow_query` | platform-db (via query-engine or metadata) | DB operation duration ↑ for one operation, pool fills secondarily | dev+test |
| F7 | `memory_leak` | stream-processor | memory ↑ monotonically, latency ↑ then restarts | dev+test |
| F8 | `consumer_backpressure` | stream-processor | consumer lag ↑, throughput ↓, downstream freshness ↓ | dev+test |
| F9 | `retry_storm` | scheduler | retries ↑, load on metadata ↑ (the *cause* is scheduler, not metadata) | **held-out** |
| F10 | `cache_disabled` | metadata | cache miss ratio → 1, DB load ↑, metadata latency ↑ | **held-out** |
| F11 | `db_disk_full` | platform-db | write errors across all writers, reads fine | **held-out** |
| F0 | `none` | — | noise only, or a symptom-free decoy change | dev+test |

F9–F11 are **never** looked at during prompt or rule tuning. They measure generalization to unseen fault types. F9 is a deliberate trap: the symptoms are loudest on `metadata`, but the root cause is `scheduler`.

### 6.2 Scenario definition (`scenarios/defs/*.yaml`)

```yaml
id: f2-metadata-latency-s03
fault:
  fault_type: dependency_latency
  target: metadata
  intensity: 0.6            # maps to +600ms median added latency
  params: { jitter_ms: 150 }
timeline: { baseline_s: 180, fault_s: 300, recovery_s: 120 }
seed: 3
label:
  root_cause: { service: metadata, fault_type: dependency_latency }
  acceptable_alternates: []  # rarely used; e.g. pool exhaustion caused by slow queries
  expected_abstain: false
  key_evidence:              # selectors used to auto-judge evidence (§8.3)
    - { kind: metric, service: metadata, signal: latency_p95, direction: up }
    - { kind: trace, service: metadata, signal: span_duration, direction: up }
    - { kind: log, service: query-engine, pattern: "(deadline|timed out).*metadata" }
```

Selector kinds: `metric` (a signal from `opspilot/store/signals.py`, optional extra `labels`), `log` (regex over the message), `trace` (`span_duration` or `span_errors` for a service's SERVER spans) and `change` (`deploy`/`config`/`restart`). Decoys (E3) and per-scenario workload settings are added when the experiments that need them are built.

Target dataset: **11 fault types × 2–3 targets/intensities × 3 seeds ≈ 60–80 base scenarios**, plus perturbation variants generated from them (§7.2).

**Split.** Base scenarios are split by `(fault_type, seed)`: seeds 1–2 → `dev`, seed 3 → `test`. F9–F11 go to `heldout` only. `test` and `heldout` results are run **once per frozen agent version** and are never used to tune anything. Keep a `CHANGELOG` of when each split was run.

### 6.3 Scenario bundle format

```
scenarios/bundles/<scenario_id>/
  manifest.json      # scenario def, label, window (start, fault_start, fault_end, end), git sha, checksums
  metrics.parquet    # (ts, metric, labels JSON, value): every platform series and recording rule, 5 s step
  logs.parquet       # (ts, service, severity, message, trace_id, span_id, attrs JSON)
  spans.parquet      # (trace_id, span_id, parent, service, name, kind, start/end, duration_ms, is_error, peer_service, attrs)
  changes.parquet    # (id, ts, service, kind, summary, details JSON)
  topology.json      # declared services, dependencies, timeouts
```

`alerts.parquet` joins the format in M3, when alerting exists; bundles are re-recorded then (definitions are the source of truth, and recording is unattended).

The label and fault window are stored in `manifest.json`, but `BundleStore` **never exposes them**: tools only see the overall window. Evaluation code reads them through `load_ground_truth`. Tests assert that no store output contains the scenario id or fault boundaries, and that no recorded string matches the leak denylist.

`python -m scenarios.check` verifies each bundle against its own `key_evidence`: every selector must show a real shift between the baseline and fault windows. A bundle that fails is mislabelled or under-powered and is not used for evaluation.

Bundles are generated artifacts and are gitignored. Commit the scenario definitions and rebuild bundles with `python -m scenarios.record scenarios/defs/*.yaml` (about 13 minutes per scenario, including a 3-minute unrecorded cooldown). Expect a few MB per bundle.

---

## 7. AI investigation agent

### 7.1 Loop

```
input: incident_id (+ optional engineer question)
1. context  : get_incident → alerts, affected services, time window
2. explore  : tool calls (metrics, logs, traces, changes, graph, runbooks)
3. hypothesize + verify : for each candidate, look for supporting AND contradicting evidence
4. report   : emit InvestigationReport (structured output), validated server-side
budget: max 16 tool calls, max 120 s wall, max 60k input tokens. Exceeding a budget → report with status=budget_exceeded
```

The loop is a plain tool-use loop: the model calls tools, results go back as tool results, and the loop ends when the model calls `submit_report`. No framework is needed, and it's easier to instrument and explain without one.

**LLM provider.** A thin `LLMClient` over Groq's OpenAI-compatible chat-completions API with tool calling. Models: `openai/gpt-oss-20b` (main results) and `openai/gpt-oss-120b` (size comparison), both on Groq's free tier. Token counts come from the API response `usage` field.

### 7.2 Tools

All tools are read-only except `propose_remediation`, which only creates a pending record. Inputs are Pydantic models, and a validation failure returns a structured error to the model (counted as `validation_error`) instead of raising. Every result includes the evidence it produced.

| Tool | Input (key fields) | Returns |
|------|--------------------|---------|
| `get_incident` | `incident_id` | title, window, alerts, affected services |
| `get_dependency_graph` | — | declared edges + observed edges from spans |
| `query_metric` | `service`, `signal` (enum of §3.1 derived signals), `start`, `end` | downsampled series (≤ 60 points) + summary stats + baseline comparison |
| `find_anomalies` | `start`, `end`, `services?` | anomaly list with onset times (from §5.4) |
| `search_logs` | `service?`, `severity?`, `query` (full-text), `start`, `end`, `limit ≤ 50` | log records, deduplicated by message template with counts |
| `get_trace_breakdown` | `start`, `end`, `service?`, `status?` | per-service share of critical-path latency, top error spans, example trace IDs |
| `get_trace` | `trace_id` | span tree |
| `list_changes` | `start`, `end`, `service?` | deploy/config events |
| `search_runbooks` | `query`, `k ≤ 5` | runbook chunks |
| `search_past_incidents` | `query`, `k ≤ 5` | summaries of resolved incidents (sim-generated postmortems from the dev split only) |
| `get_rule_based_ranking` | `incident_id` | B0 output (ablation-controlled) |
| `propose_remediation` | `action` (enum), `params`, `rationale`, `evidence_ids` | `pending_action_id` |

Remediation action enum (executed only against the simulator after approval): `rollback_deploy`, `revert_config`, `restart_service`, `scale_pool`, `pause_job`, `clear_fault` (sim-only escape hatch, not offered to the agent).

**Output discipline.** Tool results are summaries, not raw dumps. That keeps token cost predictable, and the summarisation code is ordinary, testable Python. Raw data stays reachable through `evidence_id` in the console.

### 7.3 Evidence ledger

Every tool result registers evidence items:

```python
class Evidence(BaseModel):
    id: str  # "ev_" + first 10 hex of sha256(kind + canonical selector JSON)
    kind: Literal["metric", "log", "trace", "change", "alert", "runbook", "incident"]
    selector: dict  # e.g. {"service": "metadata", "signal": "latency_p95", "start": ..., "end": ...}
    summary: (
        str  # one line shown to the model, e.g. "metadata p95 1.9s vs baseline 0.21s (+805%) from 10:03:15"
    )
```

IDs are deterministic, so the same evidence gets the same ID across runs and systems. That's what makes cross-run analysis and the automatic evidence judge (§8.3) possible.

### 7.4 Report schema

```python
class Hypothesis(BaseModel):
    service: str
    fault_type: FaultType | Literal["other"]
    confidence: float = Field(ge=0, le=1)
    summary: str
    supporting_evidence: list[str]  # evidence IDs, min 1
    contradicting_evidence: list[str]  # evidence IDs, may be empty
    reasoning: str


class InvestigationReport(BaseModel):
    status: Literal["diagnosed", "insufficient_evidence"]
    hypotheses: list[Hypothesis]  # ranked; ≤ 3; empty only if insufficient_evidence
    missing_evidence: list[str]  # what would have helped (required if insufficient_evidence)
    impact_summary: str
    recommended_next_steps: list[str]
    proposed_actions: list[str]  # pending_action IDs
```

**Server-side validation** (fails → one repair turn with the error message, then `failed`):

- every cited ID exists in this investigation's ledger;
- `diagnosed` ⇒ ≥ 1 hypothesis, and the top hypothesis has ≥ 2 supporting items of ≥ 2 different `kind`s;
- `service` is a known service.

Keep the *pre-validation* failure counts: "x% of first-draft reports cited a non-existent evidence ID" is a real, reportable finding.

### 7.5 Abstention

The agent is instructed to return `insufficient_evidence` when no hypothesis meets the evidence bar. A deterministic **abstention gate** (configurable, on by default) also downgrades a `diagnosed` report whose top hypothesis has confidence < τ or fails the two-kinds rule. The gate is an ablation (on/off), and τ is tuned on `dev` only.

This is the operational analogue of Meeting Synth's relevance gating, and the false-abstention analysis carries over directly.

### 7.6 Runbooks and past incidents

`knowledge/runbooks/*.md` holds ~15 hand-written runbooks, one or two per subsystem/symptom, written the way real teams write them: partly outdated, organised by symptom rather than cause. Embed them with a local `bge-small-en-v1.5` model (384-d, runs fine on CPU or the RTX 3050) and chunk by heading.

Past incidents: after each `dev` scenario, generate a short postmortem (template-filled from the label, not LLM-written) and index it. Only dev-split postmortems are indexed, so test/heldout scenarios can't retrieve their own answers.

---

## 8. Evaluation methodology

### 8.1 Systems compared

| ID | System | Purpose |
|----|--------|---------|
| B0 | Rules-based diagnoser (§5.4) | The bar to beat; no LLM |
| B1 | LLM, no tools: given alerts + incident summary only | Measures what telemetry access adds |
| B2 | Agent without RAG (no runbooks/past incidents) | Measures what retrieval adds |
| B3 | Agent without `get_rule_based_ranking` | Measures whether B0 output helps the agent |
| A | Full agent (Sonnet) | Main system |
| A-h | Full agent (Haiku) | Cost/accuracy trade-off |
| A-ng | Full agent, abstention gate off | Measures the gate |

### 8.2 Experiments

| ID | Name | Manipulation | Main question |
|----|------|--------------|---------------|
| E1 | Baseline | Unmodified bundles | Accuracy of each system |
| E2 | Missing evidence | Bundle transforms: drop logs for the root service; drop all traces; drop the single highest-value signal (from `key_evidence`) | Does accuracy degrade gracefully, and does the agent abstain instead of guessing? |
| E3 | Confounders | Add an unrelated latency spike on a non-root service; add a decoy deploy 2 min before onset on a non-root service | Is the agent fooled by correlation-in-time? |
| E4 | Tool failure | Runtime `ToolFaultPolicy`: p ∈ {0.1, 0.3} per call of timeout / malformed JSON / empty result | Completion rate, no fabricated evidence, accuracy under degraded tools |
| E5 | Recovery (live) | Live stack: inject → investigate → approve proposed action → executes against sim | Is the remediation correct? Is the incident correctly marked mitigated/resolved? Time to recovery |
| E6 | Regression | Run `dev` + fixed perturbation set on every prompt/agent change in CI (subset) | Catch regressions; track metrics over git history |
| E7 | Generalization | `heldout` split (F9–F11) | Performance on fault types never used during tuning |

Perturbations are pure functions `Bundle → Bundle` in `eval/perturb.py`, each with its own unit tests. A perturbed bundle's `manifest.json` records the transform chain. For E2, `expected_abstain` is recomputed: if every `key_evidence` selector has been removed, the correct answer is to abstain.

### 8.3 Metrics (exact definitions)

| Metric | Definition |
|--------|------------|
| **Top-1 accuracy** | Top hypothesis `(service, fault_type)` equals label or an `acceptable_alternate`. Abstentions count as wrong, on non-abstain scenarios only. |
| **Top-3 accuracy** | Label appears in the first 3 hypotheses. |
| **Service top-1** | Top hypothesis service matches (ignores `fault_type`). |
| **Abstention precision / recall** | Over all scenarios, treating "abstained" as the predicted positive and `expected_abstain` as truth. |
| **False-abstention rate** | Fraction of diagnosable scenarios where the system abstained. |
| **Selective accuracy** | Top-1 accuracy over scenarios where the system did not abstain. Report alongside coverage (a risk–coverage curve over τ). |
| **Citation validity** | Fraction of cited IDs that existed in the ledger, *before* validation/repair. |
| **Evidence precision** | Of evidence cited as supporting the top hypothesis (when correct), the fraction that matches a `key_evidence` selector or another selector judged relevant (see below). |
| **Evidence recall** | Fraction of `key_evidence` selectors matched by at least one cited evidence item. |
| **Tool-call success rate** | `ok` steps / tool steps; reported separately for validation errors (model's fault) vs injected/real tool errors. |
| **Completion rate** | Investigations ending in a valid report (diagnosed or abstained) / total. Must be 100% under E4 for "graceful handling". |
| **Latency** | Median and p95 wall time per investigation. |
| **Cost** | Median input/output tokens and cost per investigation. |
| **Grouping purity / completeness** | §5.3, on recorded scenarios. |

**Evidence judging.** Automatic matching against `key_evidence` selectors is the primary judge. Because selectors under-cover relevant evidence, hand-label a random sample of **50** cited evidence items (supporting / irrelevant / contradicting) and report agreement between the automatic judge and the hand labels. An LLM judge is optional and secondary. If you use one, report its agreement with the hand labels too, rather than trusting it.

**Unsupported claims.** Manually audit the `reasoning` field of 30 reports: count factual claims about telemetry and how many are backed by a cited evidence item. This is slow, so do it once on the final frozen version.

### 8.4 Statistics and reporting

- LLM systems run **3 repeats** per scenario. Report means with 95% CIs (Wilson for proportions, bootstrap over scenarios for latency/cost).
- With ~25 test scenarios per condition, CIs will be wide (±15–20 pp). Say so. Don't claim a difference between two systems whose intervals overlap heavily; use a paired comparison (McNemar on per-scenario correctness) when the comparison matters.
- Every eval run records git SHA, agent config hash, model ID, bundle checksums and perturbation chain. A results table without these is not reported.
- `eval report <run_ids>` generates `results/<date>/report.md` with tables plus a per-scenario failure listing. Read the failures: the most useful writeup material is *why* the agent fails (e.g. "blamed metadata in 7/9 retry-storm runs because it's the loudest service").

---

## 9. API contracts (`/api/v1`)

All JSON, Pydantic-validated. Errors use RFC 9457 `application/problem+json`. Auth for the console is a simple dev token mapping to a `principals` row. Slack requests are authenticated by Slack and mapped through `principals` by Slack user ID.

| Method & path | Body / query | Response | Notes |
|---------------|--------------|----------|-------|
| `POST /v1/logs`, `POST /v1/traces` | OTLP/HTTP protobuf | 200 | Called by the Collector's `otlphttp` exporter; mounted at root, not under `/api` |
| `POST /api/v1/alerts/webhook` | Alertmanager webhook | 200 always for well-formed payloads | Idempotent (§5.2) |
| `POST /api/v1/changes` | `ChangeEvent` | 201 | Used by the sim |
| `GET /api/v1/services` | — | `ServiceSummary[]` with current RED + status | |
| `GET /api/v1/services/{name}/health` | `start`, `end`, `step` | RED time series | Proxies PromQL |
| `GET /api/v1/graph` | — | nodes + declared/observed edges with health | |
| `GET /api/v1/incidents` | `status?`, `limit`, `cursor` | `IncidentSummary[]` | |
| `GET /api/v1/incidents/{id}` | — | incident + alerts + timeline | `scenario_id` never serialized |
| `POST /api/v1/incidents/{id}/investigations` | `{question?: str}` | 202 `{investigation_id}` | Role ≥ responder. Runs as a background task. One running investigation per incident (409 otherwise) |
| `GET /api/v1/investigations/{id}` | — | status, steps, report | |
| `GET /api/v1/investigations/{id}/stream` | — | SSE: `step`, `report`, `error` events | For the console's live view |
| `GET /api/v1/evidence/{investigation_id}/{evidence_id}` | — | evidence + underlying raw data | Evidence links in the UI and Slack land here |
| `GET /api/v1/runbooks/search` | `q`, `k` | chunks | |
| `GET /api/v1/actions` | `status?` | `PendingAction[]` | |
| `POST /api/v1/actions/{id}/decision` | `{decision: approve|reject, reason}` | updated action | Role = approver; approver ≠ requester of the investigation; executes against the sim on approve |
| `GET /api/v1/audit` | filters | audit entries | |
| `GET /healthz`, `GET /readyz` | — | | readyz checks DB + Prometheus |

Every mutating endpoint writes an `audit_log` row in the same transaction.

---

## 10. Interfaces

### 10.1 Web console (React + TS + Vite + Plotly)

Five views, no more until results exist:

1. **Service health:** per-service RED tiles plus a time-range picker; dependency graph coloured by health.
2. **Incidents:** list → detail with an alert timeline (Plotly) and affected services highlighted on the graph.
3. **Investigation:** live step stream (SSE), ranked hypotheses, and an evidence panel where clicking an evidence ID shows the underlying chart/logs/trace.
4. **Approvals:** pending actions with rationale and evidence, approve/reject.
5. **Evaluation:** reads `results/*.json`; system × experiment table and the risk–coverage curve.

### 10.2 Slack bot (Socket Mode, `slack_bolt`)

| Interaction | Behaviour |
|-------------|-----------|
| Incident opened/resolved | Post to `#opspilot-incidents` with severity, affected services, console link |
| `/opspilot status [service]` | Current RED summary |
| `/opspilot incident INC-0042` | Summary + latest investigation result |
| `/opspilot investigate INC-0042` | Starts an investigation, acks immediately, then posts the report into the thread when done |
| `/opspilot runbook <query>` | Top 3 runbook matches |
| Approve / Reject buttons | On proposed actions; role-checked; message updated with the decision |

**Reliability requirements:**

- ack every request within 3 s, and do the work asynchronously;
- dedupe on Slack `event_id` / interaction payload ID via `processed_events`;
- per-command timeout (investigations post "still running…" after 60 s);
- unknown Slack users are treated as `viewer`;
- every command produces an `audit_log` row.

---

## 11. Testing, CI and repo layout

### 11.1 Repo layout

```
opspilot/
  api/            FastAPI app, routers, schemas
  agent/          loop, tools, ledger, report validation, llm client
  correlate/      grouping, anomalies, B0
  store/          TelemetryStore protocol, LiveStore, BundleStore
  migrations/
sim/              service app, chaos hooks, topology.yaml, workload-gen
scenarios/
  defs/           *.yaml (committed)
  bundles/        generated (LFS or rebuilt)
  recorder.py
eval/             runner, perturb.py, metrics.py, judge.py, report.py, tool_faults.py
knowledge/        runbooks/*.md
console/          React app
slackbot/
configs/          otelcol.yaml, prometheus/ (config + rules), alertmanager/
scripts/          fetch_tools.py (Prometheus/Collector binaries)
tools/            fetched binaries (gitignored)
var/              runtime state: platform.db, Prometheus TSDB, telemetry JSONL, process logs (gitignored)
tests/            unit/, integration/, eval_smoke/
docs/             SPEC.md, ARCHITECTURE.md, RESULTS.md
```

### 11.2 Test layers

| Layer | What | Runs in CI |
|-------|------|------------|
| Unit | grouping, anomaly scoring, B0, perturbations, evidence ID hashing, report validation, Pydantic contracts | yes |
| Leak test | grep all bundles + tool outputs for leak tokens; assert no manifest fields in tool outputs | yes |
| Agent contract | Agent loop against a **scripted fake LLM** (replays canned tool calls): budgets, repair turn, tool-error handling, ledger enforcement | yes |
| Integration | `python -m sim.run up` sim + Prometheus + API; inject F1; assert an incident opens with the expected alerts within 90 s | nightly / manual (heavy) |
| Eval smoke | B0 on 10 committed small bundles; fail if top-1 drops below a recorded floor | yes |
| Agent regression (E6) | Full agent on a 10-scenario dev subset, 1 repeat | manual workflow dispatch (costs tokens; needs an API key secret) |

GitHub Actions: lint (ruff), type-check (mypy on `opspilot/`), unit + contract + smoke tests, and console build/type-check.

---

## 12. Milestones and acceptance criteria

Roughly 10 weeks. Each milestone ends with something you can demo, and its acceptance criteria are checkable.

**M1 — Simulated platform emits telemetry (week 1)**
- `python -m sim.run up` brings up 5 services + platform-db + Collector + Prometheus as native processes.
- Workload runs; Prometheus shows RED metrics for every service; logs and spans contain trace IDs.
- ✅ Accept: `python -m sim.verify` passes — RED series for every service, healthy recording rules, a single trace spanning gateway → query-engine → metadata → platform-db, and logs whose trace IDs resolve to exported traces.

**M2 — Fault injection + scenario recorder (week 2)**
- In-process faults (F1, F2, F4) via `/chaos`; operational faults (F3, F5) via the control plane; `python -m scenarios.record` produces a bundle; `BundleStore` reads it.
- ✅ Accept: 5 scenario definitions recorded; leak test passes; `python -m scenarios.check` shows every key-evidence selector visible in every bundle.

**M3 — Incidents + B0 + eval harness (weeks 3–4)**
- Alert rules, Alertmanager webhook, grouping, anomaly detection, B0.
- `eval run --system B0 --split dev` produces a metrics report.
- Console views 1–2.
- ✅ Accept: all F1–F8 dev scenarios recorded (≥ 30 bundles); each produces exactly one incident (grouping purity reported); **first B0 numbers exist**.

**M4 — Agent v1 (weeks 5–6)**
- Tools, ledger, report schema + validation, runbook RAG, abstention gate, scripted-fake-LLM contract tests.
- ✅ Accept: agent runs end-to-end on all dev bundles with 100% completion; dev top-1 reported next to B0, B1 and B2.

**M5 — Robustness experiments (week 7)**
- Perturbations (E2, E3) and the tool-fault policy (E4).
- ✅ Accept: E2–E4 results on dev; failure listing reviewed and at least one agent improvement made *and measured*.

**M6 — Slack, approvals, recovery loop (week 8)**
- Slack bot (§10.2), approvals, sim-side execution of approved actions, E5 on 5 live runs.
- ✅ Accept: in a recorded demo, an incident is opened in Slack, investigated, a fix is approved via button, and the incident resolves. Duplicate-event test passes.

**M7 — Freeze and final evaluation (week 9)**
- Freeze agent version; run `test` and `heldout` once for all systems with 3 repeats; manual evidence and claim audits.
- Console views 3–5.
- ✅ Accept: `docs/RESULTS.md` with all tables, CIs, the risk–coverage curve and failure analysis.

**M8 — Polish (week 10)**
- README with architecture diagram, setup in ≤ 3 commands, design-decision rationale (§1), limitations, a 3-minute demo video.

**Scope fallback, if behind schedule** (cut in this order): console view 5 → E5 recovery loop → Slack approval buttons (keep notifications + commands) → Haiku row → held-out F11. **Never cut:** bundles, B0, leak test, E1/E2/E7, held-out discipline.

---

## 13. Non-goals

- Training or fine-tuning any model.
- Kubernetes, Grafana, Loki, Tempo, MCP in the core build. MCP is a reasonable *post-results* extension: exposing the tool set as an MCP server is a few hundred lines once the tools exist.
- Autonomous remediation, or any action against anything other than the simulator.
- Multi-tenant auth, SSO, production hardening.
- Reproducing any company's internal systems. This is an independently built system for the same class of problems.

---

## 14. Risks

| Risk | Mitigation |
|------|------------|
| **Label leakage** makes results meaningless | Leak test in CI; neutral log phrasing; `scenario_id` never serialized to tools/agent |
| **Test-set contamination** through repeated tuning | Split by seed + held-out fault types; test/heldout run only on frozen versions, logged in a changelog |
| **Scenarios too easy**, so everything scores ~100% | Intensity sweep including subtle faults; confounders; F9 trap; if B0 is already near-perfect on E1, the story shifts to E2–E4 and E7, which is fine |
| **Small n** gives wide CIs | Report CIs and paired tests honestly; repeats; prefer fewer, sharper claims |
| **LLM nondeterminism** | 3 repeats; report variance; config hash on every result |
| **Laptop resources** | Native processes, no VM; Prometheus retention capped at 6 h; Collector rotates telemetry files at 100 MB; eval needs no live stack |
| **Scope creep** | §12 fallback order; nothing in §13 until RESULTS.md exists |

---

## 15. Resume framing (only after results exist)

Fill the brackets from `RESULTS.md`; don't write the numbers ahead of time.

- Built a simulated 6-service data platform instrumented with OpenTelemetry and Prometheus, with a fault-injection and record/replay harness producing [N] labelled incident scenarios across [K] fault types.
- Developed a tool-calling LLM incident investigator with an evidence ledger that enforces citations; achieved [x]% top-1 / [y]% top-3 root-cause accuracy on held-out scenarios vs [z]% for a rules-based baseline.
- Measured robustness under missing telemetry, confounding changes and tool failures: [one concrete finding, e.g. "abstention gate cut wrong diagnoses under missing evidence from A to B at a C-point coverage cost"].
- Integrated Slack incident workflows with human-approved remediation, idempotent event handling and audit logging; CI runs contract, leak-detection and eval regression tests.
