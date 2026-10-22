"""Evidence-grounded investigation agent (LLM via Groq's OpenAI-compatible API).

Triage-first design: deterministic code builds a compact incident briefing (opspilot.analysis),
the model reads it, may drill down with a few read-only tools, and must submit a diagnosis that
cites evidence ids. Every id a tool returns goes into the investigation's evidence ledger;
citations are checked against it. This keeps an investigation to roughly 10k tokens, which
fits a free-tier token budget.
"""

from __future__ import annotations

import json
import os
import time
from datetime import timedelta
from typing import Any

import httpx
from pydantic import ValidationError

from opspilot.analysis import REFERENCE_S, Evidence, Triage, evidence_id, template, triage
from opspilot.diagnosis import FAULT_DESCRIPTIONS, FAULT_TYPES, Diagnosis, RunStats
from opspilot.store.bundle import BundleStore
from opspilot.store.signals import SIGNALS

GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"
DEFAULT_MODEL = "openai/gpt-oss-120b"
MAX_TURNS = 7
MAX_OUTPUT_TOKENS = 2000


# --- LLM client ------------------------------------------------------------------------------


class LLMClient:
    """Minimal chat-completions client with rate-limit-aware retries."""

    def __init__(self, model: str = DEFAULT_MODEL, api_key: str | None = None) -> None:
        key = api_key or os.environ.get("GROQ_API_KEY")
        if not key:
            raise RuntimeError("GROQ_API_KEY is not set")
        self.model = model
        self._http = httpx.Client(timeout=120, headers={"Authorization": f"Bearer {key}"})

    def chat(self, messages: list[dict], tools: list[dict] | None, tool_choice: Any = "auto") -> dict:
        body: dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "temperature": 0,
            "max_completion_tokens": MAX_OUTPUT_TOKENS,
        }
        if tools:
            body["tools"] = tools
            body["tool_choice"] = tool_choice
        else:
            body["response_format"] = {"type": "json_object"}
        if self.model.startswith("openai/gpt-oss"):
            body["reasoning_effort"] = "low"
        for attempt in range(8):
            resp = self._http.post(GROQ_URL, json=body)
            if resp.status_code == 429 or resp.status_code >= 500:
                wait = float(resp.headers.get("retry-after", 0) or 0) or min(2**attempt * 5, 60)
                if "per day" in resp.text.lower() or "tokens per day" in resp.text.lower():
                    raise RuntimeError(f"daily token limit reached: {resp.text[:200]}")
                time.sleep(min(wait + 1, 120))
                continue
            if resp.status_code == 400 and attempt < 3:
                continue  # usually a malformed tool call from the model; sampling again fixes it
            if resp.status_code >= 400:
                raise RuntimeError(f"LLM request failed: {resp.status_code} {resp.text[:300]}")
            return resp.json()
        raise RuntimeError(f"LLM request kept failing: {resp.status_code} {resp.text[:200]}")


# --- Prompt ----------------------------------------------------------------------------------

SYSTEM = """You are an SRE investigating an incident on a data platform. Find the root cause: \
the single service whose own fault started the problem, and what kind of fault it is.

Platform services and dependencies (caller -> callees):
{topology}

Fault types:
{faults}

How to work:
- Start from the briefing. Symptoms propagate UP the call graph: when a service is slow or \
failing, its callers show errors/latency too. The root cause is usually the most downstream \
service with its own anomaly, or the service that changed (deploy/config) right before symptoms.
- Background noise exists: the batch pipeline runs every 2 minutes and briefly raises latency on services that touch the database. Anomalies marked "(minor)" are within that normal variation. If the briefing has only minor anomalies and no error log patterns, error spans or changes, submit "no_incident" without drilling down.
- Check the timeline: the cause starts first. Changes that happen after symptoms began, or \
that restore earlier settings, are reactions, not causes.
- Drill down with tools only when it changes your answer. You have at most {turns} turns.
- If nothing is meaningfully wrong (no clear anomaly or error pattern), submit status \
"no_incident". If something is wrong but evidence doesn't support a cause, submit \
"insufficient_evidence".
- Cite evidence ids (ev_...) exactly as given; never invent ids.
- Finish by calling submit_diagnosis with up to 3 ranked hypotheses."""


def _topology_text(topology: dict) -> str:
    lines = []
    for name, svc in topology["services"].items():
        calls = ", ".join(svc["calls"]) or "-"
        lines.append(f"- {name} ({svc['description']}) -> {calls}")
    return "\n".join(lines)


def briefing(t: Triage) -> str:
    def section(title: str, items: list[str]) -> str:
        return f"## {title}\n" + ("\n".join(items) if items else "(none)")

    window = (
        f"Window {t.start:%H:%M:%S}-{t.end:%H:%M:%S} UTC; first {REFERENCE_S}s used as the normal reference."
    )
    return "\n\n".join(
        [
            window,
            section(
                "Anomalous signals (earliest first)",
                [f"[{a.evidence.id}] {a.evidence.summary}" for a in t.anomalies[:20]],
            ),
            section(
                "New/increased WARNING+ERROR log patterns",
                [f"[{p.evidence.id}] {p.evidence.summary}" for p in t.log_patterns],
            ),
            section(
                "Changes (deploys, config, restarts)",
                [f"[{c.id}] {c.service}: {c.summary}" for c in t.changes],
            ),
            section("Span behaviour", [f"[{e.id}] {e.summary}" for e in t.span_shifts]),
        ]
    )


# --- Tools -----------------------------------------------------------------------------------

TOOLS: list[dict] = [
    {
        "type": "function",
        "function": {
            "name": "query_metric",
            "description": "Time series of one signal for one service, downsampled, with reference vs peak.",
            "parameters": {
                "type": "object",
                "properties": {
                    "signal": {"type": "string", "enum": sorted(SIGNALS)},
                    "service": {"type": "string"},
                    "peer_service": {"type": "string", "description": "for dependency_* signals"},
                    "job_name": {
                        "type": "string",
                        "description": "for job_* signals: ingest or nightly_pipeline",
                    },
                },
                "required": ["signal", "service"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_logs",
            "description": "Log message patterns (grouped, with counts) for a service, optionally filtered by regex.",
            "parameters": {
                "type": "object",
                "properties": {
                    "service": {"type": "string"},
                    "pattern": {"type": "string", "description": "regular expression over the message"},
                    "severity": {"type": "string", "enum": ["INFO", "WARNING", "ERROR"]},
                },
                "required": ["service"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_service_spans",
            "description": "A service's server-span latency before/after, its error spans, and time spent in each "
            "downstream call (to tell self-time from time waiting on dependencies).",
            "parameters": {
                "type": "object",
                "properties": {"service": {"type": "string"}},
                "required": ["service"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "submit_diagnosis",
            "description": "Submit the final diagnosis. Call exactly once, at the end.",
            "parameters": {
                "type": "object",
                "properties": {
                    "status": {
                        "type": "string",
                        "enum": ["diagnosed", "no_incident", "insufficient_evidence"],
                    },
                    "summary": {"type": "string"},
                    "hypotheses": {
                        "type": "array",
                        "maxItems": 3,
                        "items": {
                            "type": "object",
                            "properties": {
                                "service": {"type": "string"},
                                "fault_type": {"type": "string", "enum": FAULT_TYPES},
                                "confidence": {"type": "number"},
                                "summary": {"type": "string"},
                                "evidence": {"type": "array", "items": {"type": "string"}},
                            },
                            "required": ["service", "fault_type", "confidence", "evidence"],
                        },
                    },
                },
                "required": ["status", "summary", "hypotheses"],
            },
        },
    },
]


class Tools:
    def __init__(self, store: BundleStore, ledger: dict[str, Evidence]) -> None:
        self.store = store
        self.ledger = ledger
        self.services = set(store.services())

    def _register(self, kind: str, service: str, selector: dict, summary: str) -> str:
        ev = Evidence(evidence_id(kind, selector), kind, service, summary, selector)
        self.ledger[ev.id] = ev
        return ev.id

    def _check_service(self, service: str) -> None:
        if service not in self.services:
            raise ValueError(f"unknown service {service!r}; services: {sorted(self.services)}")

    def query_metric(
        self, signal: str, service: str, peer_service: str | None = None, job_name: str | None = None
    ) -> str:
        if signal not in SIGNALS:
            raise ValueError(f"unknown signal {signal!r}")
        self._check_service(service)
        labels = {k: v for k, v in (("peer_service", peer_service), ("job_name", job_name)) if v}
        series = self.store.metric(signal, service, labels=labels)
        if not series:
            return f"no data for {signal} on {service} {labels or ''}".strip()
        start, _ = self.store.time_range()
        cut = start + timedelta(seconds=REFERENCE_S)
        unit = SIGNALS[signal].unit
        out = []
        for s in series[:4]:
            extra = {
                k: v for k, v in s.labels.items() if k not in ("service_name", "instance", "service_version")
            }
            pts = s.points[:: max(len(s.points) // 12, 1)]
            ref = [v for ts, v in s.points if ts < cut]
            selector = {"signal": signal, "service": service, **extra}
            ref_txt = f"{max(ref):.4g}" if ref else "n/a"
            summary = f"{service} {signal} {extra}: reference max {ref_txt}, peak {max(v for _, v in s.points):.4g} {unit}"
            ev = self._register("metric", service, selector, summary)
            series_txt = ", ".join(f"{ts:%H:%M:%S}={v:.3g}" for ts, v in pts)
            out.append(f"[{ev}] {summary}\n  {series_txt}")
        return "\n".join(out)

    def search_logs(self, service: str, pattern: str | None = None, severity: str | None = None) -> str:
        self._check_service(service)
        rows = self.store.logs(service=service, severity=severity, pattern=pattern, limit=5000)
        groups: dict[str, dict[str, Any]] = {}
        for r in rows:
            tmpl = template(r["message"])
            g = groups.setdefault(tmpl, {"n": 0, "first": r["ts"], "sev": r["severity"], "ex": r["message"]})
            g["n"] += 1
            g["first"] = min(g["first"], r["ts"])
        if not groups:
            return "no matching logs"
        out = []
        for tmpl, g in sorted(groups.items(), key=lambda kv: -kv[1]["n"])[:8]:
            summary = f"{service} {g['sev']} x{g['n']} from {g['first']:%H:%M:%S}: {tmpl}"
            ev = self._register("log", service, {"service": service, "template": tmpl}, summary)
            out.append(f"[{ev}] {summary}\n  e.g. {g['ex'].splitlines()[0][:200]}")
        return "\n".join(out)

    def get_service_spans(self, service: str) -> str:
        self._check_service(service)
        start, end = self.store.time_range()
        cut = start + timedelta(seconds=REFERENCE_S)
        lines = []
        for label, a, b in (("before", start, cut), ("after", cut, end)):
            p50 = self.store.span_duration_quantile(service, 0.5, a, b)
            p90 = self.store.span_duration_quantile(service, 0.9, a, b)
            errs = len(
                self.store.spans(service, kind="SERVER", errors_only=True, start=a, end=b, limit=100_000)
            )
            if p50 is None:
                lines.append(f"{label}: no server spans")
                continue
            lines.append(f"{label}: server p50 {p50:.1f}ms p90 {p90:.1f}ms, error spans {errs}")
        # Downstream time after the reference period: CLIENT spans by peer.
        client = self.store.spans(service, kind="CLIENT", start=cut, end=end, limit=200_000)
        by_peer: dict[str, list[float]] = {}
        for s in client:
            by_peer.setdefault(s["peer_service"] or "?", []).append(s["duration_ms"])
        for peer, ds in sorted(by_peer.items()):
            ds.sort()
            lines.append(
                f"calls to {peer}: n={len(ds)} p50 {ds[len(ds) // 2]:.1f}ms p90 {ds[int(len(ds) * 0.9)]:.1f}ms"
            )
        summary = f"{service} spans: " + "; ".join(lines)
        ev = self._register("trace", service, {"service": service, "signal": "span_breakdown"}, summary)
        return f"[{ev}] " + "\n".join(lines)

    def call(self, name: str, args: dict) -> str:
        if name not in ("query_metric", "search_logs", "get_service_spans"):
            raise ValueError(f"unknown tool {name}")
        return getattr(self, name)(**args)


# --- Loop ------------------------------------------------------------------------------------


def investigate(
    store: BundleStore, client: LLMClient, use_tools: bool = True, max_turns: int = MAX_TURNS
) -> tuple[Diagnosis, RunStats, list[dict], dict[str, Evidence]]:
    """Run one investigation. Returns the diagnosis, run statistics, transcript and evidence ledger."""
    started = time.perf_counter()
    stats = RunStats()
    t = triage(store)
    ledger = t.all_evidence()
    tools = Tools(store, ledger)
    system = SYSTEM.format(
        topology=_topology_text(store.topology()),
        faults="\n".join(f"- {k}: {v}" for k, v in FAULT_DESCRIPTIONS.items()),
        turns=max_turns if use_tools else 1,
    )
    messages: list[dict] = [
        {"role": "system", "content": system},
        {"role": "user", "content": "Incident briefing:\n\n" + briefing(t)},
    ]
    submit_only = [t_ for t_ in TOOLS if t_["function"]["name"] == "submit_diagnosis"]
    available = TOOLS if use_tools else submit_only
    force_submit = {"type": "function", "function": {"name": "submit_diagnosis"}}
    diagnosis: Diagnosis | None = None

    for turn in range(max_turns if use_tools else 2):
        last = turn == (max_turns if use_tools else 2) - 1
        choice = force_submit if (last or not use_tools) else "auto"
        # When forcing the submission, offer only that tool: the model can't then pick another one.
        try:
            resp = client.chat(messages, submit_only if choice == force_submit else available, choice)
        except RuntimeError as exc:
            if "daily token limit" in str(exc):
                raise
            # The model keeps producing invalid tool calls; ask for the diagnosis as plain JSON.
            stats.tool_errors += 1
            diagnosis = _json_fallback(client, messages, stats)
            break
        stats.llm_calls += 1
        usage = resp.get("usage", {})
        stats.input_tokens += usage.get("prompt_tokens", 0)
        stats.output_tokens += usage.get("completion_tokens", 0)
        msg = resp["choices"][0]["message"]
        calls = msg.get("tool_calls") or []
        messages.append({k: v for k, v in msg.items() if k in ("role", "content", "tool_calls")})
        if not calls:
            messages.append({"role": "user", "content": "Call submit_diagnosis now."})
            continue
        for call in calls:
            name = call["function"]["name"]
            try:
                args = json.loads(call["function"]["arguments"] or "{}")
                if name == "submit_diagnosis":
                    diagnosis = Diagnosis.model_validate(args)
                    result = "submitted"
                else:
                    stats.tool_calls += 1
                    result = tools.call(name, args)
            except (json.JSONDecodeError, ValidationError, ValueError, TypeError) as exc:
                stats.tool_errors += 1
                result = f"error: {exc}"[:500]
            messages.append({"role": "tool", "tool_call_id": call["id"], "content": result})
        if diagnosis is not None:
            break

    if diagnosis is None:
        diagnosis = _json_fallback(client, messages, stats)
    if diagnosis is None:
        diagnosis = Diagnosis(status="insufficient_evidence", summary="agent did not submit a diagnosis")
        stats.error = "no_submission"
    cited = [e for h in diagnosis.hypotheses for e in h.evidence]
    stats.cited = len(cited)
    stats.cited_valid = sum(e in ledger for e in cited)
    stats.latency_s = round(time.perf_counter() - started, 2)
    return diagnosis, stats, messages, ledger


def _json_fallback(client: LLMClient, messages: list[dict], stats: RunStats) -> Diagnosis | None:
    """Last resort when tool calling fails: request the diagnosis as a JSON object."""
    prompt = (
        'Stop investigating. Reply with ONLY a JSON object: {"status": "diagnosed"|"no_incident"|'
        '"insufficient_evidence", "summary": str, "hypotheses": [{"service": str, "fault_type": one of '
        + ", ".join(FAULT_TYPES)
        + ', "confidence": number, "summary": str, "evidence": [ev ids]}]}'
    )
    # Drop dangling tool-call turns: a JSON-mode request can't contain unanswered tool calls.
    history = [m for m in messages if not (m["role"] == "assistant" and m.get("tool_calls"))]
    history = [m for m in history if m["role"] != "tool"]
    try:
        resp = client.chat([*history, {"role": "user", "content": prompt}], None)
        stats.llm_calls += 1
        usage = resp.get("usage", {})
        stats.input_tokens += usage.get("prompt_tokens", 0)
        stats.output_tokens += usage.get("completion_tokens", 0)
        return Diagnosis.model_validate(json.loads(resp["choices"][0]["message"]["content"]))
    except (RuntimeError, json.JSONDecodeError, ValidationError, KeyError, TypeError) as exc:
        stats.error = f"fallback failed: {exc}"[:300]
        return None
