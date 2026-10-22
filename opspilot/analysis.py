"""Deterministic triage over a telemetry store: anomalous signals, new error patterns, changes,
and per-service span behaviour. Shared by the rules baseline and the agent's tools.

Nothing here knows when a fault started. The only assumption is the one an on-call engineer
makes: the investigation window opens with a stretch of normal behaviour (the first
REFERENCE_S seconds), which is what everything else is compared against.
"""

from __future__ import annotations

import hashlib
import json
import re
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from opspilot.store.bundle import BundleStore
from opspilot.store.signals import SIGNALS

REFERENCE_S = 60
# Anomalies weaker than this (shift in units of the signal's min_delta) are labelled minor: the
# size of the platform's own background variation, e.g. batch-pipeline bursts.
MINOR_SCORE = 6
RATIO = 1.5
# Signals scanned for anomalies, with the direction that means trouble.
SCANNED = {
    "error_ratio": "up",
    "latency_p95": "up",
    "dependency_latency_p95": "up",
    "dependency_error_ratio": "up",
    "db_pool_wait_p95": "up",
    "db_pool_utilization": "up",
    "db_pool_pending": "up",
    "db_operation_p95": "up",
    "cache_hit_ratio": "down",
    "consumer_lag": "up",
    "job_duration_p95": "up",
    "job_failure_ratio": "up",
    "job_retries": "up",
}


def evidence_id(kind: str, selector: dict[str, Any]) -> str:
    """Stable id for a piece of evidence: same evidence, same id, across runs and systems."""
    raw = kind + json.dumps(selector, sort_keys=True, default=str)
    return "ev_" + hashlib.sha256(raw.encode()).hexdigest()[:8]


@dataclass
class Evidence:
    id: str
    kind: str
    service: str
    summary: str
    selector: dict[str, Any] = field(default_factory=dict)


@dataclass
class Anomaly:
    evidence: Evidence
    service: str
    signal: str
    labels: dict[str, str]
    reference: float
    peak: float
    onset: datetime | None
    score: float

    @property
    def minor(self) -> bool:
        return self.score < MINOR_SCORE


@dataclass
class LogPattern:
    evidence: Evidence
    service: str
    severity: str
    template: str
    example: str
    count: int
    reference_count: int
    first_seen: datetime


@dataclass
class Triage:
    start: datetime
    end: datetime
    anomalies: list[Anomaly]
    log_patterns: list[LogPattern]
    changes: list[Evidence]
    span_shifts: list[Evidence]

    def all_evidence(self) -> dict[str, Evidence]:
        items = [a.evidence for a in self.anomalies] + [p.evidence for p in self.log_patterns]
        return {e.id: e for e in items + self.changes + self.span_shifts}


def _q(values: list[float], q: float) -> float:
    if len(values) == 1:
        return values[0]
    return statistics.quantiles(values, n=100, method="inclusive")[round(q * 100) - 1]


def _fmt(value: float, unit: str) -> str:
    if unit == "s":
        return f"{value * 1000:.0f}ms" if value < 1 else f"{value:.2f}s"
    if unit == "ratio":
        return f"{value:.1%}"
    return f"{value:.3g}"


def _owner(signal: str, labels: dict[str, str]) -> str:
    sig = SIGNALS[signal]
    return sig.owner or labels.get(sig.service_label, "unknown")


def find_anomalies(store: BundleStore) -> list[Anomaly]:
    start, _ = store.time_range()
    cut = start + timedelta(seconds=REFERENCE_S)
    found: list[Anomaly] = []
    for signal, direction in SCANNED.items():
        sig = SIGNALS[signal]
        for series in store.metric(signal):
            ref = [v for ts, v in series.points if ts < cut]
            rest = [(ts, v) for ts, v in series.points if ts >= cut]
            if not rest:
                continue
            values = [v for _, v in rest]
            base = _q(ref, 0.9) if ref else 0.0
            if direction == "up":
                peak = _q(values, 0.9)
                delta = peak - base
                shifted = delta >= sig.min_delta and (base <= 0 or peak >= base * RATIO)
                threshold = base + delta / 2
                onset = next((ts for ts, v in rest if v >= threshold), None)
            else:
                base = _q(ref, 0.5) if ref else 0.0
                peak = _q(values, 0.1)
                delta = base - peak
                shifted = delta >= sig.min_delta and peak <= base / RATIO
                threshold = base - delta / 2
                onset = next((ts for ts, v in rest if v <= threshold), None)
            if not shifted:
                continue
            service = _owner(signal, series.labels)
            extra = {
                k: v
                for k, v in series.labels.items()
                if k not in ("service_name", "instance", "service_version")
            }
            selector = {"signal": signal, "service": service, **extra}
            where = "".join(f" {k}={v}" for k, v in extra.items())
            onset_txt = onset.strftime("%H:%M:%S") if onset else "?"
            minor = " (minor)" if delta / sig.min_delta < MINOR_SCORE else ""
            summary = (
                f"{service} {signal}{where}: {_fmt(base, sig.unit)} -> {_fmt(peak, sig.unit)}, from {onset_txt}{minor}"
            )
            found.append(
                Anomaly(
                    evidence=Evidence(evidence_id("metric", selector), "metric", service, summary, selector),
                    service=service,
                    signal=signal,
                    labels=extra,
                    reference=base,
                    peak=peak,
                    onset=onset,
                    score=delta / sig.min_delta,
                )
            )
    # One entry per (service, signal, labels): instance churn from restarts can duplicate series.
    unique: dict[str, Anomaly] = {}
    for a in sorted(found, key=lambda a: -a.score):
        unique.setdefault(a.evidence.id, a)
    return sorted(
        unique.values(), key=lambda a: (a.onset or datetime.max.replace(tzinfo=start.tzinfo), -a.score)
    )


_VOLATILE = [
    (re.compile(r"\b[0-9a-f]{16,}\b"), "<id>"),
    (re.compile(r"nightly-\d{8}T\d{6}(-[a-z_]+)?"), "<run>"),
    (re.compile(r"\b\d{8}T\d{6}\b"), "<partition>"),
    (re.compile(r"'[^']*'"), "'<v>'"),
    (re.compile(r"\d+(\.\d+)?"), "<n>"),
]


def template(message: str) -> str:
    first = message.splitlines()[0][:240]
    for pattern, repl in _VOLATILE:
        first = pattern.sub(repl, first)
    return first


def find_log_patterns(store: BundleStore, limit: int = 12) -> list[LogPattern]:
    """WARNING/ERROR message templates that are new or much more frequent after the reference period."""
    start, end = store.time_range()
    cut = start + timedelta(seconds=REFERENCE_S)
    groups: dict[tuple[str, str, str], dict[str, Any]] = {}
    for severity in ("ERROR", "WARNING"):
        for row in store.logs(severity=severity, limit=200_000):
            key = (row["service"], severity, template(row["message"]))
            g = groups.setdefault(key, {"ref": 0, "rest": 0, "first": row["ts"], "example": row["message"]})
            if row["ts"] < cut:
                g["ref"] += 1
            else:
                g["rest"] += 1
                if row["ts"] <= g["first"]:
                    g["first"], g["example"] = row["ts"], row["message"]
    # Rate-normalise: the reference period is shorter than the rest.
    span = max((end - cut).total_seconds(), 1) / REFERENCE_S
    patterns = []
    for (service, severity, tmpl), g in groups.items():
        if g["rest"] == 0 or g["rest"] < 2 * g["ref"] * span:
            continue
        selector = {"service": service, "template": tmpl}
        summary = f"{service} {severity} x{g['rest']} (x{g['ref']} before) from {g['first']:%H:%M:%S}: {tmpl}"
        patterns.append(
            LogPattern(
                evidence=Evidence(evidence_id("log", selector), "log", service, summary, selector),
                service=service,
                severity=severity,
                template=tmpl,
                example=g["example"].splitlines()[0][:300],
                count=g["rest"],
                reference_count=g["ref"],
                first_seen=g["first"],
            )
        )
    patterns.sort(key=lambda p: (p.first_seen, -p.count))
    return patterns[:limit]


def list_changes(store: BundleStore) -> list[Evidence]:
    out = []
    for c in store.changes():
        selector = {"change_id": c["id"], "kind": c["kind"]}
        out.append(
            Evidence(
                evidence_id("change", selector),
                "change",
                c["service"],
                f"{c['ts']:%H:%M:%S} {c['summary']}",
                selector,
            )
        )
    return out


def span_shifts(store: BundleStore) -> list[Evidence]:
    """Per service: SERVER span p90 and error-span counts, before vs after the reference period."""
    start, end = store.time_range()
    cut = start + timedelta(seconds=REFERENCE_S)
    out = []
    span = max((end - cut).total_seconds(), 1) / REFERENCE_S
    for service in store.services():
        before = store.span_duration_quantile(service, 0.9, start, cut)
        after = store.span_duration_quantile(service, 0.9, cut, end)
        if before is not None and after is not None and after - before >= 50 and after >= before * RATIO:
            selector = {"service": service, "signal": "span_p90"}
            out.append(
                Evidence(
                    evidence_id("trace", selector),
                    "trace",
                    service,
                    f"{service} SERVER span p90 {before:.0f}ms -> {after:.0f}ms",
                    selector,
                )
            )
        errs_before = len(
            store.spans(service, kind="SERVER", errors_only=True, start=start, end=cut, limit=100_000)
        )
        errs_after = store.spans(service, kind="SERVER", errors_only=True, start=cut, end=end, limit=100_000)
        if errs_after and len(errs_after) > 2 * errs_before * span:
            names: dict[str, int] = {}
            for s in errs_after:
                names[s["name"]] = names.get(s["name"], 0) + 1
            top = ", ".join(f"{n} x{c}" for n, c in sorted(names.items(), key=lambda kv: -kv[1])[:3])
            selector = {"service": service, "signal": "span_errors"}
            out.append(
                Evidence(
                    evidence_id("trace", selector),
                    "trace",
                    service,
                    f"{service} error spans {errs_before} -> {len(errs_after)} ({top})",
                    selector,
                )
            )
    return out


def triage(store: BundleStore) -> Triage:
    start, end = store.time_range()
    return Triage(
        start, end, find_anomalies(store), find_log_patterns(store), list_changes(store), span_shifts(store)
    )
