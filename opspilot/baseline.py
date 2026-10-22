"""Rules-based diagnoser (baseline B0): signature rules over the deterministic triage.

The bar the agent has to beat. It sees exactly the same triage the agent starts from; the
difference is that it maps evidence to causes with fixed rules instead of reasoning.
"""

from __future__ import annotations

import re
from collections import defaultdict

from opspilot.analysis import Evidence, Triage, triage
from opspilot.diagnosis import Diagnosis, Hypothesis
from opspilot.store.bundle import BundleStore

POOL_SIGNALS = {"db_pool_utilization", "db_pool_wait_p95", "db_pool_pending"}


def _callees(topology: dict, service: str) -> set[str]:
    return set(topology["services"].get(service, {}).get("calls", {}))


def diagnose_triage(t: Triage, topology: dict) -> Diagnosis:
    significant = [a for a in t.anomalies if not a.minor]
    if not (significant or t.log_patterns or t.changes or t.span_shifts):
        return Diagnosis(status="no_incident", summary="Only minor anomalies, no errors or changes: background noise.")

    by_service: dict[str, set[str]] = defaultdict(set)
    ev_by_service: dict[str, list[str]] = defaultdict(list)
    for a in t.anomalies:
        by_service[a.service].add(a.signal)
        ev_by_service[a.service].append(a.evidence.id)
    logs_by_service: dict[str, list] = defaultdict(list)
    for p in t.log_patterns:
        logs_by_service[p.service].append(p)
    # Earliest change per (service, kind): a cause precedes its rollback.
    changes: dict[tuple[str, str], Evidence] = {}
    for c in t.changes:
        changes.setdefault((c.service, c.selector.get("kind")), c)

    scores: dict[tuple[str, str], float] = {}
    evidence: dict[tuple[str, str], list[str]] = defaultdict(list)

    def add(service: str, fault: str, score: float, ev: list[str]) -> None:
        key = (service, fault)
        scores[key] = max(scores.get(key, 0.0), score)
        evidence[key].extend(e for e in ev if e not in evidence[key])

    for service in {s for s, _ in changes} | set(by_service) | set(logs_by_service):
        signals = by_service[service]
        logs = logs_by_service[service]
        deadline_logs = [
            p.evidence.id
            for p in logs
            if re.search(r"exceeded deadline|timed out", p.template, re.IGNORECASE)
        ]
        error_logs = [p.evidence.id for p in logs if p.severity == "ERROR"]

        # A config change followed by this service's own dependency calls failing.
        if (cfg := changes.get((service, "config"))) and (
            "dependency_error_ratio" in signals or deadline_logs
        ):
            add(service, "upstream_timeout_misconfig", 10, [cfg.id, *ev_by_service[service], *deadline_logs])
        # A deploy followed by this service erroring.
        if (dep := changes.get((service, "deploy"))) and ("error_ratio" in signals or error_logs):
            add(service, "bad_deploy_errors", 9, [dep.id, *ev_by_service[service], *error_logs])
        pool_logs = [
            p.evidence.id for p in logs if re.search(r"connection from pool", p.template, re.IGNORECASE)
        ]
        if signals & POOL_SIGNALS or pool_logs:
            add(service, "db_pool_exhaustion", 8, [*ev_by_service[service], *pool_logs])
        validation_logs = [
            p.evidence.id
            for p in logs
            if re.search(r"validation|malformed|schema", p.template, re.IGNORECASE)
        ]
        if validation_logs:
            add(service, "bad_input_job_failure", 7, [*validation_logs, *ev_by_service.get("scheduler", [])])

    # Latency: blame the most downstream slow service, i.e. one whose callees aren't slow.
    slow = {a.service for a in significant if a.signal == "latency_p95"} | {
        e.service for e in t.span_shifts if e.selector.get("signal") == "span_p90"
    }
    for service in slow:
        if not (_callees(topology, service) & slow):
            ev = [a.evidence.id for a in significant if a.service == service and a.signal == "latency_p95"]
            ev += [
                e.id for e in t.span_shifts if e.service == service and e.selector.get("signal") == "span_p90"
            ]
            add(service, "dependency_latency", 5, ev)

    if not scores:
        top = t.anomalies[0] if t.anomalies else None
        return Diagnosis(
            status="insufficient_evidence",
            summary="Anomalies found but no rule matched"
            + (f"; strongest: {top.evidence.summary}" if top else ""),
        )
    ranked = sorted(scores, key=lambda k: -scores[k])
    total = sum(scores.values())
    hypotheses = [
        Hypothesis(
            service=s,
            fault_type=f,
            confidence=round(scores[(s, f)] / total, 2),
            summary=f"rule match: {f} on {s}",
            evidence=evidence[(s, f)][:6],
        )
        for s, f in ranked[:3]
    ]
    return Diagnosis(status="diagnosed", hypotheses=hypotheses, summary=hypotheses[0].summary)


def diagnose(store: BundleStore) -> Diagnosis:
    return diagnose_triage(triage(store), store.topology())
