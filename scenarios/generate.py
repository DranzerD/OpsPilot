"""Generate the evaluation scenario set into scenarios/defs/.

    python -m scenarios.generate

Each fault type is varied across targets and intensities; a few no-fault scenarios check that a
diagnoser can say "nothing is wrong". Timelines are short (90 s baseline, 150 s fault) so the
whole set records in about two hours.
"""

from __future__ import annotations

from typing import Any

import yaml

from scenarios.model import DEFS_DIR, ScenarioDef

TIMELINE = {"baseline_s": 90, "fault_s": 150, "recovery_s": 0}


def _metric(service: str, signal: str, **labels: str) -> dict[str, Any]:
    sel: dict[str, Any] = {"kind": "metric", "service": service, "signal": signal, "direction": "up"}
    if labels:
        sel["labels"] = labels
    return sel


def _log(service: str, pattern: str) -> dict[str, Any]:
    return {"kind": "log", "service": service, "pattern": pattern}


def pool_exhaustion(target: str, leak_interval_s: float) -> dict[str, Any]:
    return {
        "fault": {
            "fault_type": "db_pool_exhaustion",
            "target": target,
            "intensity": 1.0,
            "params": {"leak_interval_s": leak_interval_s},
        },
        "root": target,
        "evidence": [
            _metric(target, "db_pool_utilization"),
            _metric(target, "db_pool_wait_p95"),
            _log(target, "Timed out acquiring connection from pool"),
        ],
    }


def latency(target: str, intensity: float) -> dict[str, Any]:
    return {
        "fault": {"fault_type": "dependency_latency", "target": target, "intensity": intensity},
        "root": target,
        "evidence": [
            _metric(target, "latency_p95"),
            {"kind": "trace", "service": target, "signal": "span_duration", "direction": "up"},
        ],
    }


def timeout_misconfig(caller: str, callee: str, intensity: float) -> dict[str, Any]:
    return {
        "fault": {
            "fault_type": "upstream_timeout_misconfig",
            "target": f"{caller}->{callee}",
            "intensity": intensity,
        },
        "root": caller,
        "evidence": [
            {"kind": "change", "service": caller, "change_kind": "config"},
            _metric(caller, "dependency_error_ratio", peer_service=callee),
            _log(caller, f"Upstream {callee} call .* exceeded deadline"),
        ],
    }


def bad_input(intensity: float, seed: int) -> dict[str, Any]:
    return {
        "fault": {
            "fault_type": "bad_input_job_failure",
            "target": "stream-processor",
            "intensity": intensity,
            "params": {"seed": seed},
        },
        "root": "stream-processor",
        "evidence": [
            _metric("scheduler", "job_failure_ratio", job_name="ingest"),
            _log("stream-processor", "Schema validation failed"),
        ],
    }


def bad_deploy(target: str, intensity: float) -> dict[str, Any]:
    return {
        "fault": {"fault_type": "bad_deploy_errors", "target": target, "intensity": intensity},
        "root": target,
        "evidence": [
            {"kind": "change", "service": target, "change_kind": "deploy"},
            _metric(target, "error_ratio"),
            _log(target, "Unhandled error"),
        ],
    }


def scenarios() -> list[ScenarioDef]:
    specs: list[tuple[str, dict[str, Any] | None]] = [
        ("f1-pool-query-engine-a", pool_exhaustion("query-engine", 8)),
        ("f1-pool-query-engine-b", pool_exhaustion("query-engine", 12)),
        ("f1-pool-stream-processor-a", pool_exhaustion("stream-processor", 15)),
        ("f1-pool-stream-processor-b", pool_exhaustion("stream-processor", 25)),
        ("f1-pool-metadata-a", pool_exhaustion("metadata", 15)),
        ("f2-latency-metadata-a", latency("metadata", 0.2)),
        ("f2-latency-metadata-b", latency("metadata", 0.4)),
        ("f2-latency-metadata-c", latency("metadata", 0.6)),
        ("f2-latency-query-engine-a", latency("query-engine", 0.3)),
        ("f2-latency-query-engine-b", latency("query-engine", 0.6)),
        ("f3-timeout-query-engine-metadata-a", timeout_misconfig("query-engine", "metadata", 0.5)),
        ("f3-timeout-query-engine-metadata-b", timeout_misconfig("query-engine", "metadata", 0.85)),
        ("f3-timeout-gateway-metadata-a", timeout_misconfig("gateway", "metadata", 0.7)),
        ("f3-timeout-stream-processor-metadata-a", timeout_misconfig("stream-processor", "metadata", 0.7)),
        ("f4-bad-input-a", bad_input(0.25, 1)),
        ("f4-bad-input-b", bad_input(0.5, 2)),
        ("f4-bad-input-c", bad_input(0.75, 3)),
        ("f5-deploy-query-engine-a", bad_deploy("query-engine", 0.5)),
        ("f5-deploy-gateway-a", bad_deploy("gateway", 0.3)),
        ("f5-deploy-metadata-a", bad_deploy("metadata", 0.3)),
        ("f5-deploy-stream-processor-a", bad_deploy("stream-processor", 0.5)),
        ("f0-no-fault-a", None),
        ("f0-no-fault-b", None),
        ("f0-no-fault-c", None),
    ]
    out = []
    for i, (scenario_id, spec) in enumerate(specs):
        if spec is None:
            fault = {"fault_type": "none", "target": "platform", "intensity": 0.0}
            label = {"root_cause": None, "expected_abstain": True}
        else:
            fault = spec["fault"]
            label = {
                "root_cause": {"service": spec["root"], "fault_type": fault["fault_type"]},
                "key_evidence": spec["evidence"],
            }
        out.append(
            ScenarioDef.model_validate(
                {"id": scenario_id, "fault": fault, "timeline": TIMELINE, "seed": i, "label": label}
            )
        )
    return out


def main() -> None:
    DEFS_DIR.mkdir(parents=True, exist_ok=True)
    for old in DEFS_DIR.glob("*.yaml"):
        old.unlink()
    defs = scenarios()
    for scenario in defs:
        doc = scenario.model_dump(mode="json", exclude_defaults=True)
        (DEFS_DIR / f"{scenario.id}.yaml").write_text(yaml.safe_dump(doc, sort_keys=False))
    print(f"wrote {len(defs)} scenario definitions to {DEFS_DIR}")


if __name__ == "__main__":
    main()
