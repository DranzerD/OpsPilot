"""Evaluate diagnosers on recorded scenario bundles.

    python -m opspilot.evaluate --run main --systems rules,agent,agent-notools

Results are appended per scenario to results/<run>/results.jsonl, so an interrupted run (e.g.
hitting a free-tier daily token limit) resumes where it stopped. A summary is written to
results/<run>/summary.md.
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from opspilot import baseline
from opspilot.agent import DEFAULT_MODEL, LLMClient, investigate
from opspilot.analysis import Evidence, triage
from opspilot.diagnosis import Diagnosis, RunStats
from opspilot.store.bundle import BundleStore, load_ground_truth
from scenarios.model import BUNDLES_DIR

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"
SYSTEMS = ("rules", "agent", "agent-notools")


def run_system(
    system: str, store: BundleStore, client: LLMClient | None
) -> tuple[Diagnosis, RunStats, dict[str, Evidence], list]:
    if system == "rules":
        start = time.perf_counter()
        t = triage(store)
        diagnosis = baseline.diagnose_triage(t, store.topology())
        ledger = t.all_evidence()
        cited = [e for h in diagnosis.hypotheses for e in h.evidence]
        stats = RunStats(
            latency_s=round(time.perf_counter() - start, 2),
            cited=len(cited),
            cited_valid=sum(e in ledger for e in cited),
        )
        return diagnosis, stats, ledger, []
    assert client is not None
    diagnosis, stats, transcript, ledger = investigate(store, client, use_tools=(system == "agent"))
    return diagnosis, stats, ledger, transcript


def _matches(ev: dict[str, Any], sel: dict[str, Any]) -> bool:
    if ev["service"] != sel["service"]:
        return False
    s = ev["selector"]
    if sel["kind"] == "metric":
        return ev["kind"] == "metric" and s.get("signal") == sel["signal"]
    if sel["kind"] == "log":
        return ev["kind"] == "log" and bool(re.search(sel["pattern"], s.get("template", "")))
    if sel["kind"] == "change":
        return ev["kind"] == "change" and s.get("kind") == sel["change_kind"]
    return ev["kind"] == "trace"


def score(
    diagnosis: Diagnosis, label: dict[str, Any], cited_evidence: list[dict[str, Any]]
) -> dict[str, Any]:
    expected_abstain = label.get("expected_abstain", False)
    root = label.get("root_cause")
    hyps = diagnosis.hypotheses if diagnosis.status == "diagnosed" else []
    keys = [(h.service, h.fault_type) for h in hyps]
    out: dict[str, Any] = {"expected_abstain": expected_abstain, "status": diagnosis.status}
    if expected_abstain:
        out["correct_abstain"] = diagnosis.status != "diagnosed"
        return out
    truth = (root["service"], root["fault_type"])
    alternates = {(a["service"], a["fault_type"]) for a in label.get("acceptable_alternates", [])}
    ok = {truth} | alternates
    out["top1"] = bool(keys) and keys[0] in ok
    out["top3"] = any(k in ok for k in keys[:3])
    out["service_top1"] = bool(keys) and keys[0][0] == truth[0]
    # Evidence relevance: of the evidence cited for the top hypothesis, how much is about the
    # true root-cause service or matches a key-evidence selector.
    if cited_evidence:
        relevant = [
            e
            for e in cited_evidence
            if e["service"] == truth[0] or any(_matches(e, sel) for sel in label.get("key_evidence", []))
        ]
        out["evidence_relevance"] = len(relevant) / len(cited_evidence)
    return out


def evaluate(systems: list[str], bundles: list[Path], run_dir: Path, model: str) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    results_path = run_dir / "results.jsonl"
    done = set()
    if results_path.exists():
        for line in results_path.read_text().splitlines():
            r = json.loads(line)
            done.add((r["system"], r["scenario"]))
    client = LLMClient(model) if any(s != "rules" for s in systems) else None
    (run_dir / "transcripts").mkdir(exist_ok=True)

    for bundle in bundles:
        gt = load_ground_truth(bundle)
        for system in systems:
            if (system, gt.scenario_id) in done:
                continue
            with BundleStore(bundle) as store:
                try:
                    diagnosis, stats, ledger, transcript = run_system(system, store, client)
                except RuntimeError as exc:
                    if "daily token limit" in str(exc):
                        print(f"stopping: {exc}")
                        return
                    diagnosis, stats, ledger, transcript = (
                        Diagnosis(status="insufficient_evidence"),
                        RunStats(error=str(exc)[:300]),
                        {},
                        [],
                    )
            top = (
                diagnosis.hypotheses[0] if diagnosis.hypotheses and diagnosis.status == "diagnosed" else None
            )
            cited = []
            for eid in top.evidence if top else []:
                if eid in ledger:
                    e = ledger[eid]
                    cited.append({"id": eid, "kind": e.kind, "service": e.service, "selector": e.selector})
            record = {
                "system": system,
                "model": model if system != "rules" else None,
                "scenario": gt.scenario_id,
                "fault_type": (gt.label.get("root_cause") or {}).get("fault_type", "none"),
                "diagnosis": diagnosis.model_dump(),
                "stats": stats.model_dump(),
                "score": score(diagnosis, gt.label, cited),
            }
            if stats.error and stats.error != "no_submission":
                # A crashed run is a failure, never a correct "no incident".
                record["score"].update(
                    {
                        k: False
                        for k in ("top1", "top3", "service_top1", "correct_abstain")
                        if k in record["score"]
                    },
                    error=True,
                )
            with open(results_path, "a") as f:
                f.write(json.dumps(record, default=str) + "\n")
            if transcript:
                (run_dir / "transcripts" / f"{system}--{gt.scenario_id}.json").write_text(
                    json.dumps(transcript, indent=1, default=str)
                )
            s = record["score"]
            verdict = s.get("top1", s.get("correct_abstain"))
            print(
                f"{system:<14} {gt.scenario_id:<40} {'OK ' if verdict else 'MISS'} "
                f"{(diagnosis.hypotheses[0].service + '/' + diagnosis.hypotheses[0].fault_type) if top else diagnosis.status}"
                f"  ({stats.latency_s}s, {stats.input_tokens + stats.output_tokens} tok)",
                flush=True,
            )
    summarize(run_dir)


def _pct(xs: list[bool]) -> str:
    return f"{100 * sum(xs) / len(xs):.0f}% ({sum(xs)}/{len(xs)})" if xs else "-"


def summarize(run_dir: Path) -> str:
    rows = [json.loads(line) for line in (run_dir / "results.jsonl").read_text().splitlines()]
    by_system: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_system[r["system"]].append(r)

    lines = [
        (
            "| System | Top-1 root cause | Top-3 | Right service | No-fault correctly cleared | Evidence relevance | "
            "Citations valid | Median latency | p95 latency | Mean tokens | Tool calls / run |"
        ),
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    summary: dict[str, Any] = {}
    for system, rs in by_system.items():
        faulted = [r for r in rs if not r["score"]["expected_abstain"]]
        clean = [r for r in rs if r["score"]["expected_abstain"]]
        lat = sorted(r["stats"]["latency_s"] for r in rs)
        rel = [r["score"]["evidence_relevance"] for r in faulted if "evidence_relevance" in r["score"]]
        cited = sum(r["stats"]["cited"] for r in rs)
        valid = sum(r["stats"]["cited_valid"] for r in rs)
        tokens = [r["stats"]["input_tokens"] + r["stats"]["output_tokens"] for r in rs]
        p95 = lat[min(int(len(lat) * 0.95), len(lat) - 1)] if lat else 0
        s = {
            "n": len(rs),
            "top1": _pct([r["score"]["top1"] for r in faulted]),
            "top3": _pct([r["score"]["top3"] for r in faulted]),
            "service_top1": _pct([r["score"]["service_top1"] for r in faulted]),
            "no_fault_cleared": _pct([r["score"]["correct_abstain"] for r in clean]),
            "evidence_relevance": f"{100 * statistics.mean(rel):.0f}%" if rel else "-",
            "citations_valid": f"{100 * valid / cited:.0f}% ({valid}/{cited})" if cited else "-",
            "latency_median_s": round(statistics.median(lat), 2) if lat else 0,
            "latency_p95_s": round(p95, 2),
            "mean_tokens": round(statistics.mean(tokens)) if tokens else 0,
            "tool_calls_per_run": round(statistics.mean(r["stats"]["tool_calls"] for r in rs), 1),
        }
        summary[system] = s
        lines.append(
            f"| {system} | {s['top1']} | {s['top3']} | {s['service_top1']} | {s['no_fault_cleared']} | "
            f"{s['evidence_relevance']} | {s['citations_valid']} | {s['latency_median_s']}s | {s['latency_p95_s']}s | "
            f"{s['mean_tokens']} | {s['tool_calls_per_run']} |"
        )

    # Per fault type, top-1
    fault_types = sorted({r["fault_type"] for r in rows})
    lines += [
        "",
        "Top-1 by fault type (no-fault rows: correctly cleared):",
        "",
        "| Fault type | " + " | ".join(by_system) + " |",
        "|---|" + "---|" * len(by_system),
    ]
    for ft in fault_types:
        cells = []
        for system_rows in by_system.values():
            rs = [r for r in system_rows if r["fault_type"] == ft]
            key = "correct_abstain" if ft == "none" else "top1"
            cells.append(_pct([r["score"][key] for r in rs]))
        lines.append(f"| {ft} | " + " | ".join(cells) + " |")

    text = "\n".join(lines)
    (run_dir / "summary.md").write_text(text + "\n")
    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2))
    print("\n" + text)
    return text


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--run", default="main")
    parser.add_argument("--systems", default="rules,agent,agent-notools")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--bundles", nargs="*", type=Path)
    parser.add_argument("--summary-only", action="store_true")
    args = parser.parse_args(argv)
    run_dir = RESULTS_DIR / args.run
    if args.summary_only:
        summarize(run_dir)
        return 0
    systems = [s for s in args.systems.split(",") if s]
    if bad := [s for s in systems if s not in SYSTEMS]:
        sys.exit(f"unknown systems {bad}; choose from {SYSTEMS}")
    bundles = args.bundles or sorted(p for p in BUNDLES_DIR.iterdir() if (p / "manifest.json").exists())
    evaluate(systems, bundles, run_dir, args.model)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
