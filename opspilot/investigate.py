"""Investigate one recorded incident and print a report.

python -m opspilot.investigate scenarios/bundles/f5-deploy-query-engine-a
python -m opspilot.investigate <bundle> --rules        # rules baseline only, no LLM
python -m opspilot.investigate <bundle> --show-answer  # also print the ground truth
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from opspilot import baseline
from opspilot.agent import DEFAULT_MODEL, LLMClient, briefing, investigate
from opspilot.analysis import triage
from opspilot.store.bundle import BundleStore, load_ground_truth


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("bundle", type=Path)
    parser.add_argument("--rules", action="store_true", help="use the rules baseline instead of the agent")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--show-answer", action="store_true")
    args = parser.parse_args(argv)

    with BundleStore(args.bundle) as store:
        t = triage(store)
        print("=" * 80 + "\nTRIAGE BRIEFING\n" + "=" * 80)
        print(briefing(t))
        if args.rules:
            diagnosis, ledger, stats = baseline.diagnose_triage(t, store.topology()), t.all_evidence(), None
        else:
            diagnosis, stats, transcript, ledger = investigate(store, LLMClient(args.model))
            calls = [
                c["function"]
                for m in transcript
                if m["role"] == "assistant"
                for c in m.get("tool_calls") or []
            ]
            drill = [f"{c['name']}({c['arguments']})" for c in calls if c["name"] != "submit_diagnosis"]
            if drill:
                print("\nAgent drill-down: " + "; ".join(drill))

    print("\n" + "=" * 80 + f"\nDIAGNOSIS ({'rules' if args.rules else args.model})\n" + "=" * 80)
    print(f"status: {diagnosis.status}")
    if diagnosis.summary:
        print(f"summary: {diagnosis.summary}")
    for i, h in enumerate(diagnosis.hypotheses, 1):
        print(f"\n#{i} {h.service} / {h.fault_type}  (confidence {h.confidence:.2f})")
        if h.summary:
            print(f"   {h.summary}")
        for eid in h.evidence:
            ev = ledger.get(eid)
            print(f"   [{eid}] {ev.summary if ev else '!! not in evidence ledger'}")
    if stats is not None:
        print(
            f"\n{stats.latency_s}s, {stats.llm_calls} LLM calls, {stats.tool_calls} tool calls, "
            f"{stats.input_tokens + stats.output_tokens} tokens, {stats.cited_valid}/{stats.cited} citations valid"
        )
    if args.show_answer:
        gt = load_ground_truth(args.bundle)
        print(f"\nground truth: {gt.label.get('root_cause') or 'no fault'}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
