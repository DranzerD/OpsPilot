"""Check that recorded bundles actually contain the evidence their labels claim.

    python -m scenarios.check [bundle_dir ...]      (default: every bundle)

A bundle whose key evidence isn't visible is mislabelled or under-powered, and would make
evaluation results meaningless, so this check gates which bundles are used.
"""

from __future__ import annotations

import argparse
import statistics
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from opspilot.store.bundle import BundleStore, GroundTruth, load_ground_truth
from opspilot.store.signals import SIGNALS
from scenarios.model import BUNDLES_DIR, EvidenceSelector

# Recording rules use 1–5 minute windows, so effects trail the fault by up to that long.
EFFECT_TAIL = timedelta(seconds=60)
RATIO_UP = 1.5


@dataclass
class Result:
    selector: EvidenceSelector
    ok: bool
    detail: str


def _q90(values: list[float]) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    return statistics.quantiles(values, n=10, method="inclusive")[-1]


def _shifted(base: float, fault: float, direction: str, min_delta: float) -> bool:
    if direction == "up":
        return fault - base >= min_delta and (base <= 0 or fault >= base * RATIO_UP)
    return base - fault >= min_delta and fault <= base / RATIO_UP


def _windows(
    gt: GroundTruth, store: BundleStore
) -> tuple[tuple[datetime, datetime], tuple[datetime, datetime]]:
    start, end = store.time_range()
    return (start, gt.fault_start), (gt.fault_start, min(gt.fault_end + EFFECT_TAIL, end))


def check_selector(store: BundleStore, gt: GroundTruth, sel: EvidenceSelector) -> Result:
    (b0, b1), (f0, f1) = _windows(gt, store)

    if sel.kind == "metric":
        sig = SIGNALS[sel.signal]
        best = "no series"
        for series in store.metric(sel.signal, sel.service, labels=sel.labels):
            base = _q90([v for ts, v in series.points if b0 <= ts < b1])
            fault = _q90([v for ts, v in series.points if f0 <= ts <= f1])
            if fault is None:
                continue
            base = base if base is not None else 0.0  # series that only exists during the fault
            best = f"{series.labels}: baseline q90 {base:.4g} -> fault q90 {fault:.4g}"
            if _shifted(base, fault, sel.direction, sig.min_delta):
                return Result(sel, True, best)
        return Result(sel, False, best)

    if sel.kind == "log":
        base = store.count_logs(sel.service, sel.pattern, b0, b1)
        fault = store.count_logs(sel.service, sel.pattern, f0, f1)
        return Result(
            sel, fault > 0 and fault > 2 * base, f"{base} matches in baseline, {fault} during fault"
        )

    if sel.kind == "trace":
        if sel.signal == "span_duration":
            base = store.span_duration_quantile(sel.service, 0.9, b0, b1) or 0.0
            fault = store.span_duration_quantile(sel.service, 0.9, f0, f1) or 0.0
            ok = _shifted(base, fault, sel.direction, 50.0)
            return Result(sel, ok, f"SERVER span p90 {base:.1f}ms -> {fault:.1f}ms")
        if sel.signal == "span_errors":
            base = len(store.spans(sel.service, errors_only=True, start=b0, end=b1, limit=100_000))
            fault = len(store.spans(sel.service, errors_only=True, start=f0, end=f1, limit=100_000))
            return Result(
                sel, fault > 2 * base and fault > 0, f"{base} error spans in baseline, {fault} during fault"
            )
        raise ValueError(f"unknown trace signal {sel.signal}")

    window = timedelta(seconds=10)
    changes = store.changes(sel.service, gt.fault_start - window, gt.fault_end + window)
    kinds = [c["kind"] for c in changes]
    return Result(sel, sel.change_kind in kinds, f"changes on {sel.service} during fault: {kinds or 'none'}")


def check_bundle(path: Path) -> list[Result]:
    gt = load_ground_truth(path)
    selectors = [EvidenceSelector.model_validate(s) for s in gt.label["key_evidence"]]
    with BundleStore(path) as store:
        return [check_selector(store, gt, sel) for sel in selectors]


def _describe(sel: EvidenceSelector) -> str:
    what = sel.signal or sel.pattern or sel.change_kind
    labels = f" {sel.labels}" if sel.labels else ""
    arrow = sel.direction if sel.kind in ("metric", "trace") else ""
    return f"{sel.kind:<6} {sel.service:<16} {what}{labels} {arrow}".rstrip()


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("bundles", nargs="*", type=Path)
    args = parser.parse_args(argv)
    bundles = args.bundles or sorted(p for p in BUNDLES_DIR.iterdir() if (p / "manifest.json").exists())
    failed = 0
    for bundle in bundles:
        results = check_bundle(bundle)
        bad = sum(not r.ok for r in results)
        failed += bad > 0
        print(f"{bundle.name}: {'OK' if not bad else f'{bad} selector(s) not visible'}")
        for r in results:
            print(f"  [{'PASS' if r.ok else 'FAIL'}] {_describe(r.selector)}  ({r.detail})")
    print(f"\n{len(bundles) - failed}/{len(bundles)} bundles show their key evidence")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
