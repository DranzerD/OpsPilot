"""Label-leakage test (spec §2.3, §11.2).

Recorded telemetry must not reveal that a fault was injected, or which one. If any of these
tokens appears anywhere an agent could read, diagnosis accuracy measures the leak, not the agent.
"""

import json
import re
from pathlib import Path

import duckdb
import pytest

from opspilot.store.bundle import BundleStore
from scenarios.model import BUNDLES_DIR
from sim.chaos import FaultType

LEAK = re.compile(
    r"\b(chaos\w*|inject\w*|faults?|fault_\w+|scenarios?|tuning|"
    + "|".join(re.escape(ft.value) for ft in FaultType if ft != FaultType.NONE)
    + r")\b",
    re.IGNORECASE,
)

BUNDLES = (
    sorted(p for p in BUNDLES_DIR.glob("*") if (p / "manifest.json").exists()) if BUNDLES_DIR.exists() else []
)


def _strings(bundle: Path) -> dict[str, list[str]]:
    con = duckdb.connect()
    q = {
        "logs": "SELECT message || ' ' || attrs FROM read_parquet(?)",
        "spans": "SELECT name || ' ' || attrs FROM read_parquet(?)",
        "metrics": "SELECT DISTINCT metric || ' ' || labels FROM read_parquet(?)",
        "changes": "SELECT summary || ' ' || details FROM read_parquet(?)",
    }
    out = {
        name: [r[0] for r in con.execute(sql, [str(bundle / f"{name}.parquet")]).fetchall()]
        for name, sql in q.items()
    }
    out["topology"] = [(bundle / "topology.json").read_text()]
    return out


def test_leak_pattern_catches_obvious_leaks_and_spares_ordinary_words():
    assert LEAK.search("POST /chaos")
    assert LEAK.search("injected latency")
    assert LEAK.search("db_pool_exhaustion")
    assert not LEAK.search("using default pool size")
    assert not LEAK.search("Timed out acquiring connection from pool after 5000ms")


@pytest.mark.skipif(not BUNDLES, reason="no recorded bundles")
@pytest.mark.parametrize("bundle", BUNDLES, ids=lambda p: p.name)
def test_bundle_telemetry_has_no_leak_tokens(bundle):
    hits = [
        (table, m.group(0), s[:160])
        for table, strings in _strings(bundle).items()
        for s in strings
        if (m := LEAK.search(s))
    ]
    assert not hits, f"{len(hits)} leaks, first: {hits[:3]}"


@pytest.mark.skipif(not BUNDLES, reason="no recorded bundles")
@pytest.mark.parametrize("bundle", BUNDLES, ids=lambda p: p.name)
def test_bundle_store_never_exposes_ground_truth(bundle):
    manifest = json.loads((bundle / "manifest.json").read_text())
    secrets = {manifest["scenario"]["id"], manifest["window"]["fault_start"], manifest["window"]["fault_end"]}
    with BundleStore(bundle) as store:
        exposed = json.dumps(
            {
                "range": store.time_range(),
                "services": store.services(),
                "topology": store.topology(),
                "logs": store.logs(limit=100_000),
                "changes": store.changes(),
                "spans": store.spans(limit=1000),
            },
            default=str,
        )
    assert not [s for s in secrets if s in exposed]
