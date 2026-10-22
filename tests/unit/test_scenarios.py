import pytest

from opspilot.store.signals import SIGNALS
from scenarios.model import DEFS_DIR, ScenarioDef, load_all
from sim.config import load_topology

DEFS = sorted(DEFS_DIR.glob("*.yaml"))


def test_defs_exist():
    assert len(load_all()) >= 5


@pytest.mark.parametrize("path", DEFS, ids=lambda p: p.stem)
def test_def_is_consistent(path):
    scenario = ScenarioDef.load(path)
    topo = load_topology()
    services = set(topo.services) | set(topo.databases)

    assert scenario.id == path.stem
    assert scenario.label.root_cause.service in services
    assert scenario.label.root_cause.fault_type == scenario.fault.fault_type
    for target in scenario.fault.target.split("->"):
        assert target in topo.services
    for sel in scenario.label.key_evidence:
        assert sel.service in services
        if sel.kind == "metric":
            assert sel.signal in SIGNALS
        if sel.kind == "trace":
            assert sel.signal in {"span_duration", "span_errors"}


def test_selector_requires_kind_specific_field():
    with pytest.raises(ValueError):
        ScenarioDef.model_validate(
            {
                "id": "x",
                "fault": {"fault_type": "dependency_latency", "target": "metadata", "intensity": 0.5},
                "timeline": {"baseline_s": 1, "fault_s": 1, "recovery_s": 0},
                "label": {
                    "root_cause": {"service": "metadata", "fault_type": "dependency_latency"},
                    "key_evidence": [{"kind": "log", "service": "metadata"}],
                },
            }
        )
