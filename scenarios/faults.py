"""Apply and revert a scenario's fault against the live stack.

In-process faults (F1, F2, F4) go to the target service's /chaos endpoint. Operational faults go
through the control plane, so they leave the same change events a real change would:
F3 is a config change on the caller, F5 is a deploy of a release with a buggy code path.
"""

from __future__ import annotations

from collections.abc import Callable

import httpx

from sim.chaos import IN_PROCESS_FAULTS, FaultSpec, FaultType
from sim.config import BASE_VERSION, load_topology
from sim.control import ControlClient
from sim.features import RELEASE_FLAG

Revert = Callable[[], None]


def _bump_patch(version: str) -> str:
    major, minor, patch = version.split(".")
    return f"{major}.{minor}.{int(patch) + 1}"


def apply(spec: FaultSpec, control: ControlClient) -> Revert:
    services = load_topology().services

    if spec.fault_type == FaultType.NONE:
        return lambda: None

    if spec.fault_type in IN_PROCESS_FAULTS:
        url = f"{services[spec.target].base_url}/chaos"
        httpx.post(url, json=spec.model_dump(mode="json"), timeout=30).raise_for_status()
        return lambda: httpx.delete(url, timeout=30).raise_for_status()

    if spec.fault_type == FaultType.UPSTREAM_TIMEOUT_MISCONFIG:
        caller, _, callee = spec.target.partition("->")
        original = services[caller].dependency(callee).timeout_s
        # A units slip (ms written where seconds were meant): intensity 0 -> 4 ms, 1 -> 1 ms. That's
        # inside the callee's normal latency tail (p50 ~2 ms), so a growing share of calls time out.
        new_timeout = float(spec.params.get("timeout_s", round(0.004 - 0.003 * spec.intensity, 4)))
        key = f"dependencies.{callee}.timeout_s"
        control.config(caller, key, new_timeout)
        return lambda: control.config(caller, key, original)

    if spec.fault_type == FaultType.BAD_DEPLOY_ERRORS:
        current = control.services()[spec.target]["version"]
        flag = spec.params.get("flag", RELEASE_FLAG[spec.target])
        control.deploy(spec.target, spec.params.get("version", _bump_patch(current)), {flag: spec.intensity})
        return lambda: control.deploy(spec.target, current, {})

    raise NotImplementedError(f"no injector for {spec.fault_type}")


def reset(control: ControlClient) -> None:
    """Bring every service back to a clean baseline: no active faults, base versions, default config."""
    topo = load_topology()
    for svc in topo.services.values():
        httpx.delete(f"{svc.base_url}/chaos", timeout=30).raise_for_status()
    releases = control.services()
    for name, release in releases.items():
        if release["feature_flags"] or release["version"] != BASE_VERSION:
            control.deploy(name, BASE_VERSION, {})
    for svc in topo.services.values():
        current = httpx.get(f"{svc.base_url}/admin/config", timeout=30).json()
        for dep in svc.calls:
            key = f"dependencies.{dep.name}.timeout_s"
            if key in current and current[key] != dep.timeout_s:
                # Restoring a misconfiguration is a real config change, so it goes through the control plane.
                control.config(svc.name, key, dep.timeout_s)
