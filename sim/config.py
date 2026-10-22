"""Paths, ports and topology for the simulated platform."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parent.parent
VAR = ROOT / "var"
TOOLS = ROOT / "tools"
CONFIGS = ROOT / "configs"
TOPOLOGY_PATH = Path(__file__).resolve().parent / "topology.yaml"

BASE_VERSION = "0.1.0"
VERSION = os.environ.get("SERVICE_VERSION", BASE_VERSION)
HOST = "127.0.0.1"
OTLP_ENDPOINT = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", f"http://{HOST}:4318")
PROMETHEUS_URL = os.environ.get("PROMETHEUS_URL", f"http://{HOST}:9090")
PLATFORM_DB_PATH = Path(os.environ.get("PLATFORM_DB_PATH", VAR / "platform.db"))
TELEMETRY_DIR = VAR / "telemetry"
# Deploys, config changes and restarts made through the control plane (sim/control.py).
CHANGES_PATH = TELEMETRY_DIR / "changes.jsonl"
CONTROL_PORT = 8090
CONTROL_URL = f"http://{HOST}:{CONTROL_PORT}"


@dataclass(frozen=True)
class Dependency:
    name: str
    timeout_s: float | None = None


@dataclass(frozen=True)
class ServiceConfig:
    name: str
    port: int
    tier: str
    description: str
    calls: tuple[Dependency, ...]
    options: dict[str, Any] = field(default_factory=dict)

    @property
    def base_url(self) -> str:
        return f"http://{HOST}:{self.port}"

    def dependency(self, name: str) -> Dependency:
        for dep in self.calls:
            if dep.name == name:
                return dep
        raise KeyError(f"{self.name} does not declare a dependency on {name}")


@dataclass(frozen=True)
class Topology:
    services: dict[str, ServiceConfig]
    databases: dict[str, dict[str, Any]]
    workload: dict[str, Any]

    def edges(self) -> list[tuple[str, str]]:
        return [(s.name, d.name) for s in self.services.values() for d in s.calls]


@lru_cache(maxsize=1)
def load_topology(path: Path = TOPOLOGY_PATH) -> Topology:
    raw = yaml.safe_load(path.read_text())
    services: dict[str, ServiceConfig] = {}
    databases: dict[str, dict[str, Any]] = {}
    for name, spec in raw["services"].items():
        if spec.get("kind") == "database":
            databases[name] = spec
            continue
        calls = tuple(
            Dependency(dep, (opts or {}).get("timeout_s")) for dep, opts in (spec.get("calls") or {}).items()
        )
        known = {"port", "tier", "description", "calls"}
        services[name] = ServiceConfig(
            name=name,
            port=spec["port"],
            tier=spec["tier"],
            description=spec.get("description", ""),
            calls=calls,
            options={k: v for k, v in spec.items() if k not in known},
        )
    return Topology(services=services, databases=databases, workload=raw.get("workload", {}))
