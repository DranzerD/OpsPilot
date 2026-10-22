"""Scenario definitions (spec §6.2): a fault, a timeline, and the ground-truth label."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, model_validator

from sim.chaos import FaultSpec, FaultType

DEFS_DIR = Path(__file__).resolve().parent / "defs"
BUNDLES_DIR = Path(__file__).resolve().parent / "bundles"


class Timeline(BaseModel):
    baseline_s: int = Field(gt=0)
    fault_s: int = Field(gt=0)
    recovery_s: int = Field(ge=0)

    @property
    def total_s(self) -> int:
        return self.baseline_s + self.fault_s + self.recovery_s


class EvidenceSelector(BaseModel):
    """Describes a piece of evidence that should exist in the bundle if the label is right.

    Used to sanity-check recorded bundles (scenarios.check) and later to auto-judge cited evidence.
    """

    kind: Literal["metric", "log", "trace", "change"]
    service: str
    signal: str | None = None  # metric/trace: a name from opspilot.store.signals or trace signal
    direction: Literal["up", "down"] = "up"
    labels: dict[str, str] = {}  # extra metric label matchers, e.g. {"peer_service": "metadata"}
    pattern: str | None = None  # log: regex over the message
    change_kind: Literal["deploy", "config", "restart"] | None = None

    @model_validator(mode="after")
    def _fields_for_kind(self) -> EvidenceSelector:
        need = {"metric": "signal", "trace": "signal", "log": "pattern", "change": "change_kind"}[self.kind]
        if getattr(self, need) is None:
            raise ValueError(f"{self.kind} selector needs {need}")
        return self


class RootCause(BaseModel):
    service: str
    fault_type: FaultType


class Label(BaseModel):
    root_cause: RootCause | None  # None for no-fault scenarios
    acceptable_alternates: list[RootCause] = []
    expected_abstain: bool = False
    key_evidence: list[EvidenceSelector] = []


class ScenarioDef(BaseModel):
    id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]*$")
    fault: FaultSpec
    timeline: Timeline
    seed: int = 0
    label: Label

    @classmethod
    def load(cls, path: Path) -> ScenarioDef:
        return cls.model_validate(yaml.safe_load(path.read_text()))


def load_all(defs_dir: Path = DEFS_DIR) -> list[ScenarioDef]:
    return [ScenarioDef.load(p) for p in sorted(defs_dir.glob("*.yaml"))]
