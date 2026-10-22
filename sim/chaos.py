"""Fault injection for the simulated services (spec §2.3).

Faults act inside the code paths they affect and must never reveal themselves in telemetry:
nothing here logs, creates spans, or adds metric labels. Symptoms have to be indistinguishable
from a real failure, otherwise every accuracy number downstream is meaningless. The leak test
in tests/ enforces this on recorded bundles.

Faults that are really operational changes (F3 config change, F5 bad deploy) are not injected
here; the scenario recorder performs them through the control plane (sim/control.py), which
records them as ordinary change events.
"""

from __future__ import annotations

import asyncio
import hashlib
import math
import random
import threading
from collections.abc import Awaitable, Callable
from enum import StrEnum
from time import monotonic
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import exc as sa_exc

if TYPE_CHECKING:
    from sqlalchemy import Connection

    from sim.db import PlatformDB


class FaultType(StrEnum):
    """Root-cause taxonomy (spec §6.1). Not every type is injectable through /chaos."""

    NONE = "none"
    DB_POOL_EXHAUSTION = "db_pool_exhaustion"  # F1
    DEPENDENCY_LATENCY = "dependency_latency"  # F2
    UPSTREAM_TIMEOUT_MISCONFIG = "upstream_timeout_misconfig"  # F3, via control plane
    BAD_INPUT_JOB_FAILURE = "bad_input_job_failure"  # F4
    BAD_DEPLOY_ERRORS = "bad_deploy_errors"  # F5, via control plane
    SLOW_QUERY = "slow_query"  # F6
    MEMORY_LEAK = "memory_leak"  # F7
    CONSUMER_BACKPRESSURE = "consumer_backpressure"  # F8
    RETRY_STORM = "retry_storm"  # F9 (held out)
    CACHE_DISABLED = "cache_disabled"  # F10 (held out)
    DB_DISK_FULL = "db_disk_full"  # F11 (held out)


IN_PROCESS_FAULTS = {
    FaultType.DB_POOL_EXHAUSTION,
    FaultType.DEPENDENCY_LATENCY,
    FaultType.BAD_INPUT_JOB_FAILURE,
}


class FaultSpec(BaseModel):
    fault_type: FaultType
    target: str  # service name, or "caller->callee" for edge faults
    intensity: float = Field(ge=0, le=1)
    params: dict[str, Any] = {}
    duration_s: int | None = None  # None = until cleared


class FaultState:
    """Active in-process faults for this service, thread-safe, with optional expiry."""

    def __init__(self) -> None:
        self._active: dict[FaultType, tuple[FaultSpec, float | None]] = {}
        self._lock = threading.Lock()

    def activate(self, spec: FaultSpec) -> None:
        expires = monotonic() + spec.duration_s if spec.duration_s else None
        with self._lock:
            self._active[spec.fault_type] = (spec, expires)

    def get(self, fault_type: FaultType) -> FaultSpec | None:
        with self._lock:
            entry = self._active.get(fault_type)
            if entry is None:
                return None
            spec, expires = entry
            if expires is not None and monotonic() >= expires:
                del self._active[fault_type]
                return None
            return spec

    def active(self) -> list[FaultSpec]:
        return [s for ft in list(self._active) if (s := self.get(ft)) is not None]

    def clear(self) -> None:
        with self._lock:
            self._active.clear()


faults = FaultState()


# --- F1: connection leak -> pool exhaustion ---------------------------------------------------


class ConnectionLeak:
    """Checks out one pool connection every `interval_s` and never returns it, until the pool is
    empty or the fault is cleared. Symptoms: connections in use climb stepwise with no load
    change, then requests queue for a connection and time out."""

    def __init__(self, db: PlatformDB, spec: FaultSpec) -> None:
        self.db = db
        # intensity 0 -> one connection a minute; 1 -> one every 10 s
        self.interval_s = float(spec.params.get("leak_interval_s", 60 - 50 * spec.intensity))
        self.max_leaked = int(spec.params.get("max_leaked", db.pool_size))
        self._held: list[Connection] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="pool-maintenance", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.wait(self.interval_s):
            if faults.get(FaultType.DB_POOL_EXHAUSTION) is None:
                break
            if len(self._held) >= self.max_leaked:
                continue
            try:
                self._held.append(self.db.engine.connect())
            except sa_exc.TimeoutError:  # pool already drained by traffic; silent by design
                continue
        self._release()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)
        self._release()

    def _release(self) -> None:
        while self._held:
            self._held.pop().close()


# --- F2: added latency inside the service ---------------------------------------------------


def added_latency(service: str) -> Callable[[Request], Awaitable[None]]:
    """App-level FastAPI dependency that delays every request while F2 targets this service.

    It runs inside the server span, so the time shows up as self-time in this service. It's a
    dependency, not middleware, so it has returned before the handler runs and never appears in
    the stack trace of an exception the handler raises: a middleware frame from this module
    would leak into every logged traceback.
    """
    rng = random.Random()

    async def delay(request: Request) -> None:
        spec = faults.get(FaultType.DEPENDENCY_LATENCY)
        if spec is None or spec.target != service or request.url.path in UNDELAYED_PATHS:
            return
        added_ms = float(spec.params.get("added_ms", 1000 * spec.intensity))
        jitter_ms = float(spec.params.get("jitter_ms", added_ms / 4))
        await asyncio.sleep(max(rng.gauss(added_ms, jitter_ms), 0) / 1000)

    return delay


UNDELAYED_PATHS = {"/healthz", "/chaos", "/admin/config"}


# --- F4: malformed upstream records ---------------------------------------------------------


def affected_datasets(spec: FaultSpec, datasets: list[str]) -> set[str]:
    """Datasets whose source data is malformed: explicit `params.datasets`, else a stable
    pseudo-random `intensity` fraction of all datasets."""
    if "datasets" in spec.params:
        return set(spec.params["datasets"])
    seed = str(spec.params.get("seed", 0))
    ranked = sorted(datasets, key=lambda d: hashlib.sha256(f"{seed}:{d}".encode()).hexdigest())
    return set(ranked[: math.ceil(spec.intensity * len(ranked))])


# --- Control endpoint ----------------------------------------------------------------------


def router(service: str, db: PlatformDB | None) -> APIRouter:
    """POST/GET/DELETE /chaos. Excluded from instrumentation, and silent by design."""
    api = APIRouter()
    leak: dict[str, ConnectionLeak] = {}

    def _stop_leak() -> None:
        if (current := leak.pop("current", None)) is not None:
            current.stop()

    @api.post("/chaos", status_code=204)
    async def activate(spec: FaultSpec) -> None:
        if spec.fault_type not in IN_PROCESS_FAULTS:
            raise HTTPException(400, f"{spec.fault_type} is applied through the control plane")
        if spec.target != service:
            raise HTTPException(400, f"target {spec.target} is not this service ({service})")
        if spec.fault_type == FaultType.DB_POOL_EXHAUSTION:
            if db is None:
                raise HTTPException(400, f"{service} has no database pool")
            _stop_leak()
            faults.activate(spec)
            leak["current"] = ConnectionLeak(db, spec)
            leak["current"].start()
            return
        faults.activate(spec)

    @api.get("/chaos")
    async def list_active() -> list[FaultSpec]:
        return faults.active()

    @api.delete("/chaos", status_code=204)
    async def clear() -> None:
        faults.clear()
        _stop_leak()

    return api
