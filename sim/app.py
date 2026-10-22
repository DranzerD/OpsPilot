"""FastAPI app factory for the simulated services.

All five services share one codebase; `create_app(name)` builds the app for one of them.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from pydantic import BaseModel

from sim import chaos
from sim.config import VERSION, ServiceConfig, load_topology
from sim.db import PlatformDB, PoolTimeout
from sim.deps import DependencyClient, DependencyError
from sim.features import FLAGS
from sim.telemetry import setup_telemetry

log = logging.getLogger(__name__)


class ConfigChange(BaseModel):
    key: str  # e.g. "dependencies.metadata.timeout_s"
    value: float


class Service:
    """Base class: builds routes in `setup`, may run background work between `start` and `stop`."""

    def __init__(self, config: ServiceConfig) -> None:
        self.config = config
        self.db: PlatformDB | None = None
        self.deps: dict[str, DependencyClient] = {}

    def dependency(self, peer: str) -> DependencyClient:
        client = DependencyClient(self.config, peer)
        self.deps[peer] = client
        return client

    def setup(self, app: FastAPI) -> None:
        raise NotImplementedError

    def start(self) -> None:
        pass

    def stop(self) -> None:
        for client in self.deps.values():
            client.close()

    def apply_config(self, change: ConfigChange) -> float:
        """Apply a runtime config change; returns the previous value."""
        parts = change.key.split(".")
        if (
            len(parts) == 3
            and parts[0] == "dependencies"
            and parts[2] == "timeout_s"
            and parts[1] in self.deps
        ):
            client = self.deps[parts[1]]
            previous = client.timeout_s
            client.set_timeout(change.value)
            return previous
        raise HTTPException(400, f"unsupported config key {change.key}")


def _registry() -> dict[str, Callable[[ServiceConfig], Service]]:
    from sim.services.gateway import Gateway
    from sim.services.metadata import Metadata
    from sim.services.query_engine import QueryEngine
    from sim.services.scheduler import Scheduler
    from sim.services.stream_processor import StreamProcessor

    return {
        "gateway": Gateway,
        "query-engine": QueryEngine,
        "metadata": Metadata,
        "scheduler": Scheduler,
        "stream-processor": StreamProcessor,
    }


def create_app(name: str, export_telemetry: bool = True) -> FastAPI:
    config = load_topology().services[name]
    setup_telemetry(name, export=export_telemetry)
    service = _registry()[name](config)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        service.start()
        flags = f" with feature flags {FLAGS}" if FLAGS else ""
        log.info("%s %s started on port %d%s", name, VERSION, config.port, flags)
        yield
        service.stop()

    app = FastAPI(title=f"sim {name}", lifespan=lifespan, dependencies=[Depends(chaos.added_latency(name))])

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok", "service": name, "version": VERSION}

    @app.get("/admin/config")
    def current_config() -> dict[str, float]:
        return {f"dependencies.{peer}.timeout_s": c.timeout_s for peer, c in service.deps.items()}

    @app.post("/admin/config")
    def admin_config(change: ConfigChange) -> dict[str, float | str]:
        previous = service.apply_config(change)
        log.info(
            "Config updated: %s=%s (was %s)",
            change.key,
            change.value,
            previous,
            extra={"config_key": change.key, "new_value": change.value, "old_value": previous},
        )
        return {"key": change.key, "value": change.value, "previous": previous}

    @app.exception_handler(DependencyError)
    async def on_dependency_error(request: Request, exc: DependencyError) -> JSONResponse:
        if exc.is_timeout:
            log.error(
                "Upstream %s call %s exceeded deadline (%ss)",
                exc.peer,
                exc.route,
                exc.timeout_s,
                extra={"peer": exc.peer, "route": exc.route, "timeout_s": exc.timeout_s},
            )
            return JSONResponse({"error": f"upstream {exc.peer} timed out"}, status_code=504)
        log.error(
            "Upstream %s call %s failed: %s",
            exc.peer,
            exc.route,
            exc.reason,
            extra={"peer": exc.peer, "route": exc.route, "upstream_status": exc.status_code},
        )
        return JSONResponse({"error": f"upstream {exc.peer} failed"}, status_code=502)

    @app.exception_handler(PoolTimeout)
    async def on_pool_timeout(request: Request, exc: PoolTimeout) -> JSONResponse:
        return JSONResponse({"error": "database unavailable"}, status_code=503)

    @app.exception_handler(Exception)
    async def on_unhandled(request: Request, exc: Exception) -> JSONResponse:
        # Put the error in the message itself: that's what engineers (and the agent) search.
        log.error(
            "Unhandled error in %s %s: %s: %s",
            request.method,
            request.url.path,
            type(exc).__name__,
            str(exc).splitlines()[0][:300],
            exc_info=exc,
            extra={"error_type": type(exc).__name__},
        )
        return JSONResponse({"error": "internal error"}, status_code=500)

    service.setup(app)
    app.include_router(chaos.router(name, service.db))
    # ASGI send/receive sub-spans carry no diagnostic signal and roughly double trace volume.
    # Control endpoints are not part of the platform's observable behaviour.
    FastAPIInstrumentor.instrument_app(
        app, excluded_urls="healthz,chaos,admin", exclude_spans=["receive", "send"]
    )
    return app
