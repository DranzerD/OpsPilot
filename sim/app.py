"""FastAPI app factory for the simulated services.

All five services share one codebase; `create_app(name)` builds the app for one of them.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from sim.config import ServiceConfig, load_topology
from sim.db import PoolTimeout
from sim.deps import DependencyError
from sim.telemetry import setup_telemetry

log = logging.getLogger(__name__)


class Service:
    """Base class: builds routes in `setup`, may run background work between `start` and `stop`."""

    def __init__(self, config: ServiceConfig) -> None:
        self.config = config

    def setup(self, app: FastAPI) -> None:
        raise NotImplementedError

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass


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
        log.info("%s started on port %d", name, config.port)
        yield
        service.stop()

    app = FastAPI(title=f"sim {name}", lifespan=lifespan)

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok", "service": name}

    @app.exception_handler(DependencyError)
    async def on_dependency_error(request: Request, exc: DependencyError) -> JSONResponse:
        if exc.is_timeout:
            log.error(
                "Upstream %s call %s exceeded deadline",
                exc.peer,
                exc.route,
                extra={"peer": exc.peer, "route": exc.route},
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

    service.setup(app)
    # ASGI send/receive sub-spans carry no diagnostic signal and roughly double trace volume.
    FastAPIInstrumentor.instrument_app(app, excluded_urls="healthz", exclude_spans=["receive", "send"])
    return app
