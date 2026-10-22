"""Instrumented HTTP client for service-to-service calls.

Each call creates a CLIENT span, propagates trace context, and records
`http.client.request.duration` labelled with `peer.service`, so dependency latency can be
broken down per edge of the service graph.
"""

from __future__ import annotations

import logging
from time import perf_counter
from typing import Any

import httpx
from opentelemetry import metrics, trace
from opentelemetry.propagate import inject
from opentelemetry.trace import SpanKind, Status, StatusCode

from sim.config import ServiceConfig, load_topology
from sim.telemetry import LATENCY_BUCKETS_S

log = logging.getLogger(__name__)
_tracer = trace.get_tracer("sim.deps")


class DependencyError(Exception):
    """A dependency call failed: timeout, connection error, or a 5xx response."""

    def __init__(self, peer: str, route: str, reason: str, status_code: int | None = None) -> None:
        super().__init__(f"{peer} {route}: {reason}")
        self.peer = peer
        self.route = route
        self.reason = reason
        self.status_code = status_code

    @property
    def is_timeout(self) -> bool:
        return self.reason == "timeout"


class DependencyClient:
    def __init__(self, caller: ServiceConfig, peer: str) -> None:
        dep = caller.dependency(peer)
        target = load_topology().services[peer]
        self.peer = peer
        self.timeout_s = dep.timeout_s or 5.0
        self._client = httpx.Client(base_url=target.base_url, timeout=self.timeout_s)
        self._duration = metrics.get_meter("sim.deps").create_histogram(
            "http.client.request.duration",
            unit="s",
            description="Duration of outbound calls to dependencies",
            explicit_bucket_boundaries_advisory=LATENCY_BUCKETS_S,
        )

    def request(self, method: str, route: str, path: str | None = None, **kwargs: Any) -> httpx.Response:
        """Call `route` (a template such as /datasets/{name}) at `path` (the concrete URL path).

        Returns the response for 2xx/4xx; raises DependencyError on timeouts, connection
        errors and 5xx responses.
        """
        path = path or route
        attrs = {"peer.service": self.peer, "http.request.method": method, "http.route": route}
        with _tracer.start_as_current_span(f"{method} {self.peer}{route}", kind=SpanKind.CLIENT) as span:
            span.set_attributes({**attrs, "url.path": path})
            headers: dict[str, str] = {}
            inject(headers)
            start = perf_counter()
            outcome: dict[str, Any] = {}
            try:
                resp = self._client.request(method, path, headers=headers, **kwargs)
            except httpx.TimeoutException as exc:
                outcome["error.type"] = "timeout"
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, "timeout"))
                raise DependencyError(self.peer, route, "timeout") from exc
            except httpx.TransportError as exc:
                outcome["error.type"] = "connection_error"
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, "connection error"))
                raise DependencyError(self.peer, route, "connection_error") from exc
            finally:
                if outcome:
                    self._duration.record(perf_counter() - start, {**attrs, **outcome})

            elapsed = perf_counter() - start
            span.set_attribute("http.response.status_code", resp.status_code)
            self._duration.record(elapsed, {**attrs, "http.response.status_code": resp.status_code})
            if resp.status_code >= 500:
                span.set_status(Status(StatusCode.ERROR, f"HTTP {resp.status_code}"))
                raise DependencyError(self.peer, route, f"HTTP {resp.status_code}", resp.status_code)
            return resp

    def close(self) -> None:
        self._client.close()
