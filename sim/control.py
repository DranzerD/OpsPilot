"""Control plane for the simulated platform: deploys, config changes and restarts.

The supervisor (sim/run.py) serves this API on 127.0.0.1:8090, because it owns the processes.
Every operation is appended to var/telemetry/changes.jsonl as a change event: the platform's
deploy/config log. Change events are first-class evidence (spec §3.4); they record what changed,
never why.

    GET  /services                          versions, feature flags and pids
    POST /deploy   {service, version, feature_flags}
    POST /config   {service, key, value}
    POST /restart  {service}
    GET  /changes
"""

from __future__ import annotations

import json
import threading
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx

from sim.config import CHANGES_PATH, CONTROL_PORT, CONTROL_URL, HOST, load_topology

if TYPE_CHECKING:
    from sim.run import Supervisor


class ChangeLog:
    def __init__(self, path: Path = CHANGES_PATH) -> None:
        self.path = path
        self._lock = threading.Lock()

    def append(self, service: str, kind: str, summary: str, details: dict[str, Any]) -> dict[str, Any]:
        event = {
            "id": str(uuid.uuid4()),
            "ts": datetime.now(UTC).isoformat(timespec="milliseconds"),
            "service": service,
            "kind": kind,
            "summary": summary,
            "details": details,
        }
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(json.dumps(event) + "\n")
        return event

    def read(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        return [json.loads(line) for line in self.path.read_text(encoding="utf-8").splitlines() if line]


class ControlError(Exception):
    def __init__(self, status: HTTPStatus, message: str) -> None:
        super().__init__(message)
        self.status = status


class ControlPlane:
    def __init__(self, supervisor: Supervisor, changes: ChangeLog | None = None) -> None:
        self.supervisor = supervisor
        self.changes = changes or ChangeLog()
        self.services = load_topology().services
        self._lock = threading.Lock()  # one operation at a time

    def _service(self, body: dict[str, Any]) -> str:
        name = body.get("service")
        if name not in self.services:
            raise ControlError(HTTPStatus.BAD_REQUEST, f"unknown service {name!r}")
        return name

    def describe(self) -> dict[str, Any]:
        return self.supervisor.describe_services()

    def deploy(self, body: dict[str, Any]) -> dict[str, Any]:
        name = self._service(body)
        version = str(body.get("version") or "")
        if not version:
            raise ControlError(HTTPStatus.BAD_REQUEST, "version is required")
        flags = {str(k): float(v) for k, v in (body.get("feature_flags") or {}).items()}
        with self._lock:
            started = datetime.now(UTC).isoformat(timespec="milliseconds")
            previous = self.supervisor.release(name)
            self.supervisor.redeploy(name, version, flags)
            details = {"from_version": previous["version"], "to_version": version, "started_at": started}
            return self.changes.append(
                name, "deploy", f"Deployed {name} {version} (was {previous['version']})", details
            )

    def config(self, body: dict[str, Any]) -> dict[str, Any]:
        name = self._service(body)
        key, value = body.get("key"), body.get("value")
        if not isinstance(key, str) or not isinstance(value, int | float):
            raise ControlError(HTTPStatus.BAD_REQUEST, "key (str) and value (number) are required")
        with self._lock:
            resp = httpx.post(
                f"{self.services[name].base_url}/admin/config", json={"key": key, "value": value}, timeout=5
            )
            if resp.status_code != 200:
                raise ControlError(HTTPStatus.BAD_GATEWAY, f"{name} rejected config: {resp.text}")
            previous = resp.json()["previous"]
            return self.changes.append(
                name,
                "config",
                f"{name}: {key} changed from {previous} to {value}",
                {"key": key, "old_value": previous, "new_value": value},
            )

    def restart(self, body: dict[str, Any]) -> dict[str, Any]:
        name = self._service(body)
        with self._lock:
            release = self.supervisor.release(name)
            self.supervisor.redeploy(name, release["version"], release["feature_flags"])
            return self.changes.append(name, "restart", f"Restarted {name}", {"version": release["version"]})


def serve(plane: ControlPlane) -> ThreadingHTTPServer:
    routes: dict[tuple[str, str], Callable[[dict[str, Any]], Any]] = {
        ("GET", "/services"): lambda _: plane.describe(),
        ("GET", "/changes"): lambda _: plane.changes.read(),
        ("POST", "/deploy"): plane.deploy,
        ("POST", "/config"): plane.config,
        ("POST", "/restart"): plane.restart,
    }

    class Handler(BaseHTTPRequestHandler):
        def _handle(self, method: str) -> None:
            route = routes.get((method, self.path.split("?")[0]))
            if route is None:
                return self._send(HTTPStatus.NOT_FOUND, {"error": "not found"})
            try:
                length = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(length)) if length else {}
                self._send(HTTPStatus.OK, route(body))
            except ControlError as exc:
                self._send(exc.status, {"error": str(exc)})
            except Exception as exc:  # noqa: BLE001 - report any failure to the caller, keep serving
                self._send(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": f"{type(exc).__name__}: {exc}"})

        def do_GET(self) -> None:
            self._handle("GET")

        def do_POST(self) -> None:
            self._handle("POST")

        def _send(self, status: HTTPStatus, payload: Any) -> None:
            data = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args: Any) -> None:
            pass

    server = ThreadingHTTPServer((HOST, CONTROL_PORT), Handler)
    threading.Thread(target=server.serve_forever, name="control-plane", daemon=True).start()
    return server


class ControlClient:
    """Client for the control plane, used by the scenario recorder and (later) remediation."""

    def __init__(self, base_url: str = CONTROL_URL, timeout_s: float = 60.0) -> None:
        self._client = httpx.Client(base_url=base_url, timeout=timeout_s)

    def _call(self, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
        resp = self._client.request(method, path, json=body)
        if resp.status_code != 200:
            raise RuntimeError(f"control plane {method} {path} failed: {resp.status_code} {resp.text}")
        return resp.json()

    def services(self) -> dict[str, Any]:
        return self._call("GET", "/services")

    def changes(self) -> list[dict[str, Any]]:
        return self._call("GET", "/changes")

    def deploy(
        self, service: str, version: str, feature_flags: dict[str, float] | None = None
    ) -> dict[str, Any]:
        return self._call(
            "POST", "/deploy", {"service": service, "version": version, "feature_flags": feature_flags or {}}
        )

    def config(self, service: str, key: str, value: float) -> dict[str, Any]:
        return self._call("POST", "/config", {"service": service, "key": key, "value": value})

    def restart(self, service: str) -> dict[str, Any]:
        return self._call("POST", "/restart", {"service": service})

    def close(self) -> None:
        self._client.close()
