"""Process supervisor for the whole simulated stack, run natively (no containers).

    python -m sim.run up [--reset]   start Collector, Prometheus, services, workload; Ctrl+C stops all
    python -m sim.run down           stop a stack started elsewhere (uses var/run.json)
    python -m sim.run status         show which components are running

Each component's stdout/stderr goes to var/logs/<name>.log. The supervisor also serves the
control plane (sim/control.py) on 127.0.0.1:8090 for deploys, config changes and restarts.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import IO, Any

import httpx

from sim.config import (
    BASE_VERSION,
    CONFIGS,
    CONTROL_URL,
    HOST,
    PLATFORM_DB_PATH,
    PROMETHEUS_URL,
    ROOT,
    TELEMETRY_DIR,
    TOOLS,
    VAR,
    load_topology,
)
from sim.control import ControlPlane, serve
from sim.db import init_platform_db

LOGS = VAR / "logs"
RUN_FILE = VAR / "run.json"
STARTUP_TIMEOUT_S = 30


@dataclass
class Component:
    name: str
    cmd: list[str]
    ready_port: int | None = None
    ready_url: str | None = None
    deployable: bool = False


@dataclass
class Running:
    proc: subprocess.Popen
    log_file: IO[bytes]
    version: str = BASE_VERSION
    feature_flags: dict[str, float] = field(default_factory=dict)


def components() -> list[Component]:
    python = sys.executable
    topo = load_topology()
    stack = [
        Component(
            "otelcol",
            [str(TOOLS / "otelcol-contrib.exe"), "--config", str(CONFIGS / "otelcol.yaml")],
            ready_port=4318,
        ),
        Component(
            "prometheus",
            [
                str(TOOLS / "prometheus.exe"),
                f"--config.file={CONFIGS / 'prometheus' / 'prometheus.yml'}",
                f"--storage.tsdb.path={VAR / 'prometheus'}",
                "--storage.tsdb.retention.time=6h",
                f"--web.listen-address={HOST}:9090",
                "--web.enable-lifecycle",
            ],
            ready_url=f"{PROMETHEUS_URL}/-/ready",
        ),
    ]
    for svc in topo.services.values():
        stack.append(
            Component(
                svc.name,
                [python, "-m", "sim.serve", svc.name],
                ready_url=f"{svc.base_url}/healthz",
                deployable=True,
            )
        )
    stack.append(Component("workload", [python, "-m", "sim.workload"]))
    return stack


def _port_open(port: int) -> bool:
    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex((HOST, port)) == 0


def _wait_ready(comp: Component, proc: subprocess.Popen) -> None:
    deadline = time.monotonic() + STARTUP_TIMEOUT_S
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"{comp.name} exited with code {proc.returncode}; see {LOGS / comp.name}.log")
        if comp.ready_port is not None and _port_open(comp.ready_port):
            return
        if comp.ready_url is not None:
            try:
                if httpx.get(comp.ready_url, timeout=1.0).status_code == 200:
                    return
            except httpx.HTTPError:
                pass
        if comp.ready_port is None and comp.ready_url is None:
            return
        time.sleep(0.3)
    raise RuntimeError(f"{comp.name} not ready after {STARTUP_TIMEOUT_S}s; see {LOGS / comp.name}.log")


def _terminate(proc: subprocess.Popen) -> None:
    if proc.poll() is None:
        proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()


class Supervisor:
    def __init__(self) -> None:
        self.components = {c.name: c for c in components()}
        self.running: dict[str, Running] = {}
        self.exited: set[str] = set()
        self._lock = threading.RLock()
        self._env = {**os.environ, "PYTHONUNBUFFERED": "1", "OTEL_SEMCONV_STABILITY_OPT_IN": "http"}

    def _spawn(self, name: str, version: str, feature_flags: dict[str, float]) -> None:
        comp = self.components[name]
        env = dict(self._env)
        if comp.deployable:
            env["SERVICE_VERSION"] = version
            env["FEATURE_FLAGS"] = json.dumps(feature_flags) if feature_flags else ""
        log_file = open(LOGS / f"{name}.log", "ab")  # noqa: SIM115 - closed when the process is replaced or stopped
        # Own process group, so Ctrl+C reaches only the supervisor, which then stops children in order.
        # No console needed: a supervisor whose console has gone away can still spawn children.
        flags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
        proc = subprocess.Popen(
            comp.cmd, cwd=ROOT, env=env, stdout=log_file, stderr=subprocess.STDOUT, creationflags=flags
        )
        self.running[name] = Running(proc, log_file, version, dict(feature_flags))
        _wait_ready(comp, proc)

    def start_all(self) -> None:
        for name in self.components:
            self._spawn(name, BASE_VERSION, {})
            print(f"  {name:<17} up (pid {self.running[name].proc.pid})", flush=True)
        self.write_run_file()

    def release(self, name: str) -> dict[str, Any]:
        with self._lock:
            r = self.running[name]
            return {"version": r.version, "feature_flags": dict(r.feature_flags), "pid": r.proc.pid}

    def describe_services(self) -> dict[str, Any]:
        with self._lock:
            return {name: self.release(name) for name, c in self.components.items() if c.deployable}

    def redeploy(self, name: str, version: str, feature_flags: dict[str, float]) -> None:
        """Replace a running service process with one at `version` / `feature_flags`."""
        with self._lock:
            old = self.running[name]
            _terminate(old.proc)
            old.log_file.close()
            self._spawn(name, version, feature_flags)
            self.exited.discard(name)
            self.write_run_file()
        print(f"  {name:<17} redeployed {version} (pid {self.running[name].proc.pid})", flush=True)

    def write_run_file(self) -> None:
        pids = {"supervisor": os.getpid(), **{n: r.proc.pid for n, r in self.running.items()}}
        RUN_FILE.write_text(json.dumps(pids, indent=2))

    def check_exits(self) -> None:
        with self._lock:
            for name, r in self.running.items():
                if r.proc.poll() is not None and name not in self.exited:
                    # Not restarted: a crashed service is a signal worth seeing, not hiding.
                    print(f"  {name} exited with code {r.proc.returncode}", flush=True)
                    self.exited.add(name)

    def stop_all(self) -> None:
        with self._lock:
            for r in reversed(list(self.running.values())):
                if r.proc.poll() is None:
                    r.proc.terminate()
            for r in reversed(list(self.running.values())):
                _terminate(r.proc)
                r.log_file.close()
            RUN_FILE.unlink(missing_ok=True)


def _check_tools() -> None:
    missing = [p.name for p in (TOOLS / "otelcol-contrib.exe", TOOLS / "prometheus.exe") if not p.exists()]
    if missing:
        sys.exit(f"missing {', '.join(missing)}; run: python scripts/fetch_tools.py")


def up(reset: bool) -> int:
    _check_tools()
    if RUN_FILE.exists() and _alive(json.loads(RUN_FILE.read_text())):
        sys.exit("stack already running; use `python -m sim.run down` first")
    for d in (LOGS, TELEMETRY_DIR, VAR / "prometheus"):
        d.mkdir(parents=True, exist_ok=True)
    init_platform_db(PLATFORM_DB_PATH, reset=reset)

    supervisor = Supervisor()
    server = None
    try:
        supervisor.start_all()
        server = serve(ControlPlane(supervisor))
        print(
            f"stack up. Prometheus: {PROMETHEUS_URL}  control: {CONTROL_URL}  logs: {LOGS}  Ctrl+C to stop",
            flush=True,
        )
        while True:
            time.sleep(2)
            supervisor.check_exits()
    except KeyboardInterrupt:
        print("stopping...", flush=True)
    except RuntimeError as exc:
        print(f"startup failed: {exc}", file=sys.stderr, flush=True)
        supervisor.stop_all()
        return 1
    finally:
        if server is not None:
            server.shutdown()
    supervisor.stop_all()
    return 0


def _alive(pids: dict[str, int]) -> set[str]:
    return {name for name, pid in pids.items() if _pid_alive(pid)}


def _pid_alive(pid: int) -> bool:
    if os.name == "nt":
        # os.kill(pid, 0) would terminate the process on Windows; ask tasklist instead.
        out = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/NH"], check=False, capture_output=True, text=True
        ).stdout
        return str(pid) in out
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def down() -> int:
    if not RUN_FILE.exists():
        print("no stack running")
        return 0
    pids: dict[str, int] = json.loads(RUN_FILE.read_text())
    # Supervisor first, so it doesn't report its children exiting; then children in reverse start order.
    order = [("supervisor", pids.pop("supervisor"))] if "supervisor" in pids else []
    for name, pid in order + list(reversed(pids.items())):
        try:
            os.kill(pid, signal.SIGTERM)
            print(f"  stopped {name} (pid {pid})")
        except OSError:
            pass
    RUN_FILE.unlink(missing_ok=True)
    return 0


def status() -> int:
    if not RUN_FILE.exists():
        print("no stack running")
        return 1
    pids: dict[str, int] = json.loads(RUN_FILE.read_text())
    alive = _alive(pids)
    for name, pid in pids.items():
        print(f"  {name:<17} {'running' if name in alive else 'stopped'} (pid {pid})")
    return 0 if len(alive) == len(pids) else 1


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_up = sub.add_parser("up")
    p_up.add_argument("--reset", action="store_true", help="recreate the platform database")
    sub.add_parser("down")
    sub.add_parser("status")
    args = parser.parse_args(argv)
    if args.cmd == "up":
        return up(args.reset)
    if args.cmd == "down":
        return down()
    return status()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
