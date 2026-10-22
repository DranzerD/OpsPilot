"""Process supervisor for the whole simulated stack, run natively (no containers).

    python -m sim.run up [--reset]   start Collector, Prometheus, services, workload; Ctrl+C stops all
    python -m sim.run down           stop a stack started elsewhere (uses var/run.json)
    python -m sim.run status         show which components are running

Each component's stdout/stderr goes to var/logs/<name>.log.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import time
from contextlib import ExitStack
from dataclasses import dataclass

import httpx

from sim.config import CONFIGS, HOST, PLATFORM_DB_PATH, PROMETHEUS_URL, ROOT, TOOLS, VAR, load_topology
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
            Component(svc.name, [python, "-m", "sim.serve", svc.name], ready_url=f"{svc.base_url}/healthz")
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


def _check_tools() -> None:
    missing = [p.name for p in (TOOLS / "otelcol-contrib.exe", TOOLS / "prometheus.exe") if not p.exists()]
    if missing:
        sys.exit(f"missing {', '.join(missing)}; run: python scripts/fetch_tools.py")


def _stop(procs: list[tuple[Component, subprocess.Popen]]) -> None:
    for _, proc in reversed(procs):
        if proc.poll() is None:
            proc.terminate()
    for _, proc in reversed(procs):
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
    RUN_FILE.unlink(missing_ok=True)


def up(reset: bool) -> int:
    _check_tools()
    if RUN_FILE.exists() and _alive(json.loads(RUN_FILE.read_text())):
        sys.exit("stack already running; use `python -m sim.run down` first")
    for d in (LOGS, VAR / "telemetry", VAR / "prometheus"):
        d.mkdir(parents=True, exist_ok=True)
    init_platform_db(PLATFORM_DB_PATH, reset=reset)

    env = {**os.environ, "PYTHONUNBUFFERED": "1", "OTEL_SEMCONV_STABILITY_OPT_IN": "http"}
    # Own process group, so Ctrl+C reaches only the supervisor, which then stops children in order.
    flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    procs: list[tuple[Component, subprocess.Popen]] = []
    exited: set[str] = set()
    with ExitStack() as log_files:
        try:
            for comp in components():
                log_file = log_files.enter_context(open(LOGS / f"{comp.name}.log", "ab"))
                proc = subprocess.Popen(
                    comp.cmd,
                    cwd=ROOT,
                    env=env,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    creationflags=flags,
                )
                procs.append((comp, proc))
                _wait_ready(comp, proc)
                print(f"  {comp.name:<17} up (pid {proc.pid})", flush=True)
            pids = {"supervisor": os.getpid(), **{c.name: p.pid for c, p in procs}}
            RUN_FILE.write_text(json.dumps(pids, indent=2))
            print(f"stack up. Prometheus: {PROMETHEUS_URL}  logs: {LOGS}  Ctrl+C to stop", flush=True)

            while True:
                time.sleep(2)
                for comp, proc in procs:
                    if proc.poll() is not None and comp.name not in exited:
                        # Not restarted: a crashed service is a signal worth seeing, not hiding.
                        print(f"  {comp.name} exited with code {proc.returncode}", flush=True)
                        exited.add(comp.name)
        except KeyboardInterrupt:
            print("stopping...", flush=True)
        except RuntimeError as exc:
            print(f"startup failed: {exc}", file=sys.stderr, flush=True)
            _stop(procs)
            return 1
        _stop(procs)
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
