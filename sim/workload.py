"""Interactive workload: Poisson-distributed requests against the gateway.

Batch work comes from the scheduler's own clock, not from here.

    python -m sim.workload [--rps 20] [--seed 1]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import sys
from collections import Counter
from datetime import UTC, datetime

import httpx

from sim.config import load_topology
from sim.db import DATASETS

REPORT_EVERY_S = 30
MAX_IN_FLIGHT = 64


def _pick_request(rng: random.Random) -> tuple[str, str, dict | None]:
    dataset = rng.choice(list(DATASETS))
    roll = rng.random()
    if roll < 0.5:
        kind = rng.choices(["count", "aggregate", "scan"], weights=[5, 3, 2])[0]
        return (
            "POST",
            "/api/query",
            {"dataset": dataset, "kind": kind, "last_n_partitions": rng.randint(1, 6)},
        )
    if roll < 0.8:
        return "GET", f"/api/datasets/{dataset}", None
    return "GET", "/api/datasets", None


async def run(rps: float, seed: int) -> None:
    gateway = load_topology().services["gateway"]
    rng = random.Random(seed)
    stats: Counter[str] = Counter()
    in_flight = asyncio.Semaphore(MAX_IN_FLIGHT)

    async def one(client: httpx.AsyncClient, method: str, path: str, body: dict | None) -> None:
        try:
            resp = await client.request(method, path, json=body)
            stats[f"{resp.status_code // 100}xx"] += 1
        except httpx.TimeoutException:
            stats["timeout"] += 1
        except httpx.TransportError:
            stats["connection_error"] += 1
        finally:
            in_flight.release()

    async def report() -> None:
        while True:
            await asyncio.sleep(REPORT_EVERY_S)
            line = {"timestamp": datetime.now(UTC).isoformat(timespec="seconds"), **stats}
            print(json.dumps(line), flush=True)
            stats.clear()

    async with httpx.AsyncClient(base_url=gateway.base_url, timeout=10.0) as client:
        reporter = asyncio.create_task(report())
        tasks: set[asyncio.Task] = set()
        try:
            while True:
                await asyncio.sleep(rng.expovariate(rps))
                if in_flight.locked():
                    stats["dropped"] += 1  # client-side backpressure: never queue unboundedly
                    continue
                await in_flight.acquire()
                task = asyncio.create_task(one(client, *_pick_request(rng)))
                tasks.add(task)
                task.add_done_callback(tasks.discard)
        finally:
            reporter.cancel()


def main(argv: list[str]) -> int:
    workload = load_topology().workload
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rps", type=float, default=workload.get("interactive_rps", 20))
    parser.add_argument("--seed", type=int, default=workload.get("seed", 1))
    args = parser.parse_args(argv)
    try:
        asyncio.run(run(args.rps, args.seed))
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
