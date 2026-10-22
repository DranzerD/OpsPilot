"""query-engine: runs queries over the latest partitions of a dataset."""

from __future__ import annotations

import logging
from time import perf_counter
from typing import Any, Literal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from sim.app import Service
from sim.config import ServiceConfig
from sim.db import PlatformDB
from sim.deps import DependencyClient

log = logging.getLogger(__name__)

SLOW_QUERY_MS = 1000

QUERIES = {
    "count": "SELECT COUNT(*) AS n FROM records WHERE dataset = :dataset AND partition_key IN ({keys})",
    "aggregate": (
        "SELECT category, COUNT(*) AS n, AVG(value) AS mean, MAX(value) AS max FROM records "
        "WHERE dataset = :dataset AND partition_key IN ({keys}) GROUP BY category ORDER BY category"
    ),
    "scan": (
        "SELECT id, category, value FROM records WHERE dataset = :dataset AND partition_key IN ({keys}) "
        "ORDER BY id DESC LIMIT 50"
    ),
}


class QueryIn(BaseModel):
    dataset: str
    kind: Literal["count", "aggregate", "scan"] = "count"
    last_n_partitions: int = Field(default=3, ge=1, le=20)


class QueryEngine(Service):
    def __init__(self, config: ServiceConfig) -> None:
        super().__init__(config)
        self.db = PlatformDB(config.name, pool_size=config.options["db_pool_size"])
        self.metadata = DependencyClient(config, "metadata")

    def setup(self, app: FastAPI) -> None:
        app.post("/query")(self.run_query)

    def stop(self) -> None:
        self.metadata.close()

    def run_query(self, q: QueryIn) -> dict[str, Any]:
        resp = self.metadata.request("GET", "/datasets/{name}", f"/datasets/{q.dataset}")
        if resp.status_code == 404:
            raise HTTPException(404, f"unknown dataset {q.dataset}")
        partitions = self.metadata.request(
            "GET",
            "/datasets/{name}/partitions",
            f"/datasets/{q.dataset}/partitions",
            params={"limit": q.last_n_partitions},
        ).json()
        keys = [p["partition_key"] for p in partitions]
        if not keys:
            return {"dataset": q.dataset, "kind": q.kind, "partitions": [], "rows": []}

        placeholders = ", ".join(f":k{i}" for i in range(len(keys)))
        params = {"dataset": q.dataset, **{f"k{i}": k for i, k in enumerate(keys)}}
        start = perf_counter()
        with self.db.connection() as conn:
            rows = self.db.execute(conn, f"query {q.kind}", QUERIES[q.kind].format(keys=placeholders), params)
        elapsed_ms = (perf_counter() - start) * 1000
        if elapsed_ms > SLOW_QUERY_MS:
            log.warning(
                "Slow query on dataset=%s kind=%s took %.0fms",
                q.dataset,
                q.kind,
                elapsed_ms,
                extra={"dataset": q.dataset, "query_kind": q.kind, "duration_ms": round(elapsed_ms)},
            )
        return {"dataset": q.dataset, "kind": q.kind, "partitions": keys, "rows": rows}
