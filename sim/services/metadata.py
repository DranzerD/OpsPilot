"""metadata: dataset catalog, schemas and partitions, with an in-process TTL cache."""

from __future__ import annotations

import json
import logging
import threading
from datetime import UTC, datetime
from time import monotonic
from typing import Any

from fastapi import FastAPI, HTTPException
from opentelemetry import metrics
from pydantic import BaseModel, Field

from sim.app import Service
from sim.config import ServiceConfig
from sim.db import PlatformDB

log = logging.getLogger(__name__)


class PartitionIn(BaseModel):
    partition_key: str
    row_count: int = Field(ge=0)


class TTLCache:
    def __init__(self, ttl_s: float) -> None:
        self.ttl_s = ttl_s
        self._items: dict[str, tuple[float, Any]] = {}
        self._lock = threading.Lock()
        self._requests = metrics.get_meter("sim.metadata").create_counter(
            "cache.requests", description="Catalog cache lookups by result"
        )

    def get(self, key: str) -> Any | None:
        with self._lock:
            item = self._items.get(key)
            if item and monotonic() - item[0] < self.ttl_s:
                self._requests.add(1, {"result": "hit"})
                return item[1]
        self._requests.add(1, {"result": "miss"})
        return None

    def put(self, key: str, value: Any) -> None:
        with self._lock:
            self._items[key] = (monotonic(), value)

    def invalidate(self, key: str) -> None:
        with self._lock:
            self._items.pop(key, None)


class Metadata(Service):
    def __init__(self, config: ServiceConfig) -> None:
        super().__init__(config)
        self.db = PlatformDB(config.name, pool_size=config.options["db_pool_size"])
        self.cache = TTLCache(config.options.get("cache_ttl_s", 10))

    def setup(self, app: FastAPI) -> None:
        app.get("/datasets")(self.list_datasets)
        app.get("/datasets/{name}")(self.get_dataset)
        app.get("/datasets/{name}/partitions")(self.list_partitions)
        app.post("/datasets/{name}/partitions", status_code=201)(self.register_partition)

    def list_datasets(self) -> list[dict[str, Any]]:
        cached = self.cache.get("datasets")
        if cached is not None:
            return cached
        with self.db.connection() as conn:
            rows = self.db.execute(conn, "SELECT datasets", "SELECT name, owner FROM datasets ORDER BY name")
        self.cache.put("datasets", rows)
        return rows

    def get_dataset(self, name: str) -> dict[str, Any]:
        key = f"dataset:{name}"
        cached = self.cache.get(key)
        if cached is not None:
            return cached
        with self.db.connection() as conn:
            rows = self.db.execute(
                conn,
                "SELECT datasets",
                "SELECT name, owner, schema_json FROM datasets WHERE name = :name",
                {"name": name},
            )
        if not rows:
            raise HTTPException(404, f"unknown dataset {name}")
        row = rows[0]
        dataset = {"name": row["name"], "owner": row["owner"], "schema": json.loads(row["schema_json"])}
        self.cache.put(key, dataset)
        return dataset

    def list_partitions(self, name: str, limit: int = 10) -> list[dict[str, Any]]:
        with self.db.connection() as conn:
            return self.db.execute(
                conn,
                "SELECT partitions",
                "SELECT partition_key, row_count, created_at FROM partitions "
                "WHERE dataset = :name ORDER BY partition_key DESC LIMIT :limit",
                {"name": name, "limit": min(limit, 100)},
            )

    def register_partition(self, name: str, body: PartitionIn) -> dict[str, Any]:
        self.get_dataset(name)
        with self.db.connection() as conn:
            self.db.execute(
                conn,
                "INSERT partitions",
                "INSERT INTO partitions (dataset, partition_key, row_count, created_at) "
                "VALUES (:dataset, :key, :rows, :now) "
                "ON CONFLICT (dataset, partition_key) DO UPDATE SET row_count = excluded.row_count",
                {
                    "dataset": name,
                    "key": body.partition_key,
                    "rows": body.row_count,
                    "now": datetime.now(UTC).isoformat(),
                },
            )
            conn.commit()
        log.info(
            "Registered partition %s/%s (%d rows)",
            name,
            body.partition_key,
            body.row_count,
            extra={"dataset": name, "partition_key": body.partition_key},
        )
        return {"dataset": name, **body.model_dump()}
