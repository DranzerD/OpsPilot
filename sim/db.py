"""Instrumented access to the platform database (SQLite).

Each service gets its own bounded SQLAlchemy connection pool. Pool usage, pending requests,
pool wait time and per-operation query duration are exported as metrics, and every query is a
CLIENT span with `peer.service=platform-db`, so the database shows up as a node in traces.
"""

from __future__ import annotations

import json
import logging
import random
import sqlite3
import threading
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import perf_counter
from typing import Any

from opentelemetry import metrics, trace
from opentelemetry.metrics import CallbackOptions, Observation
from opentelemetry.trace import SpanKind, Status, StatusCode
from sqlalchemy import Connection, create_engine, event, text
from sqlalchemy import exc as sa_exc
from sqlalchemy.pool import QueuePool

from sim.config import PLATFORM_DB_PATH
from sim.telemetry import LATENCY_BUCKETS_S

log = logging.getLogger(__name__)
_tracer = trace.get_tracer("sim.db")

PEER = "platform-db"


class PoolTimeout(Exception):
    """No connection became available within the pool timeout."""


class PlatformDB:
    def __init__(
        self,
        service: str,
        pool_size: int,
        pool_timeout_s: float = 5.0,
        path: Path = PLATFORM_DB_PATH,
    ) -> None:
        self.service = service
        self.pool_size = pool_size
        self.pool_timeout_s = pool_timeout_s
        self.engine = create_engine(
            f"sqlite:///{path}",
            poolclass=QueuePool,
            pool_size=pool_size,
            max_overflow=0,
            pool_timeout=pool_timeout_s,
            connect_args={"timeout": 5.0, "check_same_thread": False},
        )
        event.listen(self.engine, "connect", _configure_sqlite)

        self._pending = 0
        self._pending_lock = threading.Lock()
        meter = metrics.get_meter("sim.db")
        meter.create_observable_gauge(
            "db.client.connection.count",
            callbacks=[self._observe_connections],
            description="Connections in the pool by state",
        )
        meter.create_observable_gauge(
            "db.client.connection.pending_requests",
            callbacks=[self._observe_pending],
            description="Requests waiting for a pool connection",
        )
        self._wait_time = meter.create_histogram(
            "db.client.connection.wait_time",
            unit="s",
            description="Time spent waiting for a pool connection",
            explicit_bucket_boundaries_advisory=LATENCY_BUCKETS_S,
        )
        self._op_duration = meter.create_histogram(
            "db.client.operation.duration",
            unit="s",
            description="Duration of database operations",
            explicit_bucket_boundaries_advisory=LATENCY_BUCKETS_S,
        )

    def _observe_connections(self, _: CallbackOptions) -> Iterator[Observation]:
        pool = self.engine.pool
        used = pool.checkedout()  # type: ignore[attr-defined]
        yield Observation(used, {"state": "used"})
        yield Observation(max(self.pool_size - used, 0), {"state": "idle"})

    def _observe_pending(self, _: CallbackOptions) -> Iterator[Observation]:
        yield Observation(self._pending)

    @contextmanager
    def connection(self) -> Iterator[Connection]:
        with self._pending_lock:
            self._pending += 1
        start = perf_counter()
        try:
            conn = self.engine.connect()
        except sa_exc.TimeoutError as exc:
            waited_ms = (perf_counter() - start) * 1000
            log.error(
                "Timed out acquiring connection from pool after %.0fms (pool_size=%d)",
                waited_ms,
                self.pool_size,
                extra={"pool_size": self.pool_size, "wait_ms": round(waited_ms)},
            )
            raise PoolTimeout(f"pool exhausted after {waited_ms:.0f}ms") from exc
        finally:
            with self._pending_lock:
                self._pending -= 1
            self._wait_time.record(perf_counter() - start)
        try:
            yield conn
        finally:
            conn.close()

    def execute(
        self,
        conn: Connection,
        operation: str,
        sql: str,
        params: dict[str, Any] | Sequence[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        """Run one statement as a traced, timed operation. Returns rows as dicts (empty for writes)."""
        with _tracer.start_as_current_span(f"{PEER} {operation}", kind=SpanKind.CLIENT) as span:
            span.set_attributes(
                {
                    "db.system": "sqlite",
                    "db.operation.name": operation,
                    "db.query.text": " ".join(sql.split())[:300],
                    "peer.service": PEER,
                }
            )
            start = perf_counter()
            attrs = {"db.operation.name": operation}
            try:
                result = conn.execute(text(sql), params or {})
                rows = [dict(r._mapping) for r in result] if result.returns_rows else []
            except sa_exc.SQLAlchemyError as exc:
                attrs["error.type"] = type(exc.orig if hasattr(exc, "orig") else exc).__name__
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, attrs["error.type"]))
                raise
            finally:
                self._op_duration.record(perf_counter() - start, attrs)
            span.set_attribute("db.response.returned_rows", len(rows))
            return rows


def _configure_sqlite(dbapi_conn: sqlite3.Connection, _record: Any) -> None:
    cur = dbapi_conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL")
    cur.execute("PRAGMA busy_timeout=5000")
    cur.execute("PRAGMA synchronous=NORMAL")
    cur.close()


# --- Schema and seed data -------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS datasets (
    name TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    schema_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS partitions (
    dataset TEXT NOT NULL REFERENCES datasets(name),
    partition_key TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (dataset, partition_key)
);
CREATE TABLE IF NOT EXISTS records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    ts TEXT NOT NULL,
    category TEXT NOT NULL,
    value REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_records_dataset_partition ON records (dataset, partition_key);
"""

DATASETS = {
    "orders": ("commerce", ["order_id", "customer_id", "amount", "currency"]),
    "payments": ("commerce", ["payment_id", "order_id", "amount", "method"]),
    "clickstream": ("growth", ["session_id", "url", "referrer", "device"]),
    "sessions": ("growth", ["session_id", "user_id", "duration_s"]),
    "inventory": ("supply", ["sku", "warehouse", "quantity"]),
    "shipments": ("supply", ["shipment_id", "order_id", "carrier", "status"]),
    "reviews": ("content", ["review_id", "sku", "rating"]),
    "ad_impressions": ("ads", ["impression_id", "campaign_id", "cost"]),
}
CATEGORIES = ["a", "b", "c", "d", "e"]
SEED_PARTITIONS = 6
SEED_ROWS_PER_PARTITION = 300


def partition_key(ts: datetime) -> str:
    return ts.strftime("%Y%m%dT%H%M%S")


def make_rows(dataset: str, key: str, n: int, rng: random.Random) -> list[dict[str, Any]]:
    now = datetime.now(UTC).isoformat()
    return [
        {
            "dataset": dataset,
            "partition_key": key,
            "ts": now,
            "category": rng.choice(CATEGORIES),
            "value": rng.gauss(100, 25),
        }
        for _ in range(n)
    ]


def init_platform_db(path: Path = PLATFORM_DB_PATH, reset: bool = False, seed: int = 1) -> None:
    """Create the schema and seed datasets with a few historical partitions."""
    if reset and path.exists():
        for suffix in ("", "-wal", "-shm"):
            Path(f"{path}{suffix}").unlink(missing_ok=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript(SCHEMA)
        if conn.execute("SELECT COUNT(*) FROM datasets").fetchone()[0]:
            return
        rng = random.Random(seed)
        now = datetime.now(UTC)
        for name, (owner, columns) in DATASETS.items():
            conn.execute(
                "INSERT INTO datasets (name, owner, schema_json, created_at) VALUES (?, ?, ?, ?)",
                (name, owner, json.dumps({"columns": columns}), now.isoformat()),
            )
            for i in range(SEED_PARTITIONS, 0, -1):
                key = partition_key(now - timedelta(minutes=2 * i))
                rows = make_rows(name, key, SEED_ROWS_PER_PARTITION, rng)
                conn.executemany(
                    "INSERT INTO records (dataset, partition_key, ts, category, value) "
                    "VALUES (:dataset, :partition_key, :ts, :category, :value)",
                    rows,
                )
                conn.execute(
                    "INSERT INTO partitions (dataset, partition_key, row_count, created_at) VALUES (?, ?, ?, ?)",
                    (name, key, len(rows), now.isoformat()),
                )
        conn.commit()
    finally:
        conn.close()
