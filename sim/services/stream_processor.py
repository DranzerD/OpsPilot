"""stream-processor: runs ingest jobs that write a dataset partition, then registers it with metadata."""

from __future__ import annotations

import logging
import random
import threading
import time
from collections.abc import Iterator
from time import perf_counter
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from opentelemetry import metrics
from opentelemetry.metrics import CallbackOptions, Observation
from pydantic import BaseModel, Field

from sim import features
from sim.app import Service
from sim.chaos import FaultType, affected_datasets, faults
from sim.config import ServiceConfig
from sim.db import CATEGORIES, DATASETS, PlatformDB, make_rows

log = logging.getLogger(__name__)

BATCH_SIZE = 50
BATCH_PROCESSING_S = 0.02
MALFORMED_VALUES = ["N/A", "", "null", "#VALUE!", "1,024.5"]

INSERT_RECORDS = (
    "INSERT INTO records (dataset, partition_key, ts, category, value) "
    "VALUES (:dataset, :partition_key, :ts, :category, :value)"
)
# Next release: bulk insert written against the new schema draft, which renames "value" to
# "val". The current table has no such column.
INSERT_RECORDS_BULK = (
    "INSERT INTO records (dataset, partition_key, ts, category, val) "
    "VALUES (:dataset, :partition_key, :ts, :category, :value)"
)


class RecordValidationError(Exception):
    def __init__(self, dataset: str, partition_key: str, offset: int, column: str, value: Any) -> None:
        super().__init__(f"record {offset}: column '{column}' rejected value {value!r}")
        self.dataset = dataset
        self.partition_key = partition_key
        self.offset = offset
        self.column = column
        self.value = value


def validate_records(rows: list[dict[str, Any]], dataset: str, partition_key: str, first_offset: int) -> None:
    for i, row in enumerate(rows):
        if not isinstance(row["value"], int | float):
            raise RecordValidationError(dataset, partition_key, first_offset + i, "value", row["value"])
        if row["category"] not in CATEGORIES:
            raise RecordValidationError(dataset, partition_key, first_offset + i, "category", row["category"])


class JobIn(BaseModel):
    job_id: str
    dataset: str
    partition_key: str
    rows: int = Field(default=200, ge=1, le=10_000)


class StreamProcessor(Service):
    def __init__(self, config: ServiceConfig) -> None:
        super().__init__(config)
        self.db = PlatformDB(config.name, pool_size=config.options["db_pool_size"])
        self.metadata = self.dependency("metadata")
        self.retained = config.options.get("partitions_retained", 24)
        self._backlog = 0
        self._backlog_lock = threading.Lock()
        meter = metrics.get_meter("sim.stream")
        self._processed = meter.create_counter(
            "stream.records.processed", description="Records written by ingest jobs"
        )
        meter.create_observable_gauge(
            "stream.consumer.lag",
            callbacks=[self._observe_lag],
            description="Records accepted but not yet written",
        )

    def _observe_lag(self, _: CallbackOptions) -> Iterator[Observation]:
        yield Observation(self._backlog)

    def _add_backlog(self, n: int) -> None:
        with self._backlog_lock:
            self._backlog += n

    def setup(self, app: FastAPI) -> None:
        app.post("/jobs/run")(self.run_job)

        @app.exception_handler(RecordValidationError)
        async def on_invalid_record(request: Request, exc: RecordValidationError) -> JSONResponse:
            log.error(
                "Schema validation failed for dataset=%s partition=%s: column '%s' expected %s, "
                "got %r at record %d",
                exc.dataset,
                exc.partition_key,
                exc.column,
                "DOUBLE" if exc.column == "value" else "CATEGORY",
                exc.value,
                exc.offset,
                extra={
                    "dataset": exc.dataset,
                    "partition_key": exc.partition_key,
                    "record_offset": exc.offset,
                },
            )
            return JSONResponse({"error": "job failed: schema validation"}, status_code=500)

    def _read_source(self, job: JobIn, rng: random.Random) -> list[list[dict[str, Any]]]:
        """Read the job's input from the upstream source, in batches."""
        batches = []
        remaining = job.rows
        while remaining > 0:
            n = min(BATCH_SIZE, remaining)
            batches.append(make_rows(job.dataset, job.partition_key, n, rng))
            remaining -= n
        spec = faults.get(FaultType.BAD_INPUT_JOB_FAILURE)
        if spec is not None and job.dataset in affected_datasets(spec, list(DATASETS)):
            batch = batches[rng.randrange(len(batches))]
            batch[rng.randrange(len(batch))]["value"] = rng.choice(MALFORMED_VALUES)
        return batches

    def run_job(self, job: JobIn) -> dict[str, Any]:
        start = perf_counter()
        attrs = {"job_id": job.job_id, "dataset": job.dataset, "partition_key": job.partition_key}
        log.info("Starting ingest job %s for %s/%s", job.job_id, job.dataset, job.partition_key, extra=attrs)

        resp = self.metadata.request("GET", "/datasets/{name}", f"/datasets/{job.dataset}")
        if resp.status_code == 404:
            raise HTTPException(404, f"unknown dataset {job.dataset}")

        rng = random.Random(job.job_id)
        batches = self._read_source(job, rng)
        insert = INSERT_RECORDS_BULK if features.enabled("bulk_writer") else INSERT_RECORDS
        remaining = job.rows
        self._add_backlog(remaining)
        try:
            for rows in batches:
                validate_records(rows, job.dataset, job.partition_key, job.rows - remaining)
                with self.db.connection() as conn:
                    self.db.execute(conn, "INSERT records", insert, rows)
                    conn.commit()
                self._processed.add(len(rows), {"dataset": job.dataset})
                self._add_backlog(-len(rows))
                remaining -= len(rows)
                time.sleep(BATCH_PROCESSING_S)
        finally:
            self._add_backlog(-remaining)

        self.metadata.request(
            "POST",
            "/datasets/{name}/partitions",
            f"/datasets/{job.dataset}/partitions",
            json={"partition_key": job.partition_key, "row_count": job.rows},
        )
        dropped = self._apply_retention(job.dataset)

        elapsed_ms = (perf_counter() - start) * 1000
        log.info(
            "Finished ingest job %s: %d rows in %.0fms",
            job.job_id,
            job.rows,
            elapsed_ms,
            extra={
                **attrs,
                "rows": job.rows,
                "duration_ms": round(elapsed_ms),
                "partitions_dropped": dropped,
            },
        )
        return {"job_id": job.job_id, "rows": job.rows, "partitions_dropped": dropped}

    def _apply_retention(self, dataset: str) -> int:
        """Drop partitions older than the newest `retained`. Returns the number dropped."""
        with self.db.connection() as conn:
            old = self.db.execute(
                conn,
                "SELECT partitions",
                "SELECT partition_key FROM partitions WHERE dataset = :dataset "
                "ORDER BY partition_key DESC LIMIT -1 OFFSET :keep",
                {"dataset": dataset, "keep": self.retained},
            )
            if not old:
                return 0
            cutoff = old[0]["partition_key"]
            params = {"dataset": dataset, "cutoff": cutoff}
            self.db.execute(
                conn,
                "DELETE records",
                "DELETE FROM records WHERE dataset = :dataset AND partition_key <= :cutoff",
                params,
            )
            self.db.execute(
                conn,
                "DELETE partitions",
                "DELETE FROM partitions WHERE dataset = :dataset AND partition_key <= :cutoff",
                params,
            )
            conn.commit()
        return len(old)
