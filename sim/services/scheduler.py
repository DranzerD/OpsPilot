"""scheduler: runs the "nightly" ingest pipeline on a compressed clock, retrying failed jobs."""

from __future__ import annotations

import logging
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from time import monotonic, perf_counter
from typing import Any

from fastapi import FastAPI, HTTPException
from opentelemetry import context, metrics, trace
from opentelemetry.trace import Status, StatusCode

from sim.app import Service
from sim.config import ServiceConfig, load_topology
from sim.db import partition_key
from sim.deps import DependencyError
from sim.telemetry import JOB_BUCKETS_S

log = logging.getLogger(__name__)
_tracer = trace.get_tracer("sim.scheduler")

PIPELINE = "nightly_pipeline"
INGEST_JOB = "ingest"


class Scheduler(Service):
    def __init__(self, config: ServiceConfig) -> None:
        super().__init__(config)
        workload = load_topology().workload
        self.every_s = workload.get("batch_every_s", 120)
        self.first_after_s = workload.get("first_batch_after_s", 15)
        self.rows_per_job = workload.get("rows_per_job", 200)
        self.max_retries = config.options.get("max_retries", 3)
        self.concurrency = config.options.get("job_concurrency", 4)
        self.stream = self.dependency("stream-processor")
        self.metadata = self.dependency("metadata")

        self._stop = threading.Event()
        self._running = threading.Lock()
        self._runs: deque[dict[str, Any]] = deque(maxlen=20)
        self._thread = threading.Thread(target=self._loop, name="scheduler-loop", daemon=True)

        meter = metrics.get_meter("sim.scheduler")
        self._duration = meter.create_histogram(
            "job.duration",
            unit="s",
            description="Duration of pipeline and job runs",
            explicit_bucket_boundaries_advisory=JOB_BUCKETS_S,
        )
        self._runs_counter = meter.create_counter(
            "job.runs", description="Completed pipeline and job runs by outcome"
        )
        self._retries = meter.create_counter("job.retries", description="Job retry attempts")

    def setup(self, app: FastAPI) -> None:
        app.post("/pipelines/nightly/trigger", status_code=202)(self.trigger)
        app.get("/pipelines/runs")(self.list_runs)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        super().stop()

    def trigger(self) -> dict[str, str]:
        if self._running.locked():
            raise HTTPException(409, "pipeline already running")
        run_id = _run_id()
        threading.Thread(target=self.run_pipeline, args=(run_id, "manual"), daemon=True).start()
        return {"run_id": run_id}

    def list_runs(self) -> list[dict[str, Any]]:
        return list(reversed(self._runs))

    def _loop(self) -> None:
        if self._stop.wait(self.first_after_s):
            return
        while True:
            started = monotonic()
            self.run_pipeline(_run_id(), "schedule")
            if self._stop.wait(max(self.every_s - (monotonic() - started), 1)):
                return

    def run_pipeline(self, run_id: str, trigger: str) -> None:
        if not self._running.acquire(blocking=False):
            log.warning(
                "Skipping pipeline run %s: previous run still in progress", run_id, extra={"run_id": run_id}
            )
            return
        try:
            self._run_pipeline(run_id, trigger)
        finally:
            self._running.release()

    def _run_pipeline(self, run_id: str, trigger: str) -> None:
        start = perf_counter()
        run: dict[str, Any] = {
            "run_id": run_id,
            "trigger": trigger,
            "started_at": _now(),
            "outcome": "running",
        }
        self._runs.append(run)
        with _tracer.start_as_current_span(f"pipeline {PIPELINE}") as span:
            span.set_attributes({"job.name": PIPELINE, "job.run_id": run_id, "job.trigger": trigger})
            log.info("Starting pipeline run %s (%s)", run_id, trigger, extra={"run_id": run_id})
            try:
                datasets = [d["name"] for d in self.metadata.request("GET", "/datasets").json()]
            except DependencyError as exc:
                log.error(
                    "Pipeline run %s could not list datasets: %s", run_id, exc, extra={"run_id": run_id}
                )
                span.set_status(Status(StatusCode.ERROR, "dataset listing failed"))
                self._finish(run, start, "failed", 0, 0)
                return

            key = partition_key(datetime.now(UTC))
            ctx = context.get_current()
            with ThreadPoolExecutor(self.concurrency, thread_name_prefix="job") as pool:
                results = list(pool.map(lambda ds: self._run_job_in(ctx, run_id, ds, key), datasets))

            ok = sum(results)
            outcome = "success" if ok == len(datasets) else ("failed" if ok == 0 else "partial_failure")
            if outcome != "success":
                span.set_status(Status(StatusCode.ERROR, f"{len(datasets) - ok} jobs failed"))
            self._finish(run, start, outcome, ok, len(datasets))

    def _finish(self, run: dict[str, Any], start: float, outcome: str, ok: int, total: int) -> None:
        elapsed = perf_counter() - start
        self._duration.record(elapsed, {"job.name": PIPELINE, "outcome": outcome})
        self._runs_counter.add(1, {"job.name": PIPELINE, "outcome": outcome})
        run.update(outcome=outcome, jobs_succeeded=ok, jobs_total=total, duration_s=round(elapsed, 2))
        level = logging.INFO if outcome == "success" else logging.ERROR
        log.log(
            level,
            "Pipeline run %s finished in %.1fs: %d/%d jobs succeeded",
            run["run_id"],
            elapsed,
            ok,
            total,
            extra={"run_id": run["run_id"], "outcome": outcome, "duration_ms": round(elapsed * 1000)},
        )

    def _run_job_in(self, ctx: context.Context, run_id: str, dataset: str, key: str) -> bool:
        token = context.attach(ctx)
        try:
            return self._run_job(run_id, dataset, key)
        finally:
            context.detach(token)

    def _run_job(self, run_id: str, dataset: str, key: str) -> bool:
        job_id = f"{run_id}-{dataset}"
        attrs = {"job.name": INGEST_JOB, "dataset": dataset}
        start = perf_counter()
        with _tracer.start_as_current_span(f"job {INGEST_JOB}") as span:
            span.set_attributes({**attrs, "job.id": job_id})
            for attempt in range(1, self.max_retries + 2):
                try:
                    self.stream.request(
                        "POST",
                        "/jobs/run",
                        json={
                            "job_id": job_id,
                            "dataset": dataset,
                            "partition_key": key,
                            "rows": self.rows_per_job,
                        },
                    )
                    self._record_job(start, attrs, "success")
                    return True
                except DependencyError as exc:
                    if attempt > self.max_retries:
                        log.error(
                            "Job %s for dataset=%s failed after %d attempts: %s",
                            job_id,
                            dataset,
                            attempt,
                            exc.reason,
                            extra={"job_id": job_id, "dataset": dataset, "attempts": attempt},
                        )
                        span.set_status(Status(StatusCode.ERROR, exc.reason))
                        self._record_job(start, attrs, "failed")
                        return False
                    backoff = 2 ** (attempt - 1)
                    self._retries.add(1, attrs)
                    log.warning(
                        "Job %s for dataset=%s failed (%s), retrying in %ds (attempt %d/%d)",
                        job_id,
                        dataset,
                        exc.reason,
                        backoff,
                        attempt,
                        self.max_retries + 1,
                        extra={"job_id": job_id, "dataset": dataset, "attempt": attempt},
                    )
                    if self._stop.wait(backoff):
                        return False
        return False

    def _record_job(self, start: float, attrs: dict[str, str], outcome: str) -> None:
        self._duration.record(perf_counter() - start, {**attrs, "outcome": outcome})
        self._runs_counter.add(1, {**attrs, "outcome": outcome})


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _run_id() -> str:
    return "nightly-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
