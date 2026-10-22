"""Named telemetry signals and the Prometheus series behind them.

One registry shared by the bundle store, the live store (M3), scenario checks and the agent's
`query_metric` tool, so a signal name means the same series everywhere.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Signal:
    name: str
    series: str  # recording rule or raw metric name
    unit: str  # "s", "ratio", "count", "rps"
    # Smallest absolute change that counts as a real shift (used by scenario checks, anomaly
    # detection later). Guards ratio-of-tiny-numbers false positives.
    min_delta: float
    # Label that identifies the owning service. Job series are keyed by job_name and owned by
    # the scheduler.
    service_label: str = "service_name"
    owner: str | None = None
    description: str = ""


SIGNALS: dict[str, Signal] = {
    s.name: s
    for s in [
        Signal(
            "request_rate", "service:request_rate:1m", "rps", 1.0, description="Inbound requests per second"
        ),
        Signal("error_ratio", "service:error_ratio:1m", "ratio", 0.01, description="Share of inbound 5xx"),
        Signal("latency_p95", "service:latency_p95:1m", "s", 0.05, description="p95 inbound request latency"),
        Signal("availability", "service:availability:5m", "ratio", 0.01, description="1 - 5xx share over 5m"),
        Signal(
            "dependency_latency_p95",
            "service:dependency_latency_p95:1m",
            "s",
            0.05,
            description="p95 latency of outbound calls, by peer_service",
        ),
        Signal(
            "dependency_error_ratio",
            "service:dependency_error_ratio:1m",
            "ratio",
            0.01,
            description="Share of failed outbound calls (timeouts, connection errors, 5xx), by peer_service",
        ),
        Signal(
            "db_pool_wait_p95",
            "service:db_pool_wait_p95:1m",
            "s",
            0.05,
            description="p95 wait for a pool connection",
        ),
        Signal(
            "db_pool_utilization",
            "service:db_pool_utilization",
            "ratio",
            0.2,
            description="Pool connections in use",
        ),
        Signal(
            "db_pool_pending",
            "db_client_connection_pending_requests",
            "count",
            1.0,
            description="Requests waiting for a pool connection",
        ),
        Signal(
            "db_operation_p95",
            "service:db_operation_p95:1m",
            "s",
            0.05,
            description="p95 database operation latency, by db_operation_name",
        ),
        Signal(
            "cache_hit_ratio",
            "service:cache_hit_ratio:1m",
            "ratio",
            0.1,
            description="Catalog cache hit ratio",
        ),
        Signal(
            "consumer_lag",
            "stream_consumer_lag",
            "count",
            500.0,
            description="Records accepted but not written",
        ),
        Signal(
            "job_duration_p95",
            "job:duration_p95:5m",
            "s",
            0.5,
            service_label="job_name",
            owner="scheduler",
            description="p95 job/pipeline duration, by job_name",
        ),
        Signal(
            "job_failure_ratio",
            "job:failure_ratio:5m",
            "ratio",
            0.05,
            service_label="job_name",
            owner="scheduler",
            description="Share of job runs not succeeding, by job_name",
        ),
        Signal(
            "job_retries",
            "job:retries:5m",
            "count",
            1.0,
            service_label="job_name",
            owner="scheduler",
            description="Job retries over 5m, by job_name",
        ),
    ]
}
