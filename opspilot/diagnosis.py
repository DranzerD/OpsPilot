"""The output every diagnoser produces: the rules baseline, the agent, and its ablations."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

FAULT_TYPES = [
    "db_pool_exhaustion",
    "dependency_latency",
    "upstream_timeout_misconfig",
    "bad_input_job_failure",
    "bad_deploy_errors",
    "slow_query",
    "memory_leak",
    "consumer_backpressure",
    "retry_storm",
    "cache_disabled",
    "db_disk_full",
    "other",
]

FAULT_DESCRIPTIONS = {
    "db_pool_exhaustion": "the service's database connection pool is exhausted (e.g. leaked connections)",
    "dependency_latency": "the service itself became slow, slowing everything that calls it",
    "upstream_timeout_misconfig": "the service's timeout for calls to a dependency is misconfigured too low",
    "bad_input_job_failure": "batch jobs fail on malformed/invalid input data",
    "bad_deploy_errors": "a new deploy of the service introduced errors",
    "slow_query": "a database query became slow",
    "memory_leak": "the service's memory grows until it degrades",
    "consumer_backpressure": "a stream consumer can't keep up; lag grows",
    "retry_storm": "aggressive retries amplify load on a dependency",
    "cache_disabled": "a cache stopped working, increasing load and latency",
    "db_disk_full": "the database cannot write (disk full)",
    "other": "none of the above",
}


class Hypothesis(BaseModel):
    service: str
    fault_type: str
    confidence: float = Field(ge=0, le=1)
    summary: str = ""
    evidence: list[str] = []  # evidence ids


class Diagnosis(BaseModel):
    status: Literal["diagnosed", "no_incident", "insufficient_evidence"]
    hypotheses: list[Hypothesis] = []  # ranked, most likely first
    summary: str = ""


class RunStats(BaseModel):
    latency_s: float = 0.0
    llm_calls: int = 0
    tool_calls: int = 0
    tool_errors: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    cited: int = 0
    cited_valid: int = 0  # cited ids that exist in the investigation's evidence
    error: str | None = None
