"""Release feature flags, set per deployed version through the FEATURE_FLAGS env var.

FEATURE_FLAGS is a JSON object of flag name -> rollout fraction, e.g. {"vectorized_aggregate": 0.5}.
Each service ships one new code path behind a flag. Those code paths contain real bugs, which is
how a bad deploy (F5) fails: through ordinary application code and its ordinary error handling.
"""

from __future__ import annotations

import json
import os
import random

_rng = random.Random()


def _load() -> dict[str, float]:
    raw = os.environ.get("FEATURE_FLAGS", "")
    return {k: float(v) for k, v in json.loads(raw).items()} if raw else {}


FLAGS = _load()

# The flag each service's next release ships (used when a scenario deploys a bad release).
RELEASE_FLAG = {
    "gateway": "response_enrichment",
    "query-engine": "vectorized_aggregate",
    "metadata": "partition_stats",
    "stream-processor": "bulk_writer",
}


def enabled(flag: str) -> bool:
    """Whether this request falls inside the flag's rollout fraction."""
    fraction = FLAGS.get(flag, 0.0)
    return fraction > 0 and _rng.random() < fraction
