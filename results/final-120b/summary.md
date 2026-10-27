| System | Top-1 root cause | Top-3 | Right service | No-fault correctly cleared | Evidence relevance | Citations valid | Median latency | p95 latency | Mean tokens | Tool calls / run |
|---|---|---|---|---|---|---|---|---|---|---|
| agent | 86% (18/21) | 86% (18/21) | 95% (20/21) | 67% (2/3) | 82% | 100% (115/115) | 9.53s | 93.45s | 4980 | 1.2 |

Top-1 by fault type (no-fault rows: correctly cleared):

| Fault type | agent |
|---|---|
| bad_deploy_errors | 100% (4/4) |
| bad_input_job_failure | 100% (3/3) |
| db_pool_exhaustion | 100% (5/5) |
| dependency_latency | 40% (2/5) |
| none | 67% (2/3) |
| upstream_timeout_misconfig | 100% (4/4) |
