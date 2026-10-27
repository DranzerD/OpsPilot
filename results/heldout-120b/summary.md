| System | Top-1 root cause | Top-3 | Right service | No-fault correctly cleared | Evidence relevance | Citations valid | Median latency | p95 latency | Mean tokens | Tool calls / run |
|---|---|---|---|---|---|---|---|---|---|---|
| agent | 70% (7/10) | 80% (8/10) | 90% (9/10) | 0% (0/2) | 71% | 100% (61/61) | 16.09s | 124.84s | 6024 | 1.9 |

Top-1 by fault type (no-fault rows: correctly cleared):

| Fault type | agent |
|---|---|
| bad_deploy_errors | 100% (2/2) |
| bad_input_job_failure | 100% (1/1) |
| db_pool_exhaustion | 100% (2/2) |
| dependency_latency | 33% (1/3) |
| none | 0% (0/2) |
| upstream_timeout_misconfig | 50% (1/2) |
