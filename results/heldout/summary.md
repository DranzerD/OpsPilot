| System | Top-1 root cause | Top-3 | Right service | No-fault correctly cleared | Evidence relevance | Citations valid | Median latency | p95 latency | Mean tokens | Tool calls / run |
|---|---|---|---|---|---|---|---|---|---|---|
| rules | 90% (9/10) | 90% (9/10) | 90% (9/10) | 50% (1/2) | 87% | 100% (45/45) | 0.12s | 0.18s | 0 | 0 |
| agent | 70% (7/10) | 70% (7/10) | 80% (8/10) | 0% (0/2) | 76% | 100% (58/58) | 13.57s | 48.45s | 2755 | 0.5 |
| agent-notools | 70% (7/10) | 70% (7/10) | 80% (8/10) | 0% (0/2) | 70% | 100% (56/56) | 11.09s | 15.8s | 1511 | 0 |

Top-1 by fault type (no-fault rows: correctly cleared):

| Fault type | rules | agent | agent-notools |
|---|---|---|---|
| bad_deploy_errors | 100% (2/2) | 100% (2/2) | 100% (2/2) |
| bad_input_job_failure | 100% (1/1) | 100% (1/1) | 100% (1/1) |
| db_pool_exhaustion | 100% (2/2) | 100% (2/2) | 100% (2/2) |
| dependency_latency | 100% (3/3) | 33% (1/3) | 33% (1/3) |
| none | 50% (1/2) | 0% (0/2) | 0% (0/2) |
| upstream_timeout_misconfig | 50% (1/2) | 50% (1/2) | 50% (1/2) |
