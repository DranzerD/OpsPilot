| System | Top-1 root cause | Top-3 | Right service | No-fault correctly cleared | Evidence relevance | Citations valid | Median latency | p95 latency | Mean tokens | Tool calls / run |
|---|---|---|---|---|---|---|---|---|---|---|
| rules | 100% (21/21) | 100% (21/21) | 100% (21/21) | 100% (3/3) | 96% | 100% (101/101) | 0.1s | 0.12s | 0 | 0 |
| agent | 86% (18/21) | 86% (18/21) | 86% (18/21) | 100% (3/3) | 81% | 100% (92/92) | 13.65s | 29.43s | 2547 | 0.4 |
| agent-notools | 76% (16/21) | 81% (17/21) | 95% (20/21) | 100% (3/3) | 73% | 100% (118/118) | 9.54s | 14.83s | 1541 | 0 |

Top-1 by fault type (no-fault rows: correctly cleared):

| Fault type | rules | agent | agent-notools |
|---|---|---|---|
| bad_deploy_errors | 100% (4/4) | 100% (4/4) | 100% (4/4) |
| bad_input_job_failure | 100% (3/3) | 100% (3/3) | 100% (3/3) |
| db_pool_exhaustion | 100% (5/5) | 100% (5/5) | 100% (5/5) |
| dependency_latency | 100% (5/5) | 40% (2/5) | 0% (0/5) |
| none | 100% (3/3) | 100% (3/3) | 100% (3/3) |
| upstream_timeout_misconfig | 100% (4/4) | 100% (4/4) | 100% (4/4) |
