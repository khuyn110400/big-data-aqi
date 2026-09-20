# Phase 2 scalability benchmark

| Rows | 1 executor | 2 executors | 4 executors |
|---:|---:|---:|---:|
| 100,000 | 16.387s | 11.839s | 15.970s |
| 1,000,000 | 99.003s | 51.241s | 48.428s |
| 8,000,000 | 749.305s | 385.217s | 308.791s |

| Rows | Speedup 2 executors | Speedup 4 executors |
|---:|---:|---:|
| 100,000 | 1.384x | 1.026x |
| 1,000,000 | 1.932x | 2.044x |
| 8,000,000 | 1.945x | 2.427x |
