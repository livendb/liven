# Load benchmark change log

Baseline harness: `f2b4233`. Baseline results: `7620dd1`.
Workload: 100k inserts / 100k point reads, 128-byte values, 16-byte keys, uniform reads, seed 42, 256 MB segments, `max_index_ram_mb=1024`, `max_fds=64`, `batch=500`.
All runs are `--release`; each run uses a fresh temp data dir and disables background compaction.

| change                                           |            before ops/s |             after ops/s | p99 before → after    | commit    | decision                                                     |
| ------------------------------------------------ | ----------------------: | ----------------------: | --------------------- | --------- | ------------------------------------------------------------ |
| remove per-lookup `fstat`                        |   442,650 (embedded 1t) |   533,326 (embedded 1t) | 2.75 µs → 2.62 µs     | `100281d` | keep                                                         |
| remove per-lookup `fstat` (TCP 1 client)         |                  37,144 |                  38,828 | 52.26 µs → 55.30 µs   | `100281d` | keep (throughput within noise; no regression in 1t embedded) |
| single `pread` for small frames                  | 1,214,325 (embedded 4t) | 1,765,561 (embedded 4t) | 6.25 µs → 5.58 µs     | `4ac5704` | keep                                                         |
| single `pread` for small frames (TCP 16 clients) |                 161,617 |                 178,993 | 234.75 µs → 228.48 µs | `4ac5704` | keep                                                         |
| direct compound-key copy                         |   680,379 (embedded 1t) |   689,615 (embedded 1t) | 1.83 µs → 1.79 µs     | `29f21be` | revert (within noise)                                        |
| direct compound-key copy                         | 1,501,334 (embedded 4t) | 1,544,856 (embedded 4t) | 10.75 µs → 10.75 µs   | `29f21be` | revert (within noise)                                        |

Notes:

- The 4-thread baseline-vs-`fstat` A/B was externally noisy (throughput collapsed mid-run). The 1-thread paired A/B is the evidence used for the `fstat` row.
- Absolute throughput drifted between sessions on this 8 GB M2 laptop; only paired/interleaved medians should be compared.
- `opt3` was reverted in `97e5ca8`.
