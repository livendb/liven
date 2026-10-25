# Liven write-path benchmark (final tree)

Hardware/environment: Apple M2 (8 cores), 8 GB RAM, macOS Darwin 25.1.0 arm64,
rustc 1.92.0, release profile. Workload: one stream, unique 8-byte keys,
128-byte values, single-record appends, 200 appends per writer, fresh temp dir,
background compaction disabled, `max_index_ram_mb=1024`, `max_fds=64`.
`executor-hold` reproduces the pre-fix lock behavior; `executor-narrow` is the
current code (write_lock released before the ack wait).

| mode | writers | ops/s | p50 µs | p99 µs | fsync/s | avg batch |
|---|---:|---:|---:|---:|---:|---:|
| embedded | 1 | 256 | 3895.30 | 12599.30 | 256 | 1.00 |
| embedded | 8 | 982 | 8060.93 | 10182.66 | 123 | 8.00 |
| embedded | 32 | 3767 | 8749.06 | 10305.53 | 118 | 32.00 |
| embedded | 128 | 11599 | 8626.17 | 30638.08 | 91 | 128.00 |
| executor-hold | 1 | 264 | 3846.14 | 6537.22 | 264 | 1.00 |
| executor-hold | 8 | 285 | 3760.13 | 658505.73 | 285 | 1.00 |
| executor-hold | 32 | 266 | 3909.63 | 2443182.08 | 266 | 1.00 |
| executor-hold | 128 | 266 | 3817.47 | 10292822.02 | 266 | 1.00 |
| executor-narrow | 1 | 295 | 3686.40 | 5705.73 | 295 | 1.00 |
| executor-narrow | 8 | 1025 | 7946.24 | 10002.43 | 128 | 8.00 |
| executor-narrow | 32 | 4065 | 7946.24 | 11001.85 | 127 | 32.00 |
| executor-narrow | 128 | 14599 | 8740.86 | 15958.01 | 114 | 128.00 |
| tcp | 1 | 295 | 3737.60 | 5918.72 | 295 | 1.00 |
| tcp | 8 | 784 | 9035.77 | 19464.19 | 143 | 5.48 |
| tcp | 32 | 2422 | 14032.90 | 18726.91 | 133 | 18.18 |
| tcp | 128 | 10156 | 13967.36 | 18268.16 | 126 | 80.50 |

Notes:
- `executor-hold` is the old behavior: the global `write_lock` is held across
  the fsync ack wait. It forces a batch size of 1 and a lock convoy.
- `executor-narrow` releases the lock after validation and enqueue. At 128
  writers it reaches 14.6k ops/s versus 266 ops/s for the old path, with batch
  128 and p99 16.0 ms versus 10.3 s.
- The remaining ceiling is the disk: ~91–128 fsync/s. 128 records/fsync gives
  ~11.6–16.4k/s. `append_batch(500)` at ~120 fsync/s is ~60k/s. 100k/s needs
  ~800 records per fsync (or >800 in-flight single-record writes).
