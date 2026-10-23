# Load benchmark change log

Baseline: `f2b4233` (harness), 100k inserts / 100k point reads, 128-byte values, 16-byte keys, uniform reads, seed 42, 256 MB segments, max_index_ram_mb=1024, max_fds=64, batch=500.

| change | before ops/s | after ops/s | p99 before → after | commit |
|---|---:|---:|---|---|
| baseline embedded, 4 threads | - | 596,332 | - → 13.09 µs | f2b4233 |
| baseline embedded, 1 thread | - | 281,294 | - → 5.04 µs | f2b4233 |
| baseline TCP, 16 clients | - | 117,401 | - → 233.60 µs | f2b4233 |
| baseline TCP, 8 clients | - | 107,193 | - → 134.40 µs | f2b4233 |
