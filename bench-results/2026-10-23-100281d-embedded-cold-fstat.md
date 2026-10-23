# Liven load benchmark

- utc: `2026-10-23T12:10:07Z`
- commit: `100281d` (dirty)
- rustc: `rustc 1.92.0 (ded5c06cf 2025-12-08)`
- profile: `release`
- cpu: `Apple M2` (8 cores)
- ram_mb: 8192
- os: `Darwin 25.1.0 arm64`
- filesystem: `/`
- storage: `unverified`
- args: `Args { records: 100000, reads: 100000, value_size: 128, key_len: 16, mode: Embedded, threads: [1, 4, 8, 16], clients: [1, 4, 8, 16], batch: 500, read_pattern: Uniform, warm: Cold, seed: 42, mixed: false, segment_mb: 256, max_fds: 64, max_index_ram_mb: 1024, data_dir: None, skip_insert: false, keep_data: false, tag: "embedded-cold-fstat" }`

## Summary

| phase | mode | threads | clients | ops/s | p50 | p99 | p99.9 | max | mismatches |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| insert | - | - | - | 56410 | 8.97ms | 10.97ms | 13.05ms | 13.05ms | - |
| recover | - | - | - | - | - | - | - | - | - |
| read | embedded | 1 | 0 | 352961 | 2.54µs | 3.17µs | 3.96µs | 52.80µs | 0 |
| read | embedded | 4 | 0 | 620747 | 5.04µs | 19.14µs | 36.03µs | 198.40µs | 0 |
| read | embedded | 8 | 0 | 504696 | 13.09µs | 83.90µs | 304.64µs | 984.06µs | 0 |
| read | embedded | 16 | 0 | 505007 | 12.96µs | 618.50µs | 1.76ms | 20.15ms | 0 |

## Insert

- records: 100000, batch: 500, elapsed: 1.773s
- ops/s: 56410, logical MB/s: 7.22, disk MB/s: 10.66
- batch p50/p99/p99.9/max: 8.97ms / 10.97ms / 13.05ms / 13.05ms
- segments: 1, bytes: 18900000
- rss: 40648704 bytes, index_ram_bytes: 8600000, skipmap_len: 100000

## Recover

- rebuild: 0.278s
- rss before/after: 40697856 / 40828928 bytes
- segments: 1, bytes: 18900000, skipmap_len: 100000

## Notes

- insert uses StorageEngine::append_batch directly in both modes; TCP measures the read-path overhead on top.
- background compaction disabled (thresholds: segments=18446744073709551615, bytes=18446744073709551615); segment_mb=256, max_fds=64, max_index_ram_mb=1024
- key_len=16 value_size=128 batch=500 seed=42
- cold: cache-drop attempted before each thread/client run
- syscalls: unverified (dtruss requires root; no strace on macOS)
- flamegraph: see bench-results/flamegraph-* (generated separately with cargo flamegraph/sample)
- cold: SKIPPED (purge failed; typically requires root)
