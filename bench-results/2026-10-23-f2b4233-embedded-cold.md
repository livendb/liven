# Liven load benchmark

- utc: `2026-10-23T11:58:05Z`
- commit: `f2b4233` (dirty)
- rustc: `rustc 1.92.0 (ded5c06cf 2025-12-08)`
- profile: `release`
- cpu: `Apple M2` (8 cores)
- ram_mb: 8192
- os: `Darwin 25.1.0 arm64`
- filesystem: `/`
- storage: `unverified`
- args: `Args { records: 100000, reads: 100000, value_size: 128, key_len: 16, mode: Embedded, threads: [1, 4, 8, 16], clients: [1, 4, 8, 16], batch: 500, read_pattern: Uniform, warm: Cold, seed: 42, mixed: false, segment_mb: 256, max_fds: 64, max_index_ram_mb: 1024, data_dir: None, skip_insert: false, keep_data: false, tag: "embedded-cold" }`

## Summary

| phase | mode | threads | clients | ops/s | p50 | p99 | p99.9 | max | mismatches |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| insert | - | - | - | 52255 | 9.04ms | 17.84ms | 21.36ms | 21.36ms | - |
| recover | - | - | - | - | - | - | - | - | - |
| read | embedded | 1 | 0 | 257419 | 3.33µs | 5.29µs | 12.59µs | 296.70µs | 0 |
| read | embedded | 4 | 0 | 592977 | 6.38µs | 10.84µs | 15.84µs | 232.32µs | 0 |
| read | embedded | 8 | 0 | 398826 | 17.01µs | 111.61µs | 377.09µs | 1.24ms | 0 |
| read | embedded | 16 | 0 | 399612 | 18.46µs | 713.73µs | 2.33ms | 16.29ms | 0 |

## Insert

- records: 100000, batch: 500, elapsed: 1.914s
- ops/s: 52255, logical MB/s: 6.69, disk MB/s: 9.88
- batch p50/p99/p99.9/max: 9.04ms / 17.84ms / 21.36ms / 21.36ms
- segments: 1, bytes: 18900000
- rss: 40599552 bytes, index_ram_bytes: 8600000, skipmap_len: 100000

## Recover

- rebuild: 0.281s
- rss before/after: 40648704 / 40730624 bytes
- segments: 1, bytes: 18900000, skipmap_len: 100000

## Notes

- insert uses StorageEngine::append_batch directly in both modes; TCP measures the read-path overhead on top.
- background compaction disabled (thresholds: segments=18446744073709551615, bytes=18446744073709551615); segment_mb=256, max_fds=64, max_index_ram_mb=1024
- key_len=16 value_size=128 batch=500 seed=42
- cold: cache-drop attempted before each thread/client run
- syscalls: unverified (dtruss requires root; no strace on macOS)
- flamegraph: see bench-results/flamegraph-* (generated separately with cargo flamegraph/sample)
- cold: SKIPPED (purge failed; typically requires root)
