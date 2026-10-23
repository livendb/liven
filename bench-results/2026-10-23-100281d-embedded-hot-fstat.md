# Liven load benchmark

- utc: `2026-10-23T12:09:46Z`
- commit: `100281d` (dirty)
- rustc: `rustc 1.92.0 (ded5c06cf 2025-12-08)`
- profile: `release`
- cpu: `Apple M2` (8 cores)
- ram_mb: 8192
- os: `Darwin 25.1.0 arm64`
- filesystem: `/`
- storage: `unverified`
- args: `Args { records: 100000, reads: 100000, value_size: 128, key_len: 16, mode: Embedded, threads: [1, 4, 8, 16], clients: [1, 4, 8, 16], batch: 500, read_pattern: Uniform, warm: Hot, seed: 42, mixed: false, segment_mb: 256, max_fds: 64, max_index_ram_mb: 1024, data_dir: None, skip_insert: false, keep_data: false, tag: "embedded-hot-fstat" }`

## Summary

| phase | mode | threads | clients | ops/s | p50 | p99 | p99.9 | max | mismatches |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| insert | - | - | - | 56211 | 8.95ms | 15.62ms | 22.89ms | 22.89ms | - |
| recover | - | - | - | - | - | - | - | - | - |
| read | embedded | 1 | 0 | 351084 | 2.54µs | 3.21µs | 15.17µs | 30.51µs | 0 |
| read | embedded | 4 | 0 | 621075 | 5.04µs | 19.17µs | 36.19µs | 265.21µs | 0 |
| read | embedded | 8 | 0 | 545230 | 12.09µs | 78.97µs | 210.18µs | 706.05µs | 0 |
| read | embedded | 16 | 0 | 549293 | 12.29µs | 531.97µs | 1.10ms | 16.46ms | 0 |

## Insert

- records: 100000, batch: 500, elapsed: 1.779s
- ops/s: 56211, logical MB/s: 7.20, disk MB/s: 10.62
- batch p50/p99/p99.9/max: 8.95ms / 15.62ms / 22.89ms / 22.89ms
- segments: 1, bytes: 18900000
- rss: 40648704 bytes, index_ram_bytes: 8600000, skipmap_len: 100000

## Recover

- rebuild: 0.284s
- rss before/after: 40697856 / 40845312 bytes
- segments: 1, bytes: 18900000, skipmap_len: 100000

## Notes

- insert uses StorageEngine::append_batch directly in both modes; TCP measures the read-path overhead on top.
- background compaction disabled (thresholds: segments=18446744073709551615, bytes=18446744073709551615); segment_mb=256, max_fds=64, max_index_ram_mb=1024
- key_len=16 value_size=128 batch=500 seed=42
- hot: one untimed warmup pass over all keys before the read sweep
- syscalls: unverified (dtruss requires root; no strace on macOS)
- flamegraph: see bench-results/flamegraph-* (generated separately with cargo flamegraph/sample)
