# Liven load benchmark

- utc: `2026-10-23T12:20:08Z`
- commit: `4ac5704` (dirty)
- rustc: `rustc 1.92.0 (ded5c06cf 2025-12-08)`
- profile: `release`
- cpu: `Apple M2` (8 cores)
- ram_mb: 8192
- os: `Darwin 25.1.0 arm64`
- filesystem: `/`
- storage: `unverified`
- args: `Args { records: 100000, reads: 100000, value_size: 128, key_len: 16, mode: Embedded, threads: [1, 4, 8, 16], clients: [1, 4, 8, 16], batch: 500, read_pattern: Uniform, warm: Hot, seed: 42, mixed: false, segment_mb: 256, max_fds: 64, max_index_ram_mb: 1024, data_dir: None, skip_insert: false, keep_data: false, tag: "embedded-hot-onepread" }`

## Summary

| phase | mode | threads | clients | ops/s | p50 | p99 | p99.9 | max | mismatches |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| insert | - | - | - | 56407 | 8.97ms | 11.19ms | 14.99ms | 14.99ms | - |
| recover | - | - | - | - | - | - | - | - | - |
| read | embedded | 1 | 0 | 346797 | 2.29µs | 4.00µs | 11.09µs | 55.17µs | 0 |
| read | embedded | 4 | 0 | 1186819 | 2.83µs | 8.96µs | 14.13µs | 99.78µs | 0 |
| read | embedded | 8 | 0 | 855476 | 5.92µs | 53.31µs | 101.63µs | 221.18µs | 0 |
| read | embedded | 16 | 0 | 949159 | 6.58µs | 169.47µs | 360.96µs | 12.14ms | 0 |

## Insert

- records: 100000, batch: 500, elapsed: 1.773s
- ops/s: 56407, logical MB/s: 7.22, disk MB/s: 10.66
- batch p50/p99/p99.9/max: 8.97ms / 11.19ms / 14.99ms / 14.99ms
- segments: 1, bytes: 18900000
- rss: 40648704 bytes, index_ram_bytes: 8600000, skipmap_len: 100000

## Recover

- rebuild: 0.280s
- rss before/after: 40697856 / 40779776 bytes
- segments: 1, bytes: 18900000, skipmap_len: 100000

## Notes

- insert uses StorageEngine::append_batch directly in both modes; TCP measures the read-path overhead on top.
- background compaction disabled (thresholds: segments=18446744073709551615, bytes=18446744073709551615); segment_mb=256, max_fds=64, max_index_ram_mb=1024
- key_len=16 value_size=128 batch=500 seed=42
- hot: one untimed warmup pass over all keys before the read sweep
- syscalls: unverified (dtruss requires root; no strace on macOS)
- flamegraph: see bench-results/flamegraph-* (generated separately with cargo flamegraph/sample)
