# Liven load benchmark

- utc: `2026-10-23T12:59:42Z`
- commit: `29f21be` (dirty)
- rustc: `rustc 1.92.0 (ded5c06cf 2025-12-08)`
- profile: `release`
- cpu: `Apple M2` (8 cores)
- ram_mb: 8192
- os: `Darwin 25.1.0 arm64`
- filesystem: `/`
- storage: `unverified`
- args: `Args { records: 100000, reads: 100000, value_size: 128, key_len: 16, mode: Embedded, threads: [1, 4, 8, 16], clients: [1, 4, 8, 16], batch: 500, read_pattern: Uniform, warm: Hot, seed: 42, mixed: false, segment_mb: 256, max_fds: 64, max_index_ram_mb: 1024, data_dir: None, skip_insert: false, keep_data: false, tag: "opt3-embedded-hot" }`

## Summary

| phase | mode | threads | clients | ops/s | p50 | p99 | p99.9 | max | mismatches |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| insert | - | - | - | 52881 | 8.99ms | 25.07ms | 83.89ms | 83.89ms | - |
| recover | - | - | - | - | - | - | - | - | - |
| read | embedded | 1 | 0 | 689615 | 1.21µs | 1.79µs | 4.67µs | 52.51µs | 0 |
| read | embedded | 4 | 0 | 1495417 | 1.92µs | 11.29µs | 21.34µs | 87.30µs | 0 |
| read | embedded | 8 | 0 | 1003543 | 5.00µs | 45.34µs | 84.42µs | 223.62µs | 0 |
| read | embedded | 16 | 0 | 1030993 | 5.79µs | 156.80µs | 300.80µs | 3.71ms | 0 |

## Insert

- records: 100000, batch: 500, elapsed: 1.891s
- ops/s: 52881, logical MB/s: 6.77, disk MB/s: 9.99
- batch p50/p99/p99.9/max: 8.99ms / 25.07ms / 83.89ms / 83.89ms
- segments: 1, bytes: 18900000
- rss: 40878080 bytes, index_ram_bytes: 8600000, skipmap_len: 100000

## Recover

- rebuild: 0.165s
- rss before/after: 40927232 / 41041920 bytes
- segments: 1, bytes: 18900000, skipmap_len: 100000

## Notes

- insert uses StorageEngine::append_batch directly in both modes; TCP measures the read-path overhead on top.
- background compaction disabled (thresholds: segments=18446744073709551615, bytes=18446744073709551615); segment_mb=256, max_fds=64, max_index_ram_mb=1024
- key_len=16 value_size=128 batch=500 seed=42
- hot: one untimed warmup pass over all keys before the read sweep
- syscalls: unverified (dtruss requires root; no strace on macOS)
- flamegraph: see bench-results/flamegraph-* (generated separately with cargo flamegraph/sample)
