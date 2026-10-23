# Liven load benchmark

- utc: `2026-10-23T13:00:12Z`
- commit: `29f21be` (dirty)
- rustc: `rustc 1.92.0 (ded5c06cf 2025-12-08)`
- profile: `release`
- cpu: `Apple M2` (8 cores)
- ram_mb: 8192
- os: `Darwin 25.1.0 arm64`
- filesystem: `/`
- storage: `unverified`
- args: `Args { records: 100000, reads: 100000, value_size: 128, key_len: 16, mode: Embedded, threads: [4], clients: [4], batch: 500, read_pattern: Uniform, warm: Hot, seed: 42, mixed: false, segment_mb: 256, max_fds: 64, max_index_ram_mb: 1024, data_dir: None, skip_insert: false, keep_data: false, tag: "ab-opt3-embedded" }`

## Summary

| phase | mode | threads | clients | ops/s | p50 | p99 | p99.9 | max | mismatches |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| insert | - | - | - | 55346 | 8.99ms | 10.87ms | 21.97ms | 21.97ms | - |
| recover | - | - | - | - | - | - | - | - | - |
| read | embedded | 4 | 0 | 1544856 | 1.92µs | 10.75µs | 20.34µs | 109.76µs | 0 |

## Insert

- records: 100000, batch: 500, elapsed: 1.807s
- ops/s: 55346, logical MB/s: 7.08, disk MB/s: 10.46
- batch p50/p99/p99.9/max: 8.99ms / 10.87ms / 21.97ms / 21.97ms
- segments: 1, bytes: 18900000
- rss: 40779776 bytes, index_ram_bytes: 8600000, skipmap_len: 100000

## Recover

- rebuild: 0.165s
- rss before/after: 40828928 / 40943616 bytes
- segments: 1, bytes: 18900000, skipmap_len: 100000

## Notes

- insert uses StorageEngine::append_batch directly in both modes; TCP measures the read-path overhead on top.
- background compaction disabled (thresholds: segments=18446744073709551615, bytes=18446744073709551615); segment_mb=256, max_fds=64, max_index_ram_mb=1024
- key_len=16 value_size=128 batch=500 seed=42
- hot: one untimed warmup pass over all keys before the read sweep
- syscalls: unverified (dtruss requires root; no strace on macOS)
- flamegraph: see bench-results/flamegraph-* (generated separately with cargo flamegraph/sample)
