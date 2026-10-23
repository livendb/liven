# Liven load benchmark

- utc: `2026-10-23T12:20:15Z`
- commit: `4ac5704` (dirty)
- rustc: `rustc 1.92.0 (ded5c06cf 2025-12-08)`
- profile: `release`
- cpu: `Apple M2` (8 cores)
- ram_mb: 8192
- os: `Darwin 25.1.0 arm64`
- filesystem: `/`
- storage: `unverified`
- args: `Args { records: 100000, reads: 100000, value_size: 128, key_len: 16, mode: Tcp, threads: [1, 4, 8, 16], clients: [1, 4, 8, 16], batch: 500, read_pattern: Uniform, warm: Hot, seed: 42, mixed: false, segment_mb: 256, max_fds: 64, max_index_ram_mb: 1024, data_dir: None, skip_insert: false, keep_data: false, tag: "tcp-hot-onepread" }`

## Summary

| phase | mode | threads | clients | ops/s | p50 | p99 | p99.9 | max | mismatches |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| insert | - | - | - | 54413 | 8.99ms | 10.13ms | 19.04ms | 19.04ms | - |
| recover | - | - | - | - | - | - | - | - | - |
| read | tcp | 1 | 1 | 29497 | 32.90µs | 50.75µs | 80.45µs | 316.16µs | 0 |
| read | tcp | 4 | 4 | 76782 | 49.15µs | 93.63µs | 114.56µs | 206.97µs | 0 |
| read | tcp | 8 | 8 | 80802 | 95.23µs | 182.78µs | 218.50µs | 278.53µs | 0 |
| read | tcp | 16 | 16 | 89206 | 176.13µs | 321.28µs | 379.65µs | 473.09µs | 0 |

## Insert

- records: 100000, batch: 500, elapsed: 1.838s
- ops/s: 54413, logical MB/s: 6.96, disk MB/s: 10.28
- batch p50/p99/p99.9/max: 8.99ms / 10.13ms / 19.04ms / 19.04ms
- segments: 1, bytes: 18900000
- rss: 40714240 bytes, index_ram_bytes: 8600000, skipmap_len: 100000

## Recover

- rebuild: 0.283s
- rss before/after: 40763392 / 40878080 bytes
- segments: 1, bytes: 18900000, skipmap_len: 100000

## Notes

- insert uses StorageEngine::append_batch directly in both modes; TCP measures the read-path overhead on top.
- background compaction disabled (thresholds: segments=18446744073709551615, bytes=18446744073709551615); segment_mb=256, max_fds=64, max_index_ram_mb=1024
- key_len=16 value_size=128 batch=500 seed=42
- hot: one untimed warmup pass over all keys before the read sweep
- syscalls: unverified (dtruss requires root; no strace on macOS)
- flamegraph: see bench-results/flamegraph-* (generated separately with cargo flamegraph/sample)
