# Liven write-path benchmark

Hardware/environment: Apple M2 (8 cores), 8 GB RAM, macOS Darwin 25.1.0 arm64,
rustc 1.92.0, release profile. Workload: one stream, unique 8-byte keys,
128-byte values, single-record appends, 200 appends per writer, fresh temp dir,
background compaction disabled, `max_index_ram_mb=1024`, `max_fds=64`.
`executor-hold` is the pre-fix lock behavior; `executor-narrow` is the fix.

| mode            | writers | ops/s |   p50 µs |      p99 µs | fsync/s | avg batch |
| --------------- | ------: | ----: | -------: | ----------: | ------: | --------: |
| embedded        |       1 |   296 |  3112.96 |     9543.68 |     296 |      1.00 |
| embedded        |       8 |  1021 |  8003.58 |     9781.25 |     128 |      8.00 |
| embedded        |      32 |  3892 |  8073.22 |    13451.26 |     122 |     32.00 |
| embedded        |     128 | 16215 |  7983.10 |     9576.45 |     127 |    128.00 |
| executor-hold   |       1 |   325 |  3039.23 |     5222.40 |     325 |      1.00 |
| executor-hold   |       8 |   302 |  3272.70 |   448790.53 |     302 |      1.00 |
| executor-hold   |      32 |   293 |  3749.89 |  2096103.42 |     293 |      1.00 |
| executor-hold   |     128 |   245 |  3917.82 | 10687086.59 |     245 |      1.00 |
| executor-narrow |       1 |   295 |  3266.56 |    10510.33 |     295 |      1.00 |
| executor-narrow |       8 |  1029 |  7974.91 |     9289.73 |     129 |      8.00 |
| executor-narrow |      32 |  4047 |  7999.49 |     9527.30 |     126 |     32.00 |
| executor-narrow |     128 | 15934 |  8011.77 |    10452.99 |     124 |    128.00 |
| tcp             |       1 |   308 |  3217.41 |     5926.91 |     308 |      1.00 |
| tcp             |       8 |   729 | 10862.59 |    17006.59 |     160 |      4.56 |
| tcp             |      32 |  2379 | 14180.35 |    17711.10 |     142 |     16.75 |
| tcp             |     128 |  9700 | 14729.22 |    18890.75 |     126 |     77.11 |

Notes:

- `executor-hold` reproduces the old behavior: `write_lock` held across the ack wait.
- `executor-narrow` is the current executor path: lock held only through validation and enqueue.
- `avg batch` is flusher entries / flusher batches; `fsync/s` is successful fsyncs / wall time.
- Embedded has no global write lock and is the engine ceiling.
- 128 concurrent single-record writers need ~128 entries per fsync. At ~127 fsync/s,
  the measured ceiling is ~16k/s. `append_batch(500)` at ~127 fsync/s is ~63k/s.
  100k/s would require ~800 records per fsync (or >800 concurrent in-flight writes),
  not a code-path change alone.
