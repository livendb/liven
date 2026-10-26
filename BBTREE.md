# bbtree experiment

Replacing `crossbeam-skiplist` with [`bbtree`](https://crates.io/crates/bbtree)
as LIVEN's concurrent live-key index.

- **Branch:** `experiment-btree`
- **Baseline:** `main` at `5909b6e` (`v0.1.4`)
- **Date:** 2026-10-26
- **Machine:** Apple M2 (8 cores), 8 GB RAM, macOS Darwin 25.1.0 arm64
- **Toolchain:** rustc 1.92.0, `--release` / `bench` profile (LTO, `codegen-units=1`)

## What changed

| Area               | Before                                 | After                                                 |
| ------------------ | -------------------------------------- | ----------------------------------------------------- |
| Library dependency | `crossbeam-skiplist = "0.1.3"`         | `bbtree = "0.1.5"`                                    |
| Primary live index | `Arc<SkipMap<String, LogPointer>>`     | `Arc<IndexMap<String>>` (`bbtree::BBTreeMap`)         |
| Timestamp index    | `SkipMap<(i64, u64), LogPointer>`      | `IndexMap<u128>` with an order-preserving encoded key |
| Entry count        | `SkipMap::len()`                       | `IndexMap`'s `AtomicUsize` counter                    |
| Iteration          | borrowed `SkipMap::iter()` / `range()` | owned `(key, value)` pairs from `IndexMap`            |

The new `src/storage/index.rs` is a thin wrapper around `bbtree::BBTreeMap`:

- `insert`, `get`, `contains_key`, `remove`, `len`, `is_empty`
- `iter` and `range` return owned `(key, LogPointer)` pairs
- the length counter is maintained on insert/remove (replacement does not
  increment it)
- allocation failure from `bbtree` is treated as fatal, like an OOM in std's
  collections

`bbtree` only builds a tree for key types it can derive inline keys for
(byte strings and 64/128-bit integers under its `Auto` strategy). The
timestamp index therefore encodes `(i64, u64)` as a `u128`:

```text
ordered_timestamp = (timestamp as u64) ^ (1 << 63)
encoded           = (ordered_timestamp as u128) << 64 | seq_id as u128
```

Flipping the sign bit preserves `i64` ordering, so the encoded `u128` sorts
exactly like the original `(timestamp, seq_id)` pair. A unit test
(`encoded_key_order_matches_pair_order`) covers the edge cases.

The public field name `StorageEngine::skipmap` was kept for compatibility,
but it is now B-tree-backed; doc comments and `EXPLAIN` output were updated
to say so.

### Migration notes

- `bbtree` has no `len()` and no borrowed iterator, hence the small wrapper.
- Its borrowed `scan()` callback runs while a leaf is latched shared, so
  `scan_live_stream` keeps using `range_cloned()`. That releases the latch
  between batches, which matters because the callback performs `pread`s for
  each record. The trade-off is that keys/pointers are cloned per entry on
  that path.
- Maintenance and `EXPLAIN` paths use the cloned iterator. Their cost is
  proportional to the number of indexed keys, not to records read from disk.
- `crossbeam-skiplist` remains a **dev-dependency** only, so
  `benches/index_bench.rs` can compare the two structures side by side.

## Tests

Command:

```sh
cargo test
```

Result:

```text
passed: 227    failed: 0    ignored: 50
```

All unit and integration tests pass. One migration issue surfaced during the
first run: constructing a `BBTreeMap` with the tuple key `(i64, u64)` panicked
with `a BBTreeMap's keys need inline keys`. The `u128` encoding above fixes
it. No other behavioural changes were required.

`cargo clippy --lib` is clean. `cargo fmt --check` reports only the
pre-existing formatting drift in `tests/recovery_active_segment_tests.rs`.

## CI stress test (1M records)

`.github/workflows/btree-1m.yml` runs on every push to `experiment-btree`
(and via `workflow_dispatch`) on `ubuntu-24.04`:

1. builds the release bench binary,
2. inserts **1,000,000 records** with `load_bench` (128-byte values,
   500-record batches, one 256 MB segment),
3. reopens the database to rebuild the index,
4. runs **1,000,000 point reads** per phase at 1, 4, and 8 threads,
5. asserts that recovery reports `skipmap_len=1000000` and that every read
   phase reports `mismatches 0`.

Results are uploaded as a workflow artifact and written to the job summary.
The longer working set (189 MB on disk, ~150 MB of accounting for the index)
is the point of this job: it exercises B-tree growth and multi-level internal
nodes, unlike the 100k local run above.

The other workflows (`build.yml`, `bench.yml`, `docker.yml`, `install.yml`)
are gated off for `experiment-btree` with a job-level
`if: github.ref_name != 'experiment-btree' && github.head_ref != 'experiment-btree'`,
so the 1M job is the only CI signal on this branch.

### First CI result

GitHub Actions run
[40389750076](https://github.com/livendb/liven/actions/runs/40389750076)
on `ubuntu-24.04` passed in 4m09s:

| Phase                             | Result                                                         |
| --------------------------------- | -------------------------------------------------------------- |
| Insert 1,000,000 records          | 250,184 ops/s · 32.02 MB/s logical · 1 segment / 189,000,000 B |
| Batch latency                     | p50 1.91 ms · p99 3.16 ms · p99.9 6.11 ms                      |
| Recovery / index rebuild          | 3.443 s · `skipmap_len=1000000`                                |
| RSS after insert → after recovery | 663,957,504 B → 667,697,152 B (633 → 637 MiB)                  |
| Read, 1 thread                    | 463,129 ops/s · p50 1.79 µs · p99 2.35 µs · 0 mismatches       |
| Read, 4 threads                   | 1,440,303 ops/s · p50 2.27 µs · p99 3.23 µs · 0 mismatches     |
| Read, 8 threads                   | 1,412,276 ops/s · p50 2.29 µs · p99 3.23 µs · 0 mismatches     |

All three read phases verified every returned value; the workflow's assertion
confirmed the index held exactly 1,000,000 entries after recovery.

## End-to-end benchmark

Reproduced with the in-repo load harness:

```sh
cargo bench --bench load_bench -- \
  --records 100000 --reads 100000 --mode embedded \
  --threads 1,4,8,16
```

Workload: 100,000 records (128-byte values, 16-byte keys), 500-record
batches, one segment, hot uniform random point reads. Three runs per branch;
the table shows medians.

| Metric                   |      crossbeam-skiplist |                  bbtree |     Change |
| ------------------------ | ----------------------: | ----------------------: | ---------: |
| Insert throughput        |           116,513 ops/s |           114,878 ops/s |      −1.4% |
| Recovery / index rebuild |                 0.226 s |                 0.221 s |      −2.2% |
| Read, 1 thread           |           670,300 ops/s |           725,031 ops/s |  **+8.2%** |
| Read, 4 threads          |         1,634,778 ops/s |         1,799,161 ops/s | **+10.1%** |
| Read, 8 threads          |         1,056,793 ops/s |         1,060,293 ops/s |      +0.3% |
| Read, 16 threads         |         1,069,072 ops/s |         1,089,796 ops/s |      +1.9% |
| RSS after insert         | 43,876,352 B (41.8 MiB) | 49,659,904 B (47.4 MiB) |     +13.2% |
| RSS after recovery       | 55,689,216 B (53.1 MiB) | 61,538,304 B (58.7 MiB) |     +10.5% |

All read phases reported zero mismatches with both implementations.
`index_ram_bytes` was identical (15,000,000 B) because the engine's accounting
constants were unchanged; the RSS difference is outside that accounting.

### Reading the numbers

- **Inserts are flusher-bound.** The group-commit fsync dominates, so the
  index swap is within run-to-run noise (−1.4%).
- **Point reads improved by ~8–10%.** `StorageEngine::get` also does a
  positional file read and payload decode, so the index-only gain is diluted.
- **Scaling past 4 threads is flat** on this 8-core M2 for both
  implementations; the 8/16-thread read numbers are dominated by contention
  in the rest of the read path and by benchmark variance.
- **Memory is higher in this test.** `bbtree` uses fixed-size node pages
  (1 KiB leaves, 512 B internal nodes by default) and keeps pages emptied by
  removals for reuse; process RSS was ~5.8 MiB higher after insert for 100k
  keys. This is the clearest regression and needs a larger,
  allocator-controlled experiment before drawing conclusions.

## Index microbenchmark

To isolate the data structure from the storage engine:

```sh
cargo bench --bench index_bench
```

100,000 `String` keys, values are `LogPointer`, random key order, single
thread, Criterion 0.5.

| Workload                    |                       bbtree |          crossbeam-skiplist |   Speedup |
| --------------------------- | ---------------------------: | --------------------------: | --------: |
| 100k point lookups (hits)   |  11.53 ms · 8.67 M lookups/s | 43.59 ms · 2.29 M lookups/s | **3.78×** |
| 100k point lookups (misses) | 0.835 ms · 119.8 M lookups/s |  4.65 ms · 21.5 M lookups/s | **5.57×** |

Criterion 95% intervals did not overlap:

- hits: bbtree 10.36–13.06 ms, skiplist 40.09–47.03 ms
- misses: bbtree 0.826–0.845 ms, skiplist 4.615–4.686 ms

Random hit lookups touch leaf pages across the whole tree, so they are
memory-latency bound; misses mostly traverse internal nodes and exit. The
B-tree's shallower height and inline-key comparisons explain the larger miss
gain. This microbenchmark is the direct evidence behind the ~8–10%
end-to-end read improvement.

## Caveats

- Results are from a single laptop-class machine, on an APFS volume, with the
  database file hot in the OS page cache. Absolute numbers are not
  portable; the relative read gains are the meaningful part.
- Run-to-run variance was material (for example, 4-thread reads ranged from
  1.50 M to 1.86 M ops/s for bbtree). Medians of three runs are reported.
- The write path was not stressed with compaction or many segments. A
  multi-segment run would better exercise the key-cloning maintenance paths.
- `bbtree` uses fixed-size node pages and retains pages emptied by removals,
  so long-running delete/retention workloads may hold more RSS than the
  skiplist did.
- The microbenchmark is single-threaded. `bbtree`'s concurrency design is not
  directly measured here; the end-to-end 4-thread read result is the closest
  signal.

## Conclusion

The bbtree swap is functionally clean: all 227 tests pass, and the only
migration obstacle was the tuple-key limitation of bbtree's inline-key
strategy. It is a clear win for point reads (3.8–5.6× index-only, ~8–10%
end-to-end at 1–4 threads) with neutral insert throughput and slightly faster
recovery. The open question is memory: RSS grew ~10–13% on this workload.
Before adopting this on `main`, I would run a larger multi-segment workload
and a delete/retention-heavy workload to confirm the memory trade-off.
