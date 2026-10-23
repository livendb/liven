# LivenDB — Status Note

**As of 2026-10-23, commit `2f2f938`.** Everything below was checked against the source in this
repository. Claims that could not be verified are marked **unverified**.

---

## 1. Snapshot

|                        |                                                                                                          |
| ---------------------- | -------------------------------------------------------------------------------------------------------- |
| Language / edition     | Rust 2024, MSRV 1.88 (ratatui 0.30.2 is the highest requirement)                                         |
| Version in tree        | `0.1.3` (`Cargo.toml`)                                                                                   |
| Published on crates.io | `0.1.2` — `0.1.3` is committed but not published                                                         |
| Binary                 | one `liven` binary; server, TUI and embedded library share the same crate                                |
| CI                     | `Build & Verify` green on `2f2f938` (lint, clippy `-D warnings`, tests, 7 cross-builds)                  |
| Tests                  | 25 integration test files, 151 `#[test]` / `#[tokio::test]` cases (plus 3 unit tests in `src/`)          |
| Maturity               | Embedded single-node engine is usable. Not production-ready for multi-node or hostile networks — see §7. |

Deployment models: embedded Rust library, network server (native TCP + HTTP/WS dashboard), and a
terminal UI. There is **no replication, sharding or clustering** — a node is a single process over a
single data directory.

---

## 2. Architecture

- **Storage**: append-only segment files (`<20-digit-id>.liven`). Frame layout is
  `seq_id(8) | timestamp(8) | type_tag(1) | flags(1) | payload_len(4) | crc32(4)` plus payload
  (`src/storage.rs:180`). CRC32 is verified on read (`src/storage.rs:202-204`).
- **Index**: `crossbeam-skiplist` mapping `"stream:key" -> LogPointer { segment_id, file_offset }`,
  plus a per-stream timestamp index used by `history()`/time-range queries. Live scans are
  index-driven, so cost scales with live keys in the stream, not with segment count.
- **Write path**: append → group-commit buffer → background flusher fsyncs batches. Drain window is
  5000 µs by default, max batch 2048 (`src/storage.rs:325-326`). `sync_mode` is effectively
  `"always"`: only `"always"` is implemented.
- **Read path**: point lookups are one positional read for small frames (single `pread` for ≤512 B,
  see `read_record`), falling back to a second read for large payloads.
- **Compaction**: rewrites live records into a new segment, dropping tombstones and superseded
  versions. The server runs it automatically every 60 s when thresholds are exceeded
  (`src/server.rs:460-476`). Embedded users must call `start_auto_compact()` or `compact()`.
- **Recovery**: replays segments on startup, rebuilding the index and timestamp index; a `Strict`
  mode is available via `--strict-recovery`.

---

## 3. Features

**Data model**

- Streams of `(key, value)` records; values are typed (`Null`, `Bool`, `Int`, `UInt`, `Float`,
  `String`, `Binary`, `Array`, `Object`, `Vector`).
- Append / upsert / update (merge) / delete (tombstone) / clear stream / drop stream.
- Historical reads (`history()`) and live reads against the in-memory index.

**Query engine** (`src/parser.rs`, `src/executor.rs`, `src/query.rs`)

- Pipeline DSL: `from`, `filter`, `limit`, `sort`, `page`, `map`, `window`, `group`, `distinct`,
  `history`, `enrich`, `correlate`, `chain`, `sequence`, `vector_filter`, `vector_search`,
  `count`, `listen`.
- Typed builder API (`Pipeline`, `Filter`, `Query`) with the same stages.
- Vector search: int8 quantized vectors, cosine similarity, `top_k` via `vector_search`
  (`src/types.rs:16`, `:57-67`).
- `explain` returns the parsed plan; there is no cost-based optimizer.

**Interfaces**

- Embedded: `Liven::open`, synchronous methods (`src/embed.rs`).
- Wire: `LivenClient`, async, one in-flight request per client (`src/client.rs`); accepts
  `host:port` or `liven://host:port` with an optional `?auth_key=` segment.
- HTTP/JSON: `/api/query`, `/api/ingest`, `/api/streams`, `/api/health`, `/api/heartbeat`,
  `/api/live/stream` (WS), `/ws`, `/api/system/{config,auth/*}` (`src/server.rs:497-520`).
- Web dashboard embedded via `rust-embed` (`src/server.rs:28-30`).
- CLI: `start`, `stop`, `status`, `list`, `tail`, `vibe` (TUI), `reset-key`
  (`src/cli.rs:103-639`).

**Security**

- `auth_key` mode (default): BLAKE3-hashed keys, four roles — `read-only`, `write`,
  `write-delete`, `admin` (`src/security.rs:40-43`).
- Browser sessions: random 32-byte session id in an `HttpOnly; SameSite=Lax` cookie, 8 h max age,
  30 min idle timeout, role re-validated per request (`src/server.rs:1462-1509`).
- Live revocation: a 500 ms background checker closes connections when a key is revoked, deleted,
  or its role changes (`src/server.rs:412-458`).
- mTLS/ZTNA for the native TCP listener, with client CN mapped to an `auth_keys` record
  (`src/server.rs:339-347`). Only active when `environment = "production"` **and**
  `security.ztna.enabled = true`; otherwise the listener is cleartext.
- Master key from `LIVEN_SECURITY_MASTER_KEY`, then `liven.toml`, then `./liven.key` (created with
  mode `0600` on first start).

---

## 4. Performance

Numbers from CI run
[39969013537](https://github.com/livendb/liven/actions/runs/39969013537) (commit `3a6fea2`):
GitHub `ubuntu-latest`, Docker image `rust:1.88-bookworm`, `-C target-cpu=x86-64`, `lto = "fat"`,
`codegen-units = 1`. Criterion medians. The runner CPU model is not recorded in the artifact, so
treat these as indicative rather than absolute.

| Operation                  | Result                                                    |
| -------------------------- | --------------------------------------------------------- |
| Point lookup, existing key | 1.71 µs @1K keys, 1.69 µs @10K keys                       |
| Point lookup, missing key  | 79.8 ns @1K, 117 ns @10K                                  |
| Timestamp range index      | 84.4 ns (11.8 Gelem/s at 1K entries)                      |
| Full scan                  | 340K / 325K / 309K elem/s at 1K / 5K / 10K records        |
| Scan with `limit(100)`     | 3.2M / 15.9M / 31.9M dataset-elem/s at 1K / 5K / 10K      |
| Append, single record      | 719 µs (fsync-bound)                                      |
| Append, batch 500          | 724 µs per batch ≈ 690K ops/s (wide variance: 349K–1.34M) |
| Upsert, new key            | 699 µs                                                    |
| Upsert, existing key       | 1.43 ms (tombstone + rewrite)                             |
| Compaction                 | 688 µs @500 records, 743 µs @1K                           |
| Parse, simple / complex    | 470 ns / 1.19 µs                                          |
| Vector lookup, 512d int8   | 2.73 µs, 179 MiB/s (9.5× faster than msgpack arrays)      |
| Wire encode, 16 KB query   | 12.45 GiB/s                                               |

**End-to-end** (`benches/load_bench.rs`, same run): 100,000 records, 128-byte values, 16-byte keys,
`append_batch(500)`, embedded.

| Phase                        | Result                                                              |
| ---------------------------- | ------------------------------------------------------------------- |
| Insert                       | 75,965 ops/s; batch p50 6.29 ms / p99 6.65 ms; 18.9 MB in 1 segment |
| Recover                      | 0.43 s for 100,000 keys; RSS 47.5 MB → 63.2 MB                      |
| Read, 1 / 4 / 8 / 16 threads | 310,772 / 1,045,410 / 989,926 / 1,002,985 ops/s                     |
| Read latency                 | p50 2.77–3.37 µs, p99 4.16–4.65 µs; 16-thread p99.9 3.02 ms         |

Reads saturate around 1M ops/s from 4 threads. All reads are value-verified; 0 mismatches.

**Caveat on trend analysis:** Criterion's `change:` against the cached baseline showed every group
regressing 30–105% at once — including pure parsing, which does no I/O. That is consistent with a
runner or build-configuration change, not a code regression. The cached baseline should be reset,
and the runner CPU recorded in the artifact, before `change:` is trusted.

---

## 5. Restrictions

### Verified implementation limits

| Limit                          | Value                                    | Where                    |
| ------------------------------ | ---------------------------------------- | ------------------------ |
| Max concurrent connections     | 10,000, hardcoded                        | `src/storage.rs:263`     |
| Broadcast channel capacity     | 4,096, hardcoded                         | `src/storage.rs:263-269` |
| Max concurrent streams         | 32 (configurable)                        | `src/config.rs:167`      |
| Cached segment file handles    | 64 (configurable)                        | `src/config.rs:168`      |
| Max records returned by a scan | 100,000 (configurable)                   | `src/storage.rs:267`     |
| Index RAM budget               | auto = `clamp(host_ram/4, 32, 4096)` MiB | `src/sysinfo.rs:87-92`   |
| Commit drain window / batch    | 5 ms / 2048 entries                      | `src/storage.rs:325-326` |

### Config keys that are parsed but not honoured

- `server.max_connections` — fixed at 10,000.
- `server.broadcast_capacity` — fixed at 4,096.
- `storage.sync_interval_ms` — fixed at 100 internally; no effect.
- `limits.max_segment_size_mb` — never read; use `storage.max_segment_size_mb`.
- `security.auth_key.system_stream` and `security.auth_key.allow_local_auto_generation` — the auth
  stream is hardcoded to `auth_keys`.

### Index memory

`max_index_ram_mb` charges `len("stream:key") + 64` bytes per live key. The timestamp index,
allocator overhead and compaction scratch memory are **not** counted, so real RSS is roughly 2×
the charge (~160 B/key for short keys, ~190 B/key for 32-byte keys). Overwriting an existing key
does not grow the charge; tombstones release it. At the limit, new keys are rejected with
`IndexRamLimitExceeded` — there is no eviction or spill. Recovery enforces the same limit, so a
database whose index exceeds a lowered budget will not reopen.

### Query / scan semantics

- `limit()` is applied by the scan, but the global `max_scan_results` caps every scan; asking for
  more than the cap and hitting it returns an error rather than truncating silently
  (`src/storage.rs:1417-1422`).
- Live scans are sorted by `sequence_id` after collection (O(m log m) for m results).
- Time-range/history scans are not capped by `max_scan_results`.

### Operational

- No replication, sharding, failover or consensus. One process, one data directory.
- No directory-level lock: a second `StorageEngine` can open a directory that a server already has
  open. Doing so writes behind the running server's index and compaction can corrupt the segment —
  this actually happened during testing and is why `tests/generate_sample_data.rs` now detects a
  live server over TCP instead of probing file locks.
- REST and WebSocket traffic is plain HTTP; mTLS covers only the native TCP port.
- The Docker image sets no `WORKDIR` and ships `data_directory = "./data"`, so inside the container
  the path resolves to `/data` while `/var/lib/liven` is created and left unused. **Unverified at
  runtime** (no Docker available here), but the code path is unambiguous.
- `liven.key` doubles as the initial admin auth key on a fresh database (bootstrap uses the master
  key as the root key when no admin exists). Treat it as a root credential.

---

## 6. Known issues / open items

- **Fixed this week**: dashboard WebSockets were killed ~500 ms after login because connections
  were registered under `blake3(session_id)` instead of the session's auth key hash. Fixed in
  `0ae9d0e` with a regression test.
- **Fixed this week**: `tests/generate_sample_data.rs` wrote through a second engine into a live
  data directory (see above). It now authenticates and uses `/api/ingest` (`a71b897`).
- `0.1.3` is committed and tagged work is pending on crates.io (`0.1.2` is current); no `v0.1.3`
  tag has been cut.
- `.cargo/audit.toml` still ignores two advisories (`RUSTSEC-2025-0134` for `rustls-pemfile`,
  `RUSTSEC-2026-0190` for `anyhow` via criterion). `cargo audit` is otherwise clean.
- The TUI (`liven vibe`) has no automated tests beyond compilation.

## 7. Unverified

- Benchmark behaviour on real hardware, NVMe vs network storage, and with cold page caches —
  the CI job measures one GitHub-hosted runner with hot caches.
- The Docker image's runtime data directory (see §5) has not been exercised here.
- Behaviour of `liven reset-key` and `liven repair` end-to-end.
- Long-running stability: no soak test in the repository.

---

## 8. Reproducing the numbers

```sh
# Engine microbenchmarks
cargo bench

# Reproducible Docker run (same image CI uses)
./run-bench.sh

# End-to-end load harness
cargo bench --bench load_bench -- --records 100000 --reads 100000 --threads 1,4,8,16

# Sample dataset for the dashboard (works against a running server)
cargo test --test generate_sample_data -- --nocapture
```
