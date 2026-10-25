# Changelog

All notable changes to LivenDB are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow semver.

## [0.1.4] — unreleased

Durability and upgrade release. **0.1.0–0.1.3 can lose acknowledged data** in the two situations
described under "Fixed"; read the advisory before upgrading an existing data directory.

### Fixed

- **Torn-tail recovery deleted acknowledged writes after the tear.** Recovery stopped at the first
  bad frame and truncated the file, discarding any frames an older build had appended after a tear.
  Recovery now scans forward for a resync point (header + payload CRC + consistent lengths, two
  consecutive valid frames). If stranded frames exist, default recovery refuses to truncate and
  returns `RecoveryRequired` naming the segment, offset and unrecoverable keys; `--repair` salvages
  them into a normal-range segment using the same seq_id/tombstone-watermark merge, then truncates.
  The removed region is always copied to `<segment>.torn-<offset>.bak` (fsynced, then the directory
  fsynced) before any truncation. Repair is idempotent.
- **Compaction could destroy data through a segment-ID collision.** Compaction output now uses
  `create_new(true)` (a collision fails loudly instead of truncating), the compaction ID space is
  disjoint from normal rollover, and recovery chooses the active segment by `seq_id` rather than by
  the highest file ID. Repeated compactions without a restart consolidate earlier compacted segments.
- **A fresh database could lose its directory entry on power loss.** The data directory and each new
  parent level are now fsynced before the first acknowledgement.
- **A reopened database could refuse to start after an upgrade.** The per-key charge is now
  `len("stream:key") + 64` (primary node) + `64` (timestamp-index node). An index rebuilt over
  `max_index_ram_mb` opens by default with a prominent warning and rejects new keys only;
  `--strict-recovery` keeps the old refuse-to-open behavior.
- **A fresh production install could not start.** `setup.sh`/`install.sh` generated X.509 v1
  certificates, which rustls rejects (`UnsupportedCertVersion`). Both now emit v3 certificates with
  `basicConstraints`, `keyUsage`, `serverAuth` and a `127.0.0.1`/`localhost` SAN.
- `upsert`/`update` write one replacement frame; the previous value survives a torn write. The
  0.1.3 tombstone-then-append pair was not atomic and is gone.

### Changed

- `append_batch`/`insert_batch` are all-or-nothing: every key is validated and reserved before the
  write; a rejection applies nothing and returns `BatchIndexRamLimitExceeded { key }`. An
  acknowledged batch is fully durable; a crash before the ack can leave a prefix of whole records,
  never a hole or a torn record.
- `insert` holds `write_lock` only for validation, sequencing and enqueue; the ack wait happens
  outside it. `PendingInsertGuard` releases reserved keys on every exit path (validation error,
  budget rejection, fsync failure, closed channel, panic).
- Relative `storage.data_directory` and ZTNA certificate paths resolve against the loaded config
  file (`--config`/`LIVEN_CONFIG`), not the process CWD.
- The master key is stored at `<data_directory>/liven.key`. The previous `<CWD>/liven.key` and the
  0.1.0–0.1.3 systemd location `/liven.key` are still read, with a one-time warning naming the file.
- `liven start` warns when the configured data directory has no `.liven` segments but `/data` does
  (0.1.3 Docker images wrote to `/data` regardless of the volume mount).
- Crash-injection and power-loss hooks are behind the non-default `test-hooks` cargo feature; release
  binaries contain no hook code, strings or symbols.

### Added

- Power-loss simulation harness (`src/power_loss.rs`) covering single appends, group-commit
  batches, rollover, the nine compaction steps, replace/upsert, retention unlink, repair, and
  `insert_batch`. 29 crash points × 300 seeds.
- Regression tests for torn-tail salvage (including a real 0.1.3 data directory), repair crash
  ordering and idempotence, over-budget recovery, batch atomicity, concurrency races, config path
  resolution, key migration, and certificate version.
- `--config` / `LIVEN_CONFIG` are covered by tests and documented; `liven.service` sets
  `WorkingDirectory`.

### Known limitations (unchanged)

- No replication, sharding or failover.
- fsync is not tunable; `storage.sync_mode` accepts only `"always"`.
- Power-loss tests model unsynced page-cache loss, not drives that lie about flush; macOS
  `fsync` is not `F_FULLFSYNC`.
