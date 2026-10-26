use crate::error::LivenError;
use crate::storage::index::IndexMap;
use crate::types::{DataValue, LogPointer, Record};
use fs2::FileExt;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::broadcast;
use tracing::{error, info, warn};

/// Controls how `recover()` handles corrupted or truncated segment files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryMode {
    /// Safe default: finds a torn tail, copies the tail to a sidecar backup,
    /// and physically truncates it to the last known-good frame boundary. If
    /// valid frames are stranded after the tear, it refuses to truncate and
    /// returns `RecoveryRequired` so the operator can run `--repair`.
    Default,
    /// Like Default for torn-tail detection, but a CRC or parse failure without
    /// stranded frames aborts recovery instead of skipping/truncating.
    Strict,
    /// Write a full backup copy to
    /// `{segment_id:020}.liven.repair-{unix_timestamp}` before modifying a
    /// corrupt segment, salvage valid frames stranded after a tear into a new
    /// normal-range segment, then truncate the source. Prints a summary report
    /// of exactly what was salvaged or discarded.
    Repair,
}

/// Summary of what recovery discarded or skipped, produced after a successful
/// (non-Strict) recovery that encountered corruption.
#[derive(Debug, Clone, Default)]
pub struct RecoveryReport {
    /// Segments that had a torn write at the end and were truncated to the
    /// last valid frame boundary.  (segment_id, offset_truncated_to, bytes_discarded).
    pub truncated_segments: Vec<(u64, u64, u64)>,
    /// Segments that were skipped entirely because of mid-file corruption.
    /// (segment_id, offset_of_first_bad_frame, approximate_keys_lost).
    pub skipped_segments: Vec<(u64, u64, usize)>,
    /// Backup files created in Repair mode.
    pub repair_backups: Vec<PathBuf>,
    /// Sidecar copies of torn regions, created before every truncation in all
    /// recovery modes. Each path is `<segment>.torn-<offset>.bak`.
    pub torn_backups: Vec<PathBuf>,
    /// Frames salvaged from stranded regions by `RecoveryMode::Repair`.
    pub salvaged_frames: usize,
    /// New normal-range segments written to hold salvaged frames.
    pub salvage_segments: Vec<(u64, PathBuf)>,
    /// Whether any recovery action was taken (false for clean startups).
    pub had_issues: bool,
}

#[allow(unsafe_code)]
pub mod index;
pub mod key;
pub mod ring_buffer;
pub mod timestamp_index;

/// # Safety
/// This module contains unsafe blocks for pointer reinterpretation when handling
/// vector data types. The unsafe operations are sound because:
/// - i8 and u8 have identical size, alignment, and bit layout
/// - Unsafe slices are immediately copied into owned allocations and do not escape
/// - All pointer arithmetic is bounds-checked against the original buffer length
/// - The resulting DataValue variants do not borrow from the original payload
pub trait PositionalReader {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<()>;
    fn read_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize>;
}

#[cfg(unix)]
impl PositionalReader for File {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
        std::os::unix::fs::FileExt::read_exact_at(self, buf, offset)
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
        std::os::unix::fs::FileExt::read_at(self, buf, offset)
    }
}

#[cfg(windows)]
impl PositionalReader for File {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
        use std::os::windows::fs::FileExt;
        let mut total_read = 0;
        while total_read < buf.len() {
            let read = self.seek_read(&mut buf[total_read..], offset + total_read as u64)?;
            if read == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer (positional EOF)",
                ));
            }
            total_read += read;
        }
        Ok(())
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
        use std::os::windows::fs::FileExt;
        self.seek_read(buf, offset)
    }
}

/// First segment ID reserved for compaction output. Normal segment rollovers must
/// stay strictly below this value; compaction output must be at or above it.
/// This makes the two ID spaces structurally disjoint instead of relying on
/// counter seeding.
const COMPACTION_ID_BASE: u64 = u64::MAX / 2;

#[derive(Debug, Clone, Default)]
pub struct RetentionPolicy {
    pub default_max_age_ms: u64,
    pub streams: HashMap<String, u64>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RetentionReport {
    pub checked_segments: usize,
    pub deleted_segments: usize,
    pub deleted_keys: usize,
    pub freed_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct IndexUsage {
    pub used_bytes: u64,
    pub limit_bytes: u64,
    pub high_water_bytes: u64,
    pub high_water_reached: bool,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct FlushMetrics {
    pub fsyncs: u64,
    pub batches: u64,
    pub entries: u64,
}

impl FlushMetrics {
    pub fn average_batch_size(&self) -> f64 {
        if self.batches == 0 {
            0.0
        } else {
            self.entries as f64 / self.batches as f64
        }
    }
}

pub struct StorageEngine {
    data_dir: PathBuf,
    max_segment_size: u64,
    current_sequence_id: Arc<AtomicU64>,
    active_segment_id: Arc<AtomicU64>,
    // We protect the active file handle and its size using a Mutex to serialize append operations
    active_file: Arc<Mutex<Option<File>>>,
    active_size: Arc<Mutex<u64>>,
    /// Primary in-memory index: compound `"stream:key"` → live `LogPointer`.
    /// Backed by a concurrent B+-tree (`bbtree`); the field keeps its old
    /// name for compatibility with the previous skiplist-backed API.
    pub skipmap: Arc<IndexMap<String>>,
    broadcast_tx: broadcast::Sender<Record>,
    file_handles: Arc<Mutex<HashMap<u64, Arc<File>>>>,
    ring_buffer: ring_buffer::LogRingBuffer,
    #[allow(dead_code)]
    sync_mode: String,
    is_running: Arc<std::sync::atomic::AtomicBool>,
    /// Set when an I/O or fsync failure leaves the on-disk state uncertain.
    /// Once set, all future writes fail fast and the database must be reopened
    /// (which runs recovery and truncates any torn tail).
    failed: Arc<std::sync::atomic::AtomicBool>,
    max_streams: usize,
    max_fds: usize,
    max_index_ram_bytes: u64,
    pub index_ram_bytes: Arc<AtomicU64>,
    /// Percentage of `max_index_ram_bytes` at which the high-water metric flips.
    pub index_high_water_percent: AtomicU8,
    index_high_water_reached: Arc<AtomicBool>,
    /// Flusher counters for benchmarks and operational monitoring.
    flush_fsyncs: Arc<AtomicU64>,
    flush_batches: Arc<AtomicU64>,
    flush_entries: Arc<AtomicU64>,
    pub streams_set: Arc<Mutex<HashSet<String>>>,
    /// Keys with an in-flight `insert` that has been enqueued but not yet
    /// acknowledged. Used to preserve insert uniqueness after the executor
    /// releases `write_lock` before the ack wait.
    pending_inserts: Arc<Mutex<HashSet<String>>>,
    /// Serializes compaction and retention, which both rewrite/delete inactive
    /// segments and move index pointers.
    maintenance_lock: Arc<Mutex<()>>,
    retention: Arc<Mutex<RetentionPolicy>>,
    pub max_connections: usize,
    pub broadcast_capacity: usize,
    pub conn_semaphore: Arc<tokio::sync::Semaphore>,
    compaction_counter: Arc<AtomicU64>,
    pub compaction_threshold_segments: usize,
    pub compaction_threshold_bytes: u64,
    /// Dedicated monotonic counter for segment file IDs.
    /// Incremented on each segment rollover, independent of the sequence ID.
    pub segment_id_counter: Arc<AtomicU64>,
    /// Serialises operations that do a read-check-then-write.
    /// Held by single Insert, single Upsert, InsertBatch, UpsertBatch, Update,
    /// DeleteKey, PipelineUpdate and PipelineDelete (full-scan path). In other
    /// words every mutating path takes it, so writers are globally serialised.
    pub write_lock: Arc<Mutex<()>>,
    pub timestamp_indexes: Arc<Mutex<HashMap<String, Arc<timestamp_index::TimestampIndex>>>>,
    /// Handle to the background flusher thread for clean shutdown coordination.
    flusher_handle: Option<std::thread::JoinHandle<()>>,
    /// Maximum number of records to return from a full scan operation.
    max_scan_results: usize,
    /// Debug/testing counter: number of on-disk frames read by scans. Used to
    /// assert that the live (index) scan cost is proportional to the queried
    /// stream, not the whole database.
    pub frames_read: AtomicU64,
    /// Group-commit drain window in microseconds.
    /// The flusher waits this long for additional entries before flushing.
    /// Default: 5000 (5ms). Set lower (e.g. 300) for lower-latency single-caller workloads.
    pub commit_drain_window_us: u64,
    /// Maximum number of entries to accumulate in one group-commit batch.
    /// Default: 2048.
    pub commit_max_batch_size: usize,
    /// Controls how corruption is handled during recovery.
    /// Default: RecoveryMode::Default.
    pub recovery_mode: RecoveryMode,
    /// Timeout in seconds for advisory file locks during recovery.
    /// Default: 5. Set to 0 to disable (block indefinitely).
    pub lock_timeout_secs: u64,
    /// Populated after `recover()` completes; contains details of any
    /// truncation or segment-skipping that occurred.
    pub recovery_report: Option<RecoveryReport>,
    /// Highest segment ID observed in the normal (non-compaction) range.
    /// Used to seed the rollover counter without ever entering the reserved
    /// compaction range.
    max_normal_segment_id: u64,
    /// Highest segment ID observed across both ranges. Used only to seed the
    /// compaction counter so a leftover compacted file is never reused.
    max_segment_id: u64,
}

use std::cell::RefCell;

thread_local! {
    static SERIALIZATION_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(4096));
    static READ_STAGING_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(4096));
    static FRAME_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(256 * 1024));
}

/// Maximum stream-name length (in bytes) that a single stream-scoped scan will
/// attempt to resolve from the payload prefix before falling back to reading the
/// full payload. Stream names are the first field of every payload.
const STREAM_PEEK_CAP: usize = 128;

/// Charged bytes for one primary-index entry (`String` + `LogPointer` +
/// B-tree node overhead). The compound key length is added separately.
const PRIMARY_INDEX_NODE_BYTES: u64 = 64;

/// Charged bytes for one timestamp-index entry
/// (encoded key + `LogPointer` + B-tree node overhead).
const TIMESTAMP_INDEX_NODE_BYTES: u64 = 64;

/// Decode the fixed 26-byte on-disk frame header.
///
/// Layout: `seq_id(8) | timestamp(8) | type_tag(1) | flags(1) | payload_len(4) | crc32(4)`.
#[inline]
fn decode_frame_header(h: &[u8]) -> (u64, i64, u8, u8, usize, u32) {
    let seq_id = u64::from_be_bytes([h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7]]);
    let timestamp = i64::from_be_bytes([h[8], h[9], h[10], h[11], h[12], h[13], h[14], h[15]]);
    let type_tag = h[16];
    let flags = h[17];
    let payload_len = u32::from_be_bytes([h[18], h[19], h[20], h[21]]) as usize;
    let crc32 = u32::from_be_bytes([h[22], h[23], h[24], h[25]]);
    (seq_id, timestamp, type_tag, flags, payload_len, crc32)
}

fn decode_frame_payload(
    payload: &[u8],
    declared_crc: u32,
    segment_id: u64,
    offset: u64,
    seq_id: u64,
    timestamp: i64,
    type_tag: u8,
    flags: u8,
) -> crate::error::Result<Record> {
    let actual_crc = crc32fast::hash(payload);
    if actual_crc != declared_crc {
        return Err(LivenError::CrcMismatch { segment_id, offset });
    }

    let (stream_name, key, value) = deserialize_payload_borrowed(payload, type_tag)?;
    let stream_key = crate::storage::key::StreamKey::from_str(key)?;

    Ok(Record {
        sequence_id: seq_id,
        timestamp,
        type_tag,
        flags,
        stream_name: stream_name.to_string(),
        key: stream_key,
        value,
    })
}

/// Reads only the 26-byte frame headers of a segment and returns the highest
impl StorageEngine {
    pub fn new<P: AsRef<Path>>(data_dir: P, max_segment_size: u64) -> crate::error::Result<Self> {
        Self::new_with_recovery_mode(data_dir, max_segment_size, RecoveryMode::Default)
    }

    /// Like `new()`, but with an explicit `RecoveryMode`.
    /// Uses the standalone defaults for the limits that recovery enforces
    /// (see [`StorageEngine::new_with_limits`]).
    pub fn new_with_recovery_mode<P: AsRef<Path>>(
        data_dir: P,
        max_segment_size: u64,
        recovery_mode: RecoveryMode,
    ) -> crate::error::Result<Self> {
        Self::new_with_limits(
            data_dir,
            max_segment_size,
            recovery_mode,
            32,
            16 * 1024 * 1024,
            64,
        )
    }

    /// Opens the database and runs recovery with the supplied limits already in
    /// effect.
    ///
    /// `recover()` rejects a database whose stream count or index RAM exceeds
    /// `max_streams` / `max_index_ram_bytes`, so those limits must be known
    /// **before** recovery runs. Passing them here (instead of calling
    /// `set_max_streams` / `set_max_index_ram_bytes` afterwards) is what lets a
    /// database that is larger than the standalone defaults reopen successfully.
    pub fn new_with_limits<P: AsRef<Path>>(
        data_dir: P,
        max_segment_size: u64,
        recovery_mode: RecoveryMode,
        max_streams: usize,
        max_index_ram_bytes: u64,
        max_fds: usize,
    ) -> crate::error::Result<Self> {
        Self::new_with_tuning(
            data_dir,
            max_segment_size,
            recovery_mode,
            max_streams,
            max_index_ram_bytes,
            max_fds,
            10_000,
            4_096,
            5_000,
            2_048,
        )
    }

    /// Full constructor used by the server and embedded API. The extra tuning
    /// values must be supplied before the flusher thread starts; setting the
    /// public fields afterwards has no effect on that thread.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_tuning<P: AsRef<Path>>(
        data_dir: P,
        max_segment_size: u64,
        recovery_mode: RecoveryMode,
        max_streams: usize,
        max_index_ram_bytes: u64,
        max_fds: usize,
        max_connections: usize,
        broadcast_capacity: usize,
        commit_drain_window_us: u64,
        commit_max_batch_size: usize,
    ) -> crate::error::Result<Self> {
        let path = data_dir.as_ref().to_path_buf();
        ensure_data_dir(&path)?;

        let sync_mode: String = "always".to_string();
        let max_scan_results: usize = 100_000;

        let conn_semaphore = Arc::new(tokio::sync::Semaphore::new(max_connections));
        let (broadcast_tx, _) = broadcast::channel(broadcast_capacity);

        let current_sequence_id = Arc::new(AtomicU64::new(1));
        let active_segment_id = Arc::new(AtomicU64::new(1));
        let active_file = Arc::new(Mutex::new(None));
        let active_size = Arc::new(Mutex::new(0));
        let skipmap = Arc::new(IndexMap::new());
        let file_handles = Arc::new(Mutex::new(HashMap::new()));
        let is_running = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let failed = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let (tx, rx) = std::sync::mpsc::sync_channel(2048);
        let ring_buffer = ring_buffer::LogRingBuffer::new(tx);

        let index_ram_bytes = Arc::new(AtomicU64::new(0));
        let index_high_water_reached = Arc::new(AtomicBool::new(false));
        let flush_fsyncs = Arc::new(AtomicU64::new(0));
        let flush_batches = Arc::new(AtomicU64::new(0));
        let flush_entries = Arc::new(AtomicU64::new(0));
        let streams_set = Arc::new(Mutex::new(HashSet::new()));
        let pending_inserts = Arc::new(Mutex::new(HashSet::new()));
        let maintenance_lock = Arc::new(Mutex::new(()));
        let retention = Arc::new(Mutex::new(RetentionPolicy::default()));
        let compaction_counter = Arc::new(AtomicU64::new(u64::MAX / 2));
        let write_lock = Arc::new(Mutex::new(()));
        // Auto-compaction defaults: trigger after 4 inactive segments or 64 MB of inactive data
        // These can be overridden via config or LivenConfig before the engine is used.
        let compaction_threshold_segments = 4;
        let compaction_threshold_bytes = 64 * 1024 * 1024;
        let segment_id_counter = Arc::new(AtomicU64::new(1));
        let timestamp_indexes = Arc::new(Mutex::new(HashMap::new()));

        let mut engine = Self {
            data_dir: path,
            max_segment_size,
            current_sequence_id: current_sequence_id.clone(),
            active_segment_id: active_segment_id.clone(),
            active_file: active_file.clone(),
            active_size: active_size.clone(),
            skipmap: skipmap.clone(),
            broadcast_tx,
            file_handles: file_handles.clone(),
            ring_buffer,
            sync_mode: sync_mode.clone(),
            is_running: is_running.clone(),
            failed: failed.clone(),
            max_streams,
            max_fds,
            max_index_ram_bytes,
            index_ram_bytes: index_ram_bytes.clone(),
            index_high_water_percent: AtomicU8::new(90),
            index_high_water_reached: index_high_water_reached.clone(),
            flush_fsyncs: flush_fsyncs.clone(),
            flush_batches: flush_batches.clone(),
            flush_entries: flush_entries.clone(),
            streams_set: streams_set.clone(),
            pending_inserts: pending_inserts.clone(),
            maintenance_lock: maintenance_lock.clone(),
            retention: retention.clone(),
            max_connections,
            broadcast_capacity,
            conn_semaphore,
            compaction_counter: compaction_counter.clone(),
            compaction_threshold_segments,
            compaction_threshold_bytes,
            segment_id_counter: segment_id_counter.clone(),
            write_lock: write_lock.clone(),
            timestamp_indexes: timestamp_indexes.clone(),
            flusher_handle: None,
            max_scan_results,
            frames_read: AtomicU64::new(0),
            commit_drain_window_us,
            commit_max_batch_size,
            recovery_mode,
            lock_timeout_secs: 5,
            recovery_report: None,
            max_normal_segment_id: 0,
            max_segment_id: 0,
        };

        engine.recover()?;

        // Normal rollover IDs and compaction IDs live in disjoint ranges.
        // Seed the rollover counter from the highest *normal* segment only; a
        // compacted segment must never advance it into the reserved range.
        let active_id = engine.active_segment_id.load(Ordering::SeqCst);
        let next_normal = engine
            .max_normal_segment_id
            .max(active_id)
            .saturating_add(1);
        if next_normal >= COMPACTION_ID_BASE {
            return Err(LivenError::Internal(
                "normal segment ID space exhausted".to_string(),
            ));
        }
        engine
            .segment_id_counter
            .store(next_normal.max(1), Ordering::SeqCst);
        // Seed the compaction counter above any existing compacted file so a
        // retry after a crash never reuses an ID and `create_new` never hits a
        // stale leftover.
        engine.compaction_counter.store(
            engine
                .max_segment_id
                .saturating_add(1)
                .max(COMPACTION_ID_BASE),
            Ordering::SeqCst,
        );

        let flusher_data_dir = engine.data_dir.clone();
        let flusher_max_size = engine.max_segment_size;
        let flusher_seq = current_sequence_id;
        let flusher_active_id = active_segment_id;
        let flusher_segment_id_counter = segment_id_counter.clone();
        let flusher_file = active_file.clone();
        let flusher_size = active_size;
        let flusher_skipmap = skipmap;
        let flusher_broadcast = engine.broadcast_tx.clone();
        let flusher_handles = file_handles;
        let flusher_sync_mode = sync_mode.clone();
        let flusher_running = is_running.clone();
        let flusher_failed = failed.clone();
        let flusher_index_ram_bytes = index_ram_bytes.clone();
        let flusher_max_index_ram_bytes = engine.max_index_ram_bytes;
        let flusher_high_water_bytes = engine
            .max_index_ram_bytes
            .saturating_mul(engine.index_high_water_percent.load(Ordering::Relaxed) as u64)
            / 100;
        let flusher_high_water_reached = index_high_water_reached.clone();
        let flusher_fsyncs = flush_fsyncs.clone();
        let flusher_batches = flush_batches.clone();
        let flusher_entries = flush_entries.clone();
        let flusher_timestamp_indexes = engine.timestamp_indexes.clone();
        let flusher_drain_window_us = engine.commit_drain_window_us;
        let flusher_max_batch = engine.commit_max_batch_size;

        let flusher_handle = std::thread::spawn(move || {
            run_flusher_loop(
                rx,
                flusher_data_dir,
                flusher_max_size,
                flusher_seq,
                flusher_active_id,
                flusher_segment_id_counter,
                flusher_file,
                flusher_size,
                flusher_skipmap,
                flusher_broadcast,
                flusher_handles,
                flusher_sync_mode,
                flusher_running,
                flusher_failed,
                flusher_index_ram_bytes,
                flusher_max_index_ram_bytes,
                flusher_high_water_bytes,
                flusher_high_water_reached,
                flusher_fsyncs,
                flusher_batches,
                flusher_entries,
                flusher_timestamp_indexes,
                flusher_drain_window_us,
                flusher_max_batch,
            );
        });

        engine.flusher_handle = Some(flusher_handle);

        Ok(engine)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Record> {
        self.broadcast_tx.subscribe()
    }

    /// Get the number of active broadcast subscribers
    pub fn broadcast_subscriber_count(&self) -> usize {
        self.broadcast_tx.receiver_count()
    }

    pub fn with_max_connections(mut self, limit: usize) -> Self {
        self.max_connections = limit;
        self.conn_semaphore = Arc::new(tokio::sync::Semaphore::new(limit));
        self
    }

    /// Set the recovery mode before `recover()` is called during construction.
    /// Use before calling `new()` via the builder pattern, or call on the
    /// returned engine before any writes.
    pub fn with_recovery_mode(mut self, mode: RecoveryMode) -> Self {
        self.recovery_mode = mode;
        self
    }

    /// Run recovery in the given mode, print the report, and return without
    /// starting any server or flusher thread.  This is an offline maintenance
    /// operation — no writes are accepted.
    pub fn repair<P: AsRef<Path>>(
        data_dir: P,
        max_segment_size: u64,
        mode: RecoveryMode,
    ) -> crate::error::Result<RecoveryReport> {
        let path = data_dir.as_ref().to_path_buf();
        ensure_data_dir(&path)?;

        // Minimal engine construction — only what recover() needs.
        let skipmap = Arc::new(IndexMap::new());
        let index_ram_bytes = Arc::new(AtomicU64::new(0));
        let streams_set = Arc::new(Mutex::new(HashSet::new()));
        let file_handles = Arc::new(Mutex::new(HashMap::new()));
        let timestamp_indexes = Arc::new(Mutex::new(HashMap::new()));
        let active_segment_id = Arc::new(AtomicU64::new(1));

        let mut engine = Self {
            data_dir: path,
            max_segment_size,
            current_sequence_id: Arc::new(AtomicU64::new(1)),
            active_segment_id: active_segment_id.clone(),
            active_file: Arc::new(Mutex::new(None)),
            active_size: Arc::new(Mutex::new(0)),
            skipmap: skipmap.clone(),
            broadcast_tx: broadcast::channel(4096).0,
            file_handles,
            ring_buffer: {
                let (tx, _rx) = std::sync::mpsc::sync_channel(2048);
                ring_buffer::LogRingBuffer::new(tx)
            },
            sync_mode: String::new(),
            is_running: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            failed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            max_streams: 32,
            max_fds: 64,
            max_index_ram_bytes: 16 * 1024 * 1024,
            index_ram_bytes: index_ram_bytes.clone(),
            index_high_water_percent: AtomicU8::new(90),
            index_high_water_reached: Arc::new(AtomicBool::new(false)),
            flush_fsyncs: Arc::new(AtomicU64::new(0)),
            flush_batches: Arc::new(AtomicU64::new(0)),
            flush_entries: Arc::new(AtomicU64::new(0)),
            streams_set: streams_set.clone(),
            pending_inserts: Arc::new(Mutex::new(HashSet::new())),
            maintenance_lock: Arc::new(Mutex::new(())),
            retention: Arc::new(Mutex::new(RetentionPolicy::default())),
            max_connections: 1,
            broadcast_capacity: 4096,
            conn_semaphore: Arc::new(tokio::sync::Semaphore::new(1)),
            compaction_counter: Arc::new(AtomicU64::new(u64::MAX / 2)),
            compaction_threshold_segments: 4,
            compaction_threshold_bytes: 64 * 1024 * 1024,
            segment_id_counter: Arc::new(AtomicU64::new(1)),
            write_lock: Arc::new(Mutex::new(())),
            timestamp_indexes,
            flusher_handle: None,
            max_scan_results: 100_000,
            frames_read: AtomicU64::new(0),
            commit_drain_window_us: 5000,
            commit_max_batch_size: 2048,
            recovery_mode: mode,
            lock_timeout_secs: 5,
            recovery_report: None,
            max_normal_segment_id: 0,
            max_segment_id: 0,
        };

        engine.recover()?;
        Ok(engine.recovery_report.clone().unwrap_or_default())
    }

    /// Gets the next monotonic sequence ID
    pub fn next_sequence_id(&self) -> u64 {
        self.current_sequence_id.load(Ordering::SeqCst)
    }

    /// Returns the segment ID currently receiving writes. Normal rollover IDs
    /// are always below `u64::MAX / 2`; compaction output is always above it.
    pub fn active_segment_id(&self) -> u64 {
        self.active_segment_id.load(Ordering::SeqCst)
    }

    /// Helper to get a segment path from its ID
    fn segment_path(&self, segment_id: u64) -> PathBuf {
        self.data_dir.join(format!("{:020}.liven", segment_id))
    }

    fn get_or_load_file_handle(&self, segment_id: u64) -> crate::error::Result<Arc<File>> {
        let mut handles_guard = self.file_handles.lock().unwrap();
        if let Some(file_ref) = handles_guard.get(&segment_id) {
            return Ok(file_ref.clone());
        }

        // Enforce file descriptor limits by evicting inactive handles if cache is full
        while handles_guard.len() >= self.max_fds.saturating_sub(5) {
            if let Some(evict_id) = handles_guard.keys().next().copied() {
                handles_guard.remove(&evict_id);
            } else {
                break;
            }
        }

        let path = self.segment_path(segment_id);
        let file = File::open(&path).map_err(LivenError::Io)?;
        let file_ref = Arc::new(file);

        handles_guard.insert(segment_id, file_ref.clone());
        Ok(file_ref)
    }

    /// Scans the directory and rebuilds the concurrent B-tree index
    fn recover(&mut self) -> crate::error::Result<()> {
        let mut segments = Vec::new();

        for entry in fs::read_dir(&self.data_dir).map_err(LivenError::Io)? {
            let entry = entry.map_err(LivenError::Io)?;
            let path = entry.path();
            if path.is_file()
                && path.extension().and_then(|s| s.to_str()) == Some("liven")
                && let Some(stem) = path.file_stem().and_then(|s| s.to_str())
                && let Ok(segment_id) = stem.parse::<u64>()
            {
                segments.push((segment_id, path));
            }
        }

        segments.sort_by_key(|a| a.0);

        // Track the id ceilings up front (no file I/O). The active normal
        // segment is chosen *after* the single replay pass below using the
        // highest complete sequence id per segment, so a segment file is never
        // scanned twice just to pick the write target. Compaction output lives
        // in a reserved high range and must never become active.
        let mut max_normal_segment_id = 0;
        let mut max_segment_id = 0;
        for (segment_id, _) in &segments {
            max_segment_id = max_segment_id.max(*segment_id);
            if *segment_id < COMPACTION_ID_BASE {
                max_normal_segment_id = max_normal_segment_id.max(*segment_id);
            }
        }
        self.max_normal_segment_id = max_normal_segment_id;
        self.max_segment_id = max_segment_id;

        // Per-normal-segment summary collected during the single replay pass:
        // (segment_id, highest complete seq, has at least one complete frame).
        let mut normal_summaries: Vec<(u64, u64, bool)> = Vec::new();
        // Tears are deferred until the active segment is known; this is what
        // lets the replay pass be the only pass over each segment file.
        let mut deferred_tears: Vec<DeferredTear> = Vec::new();

        let mut highest_seq = 0;
        // Highest sequence ID seen for each key during this replay, including
        // tombstones. A tombstone removes the index entry, so without this
        // watermark an older frame replayed later (for example from a compacted
        // segment) would see `current = None` and resurrect a deleted key.
        let mut seq_watermarks: HashMap<String, u64> = HashMap::new();
        let mut report = RecoveryReport::default();

        for (segment_id, path) in &segments {
            let mut file = File::open(path).map_err(LivenError::Io)?;
            self.lock_segment_with_timeout(&mut file, *segment_id, path)?;

            let file_len = file.metadata().map_err(LivenError::Io)?.len();
            if file_len == 0 {
                file.unlock().map_err(LivenError::Io)?;
                if *segment_id < COMPACTION_ID_BASE {
                    normal_summaries.push((*segment_id, 0, false));
                }
                continue;
            }
            let mut offset = 0;
            let mut header_buf = [0u8; 26];
            // Highest complete seq in this segment (header + payload fits).
            // Mirrors the removed `segment_scan_summary` and feeds active-id
            // selection; it intentionally ignores CRC/decode failures.
            let mut seg_max_seq = 0u64;
            let mut seg_has_frame = false;
            // Highest successfully-decoded seq before a tear, used for resync.
            let mut max_seq_in_prefix = 0u64;
            // True once a tear is recorded for this segment; suppresses the
            // trailing-byte check and prevents a second tear being recorded.
            let mut segment_torn = false;

            while offset + 26 <= file_len {
                file.seek(SeekFrom::Start(offset)).map_err(LivenError::Io)?;
                if file.read_exact(&mut header_buf).is_err() {
                    // Partial header at end of file — torn write.
                    if offset < file_len {
                        deferred_tears.push(DeferredTear {
                            segment_id: *segment_id,
                            file_len,
                            offset,
                            max_seq_in_prefix,
                            kind: TearKind::Header,
                            declared_payload_len: 0,
                            parse_error: None,
                        });
                        segment_torn = true;
                    }
                    break;
                }

                let seq_id = u64::from_be_bytes([
                    header_buf[0],
                    header_buf[1],
                    header_buf[2],
                    header_buf[3],
                    header_buf[4],
                    header_buf[5],
                    header_buf[6],
                    header_buf[7],
                ]);
                let _timestamp = i64::from_be_bytes([
                    header_buf[8],
                    header_buf[9],
                    header_buf[10],
                    header_buf[11],
                    header_buf[12],
                    header_buf[13],
                    header_buf[14],
                    header_buf[15],
                ]);
                let type_tag = header_buf[16];
                let flags = header_buf[17];
                let payload_len = u32::from_be_bytes([
                    header_buf[18],
                    header_buf[19],
                    header_buf[20],
                    header_buf[21],
                ]) as u64;
                let declared_crc = u32::from_be_bytes([
                    header_buf[22],
                    header_buf[23],
                    header_buf[24],
                    header_buf[25],
                ]);

                // Incomplete payload — torn write.
                if offset + 26 + payload_len > file_len {
                    deferred_tears.push(DeferredTear {
                        segment_id: *segment_id,
                        file_len,
                        offset,
                        max_seq_in_prefix,
                        kind: TearKind::Payload,
                        declared_payload_len: payload_len,
                        parse_error: None,
                    });
                    segment_torn = true;
                    break;
                }

                // A complete frame (header and payload both fit). Count it for
                // active-id selection before any CRC/decode validation.
                seg_max_seq = seg_max_seq.max(seq_id);
                seg_has_frame = true;

                // Read payload and verify CRC
                let mut payload_buf = vec![0u8; payload_len as usize];
                file.read_exact(&mut payload_buf).map_err(LivenError::Io)?;

                let actual_crc = crc32fast::hash(&payload_buf);
                if actual_crc != declared_crc {
                    deferred_tears.push(DeferredTear {
                        segment_id: *segment_id,
                        file_len,
                        offset,
                        max_seq_in_prefix,
                        kind: TearKind::Crc,
                        declared_payload_len: 0,
                        parse_error: None,
                    });
                    segment_torn = true;
                    break;
                }

                // Decode payload to extract stream name and key
                match deserialize_payload_borrowed(&payload_buf, type_tag) {
                    Ok((stream_name, key, _value)) => {
                        let compound_key = format!("{}:{}", stream_name, key);
                        if seq_id > highest_seq {
                            highest_seq = seq_id;
                        }

                        let _timestamp = i64::from_be_bytes([
                            header_buf[8],
                            header_buf[9],
                            header_buf[10],
                            header_buf[11],
                            header_buf[12],
                            header_buf[13],
                            header_buf[14],
                            header_buf[15],
                        ]);

                        let node_overhead = primary_charge(&compound_key);

                        // Segments are replayed in ascending segment-id order, but a
                        // compacted segment carries an id in the reserved high range
                        // while holding *older* sequence ids. Merge by sequence id so a
                        // stale compacted frame can never overwrite a newer write, and an
                        // old tombstone can never resurrect or delete a newer record.
                        let current_seq = seq_watermarks
                            .get(&compound_key)
                            .copied()
                            .or_else(|| self.skipmap.get(&compound_key).map(|p| p.seq_id));
                        let is_newer = current_seq.is_none_or(|cur| seq_id > cur);
                        if is_newer {
                            seq_watermarks.insert(compound_key.clone(), seq_id);
                        }

                        if flags & 0x02 != 0 {
                            // Tombstone — drop the timestamp-index entry, then remove
                            // from the index only; do not add stream to streams_set
                            if is_newer {
                                self.prune_timestamp_index(stream_name, key);
                                if self.skipmap.remove(&compound_key).is_some() {
                                    self.index_ram_bytes
                                        .fetch_sub(node_overhead, Ordering::SeqCst);
                                }
                            }
                        } else if flags & 0x01 != 0 {
                            // Active — register the stream in streams_set and check limit
                            {
                                let mut streams_guard = self.streams_set.lock().unwrap();
                                streams_guard.insert(stream_name.to_string());
                                if streams_guard.len() > self.max_streams {
                                    return Err(LivenError::StreamLimitExceeded {
                                        max: self.max_streams,
                                    });
                                }
                            }
                            if is_newer {
                                // Drop the superseded timestamp entry first, then charge
                                // for the replacement timestamp entry. A new key also
                                // pays for its primary-index node.
                                self.prune_timestamp_index(stream_name, key);
                                let existed = self.skipmap.contains_key(&compound_key);
                                let charge = if existed {
                                    TIMESTAMP_INDEX_NODE_BYTES
                                } else {
                                    node_overhead + TIMESTAMP_INDEX_NODE_BYTES
                                };
                                let current_ram =
                                    self.index_ram_bytes.fetch_add(charge, Ordering::SeqCst)
                                        + charge;
                                if current_ram > self.max_index_ram_bytes {
                                    if self.recovery_mode == RecoveryMode::Strict {
                                        return Err(create_index_ram_limit_error(
                                            self.max_index_ram_bytes,
                                            current_ram,
                                        ));
                                    }
                                    let needed_mb = current_ram.div_ceil(1024 * 1024).max(1);
                                    warn!(
                                        "RECOVERY: index RAM limit exceeded: {} bytes used of {} allowed. Loaded anyway; new keys will be rejected until usage drops or the limit is raised. Set limits.max_index_ram_mb to at least {} to clear this.",
                                        current_ram, self.max_index_ram_bytes, needed_mb
                                    );
                                    self.index_high_water_reached.store(true, Ordering::SeqCst);
                                }

                                let pointer = LogPointer {
                                    segment_id: *segment_id,
                                    file_offset: offset,
                                    seq_id,
                                    timestamp: _timestamp,
                                };
                                self.skipmap.insert(compound_key, pointer);

                                // Populate timestamp index
                                let mut ts_idx = self.timestamp_indexes.lock().unwrap();
                                let entry =
                                    ts_idx.entry(stream_name.to_string()).or_insert_with(|| {
                                        Arc::new(timestamp_index::TimestampIndex::new())
                                    });
                                Arc::get_mut(entry)
                                    .unwrap()
                                    .insert(_timestamp, seq_id, pointer);
                            }
                        }
                    }
                    Err(e) => {
                        deferred_tears.push(DeferredTear {
                            segment_id: *segment_id,
                            file_len,
                            offset,
                            max_seq_in_prefix,
                            kind: TearKind::Parse,
                            declared_payload_len: 0,
                            parse_error: Some(e.to_string()),
                        });
                        segment_torn = true;
                        break;
                    }
                }

                max_seq_in_prefix = max_seq_in_prefix.max(seq_id);
                offset += 26 + payload_len;
            }

            file.unlock().map_err(LivenError::Io)?;

            // Detect trailing bytes after the last complete frame (a torn write
            // that leaves a partial header between frame boundaries). Only the
            // active segment is truncated; the decision is deferred below.
            if !segment_torn && offset < file_len {
                deferred_tears.push(DeferredTear {
                    segment_id: *segment_id,
                    file_len,
                    offset,
                    max_seq_in_prefix,
                    kind: TearKind::Trailing,
                    declared_payload_len: 0,
                    parse_error: None,
                });
            }

            if *segment_id < COMPACTION_ID_BASE {
                normal_summaries.push((*segment_id, seg_max_seq, seg_has_frame));
            }
        }

        // Choose the active normal segment with the exact pre-pass rules: the
        // newest normal file wins when it holds no complete frame (a crash
        // immediately after rollover), otherwise the segment with the highest
        // complete sequence id (write recency) — never by max file id.
        let highest_normal = normal_summaries
            .iter()
            .max_by_key(|(segment_id, _, _)| *segment_id);
        let active_id = match highest_normal {
            Some((segment_id, _, false)) => *segment_id,
            _ => normal_summaries
                .iter()
                .filter(|(_, _, has_frame)| *has_frame)
                .max_by_key(|(segment_id, max_seq, _)| (*max_seq, *segment_id))
                .map(|(segment_id, _, _)| *segment_id)
                .unwrap_or(1),
        };

        self.process_deferred_tears(deferred_tears, active_id, &mut report)?;

        self.current_sequence_id
            .store(highest_seq + 1, Ordering::SeqCst);
        self.active_segment_id.store(active_id, Ordering::SeqCst);

        // Open active file handle
        let active_path = self.segment_path(active_id);
        let active_existed = active_path.exists();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&active_path)
            .map_err(LivenError::Io)?;
        if !active_existed {
            pl_create(&active_path);
            // The active segment was just created. Persist its directory entry
            // before any write can be acknowledged; otherwise a power loss can
            // remove the whole file even though its data was fsynced.
            sync_data_dir(&self.data_dir).map_err(LivenError::Io)?;
        }

        let active_len = file.metadata().map_err(LivenError::Io)?.len();

        let mut active_file_guard = self.active_file.lock().unwrap();
        let mut active_size_guard = self.active_size.lock().unwrap();
        *active_file_guard = Some(file);
        *active_size_guard = active_len;

        // Log recovery summary if any issues were found
        if report.had_issues {
            warn!(
                "RECOVERY REPORT: {} truncated segment(s), {} skipped segment(s).",
                report.truncated_segments.len(),
                report.skipped_segments.len()
            );
            for (seg_id, off, discarded) in &report.truncated_segments {
                warn!(
                    "  Truncated segment {} at offset {}, {} bytes discarded",
                    seg_id, off, discarded
                );
            }
            for (seg_id, off, keys) in &report.skipped_segments {
                warn!(
                    "  Skipped segment {} (corrupted at offset {}, ~{} keys affected)",
                    seg_id, off, keys
                );
            }
            for backup in &report.repair_backups {
                warn!("  Repair backup: {}", backup.display());
            }
            for backup in &report.torn_backups {
                warn!("  Torn-tail backup: {}", backup.display());
            }
            for (target_id, target) in &report.salvage_segments {
                warn!(
                    "  Salvaged frames written to segment {} ({})",
                    target_id,
                    target.display()
                );
            }
            if report.salvaged_frames > 0 {
                warn!("  Salvaged {} frame(s) total", report.salvaged_frames);
            }
        }

        info!(
            "LIVEN Engine started. Active Segment ID: {}, Next Sequence ID: {}, Index Entries: {}",
            active_id,
            highest_seq + 1,
            self.skipmap.len()
        );

        self.recovery_report = Some(report);

        Ok(())
    }

    /// Applies deferred tear decisions after the active segment has been chosen
    /// by the single replay pass. This replaces the inline tear handling that
    /// previously required a pre-pass to know the active segment up front.
    fn process_deferred_tears(
        &mut self,
        deferred_tears: Vec<DeferredTear>,
        active_id: u64,
        report: &mut RecoveryReport,
    ) -> crate::error::Result<()> {
        for tear in deferred_tears {
            // Leftover bytes in an inactive segment are ignored, not truncated.
            if tear.kind == TearKind::Trailing && tear.segment_id != active_id {
                continue;
            }
            let is_active = tear.segment_id == active_id;
            let path = self.segment_path(tear.segment_id);
            let file = File::open(&path).map_err(LivenError::Io)?;

            if matches!(tear.kind, TearKind::Crc | TearKind::Parse) {
                self.maybe_backup_for_repair(&path, tear.segment_id, report)?;
            }
            if tear.kind == TearKind::Parse && self.recovery_mode == RecoveryMode::Strict {
                error!(
                    "Strict mode: unparseable payload in segment {} at offset {}: {}",
                    tear.segment_id,
                    tear.offset,
                    tear.parse_error.as_deref().unwrap_or("")
                );
            }

            let mut segment_corrupted = false;
            let mut truncate_to: Option<u64> = None;
            let mut truncate_marker: Option<PathBuf> = None;

            match self.handle_tear(
                tear.segment_id,
                &path,
                &file,
                tear.file_len,
                tear.offset,
                tear.max_seq_in_prefix,
                is_active,
                tear.kind,
                report,
            )? {
                TearDecision::Truncate { offset: t, marker } => {
                    match tear.kind {
                        TearKind::Header => warn!(
                            "Torn header at offset {} in segment {} ({} bytes discarded). Truncating to {}.",
                            tear.offset,
                            tear.segment_id,
                            tear.file_len - tear.offset,
                            t
                        ),
                        TearKind::Payload => warn!(
                            "Torn payload at offset {} in segment {} (declared {} bytes, {} available). Truncating to {}.",
                            tear.offset,
                            tear.segment_id,
                            tear.declared_payload_len,
                            tear.file_len.saturating_sub(tear.offset + 26),
                            t
                        ),
                        TearKind::Crc => warn!(
                            "CRC mismatch at offset {} in segment {} - torn write assumed. Truncating to offset {} ({} bytes discarded).",
                            tear.offset,
                            tear.segment_id,
                            t,
                            tear.file_len - t
                        ),
                        TearKind::Parse => warn!(
                            "Unparseable payload at offset {} in segment {} - torn write assumed. Truncating to {} ({} bytes discarded).",
                            tear.offset,
                            tear.segment_id,
                            t,
                            tear.file_len - t
                        ),
                        TearKind::Trailing => warn!(
                            "Trailing bytes after last frame in segment {} (offset {}, {} bytes discarded). Truncating to {}.",
                            tear.segment_id,
                            tear.offset,
                            tear.file_len - tear.offset,
                            t
                        ),
                    }
                    report
                        .truncated_segments
                        .push((tear.segment_id, t, tear.file_len - t));
                    truncate_to = Some(t);
                    truncate_marker = marker;
                }
                TearDecision::SkipRemainder => match tear.kind {
                    TearKind::Header => {}
                    TearKind::Payload => {
                        warn!(
                            "Incomplete payload in inactive segment {} at offset {}. Skipping remainder.",
                            tear.segment_id, tear.offset
                        );
                        report
                            .skipped_segments
                            .push((tear.segment_id, tear.offset, 0));
                        report.had_issues = true;
                    }
                    TearKind::Crc | TearKind::Parse => {
                        let approx_keys = self
                            .skipmap
                            .iter()
                            .filter(|(_, ptr)| ptr.segment_id == tear.segment_id)
                            .count();
                        if tear.kind == TearKind::Crc {
                            warn!(
                                "CRC mismatch in inactive segment {} at offset {} - skipping segment (~{} keys potentially affected).",
                                tear.segment_id, tear.offset, approx_keys
                            );
                        } else {
                            warn!(
                                "Unparseable payload in inactive segment {} at offset {} - skipping segment (~{} keys affected).",
                                tear.segment_id, tear.offset, approx_keys
                            );
                        }
                        report
                            .skipped_segments
                            .push((tear.segment_id, tear.offset, approx_keys));
                        report.had_issues = true;
                        segment_corrupted = true;
                    }
                    TearKind::Trailing => {}
                },
            }

            if segment_corrupted {
                let keys_to_remove: Vec<String> = self
                    .skipmap
                    .iter()
                    .filter(|(_, ptr)| ptr.segment_id == tear.segment_id)
                    .map(|(key, _)| key)
                    .collect();
                for key in &keys_to_remove {
                    if self.skipmap.remove(key).is_some() {
                        let overhead = primary_charge(key);
                        self.index_ram_bytes.fetch_sub(overhead, Ordering::SeqCst);
                    }
                }
            }

            if let Some(target) = truncate_to {
                let w = OpenOptions::new()
                    .write(true)
                    .open(&path)
                    .map_err(LivenError::Io)?;
                w.set_len(target).map_err(LivenError::Io)?;
                w.sync_all().map_err(LivenError::Io)?;
                pl_truncate_sync(&path, target);
                test_crash_point("repair_after_truncate");
                info!(
                    "Truncated segment {} to {} bytes (torn tail removed)",
                    tear.segment_id, target
                );
                if let Some(marker) = truncate_marker.take() {
                    pl_unlink(&marker);
                    let _ = fs::remove_file(&marker);
                    sync_data_dir(&self.data_dir).map_err(LivenError::Io)?;
                }
            }
        }
        Ok(())
    }

    /// Decide what to do at a torn/invalid frame. In Repair mode, stranded
    /// valid frames are copied to a new normal-range segment before the source
    /// tail is truncated. In Default/Strict mode, stranded frames cause
    /// `RecoveryRequired` instead of silent destruction.
    #[allow(clippy::too_many_arguments)]
    fn handle_tear(
        &mut self,
        segment_id: u64,
        path: &Path,
        file: &File,
        file_len: u64,
        tear_offset: u64,
        max_seq_in_prefix: u64,
        is_active: bool,
        kind: TearKind,
        report: &mut RecoveryReport,
    ) -> crate::error::Result<TearDecision> {
        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let normal_range = segment_id < COMPACTION_ID_BASE;
        let min_seq = if normal_range {
            Some(max_seq_in_prefix)
        } else {
            None
        };
        let start = tear_offset.saturating_add(1);
        let (stranded, stranded_end) = match find_resync(file, file_len, start, now_ms, min_seq)? {
            Some(resync) => collect_stranded(file, file_len, resync, now_ms, min_seq)?,
            None => (Vec::new(), tear_offset),
        };

        if !stranded.is_empty() {
            if self.recovery_mode != RecoveryMode::Repair {
                // Name the bad frame and any torn frame after the last good
                // stranded frame so an operator knows what is at risk.
                let mut keys = Vec::new();
                if kind != TearKind::Trailing
                    && let Some(key) = extract_key_at(file, file_len, tear_offset)
                {
                    keys.push(key);
                }
                if stranded_end < file_len
                    && let Some(key) = extract_key_at(file, file_len, stranded_end)
                {
                    keys.push(key);
                }
                keys.sort();
                keys.dedup();
                let keys_label = if keys.is_empty() {
                    "none parseable".to_string()
                } else {
                    keys.join(", ")
                };
                warn!(
                    "RECOVERY REPORT: segment {} has a torn region at offset {} followed by {} valid stranded frame(s). Unrecoverable key(s): {}. Refusing to truncate.",
                    segment_id,
                    tear_offset,
                    stranded.len(),
                    keys_label
                );
                return Err(LivenError::RecoveryRequired {
                    segment_id,
                    offset: tear_offset,
                    stranded: stranded.len(),
                    keys: keys_label,
                });
            }

            // Order matters: write and fsync the salvage segment, fsync its
            // directory entry, then write and fsync the torn-region sidecar
            // (and its directory). Only then may the caller truncate.
            let (target_id, marker) =
                self.write_salvage_segment(path, file, tear_offset, &stranded, report)?;
            test_crash_point("repair_after_salvage");

            let backup = backup_torn_region(path, tear_offset)?;
            info!(
                "Torn-tail backup for segment {}: {}",
                segment_id,
                backup.display()
            );
            report.torn_backups.push(backup);
            report.had_issues = true;
            test_crash_point("repair_after_backup");

            info!(
                "Salvaged {} stranded frame(s) from segment {} into segment {}",
                stranded.len(),
                segment_id,
                target_id
            );
            report.salvaged_frames += stranded.len();
            return Ok(TearDecision::Truncate {
                offset: tear_offset,
                marker: Some(marker),
            });
        }

        if self.recovery_mode == RecoveryMode::Strict
            && matches!(kind, TearKind::Crc | TearKind::Parse)
        {
            return Err(LivenError::StrictCrcMismatch {
                segment_id,
                offset: tear_offset,
            });
        }

        // Inactive segments are left on disk and simply stop contributing
        // frames after the tear, matching pre-salvage behavior. Repair may
        // still truncate them above, after salvaging stranded frames.
        if !is_active && kind != TearKind::Trailing {
            return Ok(TearDecision::SkipRemainder);
        }

        let backup = backup_torn_region(path, tear_offset)?;
        info!(
            "Torn-tail backup for segment {}: {}",
            segment_id,
            backup.display()
        );
        report.torn_backups.push(backup);
        report.had_issues = true;
        test_crash_point("repair_after_backup");
        Ok(TearDecision::Truncate {
            offset: tear_offset,
            marker: None,
        })
    }

    fn next_salvage_id(&self) -> crate::error::Result<u64> {
        let mut id = self.max_normal_segment_id.saturating_add(1).max(1);
        while self.segment_path(id).exists() {
            id += 1;
        }
        if id >= COMPACTION_ID_BASE {
            return Err(LivenError::Internal(
                "normal segment ID space exhausted while salvaging".to_string(),
            ));
        }
        Ok(id)
    }

    /// Write stranded frames into a fresh normal-range segment. A marker file
    /// records the target ID and length so a crash between the salvage write and
    /// the source truncation is idempotent on rerun.
    fn write_salvage_segment(
        &mut self,
        source: &Path,
        file: &File,
        tear_offset: u64,
        frames: &[StrandedFrame],
        report: &mut RecoveryReport,
    ) -> crate::error::Result<(u64, PathBuf)> {
        let total_len: u64 = frames.iter().map(|f| f.len).sum();
        let marker = salvage_marker_path(source, tear_offset);
        let existing = read_salvage_marker(&marker);

        let target_id = match existing {
            Some((id, _)) if id > 0 && id < COMPACTION_ID_BASE => id,
            _ => self.next_salvage_id()?,
        };
        let target_path = self.segment_path(target_id);
        let reuse = existing.is_some_and(|(id, len)| id == target_id && len == total_len)
            && fs::metadata(&target_path)
                .map(|m| m.len() == total_len)
                .unwrap_or(false);

        write_salvage_marker(&marker, target_id, total_len)?;
        if !reuse {
            let mut out = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&target_path)
                .map_err(LivenError::Io)?;
            pl_create(&target_path);
            let mut buf = vec![0u8; 64 * 1024];
            for frame in frames {
                let mut read = 0u64;
                while read < frame.len {
                    let n = std::cmp::min(buf.len() as u64, frame.len - read) as usize;
                    file.read_exact_at(&mut buf[..n], frame.offset + read)
                        .map_err(LivenError::Io)?;
                    out.write_all(&buf[..n]).map_err(LivenError::Io)?;
                    read += n as u64;
                }
            }
            out.sync_all().map_err(LivenError::Io)?;
            pl_sync(&target_path, total_len);
            test_crash_point("repair_after_salvage_write");
            sync_data_dir(&self.data_dir).map_err(LivenError::Io)?;
            test_crash_point("repair_after_salvage_dir_sync");
        }

        self.max_normal_segment_id = self.max_normal_segment_id.max(target_id);
        self.max_segment_id = self.max_segment_id.max(target_id);
        report.salvage_segments.push((target_id, target_path));
        Ok((target_id, marker))
    }

    /// Attempts to acquire a shared advisory lock on `file` with a timeout.
    /// If `lock_timeout_secs` is 0, blocks indefinitely.
    fn lock_segment_with_timeout(
        &self,
        file: &mut File,
        segment_id: u64,
        path: &Path,
    ) -> crate::error::Result<()> {
        if self.lock_timeout_secs == 0 {
            return file.lock_shared().map_err(LivenError::Io);
        }
        let deadline = Instant::now() + Duration::from_secs(self.lock_timeout_secs);
        loop {
            match file.try_lock_shared() {
                Ok(()) => return Ok(()),
                Err(_) => {
                    if Instant::now() >= deadline {
                        let path_str = path.display().to_string();
                        return Err(LivenError::SegmentLockTimeout {
                            segment_id,
                            path: path_str,
                        });
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }
    }

    /// In Repair mode, creates a timestamped backup copy before recovery actions.
    fn maybe_backup_for_repair(
        &self,
        path: &Path,
        segment_id: u64,
        report: &mut RecoveryReport,
    ) -> crate::error::Result<()> {
        if self.recovery_mode != RecoveryMode::Repair {
            return Ok(());
        }
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let backup_name = format!("{:020}.liven.repair-{}", segment_id, timestamp);
        let backup_path = self.data_dir.join(&backup_name);
        fs::copy(path, &backup_path).map_err(LivenError::Io)?;
        let backup_len = fs::metadata(&backup_path).map(|m| m.len()).unwrap_or(0);
        pl_create(&backup_path);
        pl_write(&backup_path, 0, backup_len);
        // The copy must be durable before the caller truncates the source.
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(&backup_path)
            .map_err(LivenError::Io)?
            .sync_all()
            .map_err(LivenError::Io)?;
        pl_sync(&backup_path, backup_len);
        sync_data_dir(&self.data_dir).map_err(LivenError::Io)?;
        info!(
            "Repair backup: {} -> {}",
            path.display(),
            backup_path.display()
        );
        report.repair_backups.push(backup_path);
        Ok(())
    }

    /// Appends a new record to the storage engine
    fn ensure_writable(&self) -> crate::error::Result<()> {
        if self.failed.load(Ordering::SeqCst) {
            return Err(LivenError::Storage(
                "storage engine is in a failed state after an I/O error; reopen the database"
                    .to_string(),
            ));
        }
        if !self.is_running.load(Ordering::Relaxed) {
            return Err(LivenError::FlusherTerminated);
        }
        Ok(())
    }

    /// True after a write/fsync failure. Future writes are rejected until the
    /// database is reopened (which runs recovery).
    pub fn is_failed(&self) -> bool {
        self.failed.load(Ordering::SeqCst)
    }

    pub fn flush_metrics(&self) -> FlushMetrics {
        FlushMetrics {
            fsyncs: self.flush_fsyncs.load(Ordering::Relaxed),
            batches: self.flush_batches.load(Ordering::Relaxed),
            entries: self.flush_entries.load(Ordering::Relaxed),
        }
    }

    pub fn reset_flush_metrics(&self) {
        self.flush_fsyncs.store(0, Ordering::Relaxed);
        self.flush_batches.store(0, Ordering::Relaxed);
        self.flush_entries.store(0, Ordering::Relaxed);
    }

    pub fn set_index_high_water_percent(&self, percent: u8) {
        self.index_high_water_percent
            .store(percent.clamp(1, 100), Ordering::Relaxed);
    }

    pub fn index_usage(&self) -> IndexUsage {
        let limit_bytes = self.max_index_ram_bytes;
        let high_water_bytes = limit_bytes
            .saturating_mul(self.index_high_water_percent.load(Ordering::Relaxed) as u64)
            / 100;
        let used_bytes = self.index_ram_bytes.load(Ordering::SeqCst);
        IndexUsage {
            used_bytes,
            limit_bytes,
            high_water_bytes,
            // Live value: retention and deletes must clear it.
            high_water_reached: used_bytes >= high_water_bytes,
        }
    }

    fn note_high_water(&self, projected_bytes: u64) {
        let high_water = self
            .max_index_ram_bytes
            .saturating_mul(self.index_high_water_percent.load(Ordering::Relaxed) as u64)
            / 100;
        if projected_bytes >= high_water
            && !self.index_high_water_reached.swap(true, Ordering::SeqCst)
        {
            warn!(
                "index RAM high-water mark reached: {} of {} bytes ({}%)",
                projected_bytes,
                self.max_index_ram_bytes,
                self.index_high_water_percent.load(Ordering::Relaxed)
            );
        }
    }

    /// Marks a key as having an in-flight insert. Returns false if another
    /// insert for the same key is already pending.
    pub(crate) fn mark_insert_pending(&self, compound_key: &str) -> bool {
        self.pending_inserts
            .lock()
            .unwrap()
            .insert(compound_key.to_string())
    }

    pub(crate) fn clear_insert_pending(&self, compound_key: &str) {
        self.pending_inserts.lock().unwrap().remove(compound_key);
    }

    #[cfg(test)]
    pub(crate) fn pending_insert_count(&self) -> usize {
        self.pending_inserts.lock().unwrap().len()
    }

    /// Enqueues an append and returns the receiver for its acknowledgement.
    /// The caller can release application locks before waiting for fsync.
    pub(crate) fn append_async(
        &self,
        stream_name: &str,
        key: &str,
        value: DataValue,
        is_tombstone: bool,
    ) -> crate::error::Result<std::sync::mpsc::Receiver<Result<Record, ring_buffer::AckError>>>
    {
        self.ensure_writable()?;
        let compound_key = format!("{}:{}", stream_name, key);

        {
            let mut streams_guard = self.streams_set.lock().unwrap();
            if !streams_guard.contains(stream_name) {
                if streams_guard.len() >= self.max_streams {
                    return Err(LivenError::StreamLimitExceeded {
                        max: self.max_streams,
                    });
                }
                streams_guard.insert(stream_name.to_string());
            }
        }

        if !is_tombstone && !self.skipmap.contains_key(&compound_key) {
            let size = primary_charge(&compound_key) + TIMESTAMP_INDEX_NODE_BYTES;
            let current_ram = self.index_ram_bytes.load(Ordering::SeqCst);
            let projected = current_ram + size;
            if projected > self.max_index_ram_bytes {
                return Err(create_index_ram_limit_error(
                    self.max_index_ram_bytes,
                    projected,
                ));
            }
            self.note_high_water(projected);
        }

        let stream_key = crate::storage::key::StreamKey::from_str(key)?;
        let (response_tx, rx) = std::sync::mpsc::sync_channel(1);
        self.ring_buffer.tx_send(ring_buffer::LogEntry::Single {
            stream_name: stream_name.to_string(),
            key: stream_key,
            value,
            is_tombstone,
            response_tx,
        })?;
        Ok(rx)
    }

    /// Atomically replaces a key with a single active frame.
    ///
    /// Unlike the old tombstone-then-append sequence, a crash while the frame
    /// is being written leaves the previous value live. This is the write path
    /// used by upsert and update.
    pub fn replace(
        &self,
        stream_name: &str,
        key: &str,
        value: DataValue,
    ) -> crate::error::Result<Record> {
        self.ensure_writable()?;
        let compound_key = format!("{}:{}", stream_name, key);

        {
            let mut streams_guard = self.streams_set.lock().unwrap();
            if !streams_guard.contains(stream_name) {
                if streams_guard.len() >= self.max_streams {
                    return Err(LivenError::StreamLimitExceeded {
                        max: self.max_streams,
                    });
                }
                streams_guard.insert(stream_name.to_string());
            }
        }

        if !self.skipmap.contains_key(&compound_key) {
            let size = primary_charge(&compound_key) + TIMESTAMP_INDEX_NODE_BYTES;
            let current_ram = self.index_ram_bytes.load(Ordering::SeqCst);
            let projected = current_ram + size;
            if projected > self.max_index_ram_bytes {
                return Err(create_index_ram_limit_error(
                    self.max_index_ram_bytes,
                    projected,
                ));
            }
            self.note_high_water(projected);
        }

        let stream_key = crate::storage::key::StreamKey::from_str(key)?;
        let (response_tx, rx) = std::sync::mpsc::sync_channel(1);
        self.ring_buffer.tx_send(ring_buffer::LogEntry::Replace {
            stream_name: stream_name.to_string(),
            key: stream_key,
            value,
            response_tx,
        })?;
        rx.recv()
            .map_err(|e| LivenError::Internal(format!("LogRingBuffer coordination error: {}", e)))?
            .map_err(Into::into)
    }

    pub fn append(
        &self,
        stream_name: &str,
        key: &str,
        value: DataValue,
        is_tombstone: bool,
    ) -> crate::error::Result<Record> {
        let rx = self.append_async(stream_name, key, value, is_tombstone)?;
        rx.recv()
            .map_err(|e| LivenError::Internal(format!("LogRingBuffer coordination error: {}", e)))?
            .map_err(Into::into)
    }

    /// Appends a batch of records to the storage engine with high performance.
    pub fn append_batch(
        &self,
        batch: Vec<(String, String, DataValue)>,
    ) -> crate::error::Result<Vec<Record>> {
        let rx = self.append_batch_async(batch)?;
        rx.recv()
            .map_err(|e| LivenError::Internal(format!("LogRingBuffer coordination error: {}", e)))?
            .map_err(Into::into)
    }

    /// Enqueues an all-or-nothing batch and returns the acknowledgement
    /// receiver. Callers can release application locks before waiting.
    pub(crate) fn append_batch_async(
        &self,
        batch: Vec<(String, String, DataValue)>,
    ) -> crate::error::Result<std::sync::mpsc::Receiver<Result<Vec<Record>, ring_buffer::AckError>>>
    {
        self.ensure_writable()?;
        // Validate the stream budget up front. Index-RAM decisions are made by
        // the flusher, which validates the whole batch atomically before writing
        // any frame: one over-budget new key rejects the entire batch.
        let mut new_streams = HashSet::new();

        {
            let streams_guard = self.streams_set.lock().unwrap();
            for (stream_name, _, _) in &batch {
                if !streams_guard.contains(stream_name) && !new_streams.contains(stream_name) {
                    new_streams.insert(stream_name.clone());
                }
            }

            if streams_guard.len() + new_streams.len() > self.max_streams {
                return Err(LivenError::StreamLimitExceeded {
                    max: self.max_streams,
                });
            }
        }

        // Apply stream registration
        if !new_streams.is_empty() {
            let mut streams_guard = self.streams_set.lock().unwrap();
            for stream in new_streams {
                streams_guard.insert(stream);
            }
        }

        let mut records = Vec::with_capacity(batch.len());
        for (stream_name, key, value) in batch {
            let stream_key = crate::storage::key::StreamKey::from_str(&key)?;
            records.push((stream_name, stream_key, value));
        }

        let (response_tx, rx) = std::sync::mpsc::sync_channel(1);
        self.ring_buffer.tx_send(ring_buffer::LogEntry::Batch {
            records,
            response_tx,
        })?;
        Ok(rx)
    }

    /// Appends a batch of tombstones to the storage engine with high performance (O(1) disk syncs)
    pub fn append_tombstone_batch(
        &self,
        stream_name: &str,
        keys: &[String],
    ) -> crate::error::Result<Vec<Record>> {
        self.ensure_writable()?;
        // Stream Limit Check
        {
            let mut streams_guard = self.streams_set.lock().unwrap();
            if !streams_guard.contains(stream_name) {
                if streams_guard.len() >= self.max_streams {
                    return Err(LivenError::StreamLimitExceeded {
                        max: self.max_streams,
                    });
                }
                streams_guard.insert(stream_name.to_string());
            }
        }

        let mut stream_keys = Vec::with_capacity(keys.len());
        for key in keys {
            stream_keys.push(crate::storage::key::StreamKey::from_str(key)?);
        }
        let (response_tx, rx) = std::sync::mpsc::sync_channel(1);
        let entry = ring_buffer::LogEntry::TombstoneBatch {
            stream_name: stream_name.to_string(),
            keys: stream_keys,
            response_tx,
        };
        self.ring_buffer.tx_send(entry)?;
        let records = rx.recv().map_err(|e| {
            LivenError::Internal(format!("LogRingBuffer coordination error: {}", e))
        })??;
        Ok(records)
    }

    /// Drops a key's entry from its stream's timestamp index using the key's
    /// current live pointer. Called when a key is tombstoned (delete, or the
    /// tombstone half of an upsert/update) so a time-range scan cannot resurrect
    /// the deleted or superseded record from a stale index entry.
    fn prune_timestamp_index(&self, stream_name: &str, key: &str) {
        let compound_key = format!("{}:{}", stream_name, key);
        let Some(ptr) = self.skipmap.get(&compound_key) else {
            return;
        };
        let guard = self.timestamp_indexes.lock().unwrap();
        if let Some(idx) = guard.get(stream_name)
            && idx.remove(ptr.timestamp, ptr.seq_id)
        {
            self.index_ram_bytes
                .fetch_sub(TIMESTAMP_INDEX_NODE_BYTES, Ordering::SeqCst);
        }
    }

    /// Reads a record from disk via its LogPointer using safe positional reads.
    ///
    /// A normal small frame is fetched with a single `pread`: a fixed-size chunk
    /// covers the 26-byte header and the payload. Larger frames fall back to a
    /// payload read, and oversized/corrupt headers get an fstat bounds check.
    pub fn read_record(&self, pointer: LogPointer) -> crate::error::Result<Record> {
        let file = self.get_or_load_file_handle(pointer.segment_id)?;
        let offset = pointer.file_offset;

        const HEADER_LEN: usize = 26;
        const READ_CHUNK: usize = 512;
        let mut chunk = [0u8; READ_CHUNK];
        let n = file.read_at(&mut chunk, offset).map_err(LivenError::Io)?;
        if n < HEADER_LEN {
            return Err(LivenError::PointerOutOfBounds {
                segment_id: pointer.segment_id,
                offset,
            });
        }
        let header_buf = &chunk[..HEADER_LEN];

        let seq_id = u64::from_be_bytes([
            header_buf[0],
            header_buf[1],
            header_buf[2],
            header_buf[3],
            header_buf[4],
            header_buf[5],
            header_buf[6],
            header_buf[7],
        ]);
        let timestamp = i64::from_be_bytes([
            header_buf[8],
            header_buf[9],
            header_buf[10],
            header_buf[11],
            header_buf[12],
            header_buf[13],
            header_buf[14],
            header_buf[15],
        ]);
        let type_tag = header_buf[16];
        let flags = header_buf[17];
        let payload_len = u32::from_be_bytes([
            header_buf[18],
            header_buf[19],
            header_buf[20],
            header_buf[21],
        ]) as usize;
        let crc32 = u32::from_be_bytes([
            header_buf[22],
            header_buf[23],
            header_buf[24],
            header_buf[25],
        ]);

        let frame_len = HEADER_LEN + payload_len;

        // Normal frames are bounded by the segment size; only large or corrupt
        // headers need an fstat to avoid a huge speculative allocation.
        if payload_len as u64 > self.max_segment_size {
            let file_len = file.metadata().map_err(LivenError::Io)?.len();
            if offset + frame_len as u64 > file_len {
                return Err(LivenError::PayloadOutOfBounds {
                    segment_id: pointer.segment_id,
                    offset,
                });
            }
        }

        if frame_len <= n {
            return decode_frame_payload(
                &chunk[HEADER_LEN..frame_len],
                crc32,
                pointer.segment_id,
                offset,
                seq_id,
                timestamp,
                type_tag,
                flags,
            );
        }

        READ_STAGING_BUFFER.with(|buf_cell| {
            let mut local_buf = buf_cell.borrow_mut();
            if local_buf.len() < payload_len {
                local_buf.resize(payload_len, 0);
            }
            let target_slice = &mut local_buf[..payload_len];

            if let Err(e) = file.read_exact_at(target_slice, offset + HEADER_LEN as u64) {
                return if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    Err(LivenError::PayloadOutOfBounds {
                        segment_id: pointer.segment_id,
                        offset,
                    })
                } else {
                    Err(LivenError::Io(e))
                };
            }

            decode_frame_payload(
                target_slice,
                crc32,
                pointer.segment_id,
                offset,
                seq_id,
                timestamp,
                type_tag,
                flags,
            )
        })
    }

    /// Performs complete point-in-time lookup for a stream key with zero stack/heap string allocations
    pub fn get(&self, stream_name: &str, key: &str) -> crate::error::Result<Option<Record>> {
        let mut key_buf = [0u8; 512];
        let combined_len = stream_name.len() + 1 + key.len();
        let lookup_key = if combined_len <= 512 {
            let mut cursor = std::io::Cursor::new(&mut key_buf[..]);
            use std::io::Write;
            let _ = write!(cursor, "{}:{}", stream_name, key);
            let pos = cursor.position() as usize;
            std::str::from_utf8(&key_buf[..pos]).unwrap_or("")
        } else {
            &format!("{}:{}", stream_name, key)
        };

        if let Some(pointer) = self.skipmap.get(lookup_key) {
            let record = self.read_record(pointer)?;
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    /// Lists all streams currently indexed in the primary index
    pub fn list_streams(&self) -> Vec<String> {
        let guard = self.streams_set.lock().unwrap();
        guard.iter().cloned().collect()
    }

    /// Lists all keys under a given stream
    pub fn list_keys(&self, stream_name: &str) -> Vec<String> {
        let prefix = format!("{}:", stream_name);
        let mut keys = Vec::new();
        for (key, _) in self.skipmap.iter() {
            if key.starts_with(&prefix) {
                keys.push(key[prefix.len()..].to_string());
            }
        }
        keys
    }

    /// Scans all historical records from all segments sequentially using safe
    /// positional reads (latest live version per key; tombstones skipped).
    ///
    /// This is a full-database scan; the default read path is
    /// [`StorageEngine::scan_live_stream`]. Kept for the opt-in history view.
    #[tracing::instrument(skip(self))]
    pub fn scan_historical(&self) -> crate::error::Result<Vec<Record>> {
        self.scan_segment_frames(usize::MAX, None, false, true)
    }

    /// Like [`StorageEngine::scan_historical`] but stops early after `max_records`.
    pub fn scan_historical_limited(&self, max_records: usize) -> crate::error::Result<Vec<Record>> {
        self.scan_segment_frames(max_records, None, false, true)
    }

    /// Segment-scan limited to `stream_name` (latest live version per key, no tombstones).
    pub fn scan_historical_stream(
        &self,
        stream_name: &str,
        max_records: usize,
    ) -> crate::error::Result<Vec<Record>> {
        self.scan_segment_frames(max_records, Some(stream_name), false, true)
    }

    /// Opt-in history view for `stream_name`: **every** frame, including tombstones
    /// and superseded versions, in ascending `sequence_id` order.
    ///
    /// Cost is proportional to the whole database (full segment scan), so this is
    /// only used when a query explicitly asks for history.
    pub fn scan_history_stream(
        &self,
        stream_name: &str,
        max_records: usize,
    ) -> crate::error::Result<Vec<Record>> {
        let mut records = self.scan_segment_frames(max_records, Some(stream_name), true, false)?;
        records.sort_by_key(|r| r.sequence_id);
        Ok(records)
    }

    /// Opt-in history view across **all** streams: every frame, including tombstones
    /// and superseded versions, in ascending `sequence_id` order. Full-database scan.
    pub fn scan_history_all(&self, max_records: usize) -> crate::error::Result<Vec<Record>> {
        let mut records = self.scan_segment_frames(max_records, None, true, false)?;
        records.sort_by_key(|r| r.sequence_id);
        Ok(records)
    }

    /// Default read path: return the live records of one stream by ranging over the
    /// in-memory key index (the index holds one live `LogPointer` per key) and
    /// reading each record with a positional read.
    ///
    /// Cost is proportional to the number of live keys in `stream_name`, **not** to
    /// the total database size or the number of segments. Results are returned in
    /// ascending `sequence_id` order (a sort of the stream's records).
    /// `max_scan_results` is applied to the number of records returned.
    pub fn scan_live_stream(
        &self,
        stream_name: &str,
        max_records: usize,
    ) -> crate::error::Result<Vec<Record>> {
        use std::ops::Bound;
        let effective_limit = std::cmp::min(max_records, self.max_scan_results);
        let prefix = format!("{}:", stream_name);

        let mut records: Vec<Record> = Vec::new();
        for (key, mut pointer) in self
            .skipmap
            .range::<str, _>((Bound::Included(prefix.as_str()), Bound::Unbounded))
        {
            if !key.starts_with(prefix.as_str()) {
                break; // past this stream's key range
            }
            if records.len() >= effective_limit {
                break;
            }
            self.frames_read.fetch_add(1, Ordering::Relaxed);
            let record = match self.read_record(pointer) {
                Ok(r) => Some(r),
                Err(_) => {
                    // Compaction may have relocated the record; retry once with a
                    // fresh pointer, and give up if it is now gone.
                    pointer = match self.skipmap.get(&key) {
                        Some(p) => p,
                        None => continue,
                    };
                    self.frames_read.fetch_add(1, Ordering::Relaxed);
                    self.read_record(pointer).ok()
                }
            };
            if let Some(rec) = record {
                // The "stream:" prefix could in principle also match a stream whose
                // name contains ':' (e.g. "stream:x"), so verify the frame's own
                // stream name before accepting it.
                if rec.stream_name == stream_name {
                    records.push(rec);
                }
            }
        }

        if records.len() >= self.max_scan_results && max_records > self.max_scan_results {
            return Err(LivenError::Query(format!(
                "Scan result limit reached ({} records).\n   Refine your query with filter(), limit(), or a key-based lookup.\n   Override with max_scan_results in liven.toml.",
                self.max_scan_results
            )));
        }

        // Deterministic order: ascending sequence_id (a sort of the stream's live
        // records; O(m log m) for m records in the stream).
        records.sort_by_key(|r| r.sequence_id);
        Ok(records)
    }

    /// Shared segment-scan implementation.
    ///
    /// * `stream_filter`  — restrict to one stream (others are dropped before
    ///   materialization).
    /// * `include_tombstones` — keep tombstone frames (history view) instead of
    ///   skipping them.
    /// * `dedup` — keep only the latest version per (stream, key).
    fn scan_segment_frames(
        &self,
        max_records: usize,
        stream_filter: Option<&str>,
        include_tombstones: bool,
        dedup: bool,
    ) -> crate::error::Result<Vec<Record>> {
        // Apply the global max_scan_results limit if it's lower than the requested max_records
        let effective_limit = std::cmp::min(max_records, self.max_scan_results);

        let mut segments = Vec::new();

        for entry in fs::read_dir(&self.data_dir).map_err(LivenError::Io)? {
            let entry = entry.map_err(LivenError::Io)?;
            let path = entry.path();
            if path.is_file()
                && path.extension().and_then(|s| s.to_str()) == Some("liven")
                && let Some(stem) = path.file_stem().and_then(|s| s.to_str())
                && let Ok(segment_id) = stem.parse::<u64>()
            {
                segments.push((segment_id, path));
            }
        }

        segments.sort_by_key(|a| a.0);

        let mut records: Vec<Record> = Vec::new();
        // Track the index of the latest record for each (stream, key) pair so that
        // older versions (from before the most recent upsert) are replaced in-place.
        // Records are read in offset order (oldest-first), so each subsequent
        // occurrence of the same (stream, key) overwrites the previous one.
        use std::collections::HashMap;
        let mut latest_idx: HashMap<(String, String), usize> = HashMap::new();

        // When scoping to a single stream we read the frame header plus a prefix of
        // the payload in one positional read. The stream name is the first field of
        // the payload (see `serialize_payload_into`), so a short prefix is enough to
        // decide whether to read the rest of the frame. Frames from other streams are
        // dropped before materialization and are not counted against the limit.
        let mut peek_buf = [0u8; 26 + 2 + STREAM_PEEK_CAP];
        let scoping = stream_filter.is_some();

        for (segment_id, _path) in &segments {
            if records.len() >= effective_limit {
                break;
            }

            let file = match self.get_or_load_file_handle(*segment_id) {
                Ok(f) => f,
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("Cannot mmap 0-byte file") || err_str.contains("is empty") {
                        continue;
                    }
                    return Err(e);
                }
            };
            let file_len = file.metadata().map_err(LivenError::Io)?.len();
            let mut offset = 0;

            while offset + 26 <= file_len && records.len() < effective_limit {
                // Read the fixed header. When scoping, read a payload prefix in the
                // same positional read so the stream can be identified cheaply.
                let (payload_prefix_avail, header_bytes): (usize, [u8; 26]) = if scoping {
                    let n1 = std::cmp::min(peek_buf.len() as u64, file_len - offset) as usize;
                    if file.read_exact_at(&mut peek_buf[..n1], offset).is_err() {
                        break;
                    }
                    let mut h = [0u8; 26];
                    h.copy_from_slice(&peek_buf[..26]);
                    (n1 - 26, h)
                } else {
                    let mut h = [0u8; 26];
                    if file.read_exact_at(&mut h, offset).is_err() {
                        break;
                    }
                    (0, h)
                };

                let (seq_id, timestamp, type_tag, flags, payload_len, crc32) =
                    decode_frame_header(&header_bytes);
                self.frames_read.fetch_add(1, Ordering::Relaxed);

                // Tombstone frames are normally skipped; the history view keeps them.
                if flags & 0x02 != 0 && !include_tombstones {
                    offset += 26 + (payload_len as u64);
                    continue;
                }

                if offset + 26 + (payload_len as u64) > file_len {
                    break;
                }

                // Decide stream membership from the payload prefix when possible.
                // `None` means "not yet decided" (stream name longer than the peek
                // window) and is resolved after the full payload is parsed below.
                let mut stream_decided: Option<bool> = None;
                if let Some(target) = stream_filter {
                    if payload_prefix_avail >= 2 {
                        let stream_len = u16::from_be_bytes([peek_buf[26], peek_buf[27]]) as usize;
                        if 2 + stream_len <= payload_prefix_avail {
                            stream_decided =
                                Some(&peek_buf[28..28 + stream_len] == target.as_bytes());
                        }
                    }
                    if stream_decided == Some(false) {
                        // Non-matching stream: drop before materialization and do not
                        // count it against max_scan_results.
                        offset += 26 + (payload_len as u64);
                        continue;
                    }
                }

                let payload_record = READ_STAGING_BUFFER.with(|buf_cell| {
                    let mut local_buf = buf_cell.borrow_mut();
                    if local_buf.len() < payload_len {
                        local_buf.resize(payload_len, 0);
                    }
                    if payload_prefix_avail >= payload_len {
                        // The whole payload was already read into the peek buffer; avoid
                        // a second positional read.
                        local_buf[..payload_len].copy_from_slice(&peek_buf[26..26 + payload_len]);
                    } else {
                        file.read_exact_at(&mut local_buf[..payload_len], offset + 26)
                            .map_err(LivenError::Io)?;
                    }
                    let target_slice = &local_buf[..payload_len];

                    let actual_crc = crc32fast::hash(target_slice);
                    if actual_crc == crc32
                        && let Ok((stream_name, key, value)) =
                            deserialize_payload_borrowed(target_slice, type_tag)
                        && let Ok(stream_key) = crate::storage::key::StreamKey::from_str(key)
                    {
                        // For stream names longer than the peek window, decide now.
                        if let Some(target) = stream_filter {
                            if stream_decided.is_none() && stream_name != target {
                                return Ok(None);
                            }
                        }
                        return Ok(Some(Record {
                            sequence_id: seq_id,
                            timestamp,
                            type_tag,
                            flags,
                            stream_name: stream_name.to_string(),
                            key: stream_key,
                            value,
                        }));
                    }
                    Ok::<Option<Record>, LivenError>(None)
                })?;

                if let Some(rec) = payload_record {
                    if dedup {
                        let dedup_key = (rec.stream_name.clone(), rec.key.to_string());
                        if let Some(&prev_idx) = latest_idx.get(&dedup_key) {
                            // Replace older version of the same key
                            records[prev_idx] = rec;
                        } else {
                            latest_idx.insert(dedup_key, records.len());
                            records.push(rec);
                        }
                    } else {
                        records.push(rec);
                    }
                }

                offset += 26 + (payload_len as u64);
            }
        }

        // Check if we hit the global scan limit
        if records.len() >= self.max_scan_results && max_records > self.max_scan_results {
            return Err(LivenError::Query(format!(
                "Scan result limit reached ({} records).\n   Refine your query with filter(), limit(), or a key-based lookup.\n   Override with max_scan_results in liven.toml.",
                self.max_scan_results
            )));
        }

        Ok(records)
    }

    /// Performs background compaction of historical files
    pub fn compact(&self) -> crate::error::Result<()> {
        let _maintenance = self.maintenance_lock.lock().unwrap();
        info!("Starting log compaction...");
        let active_id = self.active_segment_id.load(Ordering::SeqCst);

        // Gather all inactive segments on disk
        let mut inactive_segments = Vec::new();
        for entry in fs::read_dir(&self.data_dir).map_err(LivenError::Io)? {
            let entry = entry.map_err(LivenError::Io)?;
            let path = entry.path();
            if path.is_file()
                && path.extension().and_then(|s| s.to_str()) == Some("liven")
                && let Some(stem) = path.file_stem().and_then(|s| s.to_str())
                && let Ok(segment_id) = stem.parse::<u64>()
                && segment_id != active_id
            {
                inactive_segments.push((segment_id, path));
            }
        }
        inactive_segments.sort_by_key(|(segment_id, _)| *segment_id);

        if inactive_segments.is_empty() {
            info!("No inactive segments available for compaction.");
            return Ok(());
        }

        // Identify all keys in the index that point to an inactive segment
        let mut active_pointers_to_rewrite = Vec::new();
        for (key, pointer) in self.skipmap.iter() {
            if pointer.segment_id != active_id {
                active_pointers_to_rewrite.push((key, pointer));
            }
        }

        info!(
            "Compactor will consolidate {} active records into a compacted segment.",
            active_pointers_to_rewrite.len()
        );

        // We will write compacted records into a brand new compacted segment file
        // To avoid conflicts, we use a dedicated compaction counter that lives in a reserved
        // range (starting at u64::MAX / 2) that sequence IDs will never reach during normal operation.
        let compacted_segment_id = self.compaction_counter.fetch_add(1, Ordering::SeqCst);
        if compacted_segment_id < COMPACTION_ID_BASE || compacted_segment_id == active_id {
            return Err(LivenError::Internal(format!(
                "compaction produced an invalid segment ID {compacted_segment_id} (active {active_id})"
            )));
        }
        let compacted_path = self.segment_path(compacted_segment_id);

        // create_new: a collision must fail loudly instead of truncating a live
        // segment. The disjoint ID ranges make this unreachable in practice.
        let mut comp_file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(&compacted_path)
            .map_err(LivenError::Io)?;
        pl_create(&compacted_path);

        comp_file.lock_exclusive().map_err(LivenError::Io)?;
        test_crash_point("after_create");

        let mut offset = 0u64;

        // Group rewritten pointers to batch updates: (old, new) so the swap can
        // verify the entry still points at the frame we read.
        let mut new_pointers: HashMap<String, (LogPointer, LogPointer)> = HashMap::new();
        // If any live record cannot be read, abort instead of deleting segments.
        let mut read_failed = false;
        // (stream, timestamp, sequence_id) per rewritten key, used to re-point the
        // per-stream timestamp index at the compacted frame (the timestamp index
        // stores pointers, which go stale when a segment is rewritten and deleted).
        let mut rewritten_meta: HashMap<String, (String, i64, u64)> = HashMap::new();

        for (compound_key, pointer) in active_pointers_to_rewrite {
            let record = match self.read_record(pointer) {
                Ok(rec) => rec,
                Err(e) => {
                    warn!(
                        "Compaction cannot read record for key {}: {} — aborting to avoid data loss",
                        compound_key, e
                    );
                    read_failed = true;
                    continue;
                }
            };

            let new_ptr = LogPointer {
                segment_id: compacted_segment_id,
                file_offset: offset,
                seq_id: record.sequence_id,
                timestamp: record.timestamp,
            };
            rewritten_meta.insert(
                compound_key.clone(),
                (
                    record.stream_name.clone(),
                    record.timestamp,
                    record.sequence_id,
                ),
            );

            SERIALIZATION_BUFFER.with(|buf_cell| {
                let mut local_buf = buf_cell.borrow_mut();
                serialize_payload_into(
                    &record.stream_name,
                    record.key.as_str(),
                    &record.value,
                    &mut local_buf,
                );
                let payload_len = local_buf.len() as u32;
                let crc32 = crc32fast::hash(&local_buf);

                let mut frame_buf = Vec::with_capacity(26 + local_buf.len());
                frame_buf.extend_from_slice(&record.sequence_id.to_be_bytes());
                frame_buf.extend_from_slice(&record.timestamp.to_be_bytes());
                frame_buf.push(record.type_tag);
                frame_buf.push(record.flags);
                frame_buf.extend_from_slice(&payload_len.to_be_bytes());
                frame_buf.extend_from_slice(&crc32.to_be_bytes());
                frame_buf.extend_from_slice(&local_buf);

                let frame_start = offset;
                comp_file.write_all(&frame_buf).map_err(LivenError::Io)?;
                pl_write(
                    &compacted_path,
                    frame_start,
                    frame_start + frame_buf.len() as u64,
                );

                new_pointers.insert(compound_key, (pointer, new_ptr));

                offset += frame_buf.len() as u64;
                Ok::<(), LivenError>(())
            })?;
        }

        test_crash_point("after_write");
        comp_file.sync_data().map_err(LivenError::Io)?;
        pl_sync(&compacted_path, offset);
        test_crash_point("after_sync_data");
        comp_file.unlock().map_err(LivenError::Io)?;

        if read_failed {
            // Never delete sources we may have failed to copy.
            return Err(LivenError::Internal(
                "compaction aborted: a live record could not be read; old segments kept"
                    .to_string(),
            ));
        }

        // Make the new segment's directory entry durable *before* removing the
        // sources. A crash from here on therefore leaves either the old or the new
        // copy on disk, never neither.
        sync_data_dir(&self.data_dir).map_err(LivenError::Io)?;
        test_crash_point("after_sync_dir");

        // Swap pointers. The flusher holds  while applying a group
        // commit, including its index updates, so taking the same mutex here closes
        // the race with concurrent upserts and tombstones.
        {
            let _flusher_guard = self.active_file.lock().unwrap();
            for (compound_key, (old_ptr, new_ptr)) in new_pointers {
                // Only swap if the entry still points at the exact frame we read.
                // A concurrent update or delete has moved or removed it and must win.
                let should_update = self
                    .skipmap
                    .get(&compound_key)
                    .is_some_and(|cur| cur == old_ptr);

                if should_update {
                    // Re-point the timestamp index at the compacted frame before the old
                    // segment is deleted; otherwise time-range scans would follow stale
                    // pointers into a removed file and silently drop the record.
                    if let Some((stream_name, ts, seq_id)) = rewritten_meta.get(&compound_key) {
                        let guard = self.timestamp_indexes.lock().unwrap();
                        if let Some(idx) = guard.get(stream_name) {
                            idx.insert(*ts, *seq_id, new_ptr);
                        }
                    }
                    self.skipmap.insert(compound_key, new_ptr);
                }
            }
        }
        test_crash_point("after_pointer_swap");

        // Remove inactive file handles from the cache first to prevent Windows sharing violations!
        {
            let mut handles_guard = self.file_handles.lock().unwrap();
            for (segment_id, _) in &inactive_segments {
                handles_guard.remove(segment_id);
            }
        }

        // Now we can safely remove the old inactive segment files!
        test_crash_point("before_unlink");
        let mut removed_any = false;
        for (_, path) in inactive_segments {
            pl_unlink(&path);
            let _ = fs::remove_file(path);
            if !removed_any {
                removed_any = true;
                test_crash_point("after_first_unlink");
            }
        }
        test_crash_point("after_unlinks");
        // Best-effort: persist the unlinks. If this is lost, recovery finds both
        // copies and the sequence-id merge picks the newer frames.
        let _ = sync_data_dir(&self.data_dir);
        test_crash_point("after_final_sync");

        info!(
            "Log compaction completed successfully. Compacted Segment ID: {}, Size: {} bytes",
            compacted_segment_id, offset
        );
        Ok(())
    }
}

/// Test-only crash injection point for the compaction crash-consistency test.
/// Compiled only with the `test-hooks` feature; a matching env var aborts the
/// process without running destructors, simulating a hard crash.
#[inline]
fn test_should_fail_sync() -> bool {
    #[cfg(feature = "test-hooks")]
    {
        std::env::var_os("LIVEN_TEST_FAIL_SYNC").is_some()
    }
    #[cfg(not(feature = "test-hooks"))]
    {
        false
    }
}

#[inline]
fn test_crash_point(point: &str) {
    #[cfg(feature = "test-hooks")]
    {
        if let Ok(value) = std::env::var("LIVEN_TEST_CRASH_POINT")
            && value == point
        {
            if std::env::var_os("LIVEN_POWER_LOSS").is_some() {
                crate::power_loss::materialize();
            }
            std::process::abort();
        }
    }
    #[cfg(not(feature = "test-hooks"))]
    {
        let _ = point;
    }
}

#[inline]
fn pl_create(path: &Path) {
    #[cfg(feature = "test-hooks")]
    crate::power_loss::record_create(path);
    #[cfg(not(feature = "test-hooks"))]
    let _ = path;
}

#[inline]
fn pl_write(path: &Path, start: u64, end: u64) {
    #[cfg(feature = "test-hooks")]
    crate::power_loss::record_write(path, start, end);
    #[cfg(not(feature = "test-hooks"))]
    let _ = (path, start, end);
}

#[inline]
fn pl_sync(path: &Path, offset: u64) {
    #[cfg(feature = "test-hooks")]
    crate::power_loss::record_sync(path, offset);
    #[cfg(not(feature = "test-hooks"))]
    let _ = (path, offset);
}

#[inline]
fn pl_truncate_sync(path: &Path, len: u64) {
    #[cfg(feature = "test-hooks")]
    crate::power_loss::record_truncate_sync(path, len);
    #[cfg(not(feature = "test-hooks"))]
    let _ = (path, len);
}

#[inline]
fn pl_unlink(path: &Path) {
    #[cfg(feature = "test-hooks")]
    crate::power_loss::record_unlink(path);
    #[cfg(not(feature = "test-hooks"))]
    let _ = path;
}

#[inline]
fn pl_dir_sync(dir: &Path) {
    #[cfg(feature = "test-hooks")]
    crate::power_loss::record_dir_sync(dir);
    #[cfg(not(feature = "test-hooks"))]
    let _ = dir;
}

/// Create the data directory and, when it is new, persist its entry in the
/// parent directory before any segment can be acknowledged.
fn ensure_data_dir(path: &Path) -> crate::error::Result<()> {
    // Record every missing level so each new directory entry can be made
    // durable in its parent, not just the leaf.
    let mut missing = Vec::new();
    let mut cursor = path;
    while !cursor.exists() {
        missing.push(cursor.to_path_buf());
        match cursor.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => cursor = parent,
            _ => break,
        }
    }
    fs::create_dir_all(path).map_err(LivenError::Io)?;
    for dir in missing.iter().rev() {
        pl_create(dir);
        let parent = dir
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        sync_data_dir(parent).map_err(LivenError::Io)?;
    }
    Ok(())
}

/// Flushes a directory entry so freshly created or removed segment files are
/// durable. No-op on platforms where directories cannot be opened as files.
fn sync_data_dir(dir: &Path) -> std::io::Result<()> {
    #[cfg(feature = "test-hooks")]
    if std::env::var_os("LIVEN_TEST_SKIP_DIR_SYNC").is_some() {
        return Ok(());
    }
    #[cfg(unix)]
    {
        File::open(dir)?.sync_all()?;
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
    }
    pl_dir_sync(dir);
    Ok(())
}

/// Why recovery stopped replaying a segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TearKind {
    Header,
    Payload,
    Crc,
    Parse,
    Trailing,
}

#[derive(Debug, Clone)]
struct StrandedFrame {
    offset: u64,
    len: u64,
}

enum TearDecision {
    /// Truncate the segment at this offset. `marker` is removed only after the
    /// truncation has been fsynced (repair idempotence marker).
    Truncate {
        offset: u64,
        marker: Option<PathBuf>,
    },
    /// Leave the file untouched and stop replaying this segment (inactive
    /// segments only, matching pre-salvage behavior).
    SkipRemainder,
}

/// A tear recorded during the single replay pass and resolved after the active
/// segment is known (see `process_deferred_tears`). Keeping the tear decision
/// out of the replay loop is what lets recovery read each segment only once.
struct DeferredTear {
    segment_id: u64,
    file_len: u64,
    offset: u64,
    max_seq_in_prefix: u64,
    kind: TearKind,
    declared_payload_len: u64,
    parse_error: Option<String>,
}

/// Hard ceiling for a salvaged frame payload. A garbage header must not make
/// recovery allocate an arbitrary amount of memory while scanning.
const MAX_SALVAGE_PAYLOAD: u64 = 512 * 1024 * 1024;

const RESYNC_CHUNK: usize = 1024 * 1024;

fn is_plausible_timestamp(ts: i64, now_ms: i64) -> bool {
    // 2000-01-01 .. now + 1 day. The engine stamps writes with the wall clock.
    ts >= 946_684_800_000 && ts <= now_ms.saturating_add(86_400_000)
}

/// Cheap header checks run before any payload I/O. The header is not covered
/// by the payload CRC, so these checks plus the payload CRC and structural
/// checks are what make a resync candidate credible.
fn header_shape_plausible(header: &[u8; 26], now_ms: i64) -> Option<(u64, i64, u8, u8, u64, u32)> {
    let (seq_id, timestamp, type_tag, flags, payload_len, crc32) = decode_frame_header(header);
    let payload_len = payload_len as u64;
    if seq_id == 0
        || !is_plausible_timestamp(timestamp, now_ms)
        || type_tag > 9
        || (flags != 0x01 && flags != 0x02)
        || !(4..=MAX_SALVAGE_PAYLOAD).contains(&payload_len)
    {
        return None;
    }
    Some((seq_id, timestamp, type_tag, flags, payload_len, crc32))
}

fn plausible_header(
    header: &[u8; 26],
    offset: u64,
    file_len: u64,
    now_ms: i64,
) -> Option<(u64, i64, u8, u8, u64, u32)> {
    let (seq_id, timestamp, type_tag, flags, payload_len, crc32) =
        header_shape_plausible(header, now_ms)?;
    if offset.saturating_add(26).saturating_add(payload_len) > file_len {
        return None;
    }
    Some((seq_id, timestamp, type_tag, flags, payload_len, crc32))
}

/// Validate one complete frame: plausible header, payload CRC, and consistent
/// `stream_len` / `key_len` structure. Returns `(frame_len, seq_id)`.
fn validate_frame_at(
    file: &File,
    file_len: u64,
    offset: u64,
    now_ms: i64,
) -> crate::error::Result<Option<(u64, u64)>> {
    if offset.saturating_add(26) > file_len {
        return Ok(None);
    }
    let mut header = [0u8; 26];
    if file.read_exact_at(&mut header, offset).is_err() {
        return Ok(None);
    }
    let Some((seq_id, _ts, _tag, _flags, payload_len, declared_crc)) =
        plausible_header(&header, offset, file_len, now_ms)
    else {
        return Ok(None);
    };

    // Read only the structural prefix: stream_len, stream, key_len.
    let mut len_buf = [0u8; 2];
    if file.read_exact_at(&mut len_buf, offset + 26).is_err() {
        return Ok(None);
    }
    let stream_len = u16::from_be_bytes(len_buf) as u64;
    if 4 + stream_len > payload_len {
        return Ok(None);
    }
    let mut prefix = vec![0u8; (4 + stream_len) as usize];
    if file.read_exact_at(&mut prefix, offset + 26).is_err() {
        return Ok(None);
    }
    let key_len = u16::from_be_bytes([
        prefix[(2 + stream_len) as usize],
        prefix[(3 + stream_len) as usize],
    ]) as u64;
    if 4 + stream_len + key_len > payload_len {
        return Ok(None);
    }
    let stream_bytes = &prefix[2..(2 + stream_len) as usize];
    if std::str::from_utf8(stream_bytes).is_err() {
        return Ok(None);
    }
    let mut key = vec![0u8; key_len as usize];
    if file
        .read_exact_at(&mut key, offset + 26 + 4 + stream_len)
        .is_err()
    {
        return Ok(None);
    }
    let Ok(key_str) = std::str::from_utf8(&key) else {
        return Ok(None);
    };
    if crate::storage::key::StreamKey::from_str(key_str).is_err() {
        return Ok(None);
    }

    // CRC the whole payload in chunks without materializing it.
    let mut hasher = crc32fast::Hasher::new();
    let mut buf = vec![0u8; std::cmp::min(payload_len, 64 * 1024) as usize];
    let mut read = 0u64;
    while read < payload_len {
        let n = std::cmp::min(buf.len() as u64, payload_len - read) as usize;
        if file
            .read_exact_at(&mut buf[..n], offset + 26 + read)
            .is_err()
        {
            return Ok(None);
        }
        hasher.update(&buf[..n]);
        read += n as u64;
    }
    if hasher.finalize() != declared_crc {
        return Ok(None);
    }

    Ok(Some((26 + payload_len, seq_id)))
}

/// Scan forward from `start` for a credible resync point. Two consecutive valid
/// frames are required. For normal-range segments the sequence IDs must also
/// increase past `min_seq`; compacted segments are exempt because compaction
/// rewrites records in index (key) order, not sequence order.
fn find_resync(
    file: &File,
    file_len: u64,
    start: u64,
    now_ms: i64,
    min_seq: Option<u64>,
) -> crate::error::Result<Option<u64>> {
    if start.saturating_add(26) > file_len {
        return Ok(None);
    }

    let mut chunk_start = start;
    let first_len = std::cmp::min(RESYNC_CHUNK as u64, file_len - start) as usize;
    let mut chunk = vec![0u8; first_len];
    file.read_exact_at(&mut chunk, chunk_start)
        .map_err(LivenError::Io)?;

    let last_candidate = file_len - 26;
    let mut pos = start;
    while pos <= last_candidate {
        if pos + 26 > chunk_start + chunk.len() as u64 {
            let n = std::cmp::min(RESYNC_CHUNK as u64, file_len - pos) as usize;
            chunk.resize(n, 0);
            file.read_exact_at(&mut chunk, pos)
                .map_err(LivenError::Io)?;
            chunk_start = pos;
        }
        let idx = (pos - chunk_start) as usize;
        let mut header = [0u8; 26];
        header.copy_from_slice(&chunk[idx..idx + 26]);

        if plausible_header(&header, pos, file_len, now_ms).is_some()
            && let Some((frame_len, seq_id)) = validate_frame_at(file, file_len, pos, now_ms)?
            && min_seq.is_none_or(|min| seq_id > min)
        {
            let next = pos + frame_len;
            let next_valid = validate_frame_at(file, file_len, next, now_ms)?
                .is_some_and(|(_, next_seq)| min_seq.is_none_or(|_| next_seq > seq_id));
            // A single valid frame is credible when it is the last frame (EOF)
            // or when the bytes after it are a torn header/payload. This is the
            // 0.1.3 shape where the last stranded frame was itself torn.
            if next_valid || tail_looks_torn(file, file_len, next, now_ms)? {
                return Ok(Some(pos));
            }
        }
        pos += 1;
    }
    Ok(None)
}

/// True when `offset` is EOF or the bytes there cannot form a complete frame
/// but do look like the beginning of one (partial header or truncated payload).
fn tail_looks_torn(
    file: &File,
    file_len: u64,
    offset: u64,
    now_ms: i64,
) -> crate::error::Result<bool> {
    if offset >= file_len {
        return Ok(true);
    }
    let remaining = file_len - offset;
    if remaining < 26 {
        return Ok(true);
    }
    let mut header = [0u8; 26];
    if file.read_exact_at(&mut header, offset).is_err() {
        return Ok(false);
    }
    let Some((_seq, _ts, _tag, _flags, payload_len, _crc)) =
        header_shape_plausible(&header, now_ms)
    else {
        return Ok(false);
    };
    Ok(offset.saturating_add(26).saturating_add(payload_len) > file_len)
}

/// Collect every consecutive valid frame starting at `resync`. Returns the
/// frames and the offset immediately after the last valid one.
fn collect_stranded(
    file: &File,
    file_len: u64,
    resync: u64,
    now_ms: i64,
    min_seq: Option<u64>,
) -> crate::error::Result<(Vec<StrandedFrame>, u64)> {
    let mut frames = Vec::new();
    let mut pos = resync;
    let mut last_seq = min_seq.unwrap_or(0);
    while let Some((frame_len, seq_id)) = validate_frame_at(file, file_len, pos, now_ms)? {
        if let Some(min) = min_seq
            && seq_id <= min.max(last_seq)
        {
            break;
        }
        frames.push(StrandedFrame {
            offset: pos,
            len: frame_len,
        });
        last_seq = seq_id;
        pos += frame_len;
    }
    Ok((frames, pos))
}

/// Extract a `"stream:key"` label from a frame at `offset` even if the payload
/// is torn or its CRC is bad. Used only to make `RecoveryRequired` actionable.
fn extract_key_at(file: &File, file_len: u64, offset: u64) -> Option<String> {
    if offset.saturating_add(30) > file_len {
        return None;
    }
    let mut header = [0u8; 26];
    file.read_exact_at(&mut header, offset).ok()?;
    let (_seq, _ts, type_tag, _flags, payload_len, _crc) = decode_frame_header(&header);
    let payload_len = payload_len as u64;
    if type_tag > 9 || payload_len < 4 {
        return None;
    }
    let mut len_buf = [0u8; 2];
    file.read_exact_at(&mut len_buf, offset + 26).ok()?;
    let stream_len = u16::from_be_bytes(len_buf) as u64;
    if 4 + stream_len > payload_len || offset + 30 + stream_len > file_len {
        return None;
    }
    let mut prefix = vec![0u8; (4 + stream_len) as usize];
    file.read_exact_at(&mut prefix, offset + 26).ok()?;
    let stream = std::str::from_utf8(&prefix[2..(2 + stream_len) as usize]).ok()?;
    let key_len = u16::from_be_bytes([
        prefix[(2 + stream_len) as usize],
        prefix[(3 + stream_len) as usize],
    ]) as u64;
    if 4 + stream_len + key_len > payload_len || offset + 26 + 4 + stream_len + key_len > file_len {
        return None;
    }
    let mut key = vec![0u8; key_len as usize];
    file.read_exact_at(&mut key, offset + 26 + 4 + stream_len)
        .ok()?;
    let key = std::str::from_utf8(&key).ok()?;
    crate::storage::key::StreamKey::from_str(key).ok()?;
    Some(format!("{}:{}", stream, key))
}

/// Copy `tear_offset..EOF` to `<segment>.torn-<offset>.bak` and fsync both the
/// copy and its directory before any truncation. Existing backups are rewritten
/// from the still-intact source, so a crash between backup and truncation is
/// safe and idempotent.
fn backup_torn_region(path: &Path, tear_offset: u64) -> crate::error::Result<PathBuf> {
    let meta = fs::metadata(path).map_err(LivenError::Io)?;
    let file_len = meta.len();
    if tear_offset >= file_len {
        return Err(LivenError::Storage(format!(
            "no torn region to back up in {} at offset {}",
            path.display(),
            tear_offset
        )));
    }
    let name = format!(
        "{}.torn-{}.bak",
        path.file_name()
            .map(|n| n.to_string_lossy())
            .unwrap_or_default(),
        tear_offset
    );
    let backup = path.with_file_name(name);
    let mut src = File::open(path).map_err(LivenError::Io)?;
    src.seek(SeekFrom::Start(tear_offset))
        .map_err(LivenError::Io)?;
    let mut dst = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&backup)
        .map_err(LivenError::Io)?;
    pl_create(&backup);
    std::io::copy(&mut src.take(file_len - tear_offset), &mut dst).map_err(LivenError::Io)?;
    pl_write(&backup, 0, file_len - tear_offset);
    dst.sync_all().map_err(LivenError::Io)?;
    pl_sync(&backup, file_len - tear_offset);
    if let Some(parent) = backup.parent() {
        sync_data_dir(parent).map_err(LivenError::Io)?;
    }
    Ok(backup)
}

fn salvage_marker_path(source: &Path, tear_offset: u64) -> PathBuf {
    let name = format!(
        "{}.salvage-{}",
        source
            .file_name()
            .map(|n| n.to_string_lossy())
            .unwrap_or_default(),
        tear_offset
    );
    source.with_file_name(name)
}

fn read_salvage_marker(path: &Path) -> Option<(u64, u64)> {
    let text = fs::read_to_string(path).ok()?;
    let mut parts = text.split_whitespace();
    Some((parts.next()?.parse().ok()?, parts.next()?.parse().ok()?))
}

fn write_salvage_marker(path: &Path, target_id: u64, total_len: u64) -> crate::error::Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .map_err(LivenError::Io)?;
    pl_create(path);
    writeln!(file, "{} {}", target_id, total_len).map_err(LivenError::Io)?;
    let marker_len = file.metadata().map(|m| m.len()).unwrap_or(0);
    pl_write(path, 0, marker_len);
    file.sync_all().map_err(LivenError::Io)?;
    pl_sync(path, marker_len);
    if let Some(parent) = path.parent() {
        sync_data_dir(parent).map_err(LivenError::Io)?;
    }
    Ok(())
}

/// Helper function to create IndexRamLimitExceeded errors
fn primary_charge(compound_key: &str) -> u64 {
    compound_key.len() as u64 + PRIMARY_INDEX_NODE_BYTES
}

/// Removes the timestamp-index entry for a key's current pointer (if any) and
/// releases its charge. Used by the flusher so a failed write never mutates the
/// index before the frame is durable.
fn prune_timestamp_entry(
    skipmap: &Arc<IndexMap<String>>,
    timestamp_indexes: &Arc<Mutex<HashMap<String, Arc<timestamp_index::TimestampIndex>>>>,
    stream_name: &str,
    compound_key: &str,
    index_ram_bytes: &Arc<AtomicU64>,
) {
    let Some(pointer) = skipmap.get(compound_key) else {
        return;
    };
    let guard = timestamp_indexes.lock().unwrap();
    if let Some(index) = guard.get(stream_name)
        && index.remove(pointer.timestamp, pointer.seq_id)
    {
        index_ram_bytes.fetch_sub(TIMESTAMP_INDEX_NODE_BYTES, Ordering::SeqCst);
    }
}

fn create_index_ram_limit_error(max_bytes: u64, current_bytes: u64) -> LivenError {
    LivenError::IndexRamLimitExceeded {
        max_bytes,
        current_bytes,
    }
}

fn get_process_memory() -> u64 {
    #[cfg(target_os = "macos")]
    {
        if let Ok(output) = std::process::Command::new("ps")
            .args(["-o", "rss=", "-p", &std::process::id().to_string()])
            .output()
            && let Ok(stdout) = String::from_utf8(output.stdout)
            && let Ok(rss_kb) = stdout.trim().parse::<u64>()
        {
            return rss_kb * 1024;
        }
    }

    #[cfg(target_os = "linux")]
    {
        if let Ok(statm) = std::fs::read_to_string("/proc/self/statm") {
            let fields: Vec<&str> = statm.split_whitespace().collect();
            if let Some(rss_pages_str) = fields.get(1)
                && let Ok(rss_pages) = rss_pages_str.parse::<u64>()
            {
                return rss_pages * 4096;
            }
        }
    }

    0
}

impl StorageEngine {
    /// Returns true if the number of inactive segments or their total size exceeds thresholds.
    pub fn should_compact(&self) -> bool {
        let active_id = self
            .active_segment_id
            .load(std::sync::atomic::Ordering::SeqCst);
        let mut inactive_count = 0usize;
        let mut inactive_bytes = 0u64;

        if let Ok(entries) = std::fs::read_dir(&self.data_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file()
                    && path.extension().and_then(|s| s.to_str()) == Some("liven")
                    && let Some(stem) = path.file_stem().and_then(|s| s.to_str())
                    && let Ok(segment_id) = stem.parse::<u64>()
                    && segment_id != active_id
                {
                    inactive_count += 1;
                    if let Ok(meta) = path.metadata() {
                        inactive_bytes += meta.len();
                    }
                }
            }
        }

        inactive_count >= self.compaction_threshold_segments
            || inactive_bytes >= self.compaction_threshold_bytes
    }

    /// Gets database disk size and record metrics
    pub fn metrics(&self) -> crate::error::Result<(u64, u64, u64, usize)> {
        let mut ram_usage = get_process_memory();
        if ram_usage == 0 {
            ram_usage = self.index_ram_bytes.load(Ordering::SeqCst);
        }
        let mut total_size = 0u64;
        let mut segment_count = 0u64;

        for entry in fs::read_dir(&self.data_dir).map_err(LivenError::Io)? {
            let entry = entry.map_err(LivenError::Io)?;
            let path = entry.path();
            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("liven") {
                total_size += path.metadata().map_err(LivenError::Io)?.len();
                segment_count += 1;
            }
        }

        let total_streams = self.streams_set.lock().unwrap().len();

        Ok((ram_usage, total_size, segment_count, total_streams))
    }

    /// Returns true if the flusher thread is still running (has not exited or panicked).
    pub fn is_flusher_alive(&self) -> bool {
        self.is_running.load(Ordering::Relaxed)
            && self
                .flusher_handle
                .as_ref()
                .is_some_and(|h| !h.is_finished())
    }

    /// Returns the recovery report, if one was produced during startup.
    pub fn recovery_report(&self) -> Option<&RecoveryReport> {
        self.recovery_report.as_ref()
    }

    /// Scan historical records within a time range using the timestamp index.
    /// Returns records with timestamps in [start_ms, end_ms] inclusive.
    pub fn scan_by_time_range(
        &self,
        stream_name: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> crate::error::Result<Vec<Record>> {
        let ts_idx_guard = self.timestamp_indexes.lock().unwrap();
        let pointers = match ts_idx_guard.get(stream_name) {
            Some(idx) => idx.range(start_ms, end_ms),
            None => return Ok(vec![]),
        };
        drop(ts_idx_guard);

        let mut records = Vec::with_capacity(pointers.len());
        use std::collections::HashMap;
        let mut seen: HashMap<(String, String), usize> = HashMap::new();
        for ptr in pointers {
            match self.read_record(ptr) {
                Ok(rec) => {
                    if rec.stream_name == stream_name
                        && rec.timestamp >= start_ms
                        && rec.timestamp <= end_ms
                        && rec.flags & 0x02 == 0
                    // Skip tombstones
                    {
                        // Liveness check: only accept the frame the key currently
                        // points at. This rejects stale index entries left by a
                        // supersede/delete (belt-and-braces with the index
                        // maintenance in append/compact).
                        let compound = format!("{}:{}", rec.stream_name, rec.key.as_str());
                        if self.skipmap.get(&compound) != Some(ptr) {
                            continue;
                        }
                        let dedup_key = (rec.stream_name.clone(), rec.key.to_string());
                        if let Some(&prev_idx) = seen.get(&dedup_key) {
                            records[prev_idx] = rec;
                        } else {
                            seen.insert(dedup_key, records.len());
                            records.push(rec);
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "scan_by_time_range: failed to read record at {:?}: {}",
                        ptr, e
                    );
                }
            }
        }
        Ok(records)
    }
}

fn run_flusher_loop(
    rx: std::sync::mpsc::Receiver<ring_buffer::LogEntry>,
    data_dir: PathBuf,
    max_segment_size: u64,
    current_sequence_id: Arc<AtomicU64>,
    active_segment_id: Arc<AtomicU64>,
    segment_id_counter: Arc<AtomicU64>,
    active_file: Arc<Mutex<Option<File>>>,
    active_size: Arc<Mutex<u64>>,
    skipmap: Arc<IndexMap<String>>,
    broadcast_tx: broadcast::Sender<Record>,
    file_handles: Arc<Mutex<HashMap<u64, Arc<File>>>>,
    sync_mode: String,
    is_running: Arc<std::sync::atomic::AtomicBool>,
    failed: Arc<std::sync::atomic::AtomicBool>,
    index_ram_bytes: Arc<AtomicU64>,
    max_index_ram_bytes: u64,
    high_water_bytes: u64,
    high_water_reached: Arc<AtomicBool>,
    flush_fsyncs: Arc<AtomicU64>,
    flush_batches: Arc<AtomicU64>,
    flush_entries: Arc<AtomicU64>,
    timestamp_indexes: Arc<Mutex<HashMap<String, Arc<timestamp_index::TimestampIndex>>>>,
    drain_window_us: u64,
    max_batch: usize,
) {
    // Solo-caller fast path: if the channel is empty after a short check,
    // flush immediately instead of waiting out the full drain window.
    // This collapses single-call latency from 5ms to pure I/O cost.
    let solo_check_us = std::cmp::min(drain_window_us / 10, 500);
    let mut batch = Vec::with_capacity(max_batch);
    while is_running.load(Ordering::Relaxed) {
        // Wait for the first entry with a short timeout to prevent blocking indefinitely on shutdown
        let first_entry = match rx.recv_timeout(std::time::Duration::from_millis(100)) {
            Ok(entry) => entry,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        };

        // Handle shutdown sentinel
        if matches!(first_entry, ring_buffer::LogEntry::Shutdown) {
            break;
        }
        batch.push(first_entry);

        // Pull as many entries as possible without blocking
        let start_time = std::time::Instant::now();
        while batch.len() < max_batch {
            match rx.try_recv() {
                Ok(entry) => {
                    batch.push(entry);
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => {
                    let elapsed = start_time.elapsed();
                    let elapsed_us = elapsed.as_micros() as u64;
                    // Solo-caller fast path: if only one entry and channel is empty
                    // after a short check, flush immediately
                    let limit = if batch.len() == 1 {
                        solo_check_us
                    } else {
                        drain_window_us
                    };
                    if elapsed_us >= limit {
                        break;
                    }
                    std::thread::yield_now();
                }
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    break;
                }
            }
        }

        // Process the batch!
        if !matches!(batch.first(), Some(ring_buffer::LogEntry::Shutdown)) {
            flush_batches.fetch_add(1, Ordering::Relaxed);
            flush_entries.fetch_add(batch.len() as u64, Ordering::Relaxed);
        }
        if let Err(e) = execute_batch_append(
            &mut batch,
            &data_dir,
            max_segment_size,
            &current_sequence_id,
            &active_segment_id,
            &segment_id_counter,
            &active_file,
            &active_size,
            &skipmap,
            &broadcast_tx,
            &file_handles,
            &sync_mode,
            &failed,
            &index_ram_bytes,
            max_index_ram_bytes,
            high_water_bytes,
            &high_water_reached,
            &flush_fsyncs,
            &timestamp_indexes,
        ) {
            // Notify all waiting entries of the failure
            for entry in batch.drain(..) {
                let err_str = e.to_string();
                match entry {
                    ring_buffer::LogEntry::Single { response_tx, .. } => {
                        let _ =
                            response_tx.send(Err(ring_buffer::AckError::Message(err_str.clone())));
                    }
                    ring_buffer::LogEntry::TombstoneBatch { response_tx, .. } => {
                        let _ =
                            response_tx.send(Err(ring_buffer::AckError::Message(err_str.clone())));
                    }
                    ring_buffer::LogEntry::Replace { response_tx, .. } => {
                        let _ =
                            response_tx.send(Err(ring_buffer::AckError::Message(err_str.clone())));
                    }
                    ring_buffer::LogEntry::Batch { response_tx, .. } => {
                        let _ =
                            response_tx.send(Err(ring_buffer::AckError::Message(err_str.clone())));
                    }
                    ring_buffer::LogEntry::Shutdown => {
                        // Shutdown sentinel, no response needed
                    }
                }
            }
            if failed.load(Ordering::Relaxed) {
                is_running.store(false, Ordering::Relaxed);
            }
        }
        batch.clear();
    }
}

fn execute_batch_append(
    batch: &mut Vec<ring_buffer::LogEntry>,
    data_dir: &Path,
    max_segment_size: u64,
    current_sequence_id: &Arc<AtomicU64>,
    active_segment_id: &Arc<AtomicU64>,
    segment_id_counter: &Arc<AtomicU64>,
    active_file: &Arc<Mutex<Option<File>>>,
    active_size: &Arc<Mutex<u64>>,
    skipmap: &Arc<IndexMap<String>>,
    broadcast_tx: &broadcast::Sender<Record>,
    file_handles: &Arc<Mutex<HashMap<u64, Arc<File>>>>,
    sync_mode: &str,
    failed: &Arc<std::sync::atomic::AtomicBool>,
    index_ram_bytes: &Arc<AtomicU64>,
    max_index_ram_bytes: u64,
    high_water_bytes: u64,
    high_water_reached: &Arc<AtomicBool>,
    flush_fsyncs: &Arc<AtomicU64>,
    timestamp_indexes: &Arc<Mutex<HashMap<String, Arc<timestamp_index::TimestampIndex>>>>,
) -> crate::error::Result<()> {
    let mut active_file_guard = active_file.lock().unwrap();
    let mut active_size_guard = active_size.lock().unwrap();

    let mut current_seq = current_sequence_id.load(Ordering::SeqCst);

    enum PendingComplete {
        Single {
            response_tx: std::sync::mpsc::SyncSender<Result<Record, ring_buffer::AckError>>,
        },
        Replace {
            response_tx: std::sync::mpsc::SyncSender<Result<Record, ring_buffer::AckError>>,
        },
        Batch {
            response_tx: std::sync::mpsc::SyncSender<Result<Vec<Record>, ring_buffer::AckError>>,
            count: usize,
        },
    }

    let batch_has_replace = batch
        .iter()
        .any(|entry| matches!(entry, ring_buffer::LogEntry::Replace { .. }));
    let mut pending_completes = Vec::with_capacity(batch.len());
    let mut batch_records_to_add = Vec::new();

    // Use the pre-allocated thread-local frame buffer to avoid heap reallocations per batch.
    let mut frame_buf = FRAME_BUFFER.with(|fb| {
        let mut buf = fb.borrow_mut();
        buf.clear();
        std::mem::take(&mut *buf)
    });

    // Authoritative index-budget check. The caller's pre-check is only a fast
    // path; this runs on the single flusher thread, so concurrent writers cannot
    // overshoot by racing a stale `index_ram_bytes` read.
    let mut projected_new_bytes = 0u64;
    let mut batch_new_keys: HashSet<String> = HashSet::new();
    let mut high_water_logged = false;

    SERIALIZATION_BUFFER.with(|buf_cell| {
        let mut local_buf = buf_cell.borrow_mut();
        'entries: for entry in batch.iter() {
            match entry {
                ring_buffer::LogEntry::Single {
                    stream_name,
                    key,
                    value,
                    is_tombstone,
                    response_tx,
                } => {
                    if !*is_tombstone {
                        let compound_key = format!("{}:{}", stream_name, key.as_str());
                        let is_new = !skipmap.contains_key(&compound_key)
                            && !batch_new_keys.contains(&compound_key);
                        if is_new {
                            let size = primary_charge(&compound_key) + TIMESTAMP_INDEX_NODE_BYTES;
                            let projected =
                                index_ram_bytes.load(Ordering::SeqCst) + projected_new_bytes + size;
                            if projected > max_index_ram_bytes {
                                let _ = response_tx.send(Err(ring_buffer::AckError::IndexRam {
                                    max_bytes: max_index_ram_bytes,
                                    current_bytes: projected,
                                    key: None,
                                }));
                                continue;
                            }
                            if projected >= high_water_bytes
                                && !high_water_reached.swap(true, Ordering::SeqCst)
                            {
                                if !high_water_logged {
                                    warn!(
                                        "index RAM high-water mark reached: {} of {} bytes",
                                        projected, max_index_ram_bytes
                                    );
                                    high_water_logged = true;
                                }
                            }
                            projected_new_bytes += size;
                            batch_new_keys.insert(compound_key);
                        }
                    }

                    let seq_id = current_seq;
                    current_seq += 1;

                    let timestamp = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .map_err(|e| LivenError::Internal(e.to_string()))?
                        .as_millis() as i64;

                    let flags = if *is_tombstone { 0x02 } else { 0x01 };
                    let type_tag = value.type_tag();

                    serialize_payload_into(stream_name, key.as_str(), value, &mut local_buf);
                    let payload_len = local_buf.len() as u32;
                    let crc32 = crc32fast::hash(&local_buf);

                    let start_idx = frame_buf.len() as u64;

                    frame_buf.extend_from_slice(&seq_id.to_be_bytes());
                    frame_buf.extend_from_slice(&timestamp.to_be_bytes());
                    frame_buf.push(type_tag);
                    frame_buf.push(flags);
                    frame_buf.extend_from_slice(&payload_len.to_be_bytes());
                    frame_buf.extend_from_slice(&crc32.to_be_bytes());
                    frame_buf.extend_from_slice(&local_buf);

                    let end_idx = frame_buf.len() as u64;
                    let frame_len = end_idx - start_idx;

                    let record = Record {
                        sequence_id: seq_id,
                        timestamp,
                        type_tag,
                        flags,
                        stream_name: stream_name.clone(),
                        key: *key,
                        value: value.clone(),
                    };

                    batch_records_to_add.push((record, frame_len, *is_tombstone, false));
                    pending_completes.push(PendingComplete::Single {
                        response_tx: response_tx.clone(),
                    });
                }
                ring_buffer::LogEntry::Replace {
                    stream_name,
                    key,
                    value,
                    response_tx,
                } => {
                    let compound_key = format!("{}:{}", stream_name, key.as_str());
                    let is_new = !skipmap.contains_key(&compound_key)
                        && !batch_new_keys.contains(&compound_key);
                    if is_new {
                        let size = primary_charge(&compound_key) + TIMESTAMP_INDEX_NODE_BYTES;
                        let projected =
                            index_ram_bytes.load(Ordering::SeqCst) + projected_new_bytes + size;
                        if projected > max_index_ram_bytes {
                            let _ = response_tx.send(Err(ring_buffer::AckError::IndexRam {
                                max_bytes: max_index_ram_bytes,
                                current_bytes: projected,
                                key: None,
                            }));
                            continue;
                        }
                        if projected >= high_water_bytes
                            && !high_water_reached.swap(true, Ordering::SeqCst)
                        {
                            if !high_water_logged {
                                warn!(
                                    "index RAM high-water mark reached: {} of {} bytes",
                                    projected, max_index_ram_bytes
                                );
                                high_water_logged = true;
                            }
                        }
                        projected_new_bytes += size;
                        batch_new_keys.insert(compound_key);
                    }

                    let seq_id = current_seq;
                    current_seq += 1;

                    let timestamp = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .map_err(|e| LivenError::Internal(e.to_string()))?
                        .as_millis() as i64;

                    let type_tag = value.type_tag();
                    serialize_payload_into(stream_name, key.as_str(), value, &mut local_buf);
                    let payload_len = local_buf.len() as u32;
                    let crc32 = crc32fast::hash(&local_buf);

                    let start_idx = frame_buf.len() as u64;
                    frame_buf.extend_from_slice(&seq_id.to_be_bytes());
                    frame_buf.extend_from_slice(&timestamp.to_be_bytes());
                    frame_buf.push(type_tag);
                    frame_buf.push(0x01);
                    frame_buf.extend_from_slice(&payload_len.to_be_bytes());
                    frame_buf.extend_from_slice(&crc32.to_be_bytes());
                    frame_buf.extend_from_slice(&local_buf);
                    let end_idx = frame_buf.len() as u64;
                    let frame_len = end_idx - start_idx;

                    let record = Record {
                        sequence_id: seq_id,
                        timestamp,
                        type_tag,
                        flags: 0x01,
                        stream_name: stream_name.clone(),
                        key: *key,
                        value: value.clone(),
                    };
                    batch_records_to_add.push((record, frame_len, false, true));
                    pending_completes.push(PendingComplete::Replace {
                        response_tx: response_tx.clone(),
                    });
                }
                ring_buffer::LogEntry::TombstoneBatch {
                    stream_name,
                    keys,
                    response_tx,
                } => {
                    let mut count = 0;
                    for key in keys {
                        let seq_id = current_seq;
                        current_seq += 1;

                        let timestamp = SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .map_err(|e| LivenError::Internal(e.to_string()))?
                            .as_millis() as i64;

                        let flags = 0x02; // Tombstone
                        let type_tag = DataValue::Null.type_tag();

                        serialize_payload_into(
                            stream_name,
                            key.as_str(),
                            &DataValue::Null,
                            &mut local_buf,
                        );
                        let payload_len = local_buf.len() as u32;
                        let crc32 = crc32fast::hash(&local_buf);

                        let start_idx = frame_buf.len() as u64;

                        frame_buf.extend_from_slice(&seq_id.to_be_bytes());
                        frame_buf.extend_from_slice(&timestamp.to_be_bytes());
                        frame_buf.push(type_tag);
                        frame_buf.push(flags);
                        frame_buf.extend_from_slice(&payload_len.to_be_bytes());
                        frame_buf.extend_from_slice(&crc32.to_be_bytes());
                        frame_buf.extend_from_slice(&local_buf);

                        let end_idx = frame_buf.len() as u64;
                        let frame_len = end_idx - start_idx;

                        let record = Record {
                            sequence_id: seq_id,
                            timestamp,
                            type_tag,
                            flags,
                            stream_name: stream_name.clone(),
                            key: *key,
                            value: DataValue::Null,
                        };

                        batch_records_to_add.push((record, frame_len, true, false));
                        count += 1;
                    }
                    pending_completes.push(PendingComplete::Batch {
                        response_tx: response_tx.clone(),
                        count,
                    });
                }
                ring_buffer::LogEntry::Batch {
                    records,
                    response_tx,
                } => {
                    // Validate the whole batch before writing any frame. If one
                    // new key does not fit, reject all of it atomically.
                    let mut projected =
                        index_ram_bytes.load(Ordering::SeqCst) + projected_new_bytes;
                    let mut local_new: HashSet<String> = HashSet::new();
                    let mut offending: Option<(String, u64)> = None;
                    for (stream_name, key, _) in records.iter() {
                        let compound_key = format!("{}:{}", stream_name, key.as_str());
                        if skipmap.contains_key(&compound_key)
                            || batch_new_keys.contains(&compound_key)
                            || local_new.contains(&compound_key)
                        {
                            continue;
                        }
                        let size = primary_charge(&compound_key) + TIMESTAMP_INDEX_NODE_BYTES;
                        if projected + size > max_index_ram_bytes {
                            offending = Some((compound_key, projected + size));
                            break;
                        }
                        projected += size;
                        local_new.insert(compound_key);
                    }
                    if let Some((key, current_bytes)) = offending {
                        let _ = response_tx.send(Err(ring_buffer::AckError::IndexRam {
                            max_bytes: max_index_ram_bytes,
                            current_bytes,
                            key: Some(key),
                        }));
                        continue 'entries;
                    }
                    projected_new_bytes =
                        projected.saturating_sub(index_ram_bytes.load(Ordering::SeqCst));
                    batch_new_keys.extend(local_new);

                    let mut count = 0;
                    for (stream_name, key, value) in records {
                        let seq_id = current_seq;
                        current_seq += 1;
                        let timestamp = SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .map_err(|e| LivenError::Internal(e.to_string()))?
                            .as_millis() as i64;
                        let type_tag = value.type_tag();
                        serialize_payload_into(stream_name, key.as_str(), value, &mut local_buf);
                        let payload_len = local_buf.len() as u32;
                        let crc32 = crc32fast::hash(&local_buf);
                        let start_idx = frame_buf.len() as u64;
                        frame_buf.extend_from_slice(&seq_id.to_be_bytes());
                        frame_buf.extend_from_slice(&timestamp.to_be_bytes());
                        frame_buf.push(type_tag);
                        frame_buf.push(0x01);
                        frame_buf.extend_from_slice(&payload_len.to_be_bytes());
                        frame_buf.extend_from_slice(&crc32.to_be_bytes());
                        frame_buf.extend_from_slice(&local_buf);
                        let end_idx = frame_buf.len() as u64;
                        let frame_len = end_idx - start_idx;
                        let record = Record {
                            sequence_id: seq_id,
                            timestamp,
                            type_tag,
                            flags: 0x01,
                            stream_name: stream_name.clone(),
                            key: *key,
                            value: value.clone(),
                        };
                        batch_records_to_add.push((record, frame_len, false, false));
                        count += 1;
                    }
                    pending_completes.push(PendingComplete::Batch {
                        response_tx: response_tx.clone(),
                        count,
                    });
                }
                ring_buffer::LogEntry::Shutdown => {
                    // Shutdown sentinel, break out of processing
                    return Ok(());
                }
            }
        }
        Ok::<(), LivenError>(())
    })?;

    let mut rolled_over = false;
    if *active_size_guard + (frame_buf.len() as u64) > max_segment_size {
        rolled_over = true;
        let old_segment_id = active_segment_id.load(Ordering::SeqCst);
        let old_path = data_dir.join(format!("{:020}.liven", old_segment_id));
        let old_size = *active_size_guard;
        if let Some(old_file) = active_file_guard.take() {
            if let Err(e) = old_file.sync_all() {
                failed.store(true, Ordering::SeqCst);
                return Err(LivenError::Io(e));
            }
        }
        pl_sync(&old_path, old_size);
        test_crash_point("rollover_after_old_sync");

        let new_active_id = segment_id_counter.fetch_add(1, Ordering::SeqCst);
        if new_active_id >= COMPACTION_ID_BASE {
            return Err(LivenError::Internal(
                "normal segment ID space exhausted".to_string(),
            ));
        }
        active_segment_id.store(new_active_id, Ordering::SeqCst);

        let new_path = data_dir.join(format!("{:020}.liven", new_active_id));
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&new_path)
            .map_err(|e| {
                failed.store(true, Ordering::SeqCst);
                LivenError::Io(e)
            })?;
        pl_create(&new_path);
        test_crash_point("rollover_after_create");
        // Persist the directory entry before acknowledging records written into
        // this segment; otherwise a crash can lose the file (and the writes).
        sync_data_dir(data_dir).map_err(|e| {
            failed.store(true, Ordering::SeqCst);
            LivenError::Io(e)
        })?;
        test_crash_point("rollover_after_dir_sync");

        *active_file_guard = Some(file);
        *active_size_guard = 0;
        info!("Rolled over to new active segment: {}", new_active_id);
    }

    let file = active_file_guard.as_mut().ok_or_else(|| {
        failed.store(true, Ordering::SeqCst);
        LivenError::Internal("No active file handle found".to_string())
    })?;
    let current_offset = *active_size_guard;
    let write_segment_id = active_segment_id.load(Ordering::SeqCst);
    let write_path = data_dir.join(format!("{:020}.liven", write_segment_id));

    file.write_all(&frame_buf).map_err(|e| {
        // A partial write leaves the file in an unknown state. Stop accepting
        // writes; recovery will truncate the torn tail on reopen.
        failed.store(true, Ordering::SeqCst);
        if e.kind() == std::io::ErrorKind::StorageFull || e.kind() == std::io::ErrorKind::WriteZero
        {
            LivenError::DiskFull
        } else {
            LivenError::Io(e)
        }
    })?;
    pl_write(
        &write_path,
        current_offset,
        current_offset + frame_buf.len() as u64,
    );
    test_crash_point("flush_after_write");
    if rolled_over {
        test_crash_point("rollover_after_write");
    }
    if batch_has_replace {
        test_crash_point("replace_after_write");
    }

    if sync_mode == "always" {
        if test_should_fail_sync() {
            failed.store(true, Ordering::SeqCst);
            return Err(LivenError::Io(std::io::Error::other(
                "injected fsync failure",
            )));
        }
        file.sync_data().map_err(|e| {
            failed.store(true, Ordering::SeqCst);
            LivenError::Io(e)
        })?;
        flush_fsyncs.fetch_add(1, Ordering::Relaxed);
    }
    pl_sync(&write_path, current_offset + frame_buf.len() as u64);
    test_crash_point("flush_after_sync");
    if rolled_over {
        test_crash_point("rollover_after_sync");
    }
    if batch_has_replace {
        test_crash_point("replace_after_sync");
    }

    let segment_id = write_segment_id;

    let mut offset_acc = current_offset;
    let mut records_iter = batch_records_to_add.into_iter();

    for comp in pending_completes {
        match comp {
            PendingComplete::Single { response_tx } => {
                if let Some((record, frame_len, is_tombstone, _)) = records_iter.next() {
                    let compound_key = format!("{}:{}", record.stream_name, record.key.as_str());

                    let node_overhead = primary_charge(&compound_key);
                    prune_timestamp_entry(
                        skipmap,
                        timestamp_indexes,
                        &record.stream_name,
                        &compound_key,
                        index_ram_bytes,
                    );
                    if is_tombstone {
                        if skipmap.remove(&compound_key).is_some() {
                            index_ram_bytes.fetch_sub(node_overhead, Ordering::SeqCst);
                        }
                    } else {
                        let existed = skipmap.contains_key(&compound_key);
                        let pointer = LogPointer {
                            segment_id,
                            file_offset: offset_acc,
                            seq_id: record.sequence_id,
                            timestamp: record.timestamp,
                        };
                        skipmap.insert(compound_key, pointer);
                        if !existed {
                            index_ram_bytes.fetch_add(node_overhead, Ordering::SeqCst);
                        }
                        index_ram_bytes.fetch_add(TIMESTAMP_INDEX_NODE_BYTES, Ordering::SeqCst);

                        // Maintain timestamp index
                        let mut ts_idx_guard = timestamp_indexes.lock().unwrap();
                        let entry = ts_idx_guard
                            .entry(record.stream_name.clone())
                            .or_insert_with(|| Arc::new(timestamp_index::TimestampIndex::new()));
                        entry.insert(record.timestamp, record.sequence_id, pointer);
                    }

                    let _ = broadcast_tx.send(record.clone());
                    let _ = response_tx.send(Ok(record));

                    offset_acc += frame_len;
                }
            }
            PendingComplete::Replace { response_tx } => {
                if let Some((record, frame_len, _, _)) = records_iter.next() {
                    let compound_key = format!("{}:{}", record.stream_name, record.key.as_str());
                    let node_overhead = primary_charge(&compound_key);

                    // Supersede in place: drop the old timestamp-index entry
                    // before inserting the replacement's entry. This is a
                    // single frame, so a torn tail keeps the old value live.
                    prune_timestamp_entry(
                        skipmap,
                        timestamp_indexes,
                        &record.stream_name,
                        &compound_key,
                        index_ram_bytes,
                    );

                    let existed = skipmap.contains_key(&compound_key);
                    let pointer = LogPointer {
                        segment_id,
                        file_offset: offset_acc,
                        seq_id: record.sequence_id,
                        timestamp: record.timestamp,
                    };
                    skipmap.insert(compound_key, pointer);
                    if !existed {
                        index_ram_bytes.fetch_add(node_overhead, Ordering::SeqCst);
                    }
                    index_ram_bytes.fetch_add(TIMESTAMP_INDEX_NODE_BYTES, Ordering::SeqCst);
                    let mut ts_idx_guard = timestamp_indexes.lock().unwrap();
                    let entry = ts_idx_guard
                        .entry(record.stream_name.clone())
                        .or_insert_with(|| Arc::new(timestamp_index::TimestampIndex::new()));
                    entry.insert(record.timestamp, record.sequence_id, pointer);
                    drop(ts_idx_guard);

                    let _ = broadcast_tx.send(record.clone());
                    let _ = response_tx.send(Ok(record));
                    offset_acc += frame_len;
                }
            }
            PendingComplete::Batch { response_tx, count } => {
                let mut added_records = Vec::with_capacity(count);
                for _ in 0..count {
                    if let Some((record, frame_len, is_tombstone, _)) = records_iter.next() {
                        let compound_key =
                            format!("{}:{}", record.stream_name, record.key.as_str());

                        let node_overhead = primary_charge(&compound_key);
                        prune_timestamp_entry(
                            skipmap,
                            timestamp_indexes,
                            &record.stream_name,
                            &compound_key,
                            index_ram_bytes,
                        );
                        if is_tombstone {
                            if skipmap.remove(&compound_key).is_some() {
                                index_ram_bytes.fetch_sub(node_overhead, Ordering::SeqCst);
                            }
                        } else {
                            let existed = skipmap.contains_key(&compound_key);
                            let pointer = LogPointer {
                                segment_id,
                                file_offset: offset_acc,
                                seq_id: record.sequence_id,
                                timestamp: record.timestamp,
                            };
                            skipmap.insert(compound_key, pointer);
                            if !existed {
                                index_ram_bytes.fetch_add(node_overhead, Ordering::SeqCst);
                            }
                            index_ram_bytes.fetch_add(TIMESTAMP_INDEX_NODE_BYTES, Ordering::SeqCst);

                            // Maintain timestamp index
                            let mut ts_idx_guard = timestamp_indexes.lock().unwrap();
                            let entry = ts_idx_guard
                                .entry(record.stream_name.clone())
                                .or_insert_with(
                                    || Arc::new(timestamp_index::TimestampIndex::new()),
                                );
                            entry.insert(record.timestamp, record.sequence_id, pointer);
                        }

                        let _ = broadcast_tx.send(record.clone());
                        added_records.push(record);

                        offset_acc += frame_len;
                    }
                }
                let _ = response_tx.send(Ok(added_records));
            }
        }
    }

    *active_size_guard = offset_acc;
    current_sequence_id.store(current_seq, Ordering::SeqCst);

    let mut handles_guard = file_handles.lock().unwrap();
    handles_guard.remove(&segment_id);

    // Return the pre-allocated frame buffer to the thread-local for reuse
    FRAME_BUFFER.with(|fb| {
        *fb.borrow_mut() = frame_buf;
    });

    test_crash_point("flush_after_ack");
    Ok(())
}

pub fn serialize_payload_into(stream_name: &str, key: &str, value: &DataValue, buf: &mut Vec<u8>) {
    buf.clear();
    let stream_bytes = stream_name.as_bytes();
    let key_bytes = key.as_bytes();

    buf.extend_from_slice(&(stream_bytes.len() as u16).to_be_bytes());
    buf.extend_from_slice(stream_bytes);
    buf.extend_from_slice(&(key_bytes.len() as u16).to_be_bytes());
    buf.extend_from_slice(key_bytes);

    match value {
        DataValue::Vector(vec) => {
            let ptr = vec.as_ptr() as *const u8;
            let len = vec.len();
            // SAFETY: `vec` is a valid Vec<i8>, live for the duration of this function call.
            // Reinterpreting i8 as u8 is sound because both types have the same size (1 byte),
            // alignment (1), and bit layout. The resulting slice is immediately copied into
            // `buf` via `extend_from_slice`, so it does not outlive `vec`. If the DataValue
            // variant changes or the buffer is stored instead of copied, this safety
            // justification would need to be revisited.
            let u8_slice = unsafe { std::slice::from_raw_parts(ptr, len) };
            buf.extend_from_slice(u8_slice);
        }
        _ => {
            let _ = rmp_serde::encode::write(buf, value);
        }
    }
}

fn deserialize_payload_borrowed(
    payload: &[u8],
    type_tag: u8,
) -> crate::error::Result<(&str, &str, DataValue)> {
    if payload.len() < 4 {
        return Err(LivenError::PayloadTooShort { kind: "payload" });
    }
    let stream_len = u16::from_be_bytes([payload[0], payload[1]]) as usize;
    if payload.len() < 4 + stream_len {
        return Err(LivenError::PayloadTooShort {
            kind: "stream name",
        });
    }
    let stream_name = std::str::from_utf8(&payload[2..2 + stream_len])
        .map_err(|e| LivenError::Serialization(e.to_string()))?;

    let key_len_offset = 2 + stream_len;
    let key_len =
        u16::from_be_bytes([payload[key_len_offset], payload[key_len_offset + 1]]) as usize;
    if payload.len() < 4 + stream_len + key_len {
        return Err(LivenError::PayloadTooShort { kind: "key" });
    }
    let key_offset = key_len_offset + 2;
    let key = std::str::from_utf8(&payload[key_offset..key_offset + key_len])
        .map_err(|e| LivenError::Serialization(e.to_string()))?;

    let val_offset = key_offset + key_len;
    let value = if type_tag == 8 {
        let raw_bytes = &payload[val_offset..];
        let ptr = raw_bytes.as_ptr() as *const i8;
        let count = raw_bytes.len();
        // SAFETY: `raw_bytes` is a valid &[u8] slice derived from the payload, whose
        // lifetime is bounded by the caller's borrow. Reinterpreting u8 as i8 is sound
        // because both types have the same size (1 byte), alignment (1), and bit layout.
        // The resulting slice is immediately converted to an owned Vec<i8> via `.to_vec()`,
        // which copies the bytes, so the resulting DataValue does not borrow from `payload`.
        // If the type_tag encoding changes or `payload` could be mutated concurrently,
        // this safety justification would need to be revisited.
        let vec_i8 = unsafe { std::slice::from_raw_parts(ptr, count) }.to_vec();
        DataValue::Vector(vec_i8)
    } else {
        rmp_serde::from_slice(&payload[val_offset..])
            .map_err(|e| LivenError::Serialization(e.to_string()))?
    };

    Ok((stream_name, key, value))
}

pub fn deserialize_payload(payload: &[u8]) -> crate::error::Result<(String, String, DataValue)> {
    // Try MsgPack first (type_tag 0x02). If that fails, try raw Vector
    // (type_tag 8). This handles fuzz tests where serialize_payload_into may
    // write a Vector as raw bytes but the caller has no header type_tag.
    if let Ok(result) = deserialize_payload_borrowed(payload, 0x02) {
        return Ok((result.0.to_string(), result.1.to_string(), result.2));
    }
    let (_stream_name, _key, value) = deserialize_payload_borrowed(payload, 8)?;
    Ok((_stream_name.to_string(), _key.to_string(), value))
}

pub struct FrameReader<'a> {
    pub data: &'a [u8],
}

impl<'a> FrameReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

    pub fn as_vector_slice(&self) -> Option<&[i8]> {
        if self.data.len() < 26 {
            return None;
        }
        let type_tag = self.data[16];
        if type_tag != 8 {
            return None;
        }
        let payload_len =
            u32::from_be_bytes([self.data[18], self.data[19], self.data[20], self.data[21]])
                as usize;

        if self.data.len() < 26 + payload_len {
            return None;
        }

        let payload = &self.data[26..26 + payload_len];
        if payload.len() < 4 {
            return None;
        }

        let stream_len = u16::from_be_bytes([payload[0], payload[1]]) as usize;
        if payload.len() < 4 + stream_len {
            return None;
        }

        let key_len_offset = 2 + stream_len;
        let key_len =
            u16::from_be_bytes([payload[key_len_offset], payload[key_len_offset + 1]]) as usize;

        let val_offset = key_len_offset + 2 + key_len;
        if payload.len() < val_offset {
            return None;
        }

        let raw_bytes = &payload[val_offset..];
        let ptr = raw_bytes.as_ptr() as *const i8;
        let len = raw_bytes.len();
        // SAFETY: `raw_bytes` is a valid &[u8] slice derived from `self.data`, which is
        // borrowed for the lifetime `'a` of the FrameReader. Reinterpreting u8 as i8 is
        // sound because both types have the same size (1 byte), alignment (1), and bit
        // layout. The returned &[i8] has lifetime `'a` tied to the FrameReader's borrow,
        // so it cannot outlive the underlying data. If the type_tag check were removed or
        // the payload layout changed, this safety justification would need to be revisited.
        Some(unsafe { std::slice::from_raw_parts(ptr, len) })
    }
}

pub fn deserialize_payload_fuzz(
    payload: &[u8],
) -> crate::error::Result<(String, String, DataValue)> {
    deserialize_payload(payload)
}

impl StorageEngine {
    pub fn set_max_streams(&mut self, val: usize) {
        self.max_streams = val;
    }
    pub fn set_max_index_ram_bytes(&mut self, val: u64) {
        self.max_index_ram_bytes = val;
    }
    pub fn set_max_fds(&mut self, val: usize) {
        self.max_fds = val;
    }

    pub fn set_max_scan_results(&mut self, val: usize) {
        self.max_scan_results = val;
    }

    pub fn active_handles_count(&self) -> usize {
        self.file_handles.lock().unwrap().len()
    }

    /// Sets the fallback max-age for streams without an explicit policy.
    /// `0` disables retention.
    pub fn set_default_retention(&self, max_age_ms: u64) {
        self.retention.lock().unwrap().default_max_age_ms = max_age_ms;
    }

    /// Sets or clears a per-stream max-age. `None` removes the override.
    pub fn set_stream_retention(&self, stream_name: &str, max_age_ms: Option<u64>) {
        let mut policy = self.retention.lock().unwrap();
        match max_age_ms {
            Some(ms) if ms > 0 => {
                policy.streams.insert(stream_name.to_string(), ms);
            }
            _ => {
                policy.streams.remove(stream_name);
            }
        }
    }

    pub fn retention_policy(&self) -> RetentionPolicy {
        self.retention.lock().unwrap().clone()
    }

    /// Deletes inactive segments whose live records are all older than their
    /// stream's max-age. This is segment-level expiry: a segment containing any
    /// unexpired record (or a stream with retention disabled) is kept.
    pub fn enforce_retention(&self) -> crate::error::Result<RetentionReport> {
        let _maintenance = self.maintenance_lock.lock().unwrap();
        let policy = self.retention.lock().unwrap().clone();
        if policy.default_max_age_ms == 0 && policy.streams.is_empty() {
            return Ok(RetentionReport::default());
        }

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let active_id = self.active_segment_id.load(Ordering::SeqCst);
        let mut segments = Vec::new();
        for entry in fs::read_dir(&self.data_dir).map_err(LivenError::Io)? {
            let entry = entry.map_err(LivenError::Io)?;
            let path = entry.path();
            if path.is_file()
                && path.extension().and_then(|s| s.to_str()) == Some("liven")
                && let Some(stem) = path.file_stem().and_then(|s| s.to_str())
                && let Ok(segment_id) = stem.parse::<u64>()
                && segment_id != active_id
            {
                segments.push((segment_id, path));
            }
        }
        segments.sort_by_key(|(id, _)| *id);

        let mut report = RetentionReport::default();
        let mut first_unlink = true;
        test_crash_point("retention_before_unlink");
        for (segment_id, path) in segments {
            report.checked_segments += 1;
            if !segment_is_fully_expired(&path, now, &policy).unwrap_or(false) {
                continue;
            }

            let freed_bytes = path.metadata().map(|m| m.len()).unwrap_or(0);
            let keys: Vec<String> = self
                .skipmap
                .iter()
                .filter(|(_, ptr)| ptr.segment_id == segment_id)
                .map(|(key, _)| key)
                .collect();
            for key in keys {
                let Some(pointer) = self.skipmap.get(&key) else {
                    continue;
                };
                if let Ok(record) = self.read_record(pointer) {
                    let guard = self.timestamp_indexes.lock().unwrap();
                    if let Some(index) = guard.get(&record.stream_name)
                        && index.remove(record.timestamp, record.sequence_id)
                    {
                        self.index_ram_bytes
                            .fetch_sub(TIMESTAMP_INDEX_NODE_BYTES, Ordering::SeqCst);
                    }
                }
                if self.skipmap.remove(&key).is_some() {
                    self.index_ram_bytes
                        .fetch_sub(primary_charge(&key), Ordering::SeqCst);
                    report.deleted_keys += 1;
                }
            }

            self.file_handles.lock().unwrap().remove(&segment_id);
            pl_unlink(&path);
            fs::remove_file(&path).map_err(LivenError::Io)?;
            if first_unlink {
                first_unlink = false;
                test_crash_point("retention_after_unlink");
            }
            report.deleted_segments += 1;
            report.freed_bytes += freed_bytes;
        }

        if report.deleted_segments > 0 {
            sync_data_dir(&self.data_dir).map_err(LivenError::Io)?;
            test_crash_point("retention_after_dir_sync");
            info!(
                "Retention: deleted {} segment(s), {} key(s), {} bytes",
                report.deleted_segments, report.deleted_keys, report.freed_bytes
            );
        }
        Ok(report)
    }
}

/// True when every live record in the segment is older than its stream's
/// retention policy. Conservative: unreadable/torn frames, unknown streams, or
/// streams with retention disabled keep the segment.
fn segment_is_fully_expired(
    path: &Path,
    now_ms: i64,
    policy: &RetentionPolicy,
) -> crate::error::Result<bool> {
    let mut file = File::open(path).map_err(LivenError::Io)?;
    let file_len = file.metadata().map_err(LivenError::Io)?.len();
    let mut offset = 0u64;
    let mut header = [0u8; 26];
    let mut saw_live_frame = false;

    while offset + 26 <= file_len {
        file.seek(SeekFrom::Start(offset)).map_err(LivenError::Io)?;
        if file.read_exact(&mut header).is_err() {
            return Ok(false);
        }
        let timestamp = i64::from_be_bytes([
            header[8], header[9], header[10], header[11], header[12], header[13], header[14],
            header[15],
        ]);
        let flags = header[17];
        let payload_len =
            u32::from_be_bytes([header[18], header[19], header[20], header[21]]) as u64;
        if offset + 26 + payload_len > file_len {
            return Ok(false);
        }
        if flags & 0x02 == 0 {
            saw_live_frame = true;
            if payload_len < 2 {
                return Ok(false);
            }
            let mut len_buf = [0u8; 2];
            file.read_exact_at(&mut len_buf, offset + 26)
                .map_err(LivenError::Io)?;
            let stream_len = u16::from_be_bytes(len_buf) as u64;
            if stream_len + 2 > payload_len {
                return Ok(false);
            }
            let mut stream_buf = vec![0u8; stream_len as usize];
            file.read_exact_at(&mut stream_buf, offset + 28)
                .map_err(LivenError::Io)?;
            let Ok(stream_name) = std::str::from_utf8(&stream_buf) else {
                return Ok(false);
            };
            let max_age_ms = policy
                .streams
                .get(stream_name)
                .copied()
                .unwrap_or(policy.default_max_age_ms);
            if max_age_ms == 0 {
                return Ok(false);
            }
            let cutoff = now_ms.saturating_sub(max_age_ms as i64);
            if timestamp >= cutoff {
                return Ok(false);
            }
        }
        offset += 26 + payload_len;
    }

    // Empty segments and tombstone-only segments carry no live data.
    Ok(saw_live_frame || offset == 0)
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod ack_tests {
    use super::*;

    #[test]
    fn flusher_budget_rejection_is_typed() {
        let dir = tempfile::tempdir().unwrap();
        let active_path = dir.path().join(format!("{:020}.liven", 1u64));
        let active = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&active_path)
            .unwrap();

        let (response_tx, response_rx) = std::sync::mpsc::sync_channel(1);
        let mut batch = vec![ring_buffer::LogEntry::Single {
            stream_name: "s".to_string(),
            key: crate::storage::key::StreamKey::from_str("k").unwrap(),
            value: DataValue::String("v".to_string()),
            is_tombstone: false,
            response_tx,
        }];

        let seq = Arc::new(AtomicU64::new(1));
        let active_id = Arc::new(AtomicU64::new(1));
        let id_counter = Arc::new(AtomicU64::new(2));
        let file = Arc::new(Mutex::new(Some(active)));
        let size = Arc::new(Mutex::new(0u64));
        let skipmap = Arc::new(IndexMap::new());
        let (broadcast_tx, _) = broadcast::channel(16);
        let handles = Arc::new(Mutex::new(HashMap::new()));
        let failed = Arc::new(AtomicBool::new(false));
        let index_ram = Arc::new(AtomicU64::new(0));
        let high_water = Arc::new(AtomicBool::new(false));
        let fsyncs = Arc::new(AtomicU64::new(0));
        let timestamp_indexes = Arc::new(Mutex::new(HashMap::new()));

        execute_batch_append(
            &mut batch,
            dir.path(),
            1024 * 1024,
            &seq,
            &active_id,
            &id_counter,
            &file,
            &size,
            &skipmap,
            &broadcast_tx,
            &handles,
            "always",
            &failed,
            &index_ram,
            1,
            1,
            &high_water,
            &fsyncs,
            &timestamp_indexes,
        )
        .unwrap();

        match response_rx.recv().unwrap() {
            Err(ring_buffer::AckError::IndexRam { max_bytes, .. }) => {
                assert_eq!(max_bytes, 1);
            }
            other => panic!("expected typed IndexRam ack error, got {other:?}"),
        }
    }
}

impl Drop for StorageEngine {
    fn drop(&mut self) {
        // Signal shutdown to flusher thread
        self.is_running.store(false, Ordering::Relaxed);
        let _ = self.ring_buffer.send_shutdown();

        // Wait for flusher thread to complete
        if let Some(handle) = self.flusher_handle.take() {
            let _ = handle.join();
        }

        // Clean up active file. A failure here cannot be returned from Drop,
        // but it must not be silent: the next open will recover from the log.
        if let Ok(mut active_file_guard) = self.active_file.lock()
            && let Some(file) = active_file_guard.take()
        {
            let path = self
                .data_dir
                .join(format!("{:020}.liven", self.active_segment_id()));
            let len = file.metadata().map(|m| m.len()).unwrap_or(0);
            if let Err(e) = file.sync_all() {
                error!("final fsync on shutdown failed: {}", e);
            } else {
                pl_sync(&path, len);
            }
        }
    }
}
