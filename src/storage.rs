use crate::error::LivenError;
use crate::types::{DataValue, LogPointer, Record};
use crossbeam_skiplist::SkipMap;
use fs2::FileExt;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::broadcast;
use tracing::{error, info, warn};

/// Controls how `recover()` handles corrupted or truncated segment files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryMode {
    /// Safe default: auto-truncate trailing torn writes on the last segment,
    /// skip broken mid-file frames in inactive segments (log and continue),
    /// and still start the database. No files are modified on disk — truncation
    /// stops replay at the last known-good frame boundary but does not rewrite
    /// the segment file.
    Default,
    /// Fail hard on any CRC mismatch or incomplete frame — preserve today's
    /// behaviour for operators who want to investigate before auto-repairing.
    Strict,
    /// Before modifying or skipping any segment, write a full backup copy to
    /// `{segment_id:020}.liven.repair-{unix_timestamp}`. Then proceed with
    /// Default-mode recovery (truncate torn writes, skip broken inactive
    /// segments). Prints a summary report of exactly what was discarded.
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
    /// Whether any recovery action was taken (false for clean startups).
    pub had_issues: bool,
}

#[allow(unsafe_code)]
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
}

#[cfg(unix)]
impl PositionalReader for File {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
        std::os::unix::fs::FileExt::read_exact_at(self, buf, offset)
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
}

pub struct StorageEngine {
    data_dir: PathBuf,
    max_segment_size: u64,
    current_sequence_id: Arc<AtomicU64>,
    active_segment_id: Arc<AtomicU64>,
    // We protect the active file handle and its size using a Mutex to serialize append operations
    active_file: Arc<Mutex<Option<File>>>,
    active_size: Arc<Mutex<u64>>,
    pub skipmap: Arc<SkipMap<String, LogPointer>>,
    broadcast_tx: broadcast::Sender<Record>,
    file_handles: Arc<Mutex<HashMap<u64, Arc<File>>>>,
    ring_buffer: ring_buffer::LogRingBuffer,
    #[allow(dead_code)]
    sync_mode: String,
    #[allow(dead_code)]
    sync_interval_ms: u64,
    is_running: Arc<std::sync::atomic::AtomicBool>,
    max_streams: usize,
    max_fds: usize,
    max_index_ram_bytes: u64,
    pub index_ram_bytes: Arc<AtomicU64>,
    pub streams_set: Arc<Mutex<HashSet<String>>>,
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
    /// Held by: InsertBatch, DeleteKey, PipelineDelete (full scan path),
    /// PipelineUpdate. NOT held by single Insert or Upsert (last-writer-wins).
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
        let path = data_dir.as_ref().to_path_buf();
        fs::create_dir_all(&path).map_err(LivenError::Io)?;

        let max_connections: usize = 10000;
        let broadcast_capacity: usize = 4096;
        let sync_mode: String = "always".to_string();
        let sync_interval_ms: u64 = 100;
        let max_scan_results: usize = 100_000;

        let conn_semaphore = Arc::new(tokio::sync::Semaphore::new(max_connections));
        let (broadcast_tx, _) = broadcast::channel(broadcast_capacity);

        let current_sequence_id = Arc::new(AtomicU64::new(1));
        let active_segment_id = Arc::new(AtomicU64::new(1));
        let active_file = Arc::new(Mutex::new(None));
        let active_size = Arc::new(Mutex::new(0));
        let skipmap = Arc::new(SkipMap::new());
        let file_handles = Arc::new(Mutex::new(HashMap::new()));
        let is_running = Arc::new(std::sync::atomic::AtomicBool::new(true));

        let (tx, rx) = std::sync::mpsc::sync_channel(2048);
        let ring_buffer = ring_buffer::LogRingBuffer::new(tx);

        let index_ram_bytes = Arc::new(AtomicU64::new(0));
        let streams_set = Arc::new(Mutex::new(HashSet::new()));
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
            sync_interval_ms,
            is_running: is_running.clone(),
            max_streams,
            max_fds,
            max_index_ram_bytes,
            index_ram_bytes: index_ram_bytes.clone(),
            streams_set: streams_set.clone(),
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
            commit_drain_window_us: 5000,
            commit_max_batch_size: 2048,
            recovery_mode,
            lock_timeout_secs: 5,
            recovery_report: None,
        };

        engine.recover()?;

        // Initialize segment_id_counter to max(existing_segment_ids) + 1.
        // The recover() method already identifies the highest segment ID, so
        // we seed the counter from active_segment_id + 1 so the next rollover
        // produces a strictly greater ID.
        let max_seen_id = engine.active_segment_id.load(Ordering::SeqCst);
        engine
            .segment_id_counter
            .store(max_seen_id + 1, Ordering::SeqCst);

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
        let flusher_index_ram_bytes = index_ram_bytes.clone();
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
                flusher_index_ram_bytes,
                flusher_timestamp_indexes,
                flusher_drain_window_us,
                flusher_max_batch,
            );
        });

        engine.flusher_handle = Some(flusher_handle);

        if sync_mode == "periodic" {
            let active_file_clone = active_file;
            let sync_running = is_running.clone();
            std::thread::spawn(move || {
                while sync_running.load(Ordering::Relaxed) {
                    std::thread::sleep(std::time::Duration::from_millis(sync_interval_ms));
                    if !sync_running.load(Ordering::Relaxed) {
                        break;
                    }
                    if let Ok(mut file_guard) = active_file_clone.try_lock()
                        && let Some(file) = file_guard.as_mut()
                    {
                        let _ = file.sync_data();
                    }
                }
            });
        }

        Ok(engine)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Record> {
        self.broadcast_tx.subscribe()
    }

    /// Get the number of active broadcast subscribers
    pub fn broadcast_subscriber_count(&self) -> usize {
        self.broadcast_tx.receiver_count()
    }

    pub fn with_broadcast_capacity(mut self, capacity: usize) -> Self {
        self.broadcast_capacity = capacity;
        self
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
        fs::create_dir_all(&path).map_err(LivenError::Io)?;

        // Minimal engine construction — only what recover() needs.
        let skipmap = Arc::new(SkipMap::new());
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
            sync_interval_ms: 0,
            is_running: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            max_streams: 32,
            max_fds: 64,
            max_index_ram_bytes: 16 * 1024 * 1024,
            index_ram_bytes: index_ram_bytes.clone(),
            streams_set: streams_set.clone(),
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
        };

        engine.recover()?;
        Ok(engine.recovery_report.clone().unwrap_or_default())
    }

    /// Gets the next monotonic sequence ID
    pub fn next_sequence_id(&self) -> u64 {
        self.current_sequence_id.load(Ordering::SeqCst)
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

    /// Scans the directory and rebuilds the concurrent SkipMap index
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

        // Determine the last (highest-ID) segment for torn-write detection.
        let last_segment_id = segments.last().map(|(id, _)| *id);
        let mut highest_seq = 0;
        let mut active_id = 1;

        let mut report = RecoveryReport::default();

        for (segment_id, path) in &segments {
            active_id = *segment_id;
            let is_last = Some(*segment_id) == last_segment_id;

            let mut file = File::open(path).map_err(LivenError::Io)?;
            self.lock_segment_with_timeout(&mut file, *segment_id, path)?;

            let file_len = file.metadata().map_err(LivenError::Io)?.len();
            if file_len == 0 {
                file.unlock().map_err(LivenError::Io)?;
                continue;
            }
            let mut offset = 0;
            let mut header_buf = [0u8; 26];
            let mut segment_corrupted = false;
            let mut last_good_offset = 0u64;

            while offset + 26 <= file_len {
                file.seek(SeekFrom::Start(offset)).map_err(LivenError::Io)?;
                if file.read_exact(&mut header_buf).is_err() {
                    // Partial header at end of file — torn write.
                    if is_last && offset < file_len {
                        let discarded = file_len - offset;
                        warn!(
                            "Torn header at offset {} in last segment {} ({} bytes discarded). Truncating.",
                            offset, segment_id, discarded
                        );
                        report
                            .truncated_segments
                            .push((*segment_id, offset, discarded));
                        report.had_issues = true;
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

                // Incomplete payload — torn write
                if offset + 26 + payload_len > file_len {
                    if is_last {
                        let discarded = file_len.saturating_sub(offset);
                        warn!(
                            "Torn payload at offset {} in last segment {} (declared {} bytes, {} available). Truncating.",
                            offset,
                            segment_id,
                            payload_len,
                            file_len.saturating_sub(offset + 26)
                        );
                        report
                            .truncated_segments
                            .push((*segment_id, offset, discarded));
                        report.had_issues = true;
                    } else {
                        warn!(
                            "Incomplete payload in inactive segment {} at offset {}. Skipping remainder.",
                            segment_id, offset
                        );
                        report.skipped_segments.push((*segment_id, offset, 0));
                        report.had_issues = true;
                    }
                    break;
                }

                // Read payload and verify CRC
                let mut payload_buf = vec![0u8; payload_len as usize];
                file.read_exact(&mut payload_buf).map_err(LivenError::Io)?;

                let actual_crc = crc32fast::hash(&payload_buf);
                if actual_crc != declared_crc {
                    if is_last {
                        // Last segment + bad CRC near end -> likely torn write
                        let discarded = file_len.saturating_sub(offset);
                        if self.recovery_mode == RecoveryMode::Strict {
                            error!(
                                "Strict mode: CRC mismatch in last segment {} at offset {}. Aborting.",
                                segment_id, offset
                            );
                            return Err(LivenError::StrictCrcMismatch {
                                segment_id: *segment_id,
                                offset,
                            });
                        }
                        self.maybe_backup_for_repair(path, *segment_id, &mut report)?;
                        warn!(
                            "CRC mismatch at offset {} in last segment {} - torn write assumed. Truncating to offset {} ({} bytes discarded).",
                            offset, segment_id, last_good_offset, discarded
                        );
                        report
                            .truncated_segments
                            .push((*segment_id, last_good_offset, discarded));
                        report.had_issues = true;
                        break;
                    } else {
                        // Mid-file corruption in inactive segment
                        if self.recovery_mode == RecoveryMode::Strict {
                            error!(
                                "Strict mode: CRC mismatch in segment {} at offset {}. Aborting.",
                                segment_id, offset
                            );
                            return Err(LivenError::StrictCrcMismatch {
                                segment_id: *segment_id,
                                offset,
                            });
                        }
                        self.maybe_backup_for_repair(path, *segment_id, &mut report)?;
                        let approx_keys = self
                            .skipmap
                            .iter()
                            .filter(|e| e.value().segment_id == *segment_id)
                            .count();
                        warn!(
                            "CRC mismatch in inactive segment {} at offset {} - skipping segment (~{} keys potentially affected).",
                            segment_id, offset, approx_keys
                        );
                        report
                            .skipped_segments
                            .push((*segment_id, offset, approx_keys));
                        report.had_issues = true;
                        segment_corrupted = true;
                        break;
                    }
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

                        let node_overhead = compound_key.len() as u64 + 64;
                        if flags & 0x02 != 0 {
                            // Tombstone — remove from skipmap only; do not add stream to streams_set
                            if self.skipmap.remove(&compound_key).is_some() {
                                self.index_ram_bytes
                                    .fetch_sub(node_overhead, Ordering::SeqCst);
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
                            let pointer = LogPointer {
                                segment_id: *segment_id,
                                file_offset: offset,
                            };
                            let existed = self.skipmap.contains_key(&compound_key);
                            self.skipmap.insert(compound_key, pointer);
                            if !existed {
                                let current_ram = self
                                    .index_ram_bytes
                                    .fetch_add(node_overhead, Ordering::SeqCst)
                                    + node_overhead;
                                if current_ram > self.max_index_ram_bytes {
                                    return Err(create_index_ram_limit_error(
                                        self.max_index_ram_bytes,
                                        current_ram,
                                    ));
                                }
                            }

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
                    Err(e) => {
                        if is_last {
                            if self.recovery_mode == RecoveryMode::Strict {
                                error!(
                                    "Strict mode: unparseable payload in last segment {} at offset {}: {}",
                                    segment_id, offset, e
                                );
                                return Err(e);
                            }
                            self.maybe_backup_for_repair(path, *segment_id, &mut report)?;
                            let discarded = file_len.saturating_sub(offset);
                            warn!(
                                "Unparseable payload at offset {} in last segment {} - torn write assumed. Truncating ({} bytes discarded).",
                                offset, segment_id, discarded
                            );
                            report.truncated_segments.push((
                                *segment_id,
                                last_good_offset,
                                discarded,
                            ));
                            report.had_issues = true;
                            break;
                        } else {
                            if self.recovery_mode == RecoveryMode::Strict {
                                error!(
                                    "Strict mode: unparseable payload in segment {} at offset {}: {}",
                                    segment_id, offset, e
                                );
                                return Err(e);
                            }
                            self.maybe_backup_for_repair(path, *segment_id, &mut report)?;
                            let approx_keys = self
                                .skipmap
                                .iter()
                                .filter(|entry| entry.value().segment_id == *segment_id)
                                .count();
                            warn!(
                                "Unparseable payload in inactive segment {} at offset {} - skipping segment (~{} keys affected).",
                                segment_id, offset, approx_keys
                            );
                            report
                                .skipped_segments
                                .push((*segment_id, offset, approx_keys));
                            report.had_issues = true;
                            segment_corrupted = true;
                            break;
                        }
                    }
                }

                last_good_offset = offset + 26 + payload_len;
                offset = last_good_offset;
            }

            file.unlock().map_err(LivenError::Io)?;

            // If this segment was corrupted mid-file, evict skipmap entries
            // that may have been added from earlier frames in this segment.
            if segment_corrupted {
                let keys_to_remove: Vec<String> = self
                    .skipmap
                    .iter()
                    .filter(|e| e.value().segment_id == *segment_id)
                    .map(|e| e.key().clone())
                    .collect();
                for key in &keys_to_remove {
                    if self.skipmap.remove(key).is_some() {
                        let overhead = key.len() as u64 + 64;
                        self.index_ram_bytes.fetch_sub(overhead, Ordering::SeqCst);
                    }
                }
            }

            // Detect trailing bytes after last complete frame (torn write
            // that leaves a partial header between frame boundaries).
            if !segment_corrupted && is_last && offset < file_len {
                let discarded = file_len - offset;
                warn!(
                    "Trailing bytes after last frame in segment {} (offset {}, {} bytes discarded). Truncating.",
                    segment_id, offset, discarded
                );
                report
                    .truncated_segments
                    .push((*segment_id, offset, discarded));
                report.had_issues = true;
            }
        }

        self.current_sequence_id
            .store(highest_seq + 1, Ordering::SeqCst);
        self.active_segment_id.store(active_id, Ordering::SeqCst);

        // Open active file handle
        let active_path = self.segment_path(active_id);
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&active_path)
            .map_err(LivenError::Io)?;

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
        info!(
            "Repair backup: {} -> {}",
            path.display(),
            backup_path.display()
        );
        report.repair_backups.push(backup_path);
        Ok(())
    }

    /// Appends a new record to the storage engine
    pub fn append(
        &self,
        stream_name: &str,
        key: &str,
        value: DataValue,
        is_tombstone: bool,
    ) -> crate::error::Result<Record> {
        let compound_key = format!("{}:{}", stream_name, key);

        // 1. Stream Limit Check
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

        // 2. Index RAM Limit Check
        if !is_tombstone {
            let existed = self.skipmap.contains_key(&compound_key);
            if !existed {
                let size = compound_key.len() as u64 + 64;
                let current_ram = self.index_ram_bytes.load(Ordering::SeqCst);
                if current_ram + size > self.max_index_ram_bytes {
                    return Err(create_index_ram_limit_error(
                        self.max_index_ram_bytes,
                        current_ram + size,
                    ));
                }
            }
        }

        let stream_key = crate::storage::key::StreamKey::from_str(key)?;
        self.ring_buffer
            .append(stream_name, stream_key, value, is_tombstone)
    }

    /// Appends a batch of records to the storage engine with high performance
    pub fn append_batch(
        &self,
        batch: Vec<(String, String, DataValue)>,
    ) -> crate::error::Result<Vec<Record>> {
        // Pre-validate streams count and index RAM limits transactionally
        let mut new_streams = HashSet::new();
        let mut new_keys_size = 0u64;

        {
            let streams_guard = self.streams_set.lock().unwrap();
            for (stream_name, key, _) in &batch {
                if !streams_guard.contains(stream_name) && !new_streams.contains(stream_name) {
                    new_streams.insert(stream_name.clone());
                }

                let compound_key = format!("{}:{}", stream_name, key);
                if !self.skipmap.contains_key(&compound_key) {
                    new_keys_size += compound_key.len() as u64 + 64;
                }
            }

            if streams_guard.len() + new_streams.len() > self.max_streams {
                return Err(LivenError::StreamLimitExceeded {
                    max: self.max_streams,
                });
            }
        }

        let current_ram = self.index_ram_bytes.load(Ordering::SeqCst);
        if current_ram + new_keys_size > self.max_index_ram_bytes {
            return Err(create_index_ram_limit_error(
                self.max_index_ram_bytes,
                current_ram + new_keys_size,
            ));
        }

        // Apply stream registration
        if !new_streams.is_empty() {
            let mut streams_guard = self.streams_set.lock().unwrap();
            for stream in new_streams {
                streams_guard.insert(stream);
            }
        }

        let mut rx_channels = Vec::with_capacity(batch.len());

        for (stream_name, key, value) in batch {
            let stream_key = crate::storage::key::StreamKey::from_str(&key)?;
            let (response_tx, rx) = std::sync::mpsc::sync_channel(1);
            let entry = ring_buffer::LogEntry::Single {
                stream_name,
                key: stream_key,
                value,
                is_tombstone: false,
                response_tx,
            };
            self.ring_buffer.tx_send(entry)?;
            rx_channels.push(rx);
        }

        let mut results = Vec::with_capacity(rx_channels.len());
        for rx in rx_channels {
            let record = rx.recv().map_err(|e| {
                LivenError::Internal(format!("LogRingBuffer coordination error: {}", e))
            })??;
            results.push(record);
        }

        Ok(results)
    }

    /// Appends a batch of tombstones to the storage engine with high performance (O(1) disk syncs)
    pub fn append_tombstone_batch(
        &self,
        stream_name: &str,
        keys: &[String],
    ) -> crate::error::Result<Vec<Record>> {
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

    /// Reads a record from disk via its LogPointer using safe positional reads
    pub fn read_record(&self, pointer: LogPointer) -> crate::error::Result<Record> {
        let file = self.get_or_load_file_handle(pointer.segment_id)?;
        let file_len = file.metadata().map_err(LivenError::Io)?.len();
        let offset = pointer.file_offset;

        if offset + 26 > file_len {
            return Err(LivenError::PointerOutOfBounds {
                segment_id: pointer.segment_id,
                offset,
            });
        }

        let mut header_buf = [0u8; 26];
        file.read_exact_at(&mut header_buf, offset)
            .map_err(LivenError::Io)?;

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

        if offset + 26 + (payload_len as u64) > file_len {
            return Err(LivenError::PayloadOutOfBounds {
                segment_id: pointer.segment_id,
                offset,
            });
        }

        let record = READ_STAGING_BUFFER.with(|buf_cell| {
            let mut local_buf = buf_cell.borrow_mut();
            if local_buf.len() < payload_len {
                local_buf.resize(payload_len, 0);
            }
            let target_slice = &mut local_buf[..payload_len];

            file.read_exact_at(target_slice, offset + 26)
                .map_err(LivenError::Io)?;

            let actual_crc = crc32fast::hash(target_slice);
            if actual_crc != crc32 {
                return Err(LivenError::CrcMismatch {
                    segment_id: pointer.segment_id,
                    offset,
                });
            }

            let (stream_name, key, value) = deserialize_payload_borrowed(target_slice, type_tag)?;
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
        })?;

        Ok(record)
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

        if let Some(entry) = self.skipmap.get(lookup_key) {
            let record = self.read_record(*entry.value())?;
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    /// Lists all streams currently indexed in the skipmap
    pub fn list_streams(&self) -> Vec<String> {
        let guard = self.streams_set.lock().unwrap();
        guard.iter().cloned().collect()
    }

    /// Lists all keys under a given stream
    pub fn list_keys(&self, stream_name: &str) -> Vec<String> {
        let prefix = format!("{}:", stream_name);
        let mut keys = Vec::new();
        for entry in self.skipmap.iter() {
            if entry.key().starts_with(&prefix) {
                keys.push(entry.key()[prefix.len()..].to_string());
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
    /// in-memory key index (the skipmap holds one live `LogPointer` per key) and
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
        for entry in self
            .skipmap
            .range::<str, _>((Bound::Included(prefix.as_str()), Bound::Unbounded))
        {
            if !entry.key().starts_with(prefix.as_str()) {
                break; // past this stream's key range
            }
            if records.len() >= effective_limit {
                break;
            }

            let key = entry.key();
            let mut pointer = *entry.value();
            self.frames_read.fetch_add(1, Ordering::Relaxed);
            let record = match self.read_record(pointer) {
                Ok(r) => Some(r),
                Err(_) => {
                    // Compaction may have relocated the record; retry once with a
                    // fresh pointer, and give up if it is now gone.
                    pointer = match self.skipmap.get(key) {
                        Some(e) => *e.value(),
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
                && segment_id < active_id
            {
                inactive_segments.push((segment_id, path));
            }
        }

        if inactive_segments.is_empty() {
            info!("No inactive segments available for compaction.");
            return Ok(());
        }

        // Identify all keys in the SkipMap that point to an inactive segment
        let mut active_pointers_to_rewrite = Vec::new();
        for entry in self.skipmap.iter() {
            let pointer = *entry.value();
            if pointer.segment_id < active_id {
                active_pointers_to_rewrite.push((entry.key().clone(), pointer));
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
        let compacted_path = self.segment_path(compacted_segment_id);

        let mut comp_file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&compacted_path)
            .map_err(LivenError::Io)?;

        comp_file.lock_exclusive().map_err(LivenError::Io)?;

        let mut offset = 0u64;

        // Group rewritten pointers to batch updates
        let mut new_pointers = HashMap::new();

        for (compound_key, pointer) in active_pointers_to_rewrite {
            let record = match self.read_record(pointer) {
                Ok(rec) => rec,
                Err(e) => {
                    warn!(
                        "Compaction skipped record for key {} due to read error: {}",
                        compound_key, e
                    );
                    continue;
                }
            };

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

                comp_file.write_all(&frame_buf).map_err(LivenError::Io)?;

                new_pointers.insert(
                    compound_key,
                    LogPointer {
                        segment_id: compacted_segment_id,
                        file_offset: offset,
                    },
                );

                offset += frame_buf.len() as u64;
                Ok::<(), LivenError>(())
            })?;
        }

        comp_file.sync_data().map_err(LivenError::Io)?;
        comp_file.unlock().map_err(LivenError::Io)?;

        // Update SkipMap pointers
        for (compound_key, new_ptr) in new_pointers {
            // Verify if the SkipMap pointer hasn't been updated concurrently by a writer
            let should_update = if let Some(current_ptr) = self.skipmap.get(&compound_key) {
                current_ptr.value().segment_id < active_id
            } else {
                false
            };

            if should_update {
                self.skipmap.insert(compound_key, new_ptr);
            }
        }

        // Remove inactive file handles from the cache first to prevent Windows sharing violations!
        {
            let mut handles_guard = self.file_handles.lock().unwrap();
            for (segment_id, _) in &inactive_segments {
                handles_guard.remove(segment_id);
            }
        }

        // Now we can safely remove the old inactive segment files!
        for (_, path) in inactive_segments {
            let _ = fs::remove_file(path);
        }

        info!(
            "Log compaction completed successfully. Compacted Segment ID: {}, Size: {} bytes",
            compacted_segment_id, offset
        );
        Ok(())
    }
}

/// Helper function to create IndexRamLimitExceeded errors
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
                    && segment_id < active_id
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
    skipmap: Arc<SkipMap<String, LogPointer>>,
    broadcast_tx: broadcast::Sender<Record>,
    file_handles: Arc<Mutex<HashMap<u64, Arc<File>>>>,
    sync_mode: String,
    is_running: Arc<std::sync::atomic::AtomicBool>,
    index_ram_bytes: Arc<AtomicU64>,
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
            &index_ram_bytes,
            &timestamp_indexes,
        ) {
            // Notify all waiting entries of the failure
            for entry in batch.drain(..) {
                let err_str = e.to_string();
                match entry {
                    ring_buffer::LogEntry::Single { response_tx, .. } => {
                        let _ = response_tx.send(Err(err_str.clone()));
                    }
                    ring_buffer::LogEntry::TombstoneBatch { response_tx, .. } => {
                        let _ = response_tx.send(Err(err_str.clone()));
                    }
                    ring_buffer::LogEntry::Shutdown => {
                        // Shutdown sentinel, no response needed
                    }
                }
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
    skipmap: &Arc<SkipMap<String, LogPointer>>,
    broadcast_tx: &broadcast::Sender<Record>,
    file_handles: &Arc<Mutex<HashMap<u64, Arc<File>>>>,
    sync_mode: &str,
    index_ram_bytes: &Arc<AtomicU64>,
    timestamp_indexes: &Arc<Mutex<HashMap<String, Arc<timestamp_index::TimestampIndex>>>>,
) -> crate::error::Result<()> {
    let mut active_file_guard = active_file.lock().unwrap();
    let mut active_size_guard = active_size.lock().unwrap();

    let mut current_seq = current_sequence_id.load(Ordering::SeqCst);

    enum PendingComplete {
        Single {
            response_tx: std::sync::mpsc::SyncSender<Result<Record, String>>,
        },
        Batch {
            response_tx: std::sync::mpsc::SyncSender<Result<Vec<Record>, String>>,
            count: usize,
        },
    }

    let mut pending_completes = Vec::with_capacity(batch.len());
    let mut batch_records_to_add = Vec::new();

    // Use the pre-allocated thread-local frame buffer to avoid heap reallocations per batch.
    let mut frame_buf = FRAME_BUFFER.with(|fb| {
        let mut buf = fb.borrow_mut();
        buf.clear();
        std::mem::take(&mut *buf)
    });

    SERIALIZATION_BUFFER.with(|buf_cell| {
        let mut local_buf = buf_cell.borrow_mut();
        for entry in batch.iter() {
            match entry {
                ring_buffer::LogEntry::Single {
                    stream_name,
                    key,
                    value,
                    is_tombstone,
                    response_tx,
                } => {
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

                    batch_records_to_add.push((record, frame_len, *is_tombstone));
                    pending_completes.push(PendingComplete::Single {
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

                        batch_records_to_add.push((record, frame_len, true));
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

    if *active_size_guard + (frame_buf.len() as u64) > max_segment_size {
        if let Some(old_file) = active_file_guard.take() {
            let _ = old_file.sync_all();
        }

        let new_active_id = segment_id_counter.fetch_add(1, Ordering::SeqCst);
        active_segment_id.store(new_active_id, Ordering::SeqCst);

        let new_path = data_dir.join(format!("{:020}.liven", new_active_id));
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&new_path)
            .map_err(LivenError::Io)?;

        *active_file_guard = Some(file);
        *active_size_guard = 0;
        info!("Rolled over to new active segment: {}", new_active_id);
    }

    let file = active_file_guard
        .as_mut()
        .ok_or_else(|| LivenError::Internal("No active file handle found".to_string()))?;
    let current_offset = *active_size_guard;

    file.write_all(&frame_buf).map_err(|e| {
        if e.kind() == std::io::ErrorKind::StorageFull || e.kind() == std::io::ErrorKind::WriteZero
        {
            LivenError::DiskFull
        } else {
            LivenError::Io(e)
        }
    })?;

    if sync_mode == "always" {
        file.sync_data().map_err(LivenError::Io)?;
    }

    let segment_id = active_segment_id.load(Ordering::SeqCst);

    let mut offset_acc = current_offset;
    let mut records_iter = batch_records_to_add.into_iter();

    for comp in pending_completes {
        match comp {
            PendingComplete::Single { response_tx } => {
                if let Some((record, frame_len, is_tombstone)) = records_iter.next() {
                    let compound_key = format!("{}:{}", record.stream_name, record.key.as_str());

                    let node_overhead = compound_key.len() as u64 + 64;
                    if is_tombstone {
                        if skipmap.remove(&compound_key).is_some() {
                            index_ram_bytes.fetch_sub(node_overhead, Ordering::SeqCst);
                        }
                    } else {
                        let pointer = LogPointer {
                            segment_id,
                            file_offset: offset_acc,
                        };
                        let existed = skipmap.contains_key(&compound_key);
                        skipmap.insert(compound_key, pointer);
                        if !existed {
                            index_ram_bytes.fetch_add(node_overhead, Ordering::SeqCst);
                        }

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
            PendingComplete::Batch { response_tx, count } => {
                let mut added_records = Vec::with_capacity(count);
                for _ in 0..count {
                    if let Some((record, frame_len, is_tombstone)) = records_iter.next() {
                        let compound_key =
                            format!("{}:{}", record.stream_name, record.key.as_str());

                        let node_overhead = compound_key.len() as u64 + 64;
                        if is_tombstone {
                            if skipmap.remove(&compound_key).is_some() {
                                index_ram_bytes.fetch_sub(node_overhead, Ordering::SeqCst);
                            }
                        } else {
                            let pointer = LogPointer {
                                segment_id,
                                file_offset: offset_acc,
                            };
                            let existed = skipmap.contains_key(&compound_key);
                            skipmap.insert(compound_key, pointer);
                            if !existed {
                                index_ram_bytes.fetch_add(node_overhead, Ordering::SeqCst);
                            }

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

        // Clean up active file
        if let Ok(mut active_file_guard) = self.active_file.lock()
            && let Some(file) = active_file_guard.take()
        {
            let _ = file.sync_all();
        }
    }
}
