use crate::types::{DataValue, LogPointer, Record};
use crossbeam_skiplist::SkipMap;
use fs2::FileExt;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tokio::sync::broadcast;
use tracing::{error, info, warn};

pub mod key;
pub mod ring_buffer;

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
}

use std::cell::RefCell;

thread_local! {
    static SERIALIZATION_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(4096));
    static READ_STAGING_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(4096));
}

impl StorageEngine {
    pub fn new<P: AsRef<Path>>(data_dir: P, max_segment_size: u64) -> Result<Self, String> {
        let path = data_dir.as_ref().to_path_buf();
        fs::create_dir_all(&path).map_err(|e| e.to_string())?;

        let cfg_opt = crate::config::AppConfig::load().ok();

        let max_connections = if let Some(ref cfg) = cfg_opt {
            cfg.server.max_connections
        } else {
            10000
        };

        let broadcast_capacity = if let Some(ref cfg) = cfg_opt {
            cfg.server.broadcast_capacity
        } else {
            4096
        };

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

        let (sync_mode, sync_interval_ms) = if let Some(ref cfg) = cfg_opt {
            (cfg.storage.sync_mode.clone(), cfg.storage.sync_interval_ms)
        } else {
            ("always".to_string(), 100)
        };

        let max_streams = if let Some(ref cfg) = cfg_opt {
            cfg.limits.max_concurrent_streams
        } else {
            32
        };

        let max_fds = if let Some(ref cfg) = cfg_opt {
            cfg.limits.max_open_file_descriptors
        } else {
            64
        };

        let max_index_ram_bytes = if let Some(ref cfg) = cfg_opt {
            cfg.limits.max_index_ram_mb as u64 * 1024 * 1024
        } else {
            16 * 1024 * 1024
        };

        let max_segment_size = if max_segment_size < 1024 * 1024 {
            max_segment_size
        } else if let Some(ref cfg) = cfg_opt {
            cfg.limits.max_segment_size_mb as u64 * 1024 * 1024
        } else {
            max_segment_size
        };

        let index_ram_bytes = Arc::new(AtomicU64::new(0));
        let streams_set = Arc::new(Mutex::new(HashSet::new()));

        let engine = Self {
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
        };

        engine.recover()?;

        let flusher_data_dir = engine.data_dir.clone();
        let flusher_max_size = engine.max_segment_size;
        let flusher_seq = current_sequence_id;
        let flusher_active_id = active_segment_id;
        let flusher_file = active_file.clone();
        let flusher_size = active_size;
        let flusher_skipmap = skipmap;
        let flusher_broadcast = engine.broadcast_tx.clone();
        let flusher_handles = file_handles;
        let flusher_sync_mode = sync_mode.clone();
        let flusher_running = is_running.clone();
        let flusher_index_ram_bytes = index_ram_bytes.clone();

        std::thread::spawn(move || {
            run_flusher_loop(
                rx,
                flusher_data_dir,
                flusher_max_size,
                flusher_seq,
                flusher_active_id,
                flusher_file,
                flusher_size,
                flusher_skipmap,
                flusher_broadcast,
                flusher_handles,
                flusher_sync_mode,
                flusher_running,
                flusher_index_ram_bytes,
            );
        });

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

    pub fn with_broadcast_capacity(mut self, capacity: usize) -> Self {
        self.broadcast_capacity = capacity;
        self
    }

    pub fn with_max_connections(mut self, limit: usize) -> Self {
        self.max_connections = limit;
        self.conn_semaphore = Arc::new(tokio::sync::Semaphore::new(limit));
        self
    }

    /// Gets the next monotonic sequence ID
    pub fn next_sequence_id(&self) -> u64 {
        self.current_sequence_id.load(Ordering::SeqCst)
    }

    /// Helper to get a segment path from its ID
    fn segment_path(&self, segment_id: u64) -> PathBuf {
        self.data_dir.join(format!("{:020}.liven", segment_id))
    }

    fn get_or_load_file_handle(&self, segment_id: u64) -> Result<Arc<File>, String> {
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
        let file = File::open(&path).map_err(|e| format!("Failed to open segment file: {}", e))?;
        let file_ref = Arc::new(file);

        handles_guard.insert(segment_id, file_ref.clone());
        Ok(file_ref)
    }

    /// Scans the directory and rebuilds the concurrent SkipMap index
    fn recover(&self) -> Result<(), String> {
        let mut segments = Vec::new();

        for entry in fs::read_dir(&self.data_dir).map_err(|e| e.to_string())? {
            let entry = entry.map_err(|e| e.to_string())?;
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

        let mut highest_seq = 0;
        let mut active_id = 1;

        for (segment_id, path) in &segments {
            active_id = *segment_id;
            let mut file =
                File::open(path).map_err(|e| format!("Failed to open segment: {}", e))?;
            // Coop locking
            file.lock_shared()
                .map_err(|e| format!("Failed to lock segment: {}", e))?;

            let file_len = file.metadata().map_err(|e| e.to_string())?.len();
            let mut offset = 0;

            let mut header_buf = [0u8; 26];

            while offset + 26 <= file_len {
                file.seek(SeekFrom::Start(offset))
                    .map_err(|e| e.to_string())?;
                if file.read_exact(&mut header_buf).is_err() {
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
                let _type_tag = header_buf[16];
                let flags = header_buf[17];
                let payload_len = u32::from_be_bytes([
                    header_buf[18],
                    header_buf[19],
                    header_buf[20],
                    header_buf[21],
                ]) as u64;
                let crc32 = u32::from_be_bytes([
                    header_buf[22],
                    header_buf[23],
                    header_buf[24],
                    header_buf[25],
                ]);

                if offset + 26 + payload_len > file_len {
                    warn!(
                        "Incomplete payload write detected at offset {} in segment {}. Truncating.",
                        offset, segment_id
                    );
                    break;
                }

                let mut payload_buf = vec![0u8; payload_len as usize];
                file.read_exact(&mut payload_buf)
                    .map_err(|e| e.to_string())?;

                // Verify integrity
                let actual_crc = crc32fast::hash(&payload_buf);
                if actual_crc != crc32 {
                    error!(
                        "CRC32 checksum mismatch in segment {} at offset {}. Data corruption!",
                        segment_id, offset
                    );
                    return Err(format!(
                        "Data corruption in segment {} at offset {}",
                        segment_id, offset
                    ));
                }

                // Decode payload to extract stream name and key
                match deserialize_payload(&payload_buf) {
                    Ok((stream_name, key, _value)) => {
                        let compound_key = format!("{}:{}", stream_name, key);
                        if seq_id > highest_seq {
                            highest_seq = seq_id;
                        }

                        {
                            let mut streams_guard = self.streams_set.lock().unwrap();
                            streams_guard.insert(stream_name.clone());
                            if streams_guard.len() > self.max_streams {
                                return Err(format!(
                                    "Stream limit exceeded during recovery: unique streams count {} exceeds max_concurrent_streams {}",
                                    streams_guard.len(),
                                    self.max_streams
                                ));
                            }
                        }

                        let node_overhead = compound_key.len() as u64 + 64;
                        if flags & 0x02 != 0 {
                            // Tombstone
                            if self.skipmap.remove(&compound_key).is_some() {
                                self.index_ram_bytes
                                    .fetch_sub(node_overhead, Ordering::SeqCst);
                            }
                        } else if flags & 0x01 != 0 {
                            // Active
                            let existed = self.skipmap.contains_key(&compound_key);
                            self.skipmap.insert(
                                compound_key,
                                LogPointer {
                                    segment_id: *segment_id,
                                    file_offset: offset,
                                },
                            );
                            if !existed {
                                let current_ram = self
                                    .index_ram_bytes
                                    .fetch_add(node_overhead, Ordering::SeqCst)
                                    + node_overhead;
                                if current_ram > self.max_index_ram_bytes {
                                    return Err(format!(
                                        "Index RAM limit exceeded during recovery: {} bytes exceeds max_index_ram_mb {}",
                                        current_ram, self.max_index_ram_bytes
                                    ));
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            "Failed to parse payload in segment {} at offset {}: {}",
                            segment_id, offset, e
                        );
                        return Err(e);
                    }
                }

                offset += 26 + payload_len;
            }

            file.unlock().map_err(|e| e.to_string())?;
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
            .map_err(|e| e.to_string())?;

        file.lock_exclusive()
            .map_err(|e| format!("Failed to lock active segment: {}", e))?;
        let active_len = file.metadata().map_err(|e| e.to_string())?.len();

        let mut active_file_guard = self.active_file.lock().unwrap();
        let mut active_size_guard = self.active_size.lock().unwrap();

        *active_file_guard = Some(file);
        *active_size_guard = active_len;

        info!(
            "LIVEN Engine started. Active Segment ID: {}, Next Sequence ID: {}, Index Entries: {}",
            active_id,
            highest_seq + 1,
            self.skipmap.len()
        );

        Ok(())
    }

    /// Appends a new record to the storage engine
    pub fn append(
        &self,
        stream_name: &str,
        key: &str,
        value: DataValue,
        is_tombstone: bool,
    ) -> Result<Record, String> {
        let compound_key = format!("{}:{}", stream_name, key);

        // 1. Stream Limit Check
        {
            let mut streams_guard = self.streams_set.lock().unwrap();
            if !streams_guard.contains(stream_name) {
                if streams_guard.len() >= self.max_streams {
                    return Err("Maximum concurrent streams limit exceeded".to_string());
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
                    return Err("Index RAM limit exceeded".to_string());
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
    ) -> Result<Vec<Record>, String> {
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
                return Err("Maximum concurrent streams limit exceeded".to_string());
            }
        }

        let current_ram = self.index_ram_bytes.load(Ordering::SeqCst);
        if current_ram + new_keys_size > self.max_index_ram_bytes {
            return Err("Index RAM limit exceeded".to_string());
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
            let record = rx
                .recv()
                .map_err(|e| format!("LogRingBuffer coordination error: {}", e))??;
            results.push(record);
        }

        Ok(results)
    }

    /// Appends a batch of tombstones to the storage engine with high performance (O(1) disk syncs)
    pub fn append_tombstone_batch(
        &self,
        stream_name: &str,
        keys: &[String],
    ) -> Result<Vec<Record>, String> {
        // Stream Limit Check
        {
            let mut streams_guard = self.streams_set.lock().unwrap();
            if !streams_guard.contains(stream_name) {
                if streams_guard.len() >= self.max_streams {
                    return Err("Maximum concurrent streams limit exceeded".to_string());
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
        let records = rx
            .recv()
            .map_err(|e| format!("LogRingBuffer coordination error: {}", e))??;
        Ok(records)
    }

    /// Reads a record from disk via its LogPointer using safe positional reads
    pub fn read_record(&self, pointer: LogPointer) -> Result<Record, String> {
        let file = self.get_or_load_file_handle(pointer.segment_id)?;
        let file_len = file.metadata().map_err(|e| e.to_string())?.len();
        let offset = pointer.file_offset;

        if offset + 26 > file_len {
            return Err("Pointer offset out of bounds".to_string());
        }

        let mut header_buf = [0u8; 26];
        file.read_exact_at(&mut header_buf, offset)
            .map_err(|e| format!("Failed to read header positionally: {}", e))?;

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
            return Err("Payload out of bounds".to_string());
        }

        let record = READ_STAGING_BUFFER.with(|buf_cell| {
            let mut local_buf = buf_cell.borrow_mut();
            if local_buf.len() < payload_len {
                local_buf.resize(payload_len, 0);
            }
            let target_slice = &mut local_buf[..payload_len];

            file.read_exact_at(target_slice, offset + 26)
                .map_err(|e| format!("Failed to read payload positionally: {}", e))?;

            let actual_crc = crc32fast::hash(target_slice);
            if actual_crc != crc32 {
                return Err("CRC32 integrity check failed during positional read".to_string());
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
    pub fn get(&self, stream_name: &str, key: &str) -> Result<Option<Record>, String> {
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

    /// Scans all historical records from all active/read segments sequentially using safe positional reads
    pub fn scan_historical(&self) -> Result<Vec<Record>, String> {
        let mut segments = Vec::new();

        for entry in fs::read_dir(&self.data_dir).map_err(|e| e.to_string())? {
            let entry = entry.map_err(|e| e.to_string())?;
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

        let mut records = Vec::new();

        for (segment_id, _path) in &segments {
            let file = match self.get_or_load_file_handle(*segment_id) {
                Ok(f) => f,
                Err(e) => {
                    if e.contains("Cannot mmap 0-byte file") || e.contains("is empty") {
                        continue;
                    }
                    return Err(e);
                }
            };
            let file_len = file.metadata().map_err(|e| e.to_string())?.len();
            let mut offset = 0;

            while offset + 26 <= file_len {
                let mut header_buf = [0u8; 26];
                if file.read_exact_at(&mut header_buf, offset).is_err() {
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
                    break;
                }

                let payload_record = READ_STAGING_BUFFER.with(|buf_cell| {
                    let mut local_buf = buf_cell.borrow_mut();
                    if local_buf.len() < payload_len {
                        local_buf.resize(payload_len, 0);
                    }
                    let target_slice = &mut local_buf[..payload_len];
                    file.read_exact_at(target_slice, offset + 26)
                        .map_err(|e| e.to_string())?;

                    let actual_crc = crc32fast::hash(target_slice);
                    if actual_crc == crc32
                        && let Ok((stream_name, key, value)) =
                            deserialize_payload_borrowed(target_slice, type_tag)
                        && let Ok(stream_key) = crate::storage::key::StreamKey::from_str(key)
                    {
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
                    Ok::<Option<Record>, String>(None)
                })?;

                if let Some(rec) = payload_record {
                    records.push(rec);
                }

                offset += 26 + (payload_len as u64);
            }
        }

        Ok(records)
    }

    /// Performs background compaction of historical files
    pub fn compact(&self) -> Result<(), String> {
        info!("Starting log compaction...");
        let active_id = self.active_segment_id.load(Ordering::SeqCst);

        // Gather all inactive segments on disk
        let mut inactive_segments = Vec::new();
        for entry in fs::read_dir(&self.data_dir).map_err(|e| e.to_string())? {
            let entry = entry.map_err(|e| e.to_string())?;
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
        // To avoid conflicts, we use a unique segment ID, e.g., using a high sequence number or a compaction prefix
        // Let's use the highest sequence number seen + 1 as the segment ID
        let compacted_segment_id = self.current_sequence_id.load(Ordering::SeqCst) + 100000;
        let compacted_path = self.segment_path(compacted_segment_id);

        let mut comp_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&compacted_path)
            .map_err(|e| e.to_string())?;

        comp_file.lock_exclusive().map_err(|e| e.to_string())?;

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

                comp_file.write_all(&frame_buf).map_err(|e| e.to_string())?;

                new_pointers.insert(
                    compound_key,
                    LogPointer {
                        segment_id: compacted_segment_id,
                        file_offset: offset,
                    },
                );

                offset += frame_buf.len() as u64;
                Ok::<(), String>(())
            })?;
        }

        comp_file.sync_data().map_err(|e| e.to_string())?;
        comp_file.unlock().map_err(|e| e.to_string())?;

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
            if let Some(rss_pages_str) = fields.get(1) {
                if let Ok(rss_pages) = rss_pages_str.parse::<u64>() {
                    return rss_pages * 4096;
                }
            }
        }
    }

    0
}

impl StorageEngine {
    /// Gets database disk size and record metrics
    pub fn metrics(&self) -> Result<(u64, u64, u64, usize), String> {
        let mut ram_usage = get_process_memory();
        if ram_usage == 0 {
            ram_usage = self.index_ram_bytes.load(Ordering::SeqCst);
        }
        let mut total_size = 0u64;
        let mut segment_count = 0u64;

        for entry in fs::read_dir(&self.data_dir).map_err(|e| e.to_string())? {
            let entry = entry.map_err(|e| e.to_string())?;
            let path = entry.path();
            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("liven") {
                total_size += path.metadata().map_err(|e| e.to_string())?.len();
                segment_count += 1;
            }
        }

        let total_streams = self.streams_set.lock().unwrap().len();

        Ok((ram_usage, total_size, segment_count, total_streams))
    }
}

fn run_flusher_loop(
    rx: std::sync::mpsc::Receiver<ring_buffer::LogEntry>,
    data_dir: PathBuf,
    max_segment_size: u64,
    current_sequence_id: Arc<AtomicU64>,
    active_segment_id: Arc<AtomicU64>,
    active_file: Arc<Mutex<Option<File>>>,
    active_size: Arc<Mutex<u64>>,
    skipmap: Arc<SkipMap<String, LogPointer>>,
    broadcast_tx: broadcast::Sender<Record>,
    file_handles: Arc<Mutex<HashMap<u64, Arc<File>>>>,
    sync_mode: String,
    is_running: Arc<std::sync::atomic::AtomicBool>,
    index_ram_bytes: Arc<AtomicU64>,
) {
    let mut batch = Vec::with_capacity(2048);
    while is_running.load(Ordering::Relaxed) {
        // Wait for the first entry with a short timeout to prevent blocking indefinitely on shutdown
        let first_entry = match rx.recv_timeout(std::time::Duration::from_millis(100)) {
            Ok(entry) => entry,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        };
        batch.push(first_entry);

        // Pull as many entries as possible without blocking (up to 2048 or until empty)
        let start_time = std::time::Instant::now();
        while batch.len() < 2048 {
            match rx.try_recv() {
                Ok(entry) => {
                    batch.push(entry);
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => {
                    let elapsed = start_time.elapsed();
                    if elapsed.as_millis() >= 5 {
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
            &active_file,
            &active_size,
            &skipmap,
            &broadcast_tx,
            &file_handles,
            &sync_mode,
            &index_ram_bytes,
        ) {
            // Notify all waiting entries of the failure
            for entry in batch.drain(..) {
                match entry {
                    ring_buffer::LogEntry::Single { response_tx, .. } => {
                        let _ = response_tx.send(Err(e.clone()));
                    }
                    ring_buffer::LogEntry::TombstoneBatch { response_tx, .. } => {
                        let _ = response_tx.send(Err(e.clone()));
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
    active_file: &Arc<Mutex<Option<File>>>,
    active_size: &Arc<Mutex<u64>>,
    skipmap: &Arc<SkipMap<String, LogPointer>>,
    broadcast_tx: &broadcast::Sender<Record>,
    file_handles: &Arc<Mutex<HashMap<u64, Arc<File>>>>,
    sync_mode: &str,
    index_ram_bytes: &Arc<AtomicU64>,
) -> Result<(), String> {
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
    let mut frame_buf = Vec::new();

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
                        .map_err(|e| e.to_string())?
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
                            .map_err(|e| e.to_string())?
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
            }
        }
        Ok::<(), String>(())
    })?;

    if *active_size_guard + (frame_buf.len() as u64) > max_segment_size {
        if let Some(old_file) = active_file_guard.take() {
            let _ = old_file.unlock();
        }

        let new_active_id = current_sequence_id.load(Ordering::SeqCst);
        active_segment_id.store(new_active_id, Ordering::SeqCst);

        let new_path = data_dir.join(format!("{:020}.liven", new_active_id));
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&new_path)
            .map_err(|e| e.to_string())?;

        file.lock_exclusive()
            .map_err(|e| format!("Failed to lock active segment: {}", e))?;

        *active_file_guard = Some(file);
        *active_size_guard = 0;
        info!("Rolled over to new active segment: {}", new_active_id);
    }

    let file = active_file_guard
        .as_mut()
        .ok_or("No active file handle found")?;
    let current_offset = *active_size_guard;

    file.write_all(&frame_buf).map_err(|e| e.to_string())?;

    if sync_mode == "always" {
        file.sync_data().map_err(|e| e.to_string())?;
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
                        let existed = skipmap.contains_key(&compound_key);
                        skipmap.insert(
                            compound_key,
                            LogPointer {
                                segment_id,
                                file_offset: offset_acc,
                            },
                        );
                        if !existed {
                            index_ram_bytes.fetch_add(node_overhead, Ordering::SeqCst);
                        }
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
                            let existed = skipmap.contains_key(&compound_key);
                            skipmap.insert(
                                compound_key,
                                LogPointer {
                                    segment_id,
                                    file_offset: offset_acc,
                                },
                            );
                            if !existed {
                                index_ram_bytes.fetch_add(node_overhead, Ordering::SeqCst);
                            }
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
) -> Result<(&str, &str, DataValue), String> {
    if payload.len() < 4 {
        return Err("Payload too short".to_string());
    }
    let stream_len = u16::from_be_bytes([payload[0], payload[1]]) as usize;
    if payload.len() < 4 + stream_len {
        return Err("Payload too short for stream name".to_string());
    }
    let stream_name =
        std::str::from_utf8(&payload[2..2 + stream_len]).map_err(|e| e.to_string())?;

    let key_len_offset = 2 + stream_len;
    let key_len =
        u16::from_be_bytes([payload[key_len_offset], payload[key_len_offset + 1]]) as usize;
    if payload.len() < 4 + stream_len + key_len {
        return Err("Payload too short for key".to_string());
    }
    let key_offset = key_len_offset + 2;
    let key = std::str::from_utf8(&payload[key_offset..key_offset + key_len])
        .map_err(|e| e.to_string())?;

    let val_offset = key_offset + key_len;
    let value = if type_tag == 8 {
        let raw_bytes = &payload[val_offset..];
        let ptr = raw_bytes.as_ptr() as *const i8;
        let count = raw_bytes.len();
        let vec_i8 = unsafe { std::slice::from_raw_parts(ptr, count) }.to_vec();
        DataValue::Vector(vec_i8)
    } else {
        rmp_serde::from_slice(&payload[val_offset..]).map_err(|e| e.to_string())?
    };

    Ok((stream_name, key, value))
}

pub fn deserialize_payload(payload: &[u8]) -> Result<(String, String, DataValue), String> {
    let (stream_name, key, value) = deserialize_payload_borrowed(payload, 0x02)?;
    Ok((stream_name.to_string(), key.to_string(), value))
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
        Some(unsafe { std::slice::from_raw_parts(ptr, len) })
    }
}

pub fn deserialize_payload_fuzz(payload: &[u8]) -> Result<(String, String, DataValue), String> {
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
    pub fn active_handles_count(&self) -> usize {
        self.file_handles.lock().unwrap().len()
    }
}

impl Drop for StorageEngine {
    fn drop(&mut self) {
        self.is_running.store(false, Ordering::Relaxed);
        if let Ok(mut active_file_guard) = self.active_file.lock()
            && let Some(file) = active_file_guard.take()
        {
            let _ = file.unlock();
            let _ = file.sync_all();
        }
    }
}
