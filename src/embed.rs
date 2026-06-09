use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;

use crate::executor::execute_query;
use crate::parser::parse_query;
use crate::storage::StorageEngine;
use crate::types::Record;

/// Configuration for an embedded LIVEN instance.
/// All fields have sensible defaults for embedded use.
#[derive(Debug, Clone)]
pub struct LivenConfig {
    /// Maximum number of concurrent streams. Default: 32.
    pub max_streams: usize,
    /// Maximum in-memory index size in megabytes. Default: 16.
    pub max_index_ram_mb: usize,
    /// Maximum segment file size in megabytes. Default: 16.
    pub max_segment_mb: usize,
    /// Maximum number of cached file descriptors. Default: 64.
    pub max_open_fds: usize,
    /// Broadcast channel capacity for real-time subscriptions. Default: 4096.
    pub broadcast_capacity: usize,
}

impl Default for LivenConfig {
    fn default() -> Self {
        Self {
            max_streams: 32,
            max_index_ram_mb: 16,
            max_segment_mb: 16,
            max_open_fds: 64,
            broadcast_capacity: 4096,
        }
    }
}

/// An embedded LIVEN database instance.
///
/// # Example
/// ```rust
/// use liven::embed::Liven;
///
/// let dir = std::format!("./liven_test_{}", std::process::id());
/// let db = Liven::open(&dir)?;
/// db.query(r#"from("events").insert("e1", {type: "click"})"#)?;
/// let results = db.query(r#"from("events") | filter(type == "click")"#)?;
/// assert_eq!(results.len(), 1);
/// let _ = std::fs::remove_dir_all(&dir);
/// # Ok::<(), String>(())
/// ```
pub struct Liven {
    engine: Arc<StorageEngine>,
}

impl Liven {
    /// Opens or creates a LIVEN database at the given path
    /// using default configuration.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        Self::open_with_config(path, LivenConfig::default())
    }

    /// Opens or creates a LIVEN database at the given path
    /// using the provided configuration.
    pub fn open_with_config(path: impl AsRef<Path>, config: LivenConfig) -> Result<Self, String> {
        let segment_size = config.max_segment_mb as u64 * 1024 * 1024;
        let mut engine = StorageEngine::new(path, segment_size)?;
        engine.set_max_streams(config.max_streams);
        engine.set_max_index_ram_bytes(config.max_index_ram_mb as u64 * 1024 * 1024);
        engine.set_max_fds(config.max_open_fds);
        Ok(Self {
            engine: Arc::new(engine),
        })
    }

    /// Executes a LIVEN query string and returns matching records.
    ///
    /// Supports all query types: insert, upsert, update, delete,
    /// pipeline queries, streams(), status(), and drop().
    pub fn query(&self, query_str: &str) -> Result<Vec<Record>, String> {
        let query = parse_query(query_str)?;
        execute_query(&self.engine, &query)
    }

    /// Subscribes to real-time record updates across all streams.
    /// Returns a broadcast receiver that yields records as they
    /// are written. Requires a Tokio runtime to use with `.recv().await`.
    pub fn subscribe(&self) -> broadcast::Receiver<Record> {
        self.engine.subscribe()
    }

    /// Synchronous blocking subscription. Blocks the calling thread
    /// for the given timeout if no record is immediately available.
    ///
    /// Use this when embedding LIVEN in a non-async application.
    /// Use `subscribe()` when embedding in a Tokio application.
    pub fn subscribe_sync(&self, timeout: Duration) -> Result<Option<Record>, String> {
        let mut rx = self.engine.subscribe();
        match rx.try_recv() {
            Ok(record) => Ok(Some(record)),
            Err(broadcast::error::TryRecvError::Empty) => {
                std::thread::sleep(timeout);
                match rx.try_recv() {
                    Ok(record) => Ok(Some(record)),
                    Err(_) => Ok(None),
                }
            }
            Err(broadcast::error::TryRecvError::Closed) => {
                Err("Broadcast channel closed".to_string())
            }
            Err(broadcast::error::TryRecvError::Lagged(n)) => {
                Err(format!("Subscriber lagged by {} messages", n))
            }
        }
    }

    /// Returns a clone of the underlying engine Arc for advanced use.
    /// Use this only if the high-level API does not cover your use case.
    pub fn engine(&self) -> Arc<StorageEngine> {
        Arc::clone(&self.engine)
    }

    /// Returns current database metrics:
    /// `(ram_usage_bytes, disk_size_bytes, segment_count, stream_count)`
    pub fn metrics(&self) -> Result<(u64, u64, u64, usize), String> {
        self.engine.metrics()
    }

    /// Triggers a manual compaction cycle.
    /// Under normal operation this is not needed if auto compaction
    /// is configured.
    pub fn compact(&self) -> Result<(), String> {
        self.engine.compact()
    }
}
