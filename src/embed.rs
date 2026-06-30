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
    /// Trigger compaction after this many inactive segments. Default: 4.
    pub compaction_threshold_segments: usize,
    /// Trigger compaction when inactive data exceeds this many bytes. Default: 64 MB.
    pub compaction_threshold_bytes: u64,
    /// Maximum number of records to return from a full scan operation. Default: 100,000.
    pub max_scan_results: usize,
    /// Group-commit drain window in microseconds. The flusher waits this long
    /// for additional entries before flushing. Default: 5000 (5ms).
    /// Set lower (e.g. 300) for lower-latency single-caller workloads.
    pub commit_drain_window_us: u64,
    /// Maximum number of entries to accumulate in one group-commit batch. Default: 2048.
    pub commit_max_batch_size: usize,
}

impl Default for LivenConfig {
    fn default() -> Self {
        // Auto-detect system RAM for embedded mode too
        let max_index_ram_mb = if let Some(system_ram) = crate::sysinfo::detect_system_ram_mb() {
            crate::sysinfo::calculate_auto_budget(system_ram) as usize
        } else {
            512 // 512MB fallback for embedded mode
        };

        Self {
            max_streams: 32,
            max_index_ram_mb,
            max_segment_mb: 16,
            max_open_fds: 64,
            broadcast_capacity: 4096,
            compaction_threshold_segments: 4,
            compaction_threshold_bytes: 64 * 1024 * 1024,
            max_scan_results: 100_000,
            commit_drain_window_us: 5000,
            commit_max_batch_size: 2048,
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
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct Liven {
    engine: Arc<StorageEngine>,
}

impl Liven {
    /// Opens or creates a LIVEN database at the given path
    /// using default configuration.
    pub fn open(path: impl AsRef<Path>) -> crate::error::Result<Self> {
        Self::open_with_config(path, LivenConfig::default())
    }

    /// Opens or creates a LIVEN database at the given path
    /// using the provided configuration.
    pub fn open_with_config(
        path: impl AsRef<Path>,
        config: LivenConfig,
    ) -> crate::error::Result<Self> {
        let segment_size = config.max_segment_mb as u64 * 1024 * 1024;
        let mut engine = StorageEngine::new(path, segment_size)?;
        engine.set_max_streams(config.max_streams);
        engine.set_max_index_ram_bytes(config.max_index_ram_mb as u64 * 1024 * 1024);
        engine.set_max_fds(config.max_open_fds);
        engine.set_max_scan_results(config.max_scan_results);
        engine.compaction_threshold_segments = config.compaction_threshold_segments;
        engine.compaction_threshold_bytes = config.compaction_threshold_bytes;
        engine.commit_drain_window_us = config.commit_drain_window_us;
        engine.commit_max_batch_size = config.commit_max_batch_size;
        Ok(Self {
            engine: Arc::new(engine),
        })
    }

    /// Executes a LIVEN query string and returns matching records.
    ///
    /// Supports all query types: insert, upsert, update, delete,
    /// pipeline queries, streams(), status(), and drop().
    pub fn query(&self, query_str: &str) -> crate::error::Result<Vec<Record>> {
        let query = parse_query(query_str)?;
        execute_query(&self.engine, &query)
    }

    /// Executes a typed `crate::types::Query` value directly, bypassing string parsing.
    ///
    /// Use this with the builders from the [`query`](crate::query) module
    /// to construct queries with full type safety.
    pub fn run(&self, query: crate::types::Query) -> crate::error::Result<Vec<Record>> {
        execute_query(&self.engine, &query)
    }

    // ── Convenience methods ────────────────────────────────────────────
    // These mirror the typed query constructors in `liven::query::Query`
    // but call `self.run()` internally so users write `db.insert(...)`
    // instead of `db.run(Query::insert(...))`.

    /// Insert a single record into a stream.
    pub fn insert(
        &self,
        stream_name: impl Into<String>,
        key: impl Into<String>,
        value: serde_json::Value,
    ) -> crate::error::Result<Vec<Record>> {
        self.run(crate::types::Query::Insert {
            stream_name: stream_name.into(),
            key: key.into(),
            value,
        })
    }

    /// Upsert a record (insert or replace if the key exists).
    pub fn upsert(
        &self,
        stream_name: impl Into<String>,
        key: impl Into<String>,
        value: serde_json::Value,
    ) -> crate::error::Result<Vec<Record>> {
        self.run(crate::types::Query::Upsert {
            stream_name: stream_name.into(),
            key: key.into(),
            value,
        })
    }

    /// Update specific fields on an existing record (merge).
    pub fn update(
        &self,
        stream_name: impl Into<String>,
        key: impl Into<String>,
        value: serde_json::Value,
    ) -> crate::error::Result<Vec<Record>> {
        self.run(crate::types::Query::Update {
            stream_name: stream_name.into(),
            key: key.into(),
            value,
        })
    }

    /// Delete a single record by key.
    pub fn delete(
        &self,
        stream_name: impl Into<String>,
        key: impl Into<String>,
    ) -> crate::error::Result<Vec<Record>> {
        self.run(crate::types::Query::DeleteKey {
            stream_name: stream_name.into(),
            key: key.into(),
        })
    }

    /// Get a single record by key.
    pub fn get(
        &self,
        stream_name: impl Into<String>,
        key: impl Into<String>,
    ) -> crate::error::Result<Vec<Record>> {
        self.run(crate::query::Pipeline::from(stream_name).get(key).build())
    }

    /// Clear all records from a stream without removing it.
    pub fn clear(&self, stream_name: impl Into<String>) -> crate::error::Result<Vec<Record>> {
        self.run(crate::types::Query::Empty {
            stream_name: stream_name.into(),
        })
    }

    /// Drop a stream and all its data.
    pub fn drop_stream(&self, stream_name: impl Into<String>) -> crate::error::Result<Vec<Record>> {
        self.run(crate::types::Query::Drop {
            stream_name: stream_name.into(),
        })
    }

    /// Insert multiple records into a stream in one batch.
    pub fn insert_many(
        &self,
        stream_name: impl Into<String>,
        batch: Vec<(String, serde_json::Value)>,
    ) -> crate::error::Result<Vec<Record>> {
        self.run(crate::types::Query::InsertBatch {
            stream_name: stream_name.into(),
            batch,
        })
    }

    /// Upsert multiple records into a stream in one batch.
    pub fn upsert_many(
        &self,
        stream_name: impl Into<String>,
        batch: Vec<(String, serde_json::Value)>,
    ) -> crate::error::Result<Vec<Record>> {
        self.run(crate::types::Query::UpsertBatch {
            stream_name: stream_name.into(),
            batch,
        })
    }

    /// List all streams.
    pub fn streams(&self) -> crate::error::Result<Vec<Record>> {
        self.run(crate::types::Query::ListStreams)
    }

    /// Get server status.
    pub fn status(&self) -> crate::error::Result<Vec<Record>> {
        self.run(crate::types::Query::Status)
    }

    /// Explain the execution plan of a query without running it.
    pub fn explain(&self, query: crate::types::Query) -> crate::error::Result<Vec<Record>> {
        self.run(crate::types::Query::Explain {
            inner_query: Box::new(query),
        })
    }

    // ── Pipeline convenience methods ───────────────────────────────────
    // Each builds a `Pipeline::from(stream)` with the given stage
    // and executes it via `self.run()`.

    /// Filter records in a stream by a condition.
    pub fn filter(
        &self,
        stream_name: impl Into<String>,
        filter: crate::query::Filter,
    ) -> crate::error::Result<Vec<Record>> {
        self.run(
            crate::query::Pipeline::from(stream_name)
                .filter(filter)
                .build(),
        )
    }

    /// Limit the number of results from a stream.
    pub fn limit(
        &self,
        stream_name: impl Into<String>,
        count: usize,
    ) -> crate::error::Result<Vec<Record>> {
        self.run(
            crate::query::Pipeline::from(stream_name)
                .limit(count)
                .build(),
        )
    }

    /// Count records in a stream (optionally after filtering).
    pub fn count(&self, stream_name: impl Into<String>) -> crate::error::Result<Vec<Record>> {
        self.run(crate::query::Pipeline::from(stream_name).count().build())
    }

    /// Sort records by a field.
    pub fn sort(
        &self,
        stream_name: impl Into<String>,
        field: impl Into<String>,
        descending: bool,
    ) -> crate::error::Result<Vec<Record>> {
        self.run(
            crate::query::Pipeline::from(stream_name)
                .sort(field, descending)
                .build(),
        )
    }

    /// Paginate through results.
    pub fn page(
        &self,
        stream_name: impl Into<String>,
        page_number: usize,
        page_size: usize,
    ) -> crate::error::Result<Vec<Record>> {
        self.run(
            crate::query::Pipeline::from(stream_name)
                .page(page_number, page_size)
                .build(),
        )
    }

    /// Project specific fields from a stream.
    pub fn map(
        &self,
        stream_name: impl Into<String>,
        fields: Vec<String>,
    ) -> crate::error::Result<Vec<Record>> {
        self.run(
            crate::query::Pipeline::from(stream_name)
                .map(fields)
                .build(),
        )
    }

    /// Time-windowed aggregation on a stream.
    pub fn window(
        &self,
        stream_name: impl Into<String>,
        duration_ms: u64,
        strategy: crate::types::AggregateStrategy,
    ) -> crate::error::Result<Vec<Record>> {
        self.run(
            crate::query::Pipeline::from(stream_name)
                .window(duration_ms, strategy)
                .build(),
        )
    }

    /// Group records by a field with aggregations.
    pub fn group(
        &self,
        stream_name: impl Into<String>,
        field: impl Into<String>,
        aggregations: Vec<String>,
    ) -> crate::error::Result<Vec<Record>> {
        self.run(
            crate::query::Pipeline::from(stream_name)
                .group(field, aggregations)
                .build(),
        )
    }

    /// Vector similarity search on a stream.
    pub fn vector_filter(
        &self,
        stream_name: impl Into<String>,
        field: impl Into<String>,
        query_vector: Vec<i8>,
        threshold: f64,
    ) -> crate::error::Result<Vec<Record>> {
        self.run(
            crate::query::Pipeline::from(stream_name)
                .vector_filter(field, query_vector, threshold)
                .build(),
        )
    }

    /// Enrich records with a left join from another stream.
    pub fn enrich(
        &self,
        stream_name: impl Into<String>,
        source_stream: impl Into<String>,
        join_key: impl Into<String>,
    ) -> crate::error::Result<Vec<Record>> {
        self.run(
            crate::query::Pipeline::from(stream_name)
                .enrich(source_stream, join_key)
                .build(),
        )
    }

    /// Correlate events within a time window (windowed join).
    pub fn correlate(
        &self,
        stream_name: impl Into<String>,
        source_stream: impl Into<String>,
        join_key: impl Into<String>,
        within_ms: u64,
    ) -> crate::error::Result<Vec<Record>> {
        self.run(
            crate::query::Pipeline::from(stream_name)
                .correlate(source_stream, join_key, within_ms)
                .build(),
        )
    }

    /// Chain (multi-hop join) across streams.
    pub fn chain(
        &self,
        stream_name: impl Into<String>,
        target_stream: impl Into<String>,
        join_key: impl Into<String>,
    ) -> crate::error::Result<Vec<Record>> {
        self.run(
            crate::query::Pipeline::from(stream_name)
                .chain(target_stream, join_key)
                .build(),
        )
    }

    /// Sequence (ordered event pattern detection) within a time window.
    pub fn sequence(
        &self,
        stream_name: impl Into<String>,
        steps: Vec<crate::query::Filter>,
        within_ms: u64,
    ) -> crate::error::Result<Vec<Record>> {
        self.run(
            crate::query::Pipeline::from(stream_name)
                .sequence(steps, within_ms)
                .build(),
        )
    }

    /// Deduplicate records by a specific field.
    pub fn distinct(
        &self,
        stream_name: impl Into<String>,
        field: impl Into<String>,
    ) -> crate::error::Result<Vec<Record>> {
        self.run(
            crate::query::Pipeline::from(stream_name)
                .distinct(field)
                .build(),
        )
    }

    /// Cursor-based pagination.
    pub fn page_cursor(
        &self,
        stream_name: impl Into<String>,
        cursor: impl Into<String>,
        page_size: usize,
    ) -> crate::error::Result<Vec<Record>> {
        self.run(
            crate::query::Pipeline::from(stream_name)
                .page_cursor(cursor, page_size)
                .build(),
        )
    }

    /// Pipeline update: filter then update matching records.
    pub fn pipeline_update(
        &self,
        pipeline: crate::query::Pipeline,
        update_value: serde_json::Value,
    ) -> crate::error::Result<Vec<Record>> {
        self.run(pipeline.build_update(update_value))
    }

    /// Pipeline delete: filter then delete matching records.
    pub fn pipeline_delete(
        &self,
        pipeline: crate::query::Pipeline,
    ) -> crate::error::Result<Vec<Record>> {
        self.run(pipeline.build_delete())
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
    pub fn metrics(&self) -> crate::error::Result<(u64, u64, u64, usize)> {
        self.engine.metrics()
    }

    /// Triggers a manual compaction cycle.
    /// Under normal operation this is not needed if auto compaction
    /// is configured.
    pub fn compact(&self) -> crate::error::Result<()> {
        self.engine.compact()
    }

    /// Starts a background compaction loop using the provided Tokio handle.
    /// Checks every `check_interval` and compacts when thresholds are exceeded.
    pub fn start_auto_compact(
        &self,
        handle: &tokio::runtime::Handle,
        check_interval: std::time::Duration,
    ) {
        let engine = self.engine.clone();
        handle.spawn(async move {
            let mut interval = tokio::time::interval(check_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                if engine.should_compact() {
                    tracing::info!("Auto-compaction triggered");
                    // Compact runs on a blocking thread since it does file I/O
                    let eng = engine.clone();
                    tokio::task::spawn_blocking(move || {
                        if let Err(e) = eng.compact() {
                            tracing::warn!("Auto-compaction failed: {}", e);
                        }
                    })
                    .await
                    .ok();
                }
            }
        });
    }
}
