use crate::storage::key::StreamKey;
use crate::types::DataValue;
use crate::types::Record;
use std::sync::mpsc::SyncSender;

/// Represents write operations enqueued in the group-commit ring buffer.
pub enum LogEntry {
    Single {
        stream_name: String,
        key: StreamKey,
        value: DataValue,
        is_tombstone: bool,
        response_tx: SyncSender<Result<Record, String>>,
    },
    TombstoneBatch {
        stream_name: String,
        keys: Vec<StreamKey>,
        response_tx: SyncSender<Result<Vec<Record>, String>>,
    },
}

/// Thread-safe client interface to enqueue writes into the background flusher.
#[derive(Clone)]
pub struct LogRingBuffer {
    tx: std::sync::mpsc::SyncSender<LogEntry>,
}

impl LogRingBuffer {
    pub fn new(tx: std::sync::mpsc::SyncSender<LogEntry>) -> Self {
        Self { tx }
    }

    /// Enqueues a write transaction and blocks until execution by the background flusher.
    /// This provides a synchronous API with high-throughput group-commits under the hood.
    pub fn append(
        &self,
        stream_name: &str,
        key: StreamKey,
        value: DataValue,
        is_tombstone: bool,
    ) -> Result<Record, String> {
        let (response_tx, rx) = std::sync::mpsc::sync_channel(1);
        let entry = LogEntry::Single {
            stream_name: stream_name.to_string(),
            key,
            value,
            is_tombstone,
            response_tx,
        };

        self.tx.send(entry).map_err(|_| {
            "Failed to enqueue transaction: LogRingBuffer flusher thread terminated".to_string()
        })?;

        rx.recv()
            .map_err(|e| format!("LogRingBuffer coordination error: {}", e))?
    }

    /// Enqueues a transaction without waiting for response.
    pub fn tx_send(&self, entry: LogEntry) -> Result<(), String> {
        self.tx.send(entry).map_err(|_| {
            "Failed to enqueue transaction: LogRingBuffer flusher thread terminated".to_string()
        })
    }
}
