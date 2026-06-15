use crate::storage::key::StreamKey;
use crate::types::DataValue;
use crate::types::Record;
use std::sync::mpsc::SyncSender;

/// Sentinel value to signal flusher thread shutdown.
#[derive(Debug, Clone, Copy)]
pub struct ShutdownSentinel;

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
    /// Shutdown signal for clean flusher thread termination.
    Shutdown,
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
    ) -> crate::error::Result<Record> {
        let (response_tx, rx) = std::sync::mpsc::sync_channel(1);
        let entry = LogEntry::Single {
            stream_name: stream_name.to_string(),
            key,
            value,
            is_tombstone,
            response_tx,
        };

        self.tx
            .send(entry)
            .map_err(|_| crate::error::LivenError::FlusherTerminated)?;

        rx.recv()
            .map_err(|e| {
                crate::error::LivenError::Internal(format!(
                    "LogRingBuffer coordination error: {}",
                    e
                ))
            })?
            .map_err(Into::into)
    }

    /// Enqueues a transaction without waiting for response.
    pub fn tx_send(&self, entry: LogEntry) -> crate::error::Result<()> {
        self.tx
            .send(entry)
            .map_err(|_| crate::error::LivenError::FlusherTerminated)
    }

    /// Sends the shutdown sentinel to the flusher thread.
    pub fn send_shutdown(&self) -> crate::error::Result<()> {
        self.tx
            .send(LogEntry::Shutdown)
            .map_err(|_| crate::error::LivenError::FlusherTerminated)
    }
}
