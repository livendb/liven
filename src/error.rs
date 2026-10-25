use thiserror::Error;

/// Comprehensive error type for the LIVEN storage engine.
///
/// Every operation that can fail returns `Result<T, LivenError>`, allowing
/// callers to match on specific error conditions rather than parsing strings.
#[derive(Debug, Error)]
pub enum LivenError {
    // ── Storage Engine ────────────────────────────────────────────
    /// Generic storage-level failure, typically wrapping an I/O error.
    #[error("storage error: {0}")]
    Storage(String),

    /// A record pointer referenced an offset past the end of a segment file.
    #[error("pointer offset out of bounds (segment {segment_id}, offset {offset})")]
    PointerOutOfBounds { segment_id: u64, offset: u64 },

    /// The frame payload extended past the end of the file.
    #[error("payload out of bounds (segment {segment_id}, offset {offset})")]
    PayloadOutOfBounds { segment_id: u64, offset: u64 },

    /// CRC32 checksum mismatch — data on disk has been corrupted.
    #[error("CRC32 integrity check failed (segment {segment_id}, offset {offset})")]
    CrcMismatch { segment_id: u64, offset: u64 },

    /// The payload buffer was too short to contain the expected header fields.
    #[error("payload too short for {kind}")]
    PayloadTooShort { kind: &'static str },

    /// The system's disk is full and a write could not be completed.
    #[error("disk full")]
    DiskFull,

    // ── Limits ────────────────────────────────────────────────────
    /// The maximum number of concurrent streams has been reached.
    #[error("stream limit exceeded (max {max})")]
    StreamLimitExceeded { max: usize },

    /// The in-memory index has exceeded its configured RAM budget.
    #[error(
        "Index RAM limit reached ({current_bytes} bytes used of {max_bytes} bytes).\n       To increase: set max_index_ram_mb in liven.toml or remove it to enable auto-allocation."
    )]
    IndexRamLimitExceeded { max_bytes: u64, current_bytes: u64 },

    /// A batch was rejected atomically because one new key would exceed the
    /// index budget. No records in the batch were applied.
    #[error(
        "Index RAM limit reached while inserting key '{key}' ({current_bytes} bytes used of {max_bytes} bytes). Batch rejected; no records applied."
    )]
    BatchIndexRamLimitExceeded {
        key: String,
        max_bytes: u64,
        current_bytes: u64,
    },

    /// A stream with the given name was not found.
    #[error("stream not found: {name}")]
    StreamNotFound { name: String },

    /// A key already exists in the target stream (insert conflict).
    #[error("key already exists: {key} in stream {stream}")]
    KeyAlreadyExists { key: String, stream: String },

    /// A key exceeds the 32-byte maximum.
    #[error(
        "Key '{{key}}' is {len} bytes but the maximum is 32 bytes.\n   Shorten the key or use a hash of the original value."
    )]
    KeyTooLong { len: usize, key: String },

    /// The server configuration is unsafe or unsupported.
    #[error("configuration error: {0}")]
    Config(String),

    // ── Query / Pipeline ──────────────────────────────────────────
    /// The query string could not be parsed.
    #[error("parse error: {0}")]
    Parse(String),

    /// A pipeline query had an invalid stage ordering or combination.
    #[error("invalid pipeline: {0}")]
    InvalidPipeline(String),

    /// An action (insert / upsert / delete) was misapplied to a pipeline.
    #[error("query error: {0}")]
    Query(String),

    // ── Network / Protocol ────────────────────────────────────────
    /// An I/O error occurred during network communication.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// The broadcast channel for live subscriptions has lagged or closed.
    #[error("subscription error: {0}")]
    Subscription(String),

    // ── Security / Auth ───────────────────────────────────────────
    /// Authentication via symmetric key or mTLS failed.
    #[error("authentication failed: {reason}")]
    AuthFailed { reason: String },

    /// The client's capabilities do not allow the requested operation.
    #[error("insufficient capabilities")]
    InsufficientCapabilities,

    // ── Serialization ─────────────────────────────────────────────
    /// MessagePack or JSON serialization/deserialization failed.
    #[error("serialization error: {0}")]
    Serialization(String),

    // ── Internal ──────────────────────────────────────────────────
    /// A Tokio join error or async coordination failure.
    #[error("internal error: {0}")]
    Internal(String),

    /// The flusher thread terminated unexpectedly.
    #[error("ring buffer flusher terminated")]
    FlusherTerminated,

    // ── Recovery ──────────────────────────────────────────────────
    /// A segment file could not be locked within the configured timeout.
    #[error("timed out locking segment {segment_id} at {path} (lock held by another process?)")]
    SegmentLockTimeout { segment_id: u64, path: String },

    /// Strict recovery mode: a CRC mismatch was detected and recovery was aborted.
    #[error(
        "strict mode: CRC mismatch in segment {segment_id} at offset {offset} — recovery aborted. Use default mode to auto-truncate or --repair to create a backup first."
    )]
    StrictCrcMismatch { segment_id: u64, offset: u64 },

    /// Normal recovery found valid frames stranded after a torn region. It will
    /// not truncate them; the operator must run offline repair.
    #[error(
        "segment {segment_id} has a torn region at offset {offset} followed by {stranded} valid stranded frame(s). Unrecoverable key(s): {keys}. Refusing to truncate; run `liven start --repair` to salvage them."
    )]
    RecoveryRequired {
        segment_id: u64,
        offset: u64,
        stranded: usize,
        keys: String,
    },
}

pub type Result<T> = std::result::Result<T, LivenError>;

impl From<String> for LivenError {
    fn from(s: String) -> Self {
        LivenError::Storage(s)
    }
}

impl From<crate::storage::ring_buffer::AckError> for LivenError {
    fn from(e: crate::storage::ring_buffer::AckError) -> Self {
        match e {
            crate::storage::ring_buffer::AckError::IndexRam {
                max_bytes,
                current_bytes,
                key,
            } => match key {
                Some(key) => LivenError::BatchIndexRamLimitExceeded {
                    key,
                    max_bytes,
                    current_bytes,
                },
                None => LivenError::IndexRamLimitExceeded {
                    max_bytes,
                    current_bytes,
                },
            },
            crate::storage::ring_buffer::AckError::Message(m) => LivenError::Storage(m),
        }
    }
}

impl From<&str> for LivenError {
    fn from(s: &str) -> Self {
        LivenError::Storage(s.to_string())
    }
}

/// Convenience macro to create a `LivenError::Storage` from a format string.
#[macro_export]
macro_rules! storage_err {
    ($fmt:literal $(, $arg:expr)* $(,)?) => {
        $crate::error::LivenError::Storage(format!($fmt $(, $arg)*))
    };
}

/// Convenience macro to create a `LivenError::Io` from a format string.
#[macro_export]
macro_rules! io_err {
    ($fmt:literal $(, $arg:expr)* $(,)?) => {
        $crate::error::LivenError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!($fmt $(, $arg)*),
        ))
    };
}
