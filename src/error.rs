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
}

pub type Result<T> = std::result::Result<T, LivenError>;

impl From<String> for LivenError {
    fn from(s: String) -> Self {
        LivenError::Storage(s)
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
