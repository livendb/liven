use crate::types::LogPointer;
use crossbeam_skiplist::SkipMap;
use std::ops::Bound;

/// Per-stream sorted index: (timestamp_ms, sequence_id) → LogPointer
///
/// Allows O(log N + K) range scans where K is the result count.
/// The sequence_id acts as a tiebreaker to ensure uniqueness.
pub struct TimestampIndex {
    inner: SkipMap<(i64, u64), LogPointer>,
}

impl Default for TimestampIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl TimestampIndex {
    pub fn new() -> Self {
        Self {
            inner: SkipMap::new(),
        }
    }

    pub fn insert(&self, timestamp: i64, seq_id: u64, pointer: LogPointer) {
        self.inner.insert((timestamp, seq_id), pointer);
    }

    pub fn remove(&self, timestamp: i64, seq_id: u64) {
        self.inner.remove(&(timestamp, seq_id));
    }

    /// Returns all pointers with timestamp in [start_ms, end_ms] inclusive.
    pub fn range(&self, start_ms: i64, end_ms: i64) -> Vec<LogPointer> {
        self.inner
            .range((
                Bound::Included(&(start_ms, u64::MIN)),
                Bound::Included(&(end_ms, u64::MAX)),
            ))
            .map(|e| *e.value())
            .collect()
    }

    /// Returns the number of entries in the index.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}
