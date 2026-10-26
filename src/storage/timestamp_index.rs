use crate::storage::index::IndexMap;
use crate::types::LogPointer;
use std::ops::Bound;

/// Per-stream sorted index: (timestamp_ms, sequence_id) → LogPointer
///
/// Allows O(log N + K) range scans where K is the result count.
/// The sequence_id acts as a tiebreaker to ensure uniqueness.
///
/// The `(i64, u64)` pair is encoded into a `u128` whose natural order is the
/// pair's: `bbtree` recognizes `u128` keys natively, whereas it gives tuple
/// keys no inline keys (and refuses to build a tree for them).
pub struct TimestampIndex {
    inner: IndexMap<u128>,
}

impl Default for TimestampIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl TimestampIndex {
    pub fn new() -> Self {
        Self {
            inner: IndexMap::new(),
        }
    }

    /// Order-preserving encoding of `(timestamp, seq_id)`.
    ///
    /// Flipping the sign bit maps `i64` onto `u64` in ascending order, so
    /// `(timestamp, seq_id)` sorts exactly like the encoded `u128`.
    #[inline]
    fn encode(timestamp: i64, seq_id: u64) -> u128 {
        let ordered_timestamp = (timestamp as u64) ^ (1u64 << 63);
        ((ordered_timestamp as u128) << 64) | seq_id as u128
    }

    pub fn insert(&self, timestamp: i64, seq_id: u64, pointer: LogPointer) {
        self.inner.insert(Self::encode(timestamp, seq_id), pointer);
    }

    /// Removes an entry and reports whether it existed.
    pub fn remove(&self, timestamp: i64, seq_id: u64) -> bool {
        self.inner
            .remove(&Self::encode(timestamp, seq_id))
            .is_some()
    }

    /// Returns all pointers with timestamp in [start_ms, end_ms] inclusive.
    pub fn range(&self, start_ms: i64, end_ms: i64) -> Vec<LogPointer> {
        self.inner
            .range((
                Bound::Included(Self::encode(start_ms, u64::MIN)),
                Bound::Included(Self::encode(end_ms, u64::MAX)),
            ))
            .map(|(_, pointer)| pointer)
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

#[cfg(test)]
mod tests {
    use super::TimestampIndex;

    #[test]
    fn encoded_key_order_matches_pair_order() {
        let pairs = [
            (i64::MIN, u64::MIN),
            (i64::MIN, u64::MAX),
            (-1, 0),
            (0, 0),
            (0, 1),
            (1, 0),
            (i64::MAX, u64::MAX),
        ];
        for pair in pairs.windows(2) {
            assert!(
                TimestampIndex::encode(pair[0].0, pair[0].1)
                    < TimestampIndex::encode(pair[1].0, pair[1].1),
                "{:?} must sort before {:?}",
                pair[0],
                pair[1]
            );
        }
    }
}
