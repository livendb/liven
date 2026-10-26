//! Concurrent ordered index backed by [`bbtree::BBTreeMap`].
//!
//! liven keeps one live [`LogPointer`] per compound `"stream:key"` in this
//! map. It previously used `crossbeam_skiplist::SkipMap`; this wrapper keeps
//! the small API the engine relies on while the storage structure is a
//! concurrent B+-tree.
//!
//! `BBTreeMap` does not expose `len()` or a borrowed iterator, so the wrapper
//! maintains an [`AtomicUsize`] entry count and exposes owned `(key, value)`
//! pairs from range/iteration. The count is exact for the engine's write
//! paths (which are serialised by `write_lock` and the flusher) and is safe
//! under concurrent inserts/removes.

use crate::types::LogPointer;
use bbtree::BBTreeMap;
use std::borrow::Borrow;
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicUsize, Ordering};

/// An ordered concurrent map from keys to live log pointers.
pub struct IndexMap<K> {
    map: BBTreeMap<K, LogPointer>,
    len: AtomicUsize,
}

impl<K> Default for IndexMap<K> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K> IndexMap<K> {
    pub fn new() -> Self {
        Self {
            map: BBTreeMap::new(),
            len: AtomicUsize::new(0),
        }
    }

    /// Number of entries currently held by the index.
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<K: Ord> IndexMap<K> {
    /// Inserts `key` and returns the pointer it replaced, if any.
    ///
    /// # Panics
    ///
    /// If the B-tree cannot allocate a node. As with std's collections, an
    /// allocation failure here means the process has no memory left; the
    /// engine's write paths have no fallback for it.
    pub fn insert(&self, key: K, pointer: LogPointer) -> Option<LogPointer> {
        match self.map.insert(key, pointer) {
            Ok(replaced) => {
                if replaced.is_none() {
                    self.len.fetch_add(1, Ordering::Relaxed);
                }
                replaced
            }
            Err(e) => panic!("liven index: bbtree allocation failed: {e}"),
        }
    }
}

impl<K> IndexMap<K> {
    pub fn get<Q>(&self, key: &Q) -> Option<LogPointer>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.map.get_copied(key)
    }

    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.map.contains_key(key)
    }

    /// Removes `key` and returns its pointer, if it existed.
    pub fn remove<Q>(&self, key: &Q) -> Option<LogPointer>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let removed = self.map.remove(key);
        if removed.is_some() {
            self.len.fetch_sub(1, Ordering::Relaxed);
        }
        removed
    }
}

impl<K: Clone + Ord> IndexMap<K> {
    /// Iterates over owned `(key, pointer)` pairs in key order.
    pub fn iter(&self) -> impl Iterator<Item = (K, LogPointer)> + '_ {
        self.map.range_cloned::<K, _>(..)
    }
}

impl<K: Clone> IndexMap<K> {
    /// Iterates over owned `(key, pointer)` pairs in `range`, in key order.
    pub fn range<'a, Q, R>(&'a self, range: R) -> impl Iterator<Item = (K, LogPointer)> + 'a
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized + 'a,
        R: RangeBounds<Q> + 'a,
    {
        self.map.range_cloned(range)
    }
}
