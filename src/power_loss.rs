//! Test-only power-loss simulation.
//!
//! Enabled by `LIVEN_POWER_LOSS=1`. Engine hooks record, per file, the last
//! fsynced offset and the directory operations (create/unlink/rename) that have
//! not yet been followed by a directory fsync. At a crash point the process
//! materialises a plausible post-power-loss directory and aborts:
//!
//! * each recorded file is truncated to a random length between its last synced
//!   offset and its written length (including mid-frame);
//! * creates/unlinks/renames not followed by a directory fsync are reverted.
//!
//! This models process death plus loss of unsynced page cache. It does not model
//! drives that lie about flush.
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

#[derive(Debug, Clone, Copy, Default)]
struct FileState {
    written: u64,
    synced: u64,
}

#[derive(Default)]
struct State {
    files: HashMap<PathBuf, FileState>,
    creates: HashSet<PathBuf>,
    unlinks: HashMap<PathBuf, Vec<u8>>,
    renames: Vec<(PathBuf, PathBuf)>,
}

static STATE: OnceLock<Mutex<State>> = OnceLock::new();

fn enabled() -> bool {
    std::env::var_os("LIVEN_POWER_LOSS").is_some()
}

fn state() -> &'static Mutex<State> {
    STATE.get_or_init(|| Mutex::new(State::default()))
}

pub fn record_create(path: &Path) {
    if !enabled() {
        return;
    }
    state().lock().unwrap().creates.insert(path.to_path_buf());
}

pub fn record_write(path: &Path, start: u64, end: u64) {
    if !enabled() {
        return;
    }
    let mut guard = state().lock().unwrap();
    let entry = guard.files.entry(path.to_path_buf()).or_insert(FileState {
        written: start,
        synced: start,
    });
    entry.written = entry.written.max(end);
}

pub fn record_sync(path: &Path, offset: u64) {
    if !enabled() {
        return;
    }
    let mut guard = state().lock().unwrap();
    let entry = guard.files.entry(path.to_path_buf()).or_insert(FileState {
        written: offset,
        synced: offset,
    });
    entry.synced = entry.synced.max(offset);
    entry.written = entry.written.max(offset);
}

pub fn record_truncate_sync(path: &Path, len: u64) {
    if !enabled() {
        return;
    }
    let mut guard = state().lock().unwrap();
    guard.files.insert(
        path.to_path_buf(),
        FileState {
            written: len,
            synced: len,
        },
    );
}

pub fn record_unlink(path: &Path) {
    if !enabled() {
        return;
    }
    let bytes = fs::read(path).unwrap_or_default();
    let mut guard = state().lock().unwrap();
    guard.unlinks.insert(path.to_path_buf(), bytes);
    guard.creates.remove(path);
}

#[allow(dead_code)]
pub fn record_rename(from: &Path, to: &Path) {
    if !enabled() {
        return;
    }
    state()
        .lock()
        .unwrap()
        .renames
        .push((from.to_path_buf(), to.to_path_buf()));
}

/// A directory fsync makes pending create/unlink/rename operations in that
/// directory durable.
pub fn record_dir_sync(dir: &Path) {
    if !enabled() {
        return;
    }
    let mut guard = state().lock().unwrap();
    guard.creates.retain(|p| p.parent() != Some(dir));
    guard.unlinks.retain(|p, _| p.parent() != Some(dir));
    guard.renames.retain(|(p, _)| p.parent() != Some(dir));
}

/// Materialise the post-power-loss state and clear the tracker.
pub fn materialize() {
    if !enabled() {
        return;
    }
    let seed: u64 = std::env::var("LIVEN_POWER_LOSS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    eprintln!("power-loss materialize seed={seed}");
    let mut rng = StdRng::seed_from_u64(seed);
    let mut guard = std::mem::take(&mut *state().lock().unwrap());

    for (path, file_state) in &guard.files {
        let Ok(meta) = fs::metadata(path) else {
            continue;
        };
        let cur = meta.len();
        let lower = file_state.synced.min(cur);
        let upper = file_state.written.min(cur).max(lower);
        let cut = rng.gen_range(lower..=upper);
        if cut < cur {
            let _ = fs::OpenOptions::new()
                .write(true)
                .open(path)
                .and_then(|f| f.set_len(cut));
        }
    }

    // Unlinks without a directory fsync come back.
    for (path, bytes) in guard.unlinks.drain() {
        if !path.exists() {
            let _ = fs::write(&path, bytes);
        }
    }
    // Creates without a directory fsync disappear.
    for path in guard.creates.drain() {
        let _ = fs::remove_file(path);
    }
    // Renames without a directory fsync are reverted.
    for (from, to) in guard.renames.drain(..) {
        if to.exists() && !from.exists() {
            let _ = fs::rename(to, from);
        }
    }
}
