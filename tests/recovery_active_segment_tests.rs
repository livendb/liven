//! Active-segment selection during recovery.
//!
//! Recovery must choose the active normal segment by write recency (highest
//! complete sequence id), never by max file id, and an empty newest normal
//! rollover target must win. These tests pin that behavior after the pre-pass
//! that used to compute it was folded into the single replay pass.

use std::io::Write;
use std::path::Path;

use liven::storage::{serialize_payload_into, StorageEngine};
use liven::types::DataValue;
use tempfile::tempdir;

const STREAM: &str = "bench";
const SEG: u64 = 64 * 1024 * 1024;
const COMPACTION_ID_BASE: u64 = u64::MAX / 2;

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// One active (flags 0x01) frame in the on-disk format.
fn frame_bytes(seq_id: u64, key: &str, value: &str) -> Vec<u8> {
    let value = DataValue::String(value.to_string());
    let mut payload = Vec::new();
    serialize_payload_into(STREAM, key, &value, &mut payload);
    let crc = crc32fast::hash(&payload);
    let mut frame = Vec::with_capacity(26 + payload.len());
    frame.extend_from_slice(&seq_id.to_be_bytes());
    frame.extend_from_slice(&now_ms().to_be_bytes());
    frame.push(value.type_tag());
    frame.push(0x01);
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(&crc.to_be_bytes());
    frame.extend_from_slice(&payload);
    frame
}

fn segment_path(dir: &Path, id: u64) -> std::path::PathBuf {
    dir.join(format!("{id:020}.liven"))
}

fn write_segment(dir: &Path, id: u64, frames: &[Vec<u8>]) {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(segment_path(dir, id))
        .unwrap();
    for frame in frames {
        file.write_all(frame).unwrap();
    }
    file.sync_all().unwrap();
}

fn assert_value(engine: &StorageEngine, key: &str, expected: &str) {
    let rec = engine
        .get(STREAM, key)
        .unwrap()
        .unwrap_or_else(|| panic!("key {key} missing"));
    assert_eq!(rec.value, DataValue::String(expected.to_string()), "key {key} changed");
}

/// A compacted segment with the highest file id must never become active,
/// even though it holds frames; the active segment is the normal one with the
/// highest sequence id.
#[test]
fn active_segment_ignores_compacted_highest_file_id() {
    let dir = tempdir().unwrap();
    write_segment(
        dir.path(),
        1,
        &[
            frame_bytes(1, "a", "v1"),
            frame_bytes(2, "b", "v2"),
            frame_bytes(3, "c", "v3"),
        ],
    );
    // Compacted output lives in the reserved high range and reuses old seq ids.
    write_segment(
        dir.path(),
        COMPACTION_ID_BASE + 7,
        &[frame_bytes(1, "x", "vx"), frame_bytes(2, "y", "vy")],
    );

    let engine = StorageEngine::new(dir.path(), SEG).unwrap();
    assert_eq!(engine.active_segment_id(), 1);
    for (k, v) in [("a", "v1"), ("b", "v2"), ("c", "v3"), ("x", "vx"), ("y", "vy")] {
        assert_value(&engine, k, v);
    }
}

/// A newest normal segment with no complete frame is the rollover target and
/// must be chosen as active even though a lower-id segment holds all the data.
#[test]
fn active_segment_is_empty_newest_rollover_target() {
    let dir = tempdir().unwrap();
    write_segment(
        dir.path(),
        1,
        &[frame_bytes(1, "a", "v1"), frame_bytes(2, "b", "v2")],
    );
    write_segment(dir.path(), 2, &[]);

    let engine = StorageEngine::new(dir.path(), SEG).unwrap();
    assert_eq!(engine.active_segment_id(), 2);
    assert_value(&engine, "a", "v1");
    assert_value(&engine, "b", "v2");
}

/// A torn tail in the segment with the highest sequence id marks it active; the
/// valid prefix survives and the torn frame is discarded.
#[test]
fn active_segment_is_torn_tail_with_highest_seq() {
    let dir = tempdir().unwrap();
    let torn = frame_bytes(4, "d", "v4");
    let cut = torn.len() / 2; // keep a complete header, truncate the payload
    let frames = vec![
        frame_bytes(1, "a", "v1"),
        frame_bytes(2, "b", "v2"),
        frame_bytes(3, "c", "v3"),
        torn[..cut].to_vec(),
    ];
    // The write_segment helper writes each frame verbatim; rebuild so the torn
    // tail is written as a single partial frame.
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(segment_path(dir.path(), 1))
        .unwrap();
    for frame in &frames[..3] {
        file.write_all(frame).unwrap();
    }
    file.write_all(&frames[3]).unwrap();
    file.sync_all().unwrap();
    drop(file);

    let engine = StorageEngine::new(dir.path(), SEG).unwrap();
    assert_eq!(engine.active_segment_id(), 1);
    for (k, v) in [("a", "v1"), ("b", "v2"), ("c", "v3")] {
        assert_value(&engine, k, v);
    }
    assert!(engine.get(STREAM, "d").unwrap().is_none(), "torn frame d survived");
}
