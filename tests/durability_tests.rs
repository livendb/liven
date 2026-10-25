//! Durability regression tests for the two reported data-loss bugs.
//!
//! Phase 1 of the fix: these tests reproduce the losses. They are expected to
//! fail on the current implementation and become permanent regression tests.

use liven::executor::execute_query;
use liven::storage::StorageEngine;
use liven::types::{DataValue, Query};
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

fn segment_ids(dir: &Path) -> Vec<u64> {
    let mut ids: Vec<u64> = segment_files(dir)
        .iter()
        .filter_map(|p| {
            p.file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse().ok())
        })
        .collect();
    ids.sort();
    ids
}

fn segment_files(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_file() && p.extension().and_then(|s| s.to_str()) == Some("liven"))
        .collect();
    files.sort();
    files
}

/// Walks the frame headers of a segment and returns the byte offset of the
/// final frame (and its length).
fn last_frame_geometry(path: &Path) -> (u64, u64) {
    let mut file = fs::File::open(path).unwrap();
    let len = file.metadata().unwrap().len();
    let mut offset = 0u64;
    let mut last_start = 0u64;
    let mut header = [0u8; 26];
    loop {
        if offset + 26 > len {
            break;
        }
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.read_exact(&mut header).unwrap();
        let payload_len =
            u32::from_be_bytes([header[18], header[19], header[20], header[21]]) as u64;
        let frame_len = 26 + payload_len;
        if offset + frame_len > len {
            break;
        }
        last_start = offset;
        offset += frame_len;
    }
    (last_start, len - last_start)
}

fn truncate_to(path: &Path, len: u64) {
    let file = fs::OpenOptions::new().write(true).open(path).unwrap();
    file.set_len(len).unwrap();
}

/// Every acknowledged append must survive a restart, even when the previous
/// life of the database ended with a torn tail that recovery discarded.
///
/// Cuts the last record at every byte offset from "header only" up to
/// "everything but the final byte", reopening after each cut and then
/// appending a fresh record.
#[test]
fn writes_acknowledged_after_a_torn_tail_survive_restart() {
    // Learn the geometry of the last frame from a pristine database.
    let probe = tempfile::tempdir().unwrap();
    {
        let engine = StorageEngine::new(probe.path(), 1024 * 1024).unwrap();
        for i in 0..6 {
            engine
                .append(
                    "s",
                    &format!("k{i}"),
                    DataValue::String(format!("v{i}")),
                    false,
                )
                .unwrap();
        }
    }
    let probe_segments = segment_files(probe.path());
    let (_last_start, last_frame_len) =
        last_frame_geometry(probe_segments.last().expect("a segment"));

    // Keep the sweep bounded: every byte from the last frame's header through
    // its final payload byte.
    let cuts: Vec<u64> = (1..last_frame_len).collect();
    let mut failures = Vec::new();

    for cut in cuts {
        let dir = tempfile::tempdir().unwrap();
        {
            let engine = StorageEngine::new(dir.path(), 1024 * 1024).unwrap();
            for i in 0..6 {
                engine
                    .append(
                        "s",
                        &format!("k{i}"),
                        DataValue::String(format!("v{i}")),
                        false,
                    )
                    .unwrap();
            }
        }

        let segments = segment_files(dir.path());
        let last = segments.last().unwrap();
        let len = fs::metadata(last).unwrap().len();
        truncate_to(last, len - cut);

        // 1. Recovery must keep every record that preceded the torn frame.
        let engine = StorageEngine::new(dir.path(), 1024 * 1024).unwrap();
        let recovered_len = fs::metadata(last).unwrap().len();
        if recovered_len != len - last_frame_len {
            failures.push(format!(
                "cut={cut}: recovery left {} bytes; expected truncation to the last complete frame ({})",
                recovered_len,
                len - last_frame_len
            ));
        }
        for i in 0..5 {
            if engine.get("s", &format!("k{i}")).unwrap().is_none() {
                failures.push(format!(
                    "cut={cut}: record k{i} from a complete, fsynced frame was discarded"
                ));
            }
        }

        // 2. A write acknowledged after recovery must survive the next restart.
        engine
            .append("s", "after", DataValue::String("ok".to_string()), false)
            .unwrap();
        drop(engine);

        let reopened = StorageEngine::new(dir.path(), 1024 * 1024).unwrap();
        if reopened.get("s", "after").unwrap().is_none() {
            failures.push(format!(
                "cut={cut}: append acknowledged after recovery was lost on the next restart"
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "torn-tail recovery failures ({} of {} offsets):\n{}",
        failures.len(),
        last_frame_len - 1,
        failures.join("\n")
    );
}

/// Compaction must not resurrect stale values after a restart.
///
/// After `compact()` rewrites keys into a compacted segment whose ID is in the
/// reserved high range, a later update lands in a segment with a *lower* ID.
/// Recovery replays segments in ascending ID order, so the compacted (older)
/// frame wins unless the compacted segment is ordered by sequence ID instead.
#[test]
fn compaction_must_not_regress_values_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = StorageEngine::new(dir.path(), 120).unwrap();
        engine
            .append("s", "a", DataValue::String("v1".to_string()), false)
            .unwrap();
        engine
            .append("s", "b", DataValue::String("b1".to_string()), false)
            .unwrap();
        // Forces a rollover so "a" and "b" live in an inactive segment.
        engine
            .append("s", "c", DataValue::String("c1".to_string()), false)
            .unwrap();
        assert!(
            segment_files(dir.path()).len() >= 2,
            "test setup requires a segment rollover"
        );

        engine.compact().unwrap();
        engine
            .append("s", "a", DataValue::String("v2".to_string()), false)
            .unwrap();
    }

    let engine = StorageEngine::new(dir.path(), 120).unwrap();
    let record = engine.get("s", "a").unwrap().expect("key a must exist");
    assert_eq!(
        record.value,
        DataValue::String("v2".to_string()),
        "restart after compaction resurrected the pre-compaction value"
    );
}

/// The crash window between "compacted segment fsynced" and "old segments
/// deleted": the data directory contains both copies. Recovery must prefer the
/// newest sequence ID, not the highest segment ID.
#[test]
fn recovery_prefers_higher_sequence_id_over_segment_id_order() {
    let dir = tempfile::tempdir().unwrap();
    let engine = StorageEngine::new(dir.path(), 120).unwrap();
    engine
        .append("s", "a", DataValue::String("v1".to_string()), false)
        .unwrap();
    engine
        .append("s", "b", DataValue::String("b1".to_string()), false)
        .unwrap();
    engine
        .append("s", "c", DataValue::String("c1".to_string()), false)
        .unwrap();
    drop(engine);

    // Now update a key, exactly as a writer would between compaction and crash.
    {
        let engine = StorageEngine::new(dir.path(), 120).unwrap();
        engine
            .append("s", "a", DataValue::String("v2".to_string()), false)
            .unwrap();
    }

    // Model the crash: the compacted segment exists alongside the originals.
    // Copying the first segment's frames into a high-ID file is exactly what
    // compact() writes before it deletes the source.
    let segments = segment_files(dir.path());
    let first = segments.first().expect("first segment");
    let compacted = dir.path().join(format!("{:020}.liven", u64::MAX / 2));
    fs::copy(first, &compacted).unwrap();

    let engine = StorageEngine::new(dir.path(), 120).unwrap();
    let record = engine.get("s", "a").unwrap().expect("key a must exist");
    assert_eq!(
        record.value,
        DataValue::String("v2".to_string()),
        "recovery replayed an older compacted frame over a newer write"
    );
}

/// Property test: a random sequence of appends, overwrites and deletes must
/// survive arbitrary clean restarts and compactions unchanged.
#[test]
fn randomized_ops_match_the_model_across_restarts_and_compactions() {
    use std::collections::HashMap;

    let dir = tempfile::tempdir().unwrap();
    let mut model: HashMap<String, String> = HashMap::new();
    // Small segments so rollovers happen constantly.
    let mut engine = StorageEngine::new(dir.path(), 400).unwrap();

    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let next = |state: &mut u64| {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    };

    for step in 0..400u64 {
        let r = next(&mut state);
        let key = format!("k{}", r % 10);
        match r % 12 {
            0..=6 => {
                let value = format!("v{step}");
                engine
                    .append("s", &key, DataValue::String(value.clone()), false)
                    .unwrap();
                model.insert(key, value);
            }
            7..=8 => {
                engine.append("s", &key, DataValue::Null, true).unwrap();
                model.remove(&key);
            }
            9 => {
                engine.compact().unwrap();
            }
            _ => {
                drop(engine);
                engine = StorageEngine::new(dir.path(), 400).unwrap();
            }
        }

        // Compare the full model after every operation, including restarts.
        for i in 0..10 {
            let k = format!("k{i}");
            let got = engine.get("s", &k).unwrap();
            match (model.get(&k), got) {
                (Some(expected), Some(record)) => assert_eq!(
                    record.value,
                    DataValue::String(expected.clone()),
                    "step {step}: key {k} changed across restart/compaction"
                ),
                (None, None) => {}
                (Some(_), None) => {
                    panic!("step {step}: key {k} lost across restart/compaction")
                }
                (None, Some(_)) => {
                    panic!("step {step}: deleted key {k} was resurrected")
                }
            }
        }
    }
}

/// Upsert must be a one-frame replacement. Tearing the replacement frame at
/// every byte offset must leave the previous value, never delete the key.
#[test]
fn upsert_is_atomic_under_a_torn_tail() {
    let probe = tempfile::tempdir().unwrap();
    {
        let engine = StorageEngine::new(probe.path(), 1024 * 1024).unwrap();
        engine
            .append("s", "k", DataValue::String("v1".to_string()), false)
            .unwrap();
        execute_query(
            &engine,
            &Query::Upsert {
                stream_name: "s".to_string(),
                key: "k".to_string(),
                value: serde_json::json!("v2"),
            },
        )
        .unwrap();
    }
    let probe_segments = segment_files(probe.path());
    let (_, last_frame_len) = last_frame_geometry(probe_segments.last().expect("a segment"));

    for cut in 1..last_frame_len {
        let dir = tempfile::tempdir().unwrap();
        {
            let engine = StorageEngine::new(dir.path(), 1024 * 1024).unwrap();
            engine
                .append("s", "k", DataValue::String("v1".to_string()), false)
                .unwrap();
            execute_query(
                &engine,
                &Query::Upsert {
                    stream_name: "s".to_string(),
                    key: "k".to_string(),
                    value: serde_json::json!("v2"),
                },
            )
            .unwrap();
        }
        let segments = segment_files(dir.path());
        let last = segments.last().unwrap();
        let len = fs::metadata(last).unwrap().len();
        truncate_to(last, len - cut);

        let engine = StorageEngine::new(dir.path(), 1024 * 1024).unwrap();
        let got = engine.get("s", "k").unwrap();
        assert!(
            got.is_some(),
            "cut={cut}: a torn upsert deleted the previously acknowledged value"
        );
        assert_eq!(
            got.unwrap().value,
            DataValue::String("v1".to_string()),
            "cut={cut}: expected the old value to survive a torn replacement"
        );
    }
}

/// Regression: after a restart, the active segment must be a normal-range
/// segment chosen by write recency, never a compacted segment with a high ID.
#[test]
fn active_segment_stays_out_of_the_compaction_id_range() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = StorageEngine::new(dir.path(), 120).unwrap();
        for (key, value) in [("a", "v1"), ("b", "v2"), ("c", "v3")] {
            engine
                .append("s", key, DataValue::String(value.to_string()), false)
                .unwrap();
        }
        engine.compact().unwrap();
    }

    let engine = StorageEngine::new(dir.path(), 120).unwrap();
    let active = engine.active_segment_id();
    assert!(
        active < u64::MAX / 2,
        "active segment {active} is in the reserved compaction range"
    );
    engine
        .append("s", "after", DataValue::String("ok".to_string()), false)
        .unwrap();
    drop(engine);

    let engine = StorageEngine::new(dir.path(), 120).unwrap();
    assert!(engine.get("s", "after").unwrap().is_some());
    assert_eq!(
        engine.get("s", "a").unwrap().unwrap().value,
        DataValue::String("v1".to_string())
    );
}

/// Heavy breadth test: 10,000 seeded runs of the same model. Ignored by default
/// because it performs ~200k fsync-backed appends; run explicitly with
/// `cargo test --release --test durability_tests randomized_10k -- --ignored`.
#[test]
#[ignore = "10k seeded runs; run explicitly"]
fn randomized_10k_seeded_runs() {
    use std::collections::HashMap;

    for seed in 0..10_000u64 {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = StorageEngine::new(dir.path(), 400).unwrap();
        let mut model: HashMap<String, String> = HashMap::new();
        let mut s = seed
            .wrapping_add(1)
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .max(1);

        for step in 0..20u64 {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            let r = s;
            let key = format!("k{}", r % 8);
            match r % 12 {
                0..=6 => {
                    let value = format!("v{step}");
                    engine
                        .append("s", &key, DataValue::String(value.clone()), false)
                        .unwrap();
                    model.insert(key, value);
                }
                7..=8 => {
                    engine.append("s", &key, DataValue::Null, true).unwrap();
                    model.remove(&key);
                }
                9 => engine.compact().unwrap(),
                _ => {
                    drop(engine);
                    engine = StorageEngine::new(dir.path(), 400).unwrap();
                }
            }

            for i in 0..8 {
                let k = format!("k{i}");
                match (model.get(&k), engine.get("s", &k).unwrap()) {
                    (Some(expected), Some(record)) => assert_eq!(
                        record.value,
                        DataValue::String(expected.clone()),
                        "seed {seed} step {step}: key {k} changed"
                    ),
                    (None, None) => {}
                    (Some(_), None) => {
                        panic!("seed {seed} step {step}: key {k} lost")
                    }
                    (None, Some(_)) => {
                        panic!("seed {seed} step {step}: deleted key {k} resurrected")
                    }
                }
            }
        }
    }
}

/// Repeated compactions without a restart must consolidate earlier compacted
/// segments, leaving exactly one compacted segment and the active segment.
#[test]
fn repeated_compactions_consolidate_prior_compacted_segments() {
    let dir = tempfile::tempdir().unwrap();
    let engine = StorageEngine::new(dir.path(), 120).unwrap();
    for (key, value) in [("a", "v1"), ("b", "v2"), ("c", "v3")] {
        engine
            .append("s", key, DataValue::String(value.to_string()), false)
            .unwrap();
    }

    let active = engine.active_segment_id();
    let compacted_after = || -> Vec<u64> {
        segment_ids(dir.path())
            .into_iter()
            .filter(|id| *id >= u64::MAX / 2 && *id != active)
            .collect()
    };

    engine.compact().unwrap();
    let first = compacted_after();
    assert_eq!(first.len(), 1, "first compaction should create one segment");

    engine.compact().unwrap();
    let second = compacted_after();
    assert_eq!(
        second.len(),
        1,
        "second compaction must delete the prior compacted segment"
    );
    assert_ne!(
        first[0], second[0],
        "second compaction must write a new segment"
    );

    drop(engine);
    let engine = StorageEngine::new(dir.path(), 120).unwrap();
    for (key, value) in [("a", "v1"), ("b", "v2"), ("c", "v3")] {
        assert_eq!(
            engine.get("s", key).unwrap().unwrap().value,
            DataValue::String(value.to_string())
        );
    }
}
