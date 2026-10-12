//! Step 5c tests: the per-stream timestamp index stays consistent with the live
//! key index across compaction and tombstones.
//!
//! The timestamp index stores `LogPointer`s. Compaction rewrites live records to
//! a new segment and deletes the old files, so those pointers go stale unless the
//! index is re-pointed; a tombstone leaves a stale entry unless it is pruned.
//! Both bugs made `filter(timestamp ...)` time-range queries return nothing (after
//! compaction) or a deleted record (after a delete).

use liven::executor::execute_query;
use liven::parser::parse_query;
use liven::storage::StorageEngine;
use liven::types::DataValue;
use tempfile::TempDir;

fn open_engine(tag: &str) -> (StorageEngine, TempDir) {
    let dir = tempfile::Builder::new()
        .prefix(&format!("liven_tsidx_{}_", tag))
        .tempdir()
        .expect("tempdir");
    // Tiny segments force frequent rollovers so compaction has inactive segments
    // to rewrite.
    let engine = StorageEngine::new(dir.path(), 150).expect("StorageEngine::new");
    (engine, dir)
}

fn range_keys(engine: &StorageEngine) -> Vec<String> {
    let q = parse_query(r#"from("s") | filter(timestamp >= 0)"#).unwrap();
    let mut keys: Vec<String> = execute_query(engine, &q)
        .unwrap()
        .iter()
        .map(|r| r.key.to_string())
        .collect();
    keys.sort();
    keys
}

#[test]
fn time_range_survives_compaction() {
    let (engine, _dir) = open_engine("compact");

    for i in 0..6 {
        engine
            .append(
                "s",
                &format!("k{i}"),
                DataValue::String(format!("v{i}")),
                false,
            )
            .unwrap();
        // Pad the stream so segment rollover happens between records.
        engine
            .append(
                "s",
                &format!("pad{i}"),
                DataValue::String("p".repeat(200)),
                false,
            )
            .unwrap();
    }

    let before = range_keys(&engine);
    assert!(before.contains(&"k0".to_string()), "sanity: k0 visible");

    // Force a rollover, then compact away the inactive segments that hold the
    // originals. Pre-fix this leaves the timestamp index pointing at deleted files.
    engine
        .append("other", "big", DataValue::String("x".repeat(400)), false)
        .unwrap();
    engine.compact().unwrap();

    let after = range_keys(&engine);
    assert_eq!(
        after, before,
        "a time-range query must return the same live keys after compaction"
    );
    for i in 0..6 {
        assert!(
            after.contains(&format!("k{i}")),
            "k{i} must survive compaction in a time-range scan"
        );
    }
}

#[test]
fn time_range_excludes_deleted_keys() {
    let (engine, _dir) = open_engine("tombstone");

    for i in 0..5 {
        engine
            .append(
                "s",
                &format!("k{i}"),
                DataValue::String(format!("v{i}")),
                false,
            )
            .unwrap();
    }

    let before = range_keys(&engine);
    assert_eq!(before.len(), 5);

    // Tombstone k2. Pre-fix its stale index entry kept it in time-range results.
    engine.append("s", "k2", DataValue::Null, true).unwrap();

    let after = range_keys(&engine);
    assert_eq!(
        after,
        vec![
            "k0".to_string(),
            "k1".to_string(),
            "k3".to_string(),
            "k4".to_string()
        ],
        "a deleted key must not be returned by a time-range scan"
    );
}

#[test]
fn time_range_returns_latest_version_only() {
    let (engine, _dir) = open_engine("supersede");

    // Upsert writes a tombstone + a new frame, so k1 gets two frames with the
    // same timestamp resolution; the index must end up pointing at the new one.
    engine
        .append("s", "k1", DataValue::String("old".to_string()), false)
        .unwrap();
    engine.append("s", "k1", DataValue::Null, true).unwrap();
    engine
        .append("s", "k1", DataValue::String("new".to_string()), false)
        .unwrap();

    let q = parse_query(r#"from("s") | filter(timestamp >= 0)"#).unwrap();
    let recs = execute_query(&engine, &q).unwrap();
    assert_eq!(recs.len(), 1, "only the latest live version is returned");
    assert_eq!(recs[0].value, DataValue::String("new".to_string()));
}
