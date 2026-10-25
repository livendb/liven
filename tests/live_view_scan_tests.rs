//! Step 4b tests: the default read path is the live key index
//! (`StorageEngine::scan_live_stream`).
//!
//! Guarantees under test:
//!  * tombstoned keys are never returned by queries,
//!  * superseded versions are never returned (only the latest live version),
//!  * `vector_search` never surfaces a forgotten or superseded memory,
//!  * `history()` opts into the raw-log view (tombstones + superseded versions),
//!  * the live scan reads a number of on-disk frames proportional to the queried
//!    stream — not to the total database size.

use liven::Liven;
use liven::executor::execute_query;
use liven::parser::parse_query;
use liven::storage::StorageEngine;
use liven::types::{DataValue, Query, Record};
use std::sync::atomic::Ordering;
use tempfile::TempDir;

fn open_db(tag: &str) -> (Liven, TempDir) {
    let dir = tempfile::Builder::new()
        .prefix(&format!("liven_liveview_{}_", tag))
        .tempdir()
        .expect("tempdir");
    (Liven::open(dir.path()).expect("Liven::open"), dir)
}

fn keys(records: &[Record]) -> Vec<String> {
    let mut k: Vec<String> = records.iter().map(|r| r.key.to_string()).collect();
    k.sort();
    k
}

#[test]
fn deleted_keys_never_returned() {
    let (db, _dir) = open_db("deleted");
    db.insert("mem", "k1", serde_json::json!({"v": 1})).unwrap();
    db.insert("mem", "k2", serde_json::json!({"v": 2})).unwrap();
    db.insert("mem", "k3", serde_json::json!({"v": 3})).unwrap();
    db.delete("mem", "k2").unwrap();

    let live = db.query(r#"from("mem")"#).unwrap();
    assert_eq!(
        keys(&live),
        vec!["k1".to_string(), "k3".to_string()],
        "the deleted key k2 must not be returned by the live read path"
    );

    // get() on the tombstoned key is also empty.
    assert!(db.engine().get("mem", "k2").unwrap().is_none());
}

#[test]
fn superseded_versions_never_returned() {
    let (db, _dir) = open_db("superseded");
    db.insert("mem", "k1", serde_json::json!({"v": 1})).unwrap();
    db.upsert("mem", "k1", serde_json::json!({"v": 2})).unwrap();
    db.upsert("mem", "k1", serde_json::json!({"v": 3})).unwrap();

    let live = db.query(r#"from("mem")"#).unwrap();
    assert_eq!(live.len(), 1, "only the latest live version is returned");
    match &live[0].value {
        DataValue::String(s) => {
            let j: serde_json::Value = serde_json::from_str(s).unwrap();
            assert_eq!(j.get("v").and_then(|v| v.as_i64()), Some(3));
        }
        other => panic!("expected String JSON value, got {other:?}"),
    }

    // Upsert is a one-frame replacement now, so history sees the insert plus
    // one replacement frame per upsert: 1 + 2 = 3 k1 frames.
    let history = db.query(r#"from("mem") | history()"#).unwrap();
    let k1_versions = history.iter().filter(|r| r.key.as_str() == "k1").count();
    assert_eq!(
        k1_versions, 3,
        "history() returns every frame: 1 insert + 1 replacement per upsert"
    );
}

#[test]
fn vector_search_never_returns_forgotten_or_superseded_memory() {
    let (db, _dir) = open_db("vsearch_live");
    db.insert(
        "mem",
        "m1",
        serde_json::json!({"embedding": {"$vec": [10, 0, 0]}, "user_id": "u1"}),
    )
    .unwrap();
    db.insert(
        "mem",
        "m2",
        serde_json::json!({"embedding": {"$vec": [0, 10, 0]}, "user_id": "u1"}),
    )
    .unwrap();
    db.insert(
        "mem",
        "m3",
        serde_json::json!({"embedding": {"$vec": [0, 0, 10]}, "user_id": "u1"}),
    )
    .unwrap();

    // Forget m2, supersede m3 with a new embedding.
    db.delete("mem", "m2").unwrap();
    db.upsert(
        "mem",
        "m3",
        serde_json::json!({"embedding": {"$vec": [10, 0, 0]}, "user_id": "u1"}),
    )
    .unwrap();

    let hits = db
        .query(r#"from("mem") | vector_search(embedding, vec([10, 0, 0]), 10)"#)
        .unwrap();
    let ks = keys(&hits);
    assert!(
        !ks.contains(&"m2".to_string()),
        "a forgotten memory must never be returned by vector_search; got {ks:?}"
    );
    assert_eq!(
        ks,
        vec!["m1".to_string(), "m3".to_string()],
        "exactly one live version each of m1 and m3"
    );
}

#[test]
fn history_returns_forgotten_and_superseded_records() {
    let (db, _dir) = open_db("history");
    db.insert("mem", "k1", serde_json::json!({"v": 1})).unwrap();
    db.insert("mem", "k2", serde_json::json!({"v": 2})).unwrap();
    db.delete("mem", "k2").unwrap();

    let history = db.query(r#"from("mem") | history()"#).unwrap();
    let ks = keys(&history);
    assert_eq!(
        ks,
        vec!["k1".to_string(), "k2".to_string(), "k2".to_string()],
        "history() returns the live k1, the (pre-delete) k2 frame and the k2 tombstone frame"
    );
    assert!(
        history
            .iter()
            .any(|r| r.key.as_str() == "k2" && r.flags & 0x02 != 0),
        "history() must include the tombstone frame for k2"
    );
    assert!(
        history
            .iter()
            .any(|r| r.key.as_str() == "k2" && r.flags & 0x02 == 0),
        "history() must include the pre-delete active frame for k2"
    );
}

#[test]
fn history_stage_round_trips_through_dsl() {
    let q = parse_query(r#"from("mem") | history()"#).unwrap();
    let dsl = q.to_dsl_string();
    assert_eq!(dsl, r#"from("mem") | history()"#);
    let reparsed = parse_query(&dsl).unwrap();
    assert_eq!(reparsed, q);
}

/// The live scan must read only the queried stream's frames, regardless of how
/// many records live in other streams. 500 target records + 50k filler records:
/// the frame counter must be ~500, never ~50k. No timing is measured.
#[test]
fn live_scan_reads_only_target_stream_frames() {
    let dir = tempfile::Builder::new()
        .prefix("liven_liveview_frames_")
        .tempdir()
        .expect("tempdir");
    let engine = StorageEngine::new(dir.path(), 64 * 1024 * 1024).unwrap();

    // 500 records in the target stream.
    let target: Vec<(String, String, DataValue)> = (0..500)
        .map(|i| {
            (
                "target".to_string(),
                format!("k{i:04}"),
                DataValue::Int(i as i64),
            )
        })
        .collect();
    engine.append_batch(target).unwrap();

    // 50k filler records spread across a handful of other streams.
    for chunk in 0..50 {
        let batch: Vec<(String, String, DataValue)> = (0..1000)
            .map(|i| {
                (
                    format!("filler_{}", chunk % 8),
                    format!("f{chunk:03}_{i:04}"),
                    DataValue::Int(i as i64),
                )
            })
            .collect();
        engine.append_batch(batch).unwrap();
    }

    // Sanity: the whole database really holds 50_500 frames.
    assert_eq!(
        engine.skipmap.len(),
        50_500,
        "expected 500 target + 50_000 filler live keys"
    );

    let query = Query::Pipeline(vec![liven::types::PipelineStage::From {
        stream_name: "target".to_string(),
    }]);

    engine.frames_read.store(0, Ordering::Relaxed);
    let results = execute_query(&engine, &query).unwrap();
    let frames = engine.frames_read.load(Ordering::Relaxed);

    assert_eq!(results.len(), 500, "all 500 target records returned");
    assert!(
        (500..=600).contains(&frames),
        "live scan read {frames} frames; expected ~500 (one per target key), \
         definitely not ~50_500 (whole-database scan)"
    );
}
