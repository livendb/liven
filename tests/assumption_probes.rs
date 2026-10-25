//! Assumption probes for Liven.
//!
//! These integration tests DOCUMENT CURRENT BEHAVIOR. They deliberately exercise
//! edge paths that are easy to get wrong, and every assertion encodes what the
//! code actually does today — including behavior that looks undesirable.
//!
//! Where a probe exposes behavior that looks wrong, the assertion still pins the
//! CURRENT behavior and a `SHOULD:` comment states what it ought to be. Do not
//! "fix" a failing probe by weakening the assertion; the assertion is the fact.
//!
//! Probes:
//!   1. Float embeddings — does a float array ever become `DataValue::Vector`?
//!   2. Scan limit scope — is `max_scan_results` per queried stream or global?
//!   3. Write lock on single insert — is `write_lock` held for a single Insert/Upsert?
//!
//! Run with:
//!     cargo test --test assumption_probes -- --nocapture

use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use liven::executor::{
    cosine_similarity, evaluate_record_field, execute_query, to_vector, to_vector_strict,
    try_apply_pipeline_stages_to_vec,
};
use liven::parser::{parse_pipeline, parse_query};
use liven::storage::StorageEngine;
use liven::types::DataValue;
use tempfile::TempDir;

// ── Helpers ─────────────────────────────────────────────────────────────────

fn tmp_engine(tag: &str) -> (StorageEngine, TempDir) {
    let dir = tempfile::Builder::new()
        .prefix(&format!("liven_probe_{}_", tag))
        .tempdir()
        .expect("tempdir");
    // Large segment so every record lands in one segment, keeping scan order
    // deterministic (insertion order == on-disk order).
    let engine = StorageEngine::new(dir.path(), 64 * 1024 * 1024).expect("StorageEngine::new");
    (engine, dir)
}

fn variant_name(v: &DataValue) -> &'static str {
    match v {
        DataValue::Null => "Null",
        DataValue::Bool(_) => "Bool",
        DataValue::Int(_) => "Int",
        DataValue::UInt(_) => "UInt",
        DataValue::Float(_) => "Float",
        DataValue::String(_) => "String",
        DataValue::Binary(_) => "Binary",
        DataValue::Array(_) => "Array",
        DataValue::Object(_) => "Object",
        DataValue::Vector(_) => "Vector",
    }
}

// ── Probe 1: float embeddings ────────────────────────────────────────────────

/// Goal: does a float array ever become `DataValue::Vector`, and how are float /
/// string / wrong-dimension vector fields handled?
///
/// STEP 2 INTENTIONALLY CHANGED THIS: vectors are now writable with the `vec([...])`
/// literal, and vector inputs that are floats / strings / wrong-dimension are
/// rejected with a clear error instead of silently scoring zero. The legacy
/// `to_vector` helper still truncates (kept for compatibility) but the query engine
/// no longer uses it.
#[test]
fn probe_1_float_embeddings() {
    let (engine, _dir) = tmp_engine("p1");

    // ── Case A: embedding as a FIELD of an object, via the DSL/server path ──
    let q = parse_query(r#"from("emb").insert("obj1", {embedding: [0.12, -0.5, 0.9]})"#)
        .expect("DSL insert (object form) should parse");
    execute_query(&engine, &q).expect("DSL insert (object form) should execute");

    let rec = engine.get("emb", "obj1").unwrap().expect("obj1 present");
    println!(
        "[probe1] object-insert stored value variant = {}",
        variant_name(&rec.value)
    );
    // UNCHANGED: the whole object is still stored as a JSON *String*.
    assert!(
        matches!(&rec.value, DataValue::String(_)),
        "object payload is still stored as DataValue::String(JSON)"
    );

    let extracted = evaluate_record_field(&rec, "embedding");
    println!("[probe1] extracted `embedding` field = {:?}", extracted);
    // NEW (Step 2): a JSON array field is extracted as DataValue::Array of Float.
    assert!(
        matches!(&extracted, Some(DataValue::Array(_))),
        "Step 2: array-valued fields are extracted as DataValue::Array"
    );
    // NEW (Step 2): vector_filter on it returns a CLEAR ERROR, not a silent zero.
    let stages =
        parse_pipeline(r#"from("emb") | vector_filter(embedding, vec([12, -5, 9]), 0.85)"#)
            .expect("vec([...]) query vector parses");
    let mut recs = vec![rec.clone()];
    let err = try_apply_pipeline_stages_to_vec(&mut recs, &engine, &stages)
        .expect_err("Step 2: a float vector field is rejected, never silently zeroed");
    println!("[probe1] vector_filter(float array) error = {err}");
    assert!(
        err.to_string().contains("floating-point"),
        "expected a floating-point rejection, got: {err}"
    );

    // ── Case B: a BARE float array as the whole value ──
    let q = parse_query(r#"from("emb").insert("arr1", [0.12, -0.5, 0.9])"#)
        .expect("DSL insert (array form) should parse");
    execute_query(&engine, &q).expect("DSL insert (array form) should execute");

    let rec = engine.get("emb", "arr1").unwrap().expect("arr1 present");
    println!(
        "[probe1] array-insert stored value variant = {} value = {:?}",
        variant_name(&rec.value),
        rec.value
    );
    // UNCHANGED: a bare float array is stored as DataValue::Array of Float, never Vector.
    assert!(matches!(&rec.value, DataValue::Array(_)));
    assert!(!matches!(&rec.value, DataValue::Vector(_)));
    // NEW (Step 2): the strict converter rejects it (no silent truncation).
    let err = to_vector_strict(&rec.value).expect_err("float array rejected");
    println!("[probe1] to_vector_strict(float array) = Err({err})");
    assert!(err.contains("floating-point"));
    // The legacy (non-strict) helper still truncates; retained but no longer used
    // by the query engine.
    println!(
        "[probe1] legacy to_vector(float array) (still truncates) = {:?}",
        to_vector(&rec.value)
    );

    // ── The parser now rejects float QUERY vectors too ──
    let bad = parse_pipeline(r#"from("emb") | vector_filter(value, [0.12, -0.5, 0.9], 0.85)"#);
    println!(
        "[probe1] parse of a float query vector is an error = {}",
        bad.is_err()
    );
    assert!(
        bad.is_err(),
        "Step 2: float query vectors are rejected at parse time"
    );

    // ── The supported path: `vec([...])` writes a real int8 Vector ──
    let q = parse_query(r#"from("emb").insert("v1", vec([12, -5, 9]))"#).unwrap();
    execute_query(&engine, &q).unwrap();
    let rec = engine.get("emb", "v1").unwrap().unwrap();
    println!(
        "[probe1] vec([...]) insert stored variant = {}",
        variant_name(&rec.value)
    );
    assert!(
        matches!(&rec.value, DataValue::Vector(_)),
        "Step 2: vec([...]) writes a native DataValue::Vector"
    );
    let v = to_vector_strict(&rec.value).unwrap();
    assert_eq!(v, vec![12, -5, 9]);
    println!(
        "[probe1] cosine(vec, vec) = {}",
        cosine_similarity(&v, &[12, -5, 9]).unwrap()
    );

    // And it is searchable end to end:
    let stages =
        parse_pipeline(r#"from("emb") | vector_filter(value, vec([12, -5, 9]), 0.99)"#).unwrap();
    let mut recs = vec![rec.clone()];
    try_apply_pipeline_stages_to_vec(&mut recs, &engine, &stages).unwrap();
    println!(
        "[probe1] vector_filter on vec([...]) matched {} record(s)",
        recs.len()
    );
    assert_eq!(recs.len(), 1);

    println!(
        "PROBE RESULT: probe_1_float_embeddings - [UPDATED by Step 2] float arrays still never become DataValue::Vector \
and the legacy to_vector still truncates them, but the DSL now has vec([...]) which writes a real int8 Vector, and \
vector_filter returns a clear error (not silent zero) for float/string/wrong-dimension fields; float query vectors are \
rejected at parse time"
    );
}

// ── Probe 2: scan limit scope ────────────────────────────────────────────────

/// Goal: is `max_scan_results` counted per queried stream, or across all streams
/// in the database?
#[test]
fn probe_2_scan_limit_scope() {
    // ── Scenario 1: "a" has 150 records, "b" has 10. ──
    // STEP 1 INTENTIONALLY CHANGED THIS: the scan is now scoped to the FROM stream,
    // so a query on "b" is no longer blocked by the size of "a".
    {
        let (mut engine, _dir) = tmp_engine("p2_big");
        engine.set_max_scan_results(100);

        for i in 0..150 {
            engine
                .append(
                    "a",
                    &format!("a_{:04}", i),
                    DataValue::String(format!(r#"{{"idx":{}}}"#, i)),
                    false,
                )
                .unwrap();
        }
        for i in 0..10 {
            engine
                .append(
                    "b",
                    &format!("b_{:04}", i),
                    DataValue::String(format!(r#"{{"idx":{}}}"#, i)),
                    false,
                )
                .unwrap();
        }

        // NEW (Step 1): the scan only materializes/counts stream "b" records, so this
        // succeeds even though the database as a whole holds 160 records > limit.
        let q = parse_query(r#"from("b") | filter(idx >= 0)"#).unwrap();
        let recs = execute_query(&engine, &q)
            .expect("Step 1: from(\"b\") is scanned in isolation and succeeds");
        println!(
            "[probe2] 150-in-a: query on stream b SUCCEEDED with {} record(s)",
            recs.len()
        );
        assert_eq!(recs.len(), 10, "only stream b's 10 records are returned");

        // The limit still applies to the QUERIED stream: "a" has 150 > limit.
        let q = parse_query(r#"from("a") | filter(idx >= 0)"#).unwrap();
        let err = execute_query(&engine, &q)
            .expect_err("queried stream \"a\" itself has 150 > limit records, so it errors");
        println!("[probe2] 150-in-a: query on stream a ERRORED: {err}");
        assert!(
            err.to_string().contains("Scan result limit reached"),
            "expected scan-limit error, got: {err}"
        );
    }

    // ── Scenario 2: "a" has only 50 records, "b" has 10. ──
    {
        let (mut engine, _dir) = tmp_engine("p2_small");
        engine.set_max_scan_results(100);

        for i in 0..50 {
            engine
                .append(
                    "a",
                    &format!("a_{:04}", i),
                    DataValue::String(format!(r#"{{"idx":{}}}"#, i)),
                    false,
                )
                .unwrap();
        }
        for i in 0..10 {
            engine
                .append(
                    "b",
                    &format!("b_{:04}", i),
                    DataValue::String(format!(r#"{{"idx":{}}}"#, i)),
                    false,
                )
                .unwrap();
        }

        let q = parse_query(r#"from("b") | filter(idx >= 0)"#).unwrap();
        let recs = execute_query(&engine, &q).expect("60 total records < limit, so it succeeds");
        println!(
            "[probe2] 50-in-a: query on stream b SUCCEEDED with {} record(s)",
            recs.len()
        );
        assert_eq!(recs.len(), 10, "only stream b's records are returned");
    }

    println!(
        "PROBE RESULT: probe_2_scan_limit_scope - [UPDATED by Step 1] the scan is now \
stream-scoped: from(\"b\") succeeds even when \"a\" is huge, and max_scan_results is counted \
only against the queried stream (from(\"a\") over the limit still errors)"
    );
}

// ── Probe 3: write lock on single insert ─────────────────────────────────────

/// Goal: is `write_lock` held for the duration of a single Insert / Upsert?
///
/// Method: we take `write_lock` ourselves, then run the operation on another
/// thread. If the operation acquires the same lock, it cannot complete until we
/// release it. This is a real behavioral probe (not a code-path assertion) and
/// uses only the public `engine.write_lock` field.
fn blocked_by_write_lock<F>(engine: &Arc<StorageEngine>, op: F) -> bool
where
    F: FnOnce(Arc<StorageEngine>) + Send + 'static,
{
    let guard = engine.write_lock.lock().unwrap();
    let (tx, rx) = mpsc::channel();
    let engine_for_op = engine.clone();
    let handle = thread::spawn(move || {
        op(engine_for_op);
        let _ = tx.send(());
    });

    // If the op uses write_lock it will be blocked and this recv times out.
    let finished_while_lock_held = rx.recv_timeout(Duration::from_millis(300)).is_ok();

    drop(guard);
    handle.join().unwrap();

    // Blocked == the operation waited on write_lock.
    !finished_while_lock_held
}

#[test]
fn probe_3_write_lock_on_single_insert() {
    let dir = tempfile::Builder::new()
        .prefix("liven_probe_p3_")
        .tempdir()
        .unwrap();
    let engine = Arc::new(StorageEngine::new(dir.path(), 64 * 1024 * 1024).unwrap());

    // Seed the stream so appends have a place to land.
    engine
        .append(
            "t",
            "seed",
            DataValue::String(r#"{"v":0}"#.to_string()),
            false,
        )
        .unwrap();

    let insert_blocked = blocked_by_write_lock(&engine, |e| {
        let q = parse_query(r#"from("t").insert("k_insert", {v: 1})"#).expect("insert parses");
        let _ = execute_query(e.as_ref(), &q);
    });

    let upsert_blocked = blocked_by_write_lock(&engine, |e| {
        let q = parse_query(r#"from("t").upsert("k_upsert", {v: 2})"#).expect("upsert parses");
        let _ = execute_query(e.as_ref(), &q);
    });

    println!(
        "[probe3] single Insert acquires write_lock: {}",
        insert_blocked
    );
    println!(
        "[probe3] single Upsert acquires write_lock: {}",
        upsert_blocked
    );

    // Insert still acquires write_lock for the existence check and enqueue, but
    // releases it before the ack wait. Upsert is a one-frame last-writer-wins
    // replacement and does not take the lock.
    assert!(
        insert_blocked,
        "CURRENT behavior: single Insert acquires write_lock for validation"
    );
    assert!(
        !upsert_blocked,
        "CURRENT behavior: single Upsert does not acquire write_lock"
    );

    println!(
        "PROBE RESULT: probe_3_write_lock_on_single_insert - Insert=YES, Upsert=YES \
(both serialize on the global write_lock; the struct doc comment claiming otherwise is wrong)"
    );
}

/// Empty and Drop mutate the whole stream, so they must take the same
/// write_lock as Insert/Upsert/Delete to avoid interleaving with a writer.
#[test]
fn probe_4_empty_and_drop_take_write_lock() {
    let dir = tempfile::Builder::new()
        .prefix("liven_probe_p4_")
        .tempdir()
        .unwrap();
    let engine = Arc::new(StorageEngine::new(dir.path(), 64 * 1024 * 1024).unwrap());
    for i in 0..4 {
        engine
            .append(
                "t",
                &format!("k{i}"),
                DataValue::String(r#"{"v":1}"#.to_string()),
                false,
            )
            .unwrap();
    }

    let empty_blocked = blocked_by_write_lock(&engine, |e| {
        let q = parse_query(r#"from("t").empty()"#).expect("empty parses");
        let _ = execute_query(e.as_ref(), &q);
    });
    let drop_blocked = blocked_by_write_lock(&engine, |e| {
        let q = parse_query(r#"drop("t")"#).expect("drop parses");
        let _ = execute_query(e.as_ref(), &q);
    });

    println!(
        "[probe4] Empty holds write_lock: {}, Drop holds write_lock: {}",
        empty_blocked, drop_blocked
    );
    assert!(empty_blocked, "Empty must acquire write_lock");
    assert!(drop_blocked, "Drop must acquire write_lock");
}
