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
    apply_pipeline_stages_to_vec, cosine_similarity, evaluate_record_field, execute_query,
    to_vector,
};
use liven::parser::{parse_pipeline, parse_query};
use liven::storage::StorageEngine;
use liven::types::{DataValue, PipelineStage};
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

/// Goal: does a float array ever become `DataValue::Vector`, and does `as i8`
/// truncation zero it out anywhere?
#[test]
fn probe_1_float_embeddings() {
    let (engine, _dir) = tmp_engine("p1");

    // ── Case A: embedding as a FIELD of an object, via the DSL/server path ──
    // This is the shape the docs show: `{..., embedding: [...]}`.
    let q = parse_query(r#"from("emb").insert("obj1", {embedding: [0.12, -0.5, 0.9]})"#)
        .expect("DSL insert (object form) should parse");
    execute_query(&engine, &q).expect("DSL insert (object form) should execute");

    let rec = engine.get("emb", "obj1").unwrap().expect("obj1 present");
    println!(
        "[probe1] object-insert stored value variant = {}",
        variant_name(&rec.value)
    );
    // CURRENT: the whole object is stored as a JSON *String*; nested arrays are
    // NOT typed as Array and NOT typed as Vector.
    assert!(
        matches!(&rec.value, DataValue::String(_)),
        "CURRENT behavior: object payload is stored as DataValue::String(JSON)"
    );

    let extracted = evaluate_record_field(&rec, "embedding");
    println!("[probe1] extracted `embedding` field = {:?}", extracted);
    // CURRENT: a JSON *array* field is extracted as a String (the JSON text),
    // not as an Array and not as a Vector — see executor::extract_field's final
    // `_ => Some(DataValue::String(field_val.to_string()))` arm.
    assert!(
        matches!(&extracted, Some(DataValue::String(_))),
        "CURRENT behavior: an array-valued field is extracted as DataValue::String"
    );
    // Consequence: the vector path cannot see it at all.
    assert_eq!(
        to_vector(extracted.as_ref().unwrap()),
        None,
        "CURRENT behavior: to_vector(String) -> None, so vector_filter cannot match it"
    );
    // SHOULD: an embedding field should round-trip as a numeric vector so that
    // `vector_filter(embedding, ...)` can search it.

    // ── Case B: a BARE float array as the whole value, via the DSL/server path ──
    let q = parse_query(r#"from("emb").insert("arr1", [0.12, -0.5, 0.9])"#)
        .expect("DSL insert (array form) should parse");
    execute_query(&engine, &q).expect("DSL insert (array form) should execute");

    let rec = engine.get("emb", "arr1").unwrap().expect("arr1 present");
    println!(
        "[probe1] array-insert stored value variant = {} value = {:?}",
        variant_name(&rec.value),
        rec.value
    );
    // CURRENT: the bare float array is stored as `DataValue::Array` of `Float`.
    assert!(
        matches!(&rec.value, DataValue::Array(_)),
        "CURRENT behavior: a float array is stored as DataValue::Array"
    );
    if let DataValue::Array(items) = &rec.value {
        assert!(
            items.iter().all(|d| matches!(d, DataValue::Float(_))),
            "CURRENT behavior: array elements remain DataValue::Float"
        );
    }
    // CURRENT: the DSL never produces a `DataValue::Vector`.
    assert!(
        !matches!(&rec.value, DataValue::Vector(_)),
        "CURRENT behavior: the DSL/parse path never yields DataValue::Vector"
    );

    // `as i8` truncation: each float is cast toward zero -> [0, 0, 0].
    let stored_vec = to_vector(&rec.value).expect("to_vector accepts DataValue::Array");
    println!("[probe1] to_vector(stored float array) = {:?}", stored_vec);
    assert_eq!(
        stored_vec,
        vec![0, 0, 0],
        "CURRENT behavior: `Float as i8` truncates 0.12 / -0.5 / 0.9 all to 0 (total signal loss)"
    );
    // SHOULD: a documented quantization step (e.g. round(x * 127)) with a known
    // scale, rather than a silent truncating cast.

    // The QUERY vector goes through the same `as i8` path during parsing.
    let stages = parse_pipeline(r#"from("emb") | vector_filter(value, [0.12, -0.5, 0.9], 0.85)"#)
        .expect("vector_filter pipeline should parse");
    let query_vector = match &stages[1] {
        PipelineStage::VectorFilter { query_vector, .. } => query_vector.clone(),
        other => panic!("expected PipelineStage::VectorFilter, got {other:?}"),
    };
    println!("[probe1] parsed query vector = {:?}", query_vector);
    assert_eq!(
        query_vector,
        vec![0, 0, 0],
        "CURRENT behavior: query-vector floats also truncate to 0 via `as i8`"
    );

    let sim = cosine_similarity(&stored_vec, &query_vector).unwrap();
    println!("[probe1] cosine_similarity(stored, query) = {}", sim);

    let mut records = vec![rec.clone()];
    apply_pipeline_stages_to_vec(&mut records, &engine, &stages);
    println!(
        "[probe1] vector_filter(threshold=0.85) matched {} of 1 record(s)",
        records.len()
    );
    // CURRENT: a zero-norm vector scores exactly 0.0, so any positive threshold
    // drops it (0.0 >= 0.85 is false).
    assert_eq!(
        records.len(),
        0,
        "CURRENT behavior: a truncated-to-zero vector scores 0.0 and is dropped"
    );

    // ── Contrast: only an EXPLICIT DataValue::Vector is searchable ──
    engine
        .append("emb", "vec1", DataValue::Vector(vec![10, -5, 9]), false)
        .unwrap();
    let genuine = engine.get("emb", "vec1").unwrap().unwrap();
    let gv = to_vector(&genuine.value).unwrap();
    let gsim = cosine_similarity(&gv, &[10, -5, 9]).unwrap();
    println!(
        "[probe1] genuine DataValue::Vector cosine vs itself = {}",
        gsim
    );
    assert!((gsim - 1.0).abs() < 1e-9);

    println!(
        "PROBE RESULT: probe_1_float_embeddings - object field stored as String and invisible to vector_filter; \
bare float array stored as Array(Float) and truncated to zeros by `as i8`; float arrays NEVER become DataValue::Vector \
(only an explicit DataValue::Vector is searchable)"
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
        "[probe3] single Insert holds write_lock for its duration: {}",
        insert_blocked
    );
    println!(
        "[probe3] single Upsert holds write_lock for its duration: {}",
        upsert_blocked
    );

    // CURRENT: both single Insert and single Upsert DO acquire write_lock
    // (executor.rs `execute_query`, Insert arm ~L1224 and Upsert arm ~L1272).
    assert!(
        insert_blocked,
        "CURRENT behavior: single Insert acquires write_lock"
    );
    assert!(
        upsert_blocked,
        "CURRENT behavior: single Upsert acquires write_lock"
    );
    // NOTE / doc-vs-code conflict: `StorageEngine::write_lock`'s doc comment in
    // storage.rs claims "NOT held by single Insert or Upsert (last-writer-wins)".
    // The code does hold it. The comment is wrong.
    // SHOULD: reconcile the doc comment with the code, or narrow the lock scope so
    // single inserts really are last-writer-wins without a global mutex.

    println!(
        "PROBE RESULT: probe_3_write_lock_on_single_insert - Insert=YES, Upsert=YES \
(both serialize on the global write_lock; the struct doc comment claiming otherwise is wrong)"
    );
}
