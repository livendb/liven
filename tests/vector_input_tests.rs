//! Step 2 tests: the vector input contract.
//!
//! Vectors are **int8** (`-128..=127`) and every vector in a comparison must have the
//! **same dimension** as the query vector. A vector can be written with:
//!   * the DSL literal `vec([12, -5, 9])`
//!   * the JSON tag `{"$vec": [12, -5, 9]}` (REST / ingest / embedded `insert`)
//!   * an integer `DataValue::Array` (accepted for compatibility)
//!
//! Float, string, out-of-range, and wrong-dimension inputs are rejected with a clear
//! error instead of silently scoring zero. Float→int8 quantization is intentionally
//! NOT done in the core; it belongs in the SDKs.

use liven::Liven;
use liven::executor::to_vector_strict;
use liven::parser::parse_pipeline;
use liven::types::DataValue;
use serde_json::json;
use tempfile::TempDir;

fn open_db(tag: &str) -> (Liven, TempDir) {
    let dir = tempfile::Builder::new()
        .prefix(&format!("liven_vecinput_{}_", tag))
        .tempdir()
        .expect("tempdir");
    let db = Liven::open(dir.path()).expect("Liven::open");
    (db, dir)
}

#[test]
fn dsl_vec_writes_native_vector_and_reads_back() {
    let (db, _dir) = open_db("dsl_roundtrip");
    db.query(r#"from("emb").insert("k1", vec([12, -5, 9]))"#)
        .expect("vec([...]) insert should execute");

    let got = db.get("emb", "k1").unwrap();
    assert_eq!(got.len(), 1);
    assert!(
        matches!(&got[0].value, DataValue::Vector(_)),
        "vec([...]) must produce DataValue::Vector, got {:?}",
        got[0].value
    );
    assert_eq!(to_vector_strict(&got[0].value).unwrap(), vec![12, -5, 9]);
}

#[test]
fn dsl_vec_is_searchable() {
    let (db, _dir) = open_db("dsl_search");
    db.query(r#"from("emb").insert("a", vec([12, -5, 9]))"#)
        .unwrap();
    db.query(r#"from("emb").insert("b", vec([-100, 100, 0]))"#)
        .unwrap();
    db.query(r#"from("emb").insert("c", vec([12, -5, 8]))"#)
        .unwrap();

    let hits = db
        .query(r#"from("emb") | vector_filter(value, vec([12, -5, 9]), 0.999)"#)
        .unwrap();
    assert_eq!(
        hits.len(),
        1,
        "only the exact match should pass a 0.999 threshold"
    );
    assert_eq!(hits[0].key.to_string(), "a");
}

#[test]
fn structured_embedding_plus_metadata_via_dsl() {
    let (db, _dir) = open_db("structured_dsl");
    // Embedding is a field next to metadata. The vector is written with vec([...]).
    db.query(
        r#"from("mem").insert("m1", {embedding: vec([1, 2, 3]), user_id: "u1", kind: "note"})"#,
    )
    .unwrap();
    db.query(
        r#"from("mem").insert("m2", {embedding: vec([1, 2, 3]), user_id: "u2", kind: "note"})"#,
    )
    .unwrap();

    // Filter by metadata first, then vector-search the embedding field.
    let hits = db
        .query(r#"from("mem") | filter(user_id == "u1") | vector_filter(embedding, vec([1, 2, 3]), 0.999)"#)
        .unwrap();
    assert_eq!(
        hits.len(),
        1,
        "metadata filter must scope the vector search"
    );
    assert_eq!(hits[0].key.to_string(), "m1");
}

#[test]
fn structured_embedding_via_json_tag_matches_rest_shape() {
    let (db, _dir) = open_db("structured_rest");
    // This mirrors the REST/ingest body: {"stream":"mem","key":"r1",
    // "value":{"embedding":{"$vec":[1,2,3]},"user_id":"u9"}}
    db.insert(
        "mem",
        "r1",
        json!({"embedding": {"$vec": [1, 2, 3]}, "user_id": "u9"}),
    )
    .unwrap();

    let hits = db
        .query(r#"from("mem") | filter(user_id == "u9") | vector_filter(embedding, vec([1, 2, 3]), 0.999)"#)
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].key.to_string(), "r1");

    // The tag also works as a bare (non-nested) value.
    db.insert("mem", "r2", json!({"$vec": [7, 8, 9]})).unwrap();
    let rec = db.get("mem", "r2").unwrap();
    assert!(matches!(&rec[0].value, DataValue::Vector(_)));
    assert_eq!(to_vector_strict(&rec[0].value).unwrap(), vec![7, 8, 9]);
}

#[test]
fn float_array_field_is_rejected_with_clear_error() {
    let (db, _dir) = open_db("float_reject");
    db.query(r#"from("emb").insert("f1", {embedding: [0.12, -0.5, 0.9]})"#)
        .unwrap();

    let err = db
        .query(r#"from("emb") | vector_filter(embedding, vec([1, 2, 3]), 0.0)"#)
        .expect_err("a float vector field must be rejected, not silently zeroed");
    let msg = err.to_string();
    assert!(msg.contains("floating-point"), "unexpected error: {msg}");
    assert!(
        msg.contains("embedding"),
        "error should name the field: {msg}"
    );
}

#[test]
fn string_vector_field_is_rejected_with_clear_error() {
    let (db, _dir) = open_db("string_reject");
    db.query(r#"from("emb").insert("s1", {embedding: "[1, 2, 3]"})"#)
        .unwrap();

    let err = db
        .query(r#"from("emb") | vector_filter(embedding, vec([1, 2, 3]), 0.0)"#)
        .expect_err("a string vector field must be rejected");
    assert!(
        err.to_string().contains("string"),
        "unexpected error: {err}"
    );
}

#[test]
fn dimension_mismatch_is_reported() {
    let (db, _dir) = open_db("dim_mismatch");
    db.query(r#"from("emb").insert("d1", vec([1, 2, 3]))"#)
        .unwrap();

    let err = db
        .query(r#"from("emb") | vector_filter(value, vec([1, 2]), 0.0)"#)
        .expect_err("a dimension mismatch must be reported");
    let msg = err.to_string();
    assert!(
        msg.contains("dimension mismatch"),
        "unexpected error: {msg}"
    );
    assert!(
        msg.contains('3') && msg.contains('2'),
        "should report both dims: {msg}"
    );
}

#[test]
fn out_of_range_int_field_is_rejected() {
    let (db, _dir) = open_db("range_reject");
    // A bare integer array is accepted as a vector, but elements must fit in int8.
    db.query(r#"from("emb").insert("o1", [300, 1])"#).unwrap();

    let err = db
        .query(r#"from("emb") | vector_filter(value, vec([1, 2]), 0.0)"#)
        .expect_err("out-of-range int8 elements must be rejected");
    assert!(
        err.to_string().contains("out of range"),
        "unexpected error: {err}"
    );
}

#[test]
fn float_query_vector_is_rejected_at_parse_time() {
    let bad = parse_pipeline(r#"from("emb") | vector_filter(value, [0.12, -0.5, 0.9], 0.85)"#);
    assert!(
        bad.is_err(),
        "float query vectors must be rejected at parse time, got {bad:?}"
    );

    // Integer query vectors (bare array or vec(...)) are accepted.
    assert!(parse_pipeline(r#"from("emb") | vector_filter(value, [12, -5, 9], 0.85)"#).is_ok());
    assert!(
        parse_pipeline(r#"from("emb") | vector_filter(value, vec([12, -5, 9]), 0.85)"#).is_ok()
    );
}
