//! Step 3 tests: `vector_search(field, [int8 query], k)`.
//!
//! Returns the top-k records by cosine similarity (descending), each carrying a
//! `_score` field, with ties broken deterministically by `sequence_id`.

use liven::Liven;
use liven::executor::{cosine_similarity, execute_query};
use liven::parser::parse_pipeline;
use liven::storage::StorageEngine;
use liven::types::{DataValue, PipelineStage, Query, Record};
use tempfile::TempDir;

fn open_db(tag: &str) -> (Liven, TempDir) {
    let dir = tempfile::Builder::new()
        .prefix(&format!("liven_vsearch_{}_", tag))
        .tempdir()
        .expect("tempdir");
    (Liven::open(dir.path()).expect("Liven::open"), dir)
}

fn score_of(rec: &Record) -> f64 {
    let s = match &rec.value {
        DataValue::String(s) => s,
        other => panic!("expected String value with _score, got {other:?}"),
    };
    let j: serde_json::Value = serde_json::from_str(s).unwrap();
    j.get("_score")
        .and_then(|v| v.as_f64())
        .expect("_score present and numeric")
}

#[test]
fn orders_by_score_descending_with_sequence_tiebreak() {
    let (db, _dir) = open_db("order");
    // Query is [10, 0, 0]. Insertion order fixes sequence_id order.
    db.insert("emb", "v1", serde_json::json!({"$vec": [10, 0, 0]}))
        .unwrap(); // cos 1.0
    db.insert("emb", "v2", serde_json::json!({"$vec": [5, 5, 0]}))
        .unwrap(); // cos 0.7071
    db.insert("emb", "v3", serde_json::json!({"$vec": [5, -5, 0]}))
        .unwrap(); // cos 0.7071 (tie)
    db.insert("emb", "v4", serde_json::json!({"$vec": [0, 10, 0]}))
        .unwrap(); // cos 0.0

    let hits = db
        .query(r#"from("emb") | vector_search(value, vec([10, 0, 0]), 3)"#)
        .unwrap();

    let keys: Vec<String> = hits.iter().map(|r| r.key.to_string()).collect();
    assert_eq!(
        keys,
        vec!["v1", "v2", "v3"],
        "top-3 must be score-desc, ties broken by ascending sequence_id"
    );

    // Scores are descending and match the analytic values.
    let s: Vec<f64> = hits.iter().map(score_of).collect();
    assert!((s[0] - 1.0).abs() < 1e-9);
    assert!((s[1] - 0.70710678).abs() < 1e-6);
    assert!(s[0] >= s[1] && s[1] >= s[2]);
}

#[test]
fn score_matches_cosine_similarity() {
    let (db, _dir) = open_db("score");
    db.insert("emb", "a", serde_json::json!({"$vec": [5, 5, 0]}))
        .unwrap();

    let hits = db
        .query(r#"from("emb") | vector_search(value, vec([10, 0, 0]), 1)"#)
        .unwrap();
    assert_eq!(hits.len(), 1);
    let expected = cosine_similarity(&[5, 5, 0], &[10, 0, 0]).unwrap();
    assert!((score_of(&hits[0]) - expected).abs() < 1e-12);
}

#[test]
fn k_greater_than_n_returns_all() {
    let (db, _dir) = open_db("k_gt_n");
    db.insert("emb", "a", serde_json::json!({"$vec": [1, 0, 0]}))
        .unwrap();
    db.insert("emb", "b", serde_json::json!({"$vec": [0, 1, 0]}))
        .unwrap();

    let hits = db
        .query(r#"from("emb") | vector_search(value, vec([1, 0, 0]), 10)"#)
        .unwrap();
    assert_eq!(hits.len(), 2, "k larger than N returns all N records");
}

#[test]
fn top_k_bounds_the_result_count() {
    let (db, _dir) = open_db("bounded");
    for i in 0..50 {
        db.insert(
            "emb",
            format!("v{i}"),
            serde_json::json!({"$vec": [i as i64 - 25, 1, 0]}),
        )
        .unwrap();
    }
    let hits = db
        .query(r#"from("emb") | vector_search(value, vec([10, 1, 0]), 7)"#)
        .unwrap();
    assert_eq!(hits.len(), 7, "only k records are returned");
}

#[test]
fn k_zero_is_an_error() {
    let (db, _dir) = open_db("k_zero");
    db.insert("emb", "a", serde_json::json!({"$vec": [1, 0, 0]}))
        .unwrap();

    let err = db
        .query(r#"from("emb") | vector_search(value, vec([1, 0, 0]), 0)"#)
        .expect_err("k = 0 must error");
    assert!(
        err.to_string().contains("k must be"),
        "unexpected error: {err}"
    );
}

#[test]
fn empty_stream_returns_no_results() {
    // A listed stream with zero live records. With the live key-index read path,
    // tombstoning the record removes it from queries immediately (no compaction
    // required); the rollover+compaction below is retained to also cover the case
    // where the original frame has been evicted from an inactive segment.
    let dir = tempfile::Builder::new()
        .prefix("liven_vsearch_empty_")
        .tempdir()
        .expect("tempdir");
    let engine = StorageEngine::new(dir.path(), 150).unwrap();
    engine
        .append("emb", "k1", DataValue::Vector(vec![1, 0, 0]), false)
        .unwrap();
    // Write a large payload into another stream to force a segment rollover, so k1's
    // original frame lands in an inactive segment.
    engine
        .append("other", "big", DataValue::String("x".repeat(200)), false)
        .unwrap();
    engine.append("emb", "k1", DataValue::Null, true).unwrap(); // tombstone k1
    engine.compact().unwrap(); // evicts the inactive segment holding k1's original

    let q = Query::Pipeline(vec![
        PipelineStage::From {
            stream_name: "emb".to_string(),
        },
        PipelineStage::VectorSearch {
            field: "value".to_string(),
            query_vector: vec![1, 0, 0],
            top_k: 5,
        },
    ]);
    let hits = execute_query(&engine, &q).expect("an empty stream is not an error");
    assert!(hits.is_empty());
}

#[test]
fn composes_after_filter() {
    let (db, _dir) = open_db("compose");
    db.insert(
        "mem",
        "m1",
        serde_json::json!({"embedding": {"$vec": [1, 2, 3]}, "user_id": "u1"}),
    )
    .unwrap();
    db.insert(
        "mem",
        "m2",
        serde_json::json!({"embedding": {"$vec": [1, 2, 3]}, "user_id": "u2"}),
    )
    .unwrap();

    let hits = db
        .query(r#"from("mem") | filter(user_id == "u2") | vector_search(embedding, vec([1, 2, 3]), 5)"#)
        .unwrap();
    assert_eq!(hits.len(), 1, "filter must scope the search");
    assert_eq!(hits[0].key.to_string(), "m2");
    // The object value is merged with _score.
    match &hits[0].value {
        DataValue::String(s) => {
            let j: serde_json::Value = serde_json::from_str(s).unwrap();
            assert!(j.get("_score").is_some());
            assert_eq!(j.get("user_id").and_then(|v| v.as_str()), Some("u2"));
        }
        other => panic!("expected merged object, got {other:?}"),
    }
}

#[test]
fn parser_round_trip() {
    let stages = vec![
        PipelineStage::From {
            stream_name: "emb".to_string(),
        },
        PipelineStage::VectorSearch {
            field: "value".to_string(),
            query_vector: vec![12, -5, 9],
            top_k: 5,
        },
    ];
    let dsl = stages
        .iter()
        .map(|s| s.to_dsl_string())
        .collect::<Vec<_>>()
        .join(" | ");
    assert_eq!(dsl, r#"from("emb") | vector_search(value, [12, -5, 9], 5)"#);

    let parsed = parse_pipeline(&dsl).expect("vector_search DSL round-trips");
    assert_eq!(parsed, stages);

    // The vec(...) literal form also parses to the same stage shape.
    let parsed2 =
        parse_pipeline(r#"from("emb") | vector_search(value, vec([1, 2, 3]), 7)"#).unwrap();
    assert_eq!(
        parsed2,
        vec![
            PipelineStage::From {
                stream_name: "emb".to_string(),
            },
            PipelineStage::VectorSearch {
                field: "value".to_string(),
                query_vector: vec![1, 2, 3],
                top_k: 7,
            },
        ]
    );
}

#[test]
fn float_query_vector_rejected_at_parse() {
    assert!(
        parse_pipeline(r#"from("emb") | vector_search(value, [0.5, 0.5], 3)"#).is_err(),
        "float query vectors must be rejected"
    );
}

#[test]
fn vector_filter_round_trips_through_dsl() {
    let stages = vec![
        PipelineStage::From {
            stream_name: "emb".to_string(),
        },
        PipelineStage::VectorFilter {
            field: "embedding".to_string(),
            query_vector: vec![12, -5, 9],
            threshold: ordered_float::OrderedFloat(0.85),
        },
    ];
    let dsl = stages
        .iter()
        .map(|s| s.to_dsl_string())
        .collect::<Vec<_>>()
        .join(" | ");
    assert_eq!(
        dsl, r#"from("emb") | vector_filter(embedding, [12, -5, 9], 0.85)"#,
        "the field must be emitted unquoted so it reparses"
    );
    let parsed = parse_pipeline(&dsl).expect("vector_filter DSL round-trips");
    assert_eq!(parsed, stages);
}
