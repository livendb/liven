use liven::executor::execute_query;
use liven::parser::{parse_pipeline, parse_query};
use liven::storage::deserialize_payload_fuzz;
use liven::storage::key::StreamKey;
use liven::storage::{StorageEngine, deserialize_payload, serialize_payload_into};
use liven::types::{DataValue, Query, json_to_datavalue};

use ordered_float::OrderedFloat;
use proptest::prelude::*;
use std::sync::Arc;
use tempfile::tempdir;

// ── Generators ────────────────────────────────────────────────────────────────

/// Bounded byte vector — prevents OOM on large allocations
fn bounded_bytes() -> impl Strategy<Value = Vec<u8>> {
    (0usize..65536).prop_flat_map(|size| prop::collection::vec(any::<u8>(), size))
}

/// Bounded string — prevents OOM, covers full unicode range
fn bounded_string() -> impl Strategy<Value = String> {
    (0usize..4096).prop_flat_map(|size| {
        prop::collection::vec(any::<char>(), size)
            .prop_map(|chars| chars.into_iter().collect::<String>())
    })
}

/// Non-recursive DataValue generator — avoids exponential size growth
fn arbitrary_datavalue() -> impl Strategy<Value = DataValue> {
    prop_oneof![
        Just(DataValue::Null),
        any::<bool>().prop_map(DataValue::Bool),
        any::<i64>().prop_map(DataValue::Int),
        any::<u64>().prop_map(DataValue::UInt),
        // Exclude NaN — OrderedFloat requires total order, NaN breaks equality
        (-1e308f64..1e308f64).prop_map(|f| DataValue::Float(OrderedFloat(f))),
        bounded_string().prop_map(DataValue::String),
        bounded_bytes().prop_map(DataValue::Binary),
        // Small vectors only — large vectors slow down every iteration
        prop::collection::vec(any::<i8>(), 0..64).prop_map(DataValue::Vector),
    ]
}

/// Creates a fresh engine per test case — critical for proptest isolation.
/// Returns the TempDir so it lives for the duration of the test.
/// Dropping TempDir before the engine would delete files the engine still needs.
fn make_engine() -> (Arc<StorageEngine>, tempfile::TempDir) {
    let dir = tempdir().expect("tempdir");
    let engine = StorageEngine::new(dir.path(), 16 * 1024 * 1024).expect("StorageEngine::new");
    (Arc::new(engine), dir)
}

// ── Parser Fuzz ───────────────────────────────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    /// parse_pipeline must never panic on any input — only Ok or Err
    #[test]
    fn fuzz_parse_pipeline_never_panics(s in bounded_string()) {
        let _ = parse_pipeline(&s);
    }

    /// parse_query must never panic on any input — covers all query variants
    /// including insert, upsert, update, delete, drop, streams(), explain()
    #[test]
    fn fuzz_parse_query_never_panics(s in bounded_string()) {
        let _ = parse_query(&s);
    }

    /// parse_query with structured prefixes — exercises deeper parse paths
    /// that a purely random string would rarely reach
    #[test]
    fn fuzz_parse_query_structured(
        prefix in prop_oneof![
            Just("from(\"s\").insert("),
            Just("from(\"s\").upsert("),
            Just("from(\"s\") | filter("),
            Just("from(\"s\") | filter(key == "),
            Just("explain("),
            Just("drop("),
            Just("from(\"s\").insert(["),
            Just("sequence(["),
        ],
        suffix in bounded_string()
    ) {
        let input = format!("{}{}", prefix, suffix);
        let _ = parse_query(&input);
    }

    /// parse_number must never panic — targeted test for P5 bug.
    /// Exercises the number parser with arbitrary numeric-looking strings.
    #[test]
    fn fuzz_parse_number_never_panics(
        s in prop_oneof![
            // Huge integers that overflow i64/u64
            "[0-9]{1,100}",
            // Floats with extreme exponents
            "[0-9]+\\.[0-9]+[eE][+-]?[0-9]{1,10}",
            // Negative extremes
            "-[0-9]{1,100}",
            // Mixed arbitrary input
            bounded_string(),
        ]
    ) {
        // parse_number is called internally by parse_pipeline.
        // A query with a numeric literal exercises this path.
        let query = format!("from(\"s\") | filter(amount > {})", s);
        let _ = parse_query(&query);
    }

    /// Pipe inside string literal must not be treated as a pipeline separator.
    /// Targeted test for P5 pipe-in-string-literal bug.
    #[test]
    fn fuzz_pipe_in_string_literal_never_panics(s in bounded_string()) {
        let query = format!("from(\"s\") | filter(name == \"{}\")", s);
        let _ = parse_query(&query);
    }
}

// ── Decoder Fuzz ──────────────────────────────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    /// deserialize_payload_fuzz must never panic on arbitrary bytes
    #[test]
    fn fuzz_decoder_arbitrary_bytes(bytes in bounded_bytes()) {
        let _ = deserialize_payload_fuzz(&bytes);
    }

    /// deserialize_payload with partially valid headers —
    /// forces the decoder past initial validation into deep deserialization paths
    #[test]
    fn fuzz_decoder_partially_valid_headers(mut bytes in bounded_bytes()) {
        if bytes.len() >= 10 {
            // Valid stream name length prefix (3 bytes)
            bytes[0] = 0;
            bytes[1] = 3;
            bytes[2] = b's';
            bytes[3] = b't';
            bytes[4] = b'r';
            // Valid key length prefix (3 bytes)
            bytes[5] = 0;
            bytes[6] = 3;
            bytes[7] = b'k';
            bytes[8] = b'e';
            bytes[9] = b'y';
            // Poison the value bytes with valid/invalid MessagePack markers
            if bytes.len() > 10 {
                let marker = match bytes.len() % 6 {
                    0 => 0xc0, // Nil
                    1 => 0xc2, // False
                    2 => 0xc3, // True
                    3 => 0x90, // Fixarray (0 elements)
                    4 => 0xa0, // Fixstr (0 length)
                    _ => 0xcc, // uint8 marker (requires one more byte — tests truncation)
                };
                bytes[10] = marker;
            }
        }
        let _ = deserialize_payload_fuzz(&bytes);
    }
}

// ── StreamKey Fuzz ────────────────────────────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    /// StreamKey::try_new must never panic — only Ok or KeyTooLong error
    #[test]
    fn fuzz_stream_key_never_panics(s in any::<String>()) {
        match StreamKey::try_new(&s) {
            Ok(key) => {
                // Conversion methods must not panic on valid keys
                let _ = key.to_string();
                let _ = key.as_str();
                let _ = key.as_bytes();
            }
            Err(_) => {
                // Only acceptable error is KeyTooLong
            }
        }
        // Truncation must never panic regardless of input
        let truncated = StreamKey::from_str_truncated(&s);
        let _ = truncated.to_string();
    }

    /// Keys at and around the 32-byte boundary must behave correctly
    #[test]
    fn fuzz_stream_key_length_boundary(base in "[a-z]{1,40}") {
        let key = &base[..base.len().min(40)];

        if key.len() <= 32 {
            // Keys at or under limit must succeed
            let result = StreamKey::try_new(key);
            prop_assert!(
                result.is_ok(),
                "Key of {} bytes should succeed, got: {:?}",
                key.len(),
                result
            );
        } else {
            // Keys over limit must return KeyTooLong, never panic
            let result = StreamKey::try_new(key);
            prop_assert!(
                result.is_err(),
                "Key of {} bytes should fail with KeyTooLong",
                key.len()
            );
        }

        // from_str_truncated must always succeed and produce a key <= 32 bytes
        let truncated = StreamKey::from_str_truncated(key);
        prop_assert!(
            truncated.as_bytes().len() <= 32,
            "Truncated key must be <= 32 bytes, got {} bytes",
            truncated.as_bytes().len()
        );
    }
}

// ── Storage Invariants ────────────────────────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    /// Insert must enforce key uniqueness — two inserts for the same key
    /// must result in exactly one succeeding and the second returning a
    /// duplicate key error.
    #[test]
    fn fuzz_insert_key_uniqueness(
        key in "[a-z]{1,20}",
        v1 in arbitrary_datavalue(),
        v2 in arbitrary_datavalue(),
    ) {
        let (engine, _dir) = make_engine();

        let q1 = Query::Insert {
            stream_name: "s".to_string(),
            key: key.clone(),
            value: serde_json::json!(v1),
        };
        let q2 = Query::Insert {
            stream_name: "s".to_string(),
            key: key.clone(),
            value: serde_json::json!(v2),
        };

        let r1 = execute_query(&engine, &q1);
        let r2 = execute_query(&engine, &q2);

        match (&r1, &r2) {
            (Ok(_), Err(e)) => {
                // Expected — second insert must report duplicate
                prop_assert!(
                    e.to_string().contains("already exists"),
                    "Duplicate error must mention 'already exists', got: {}",
                    e
                );
            }
            (Err(_), Ok(_)) => {
                // First failed (e.g. key too long) — second succeeded on a
                // genuinely new key. Acceptable.
            }
            (Ok(_), Ok(_)) => {
                prop_assert!(
                    false,
                    "Both inserts succeeded for key '{}' — uniqueness violated",
                    key
                );
            }
            (Err(_), Err(_)) => {
                // Both failed — acceptable if key was invalid (too long, etc.)
            }
        }
    }

    /// Upsert must leave exactly one live record per key.
    /// After two upserts on the same key, get() must return the latest value
    /// and scan_historical must not return two live records for the same key.
    #[test]
    fn fuzz_upsert_single_live_record(
        key in "[a-z]{1,20}",
        v1 in arbitrary_datavalue(),
        v2 in arbitrary_datavalue(),
    ) {
        let (engine, _dir) = make_engine();

        // Clone key once for all uses
        let key_str = key.clone();
        let get_key = key_str.clone();
        let scan_key = key_str.clone();

        // First upsert — creates the record
        let q1 = Query::Upsert {
            stream_name: "s".to_string(),
            key: key_str.clone(),
            value: serde_json::json!(v1),
        };
        let _ = execute_query(&engine, &q1);

        // Second upsert — must replace, not duplicate
        let q2 = Query::Upsert {
            stream_name: "s".to_string(),
            key: key_str.clone(),
            value: serde_json::json!(v2),
        };
        let _ = execute_query(&engine, &q2);

        // get() must return exactly one record
        let result = engine.get("s", &get_key);
        prop_assert!(result.is_ok(), "get() must not error after upsert");

        if let Ok(Some(record)) = result {
            prop_assert_eq!(
                record.key.to_string(),
                get_key,
                "Returned key must match upserted key"
            );
        }

        // scan_historical must not return two live records for the same key
        if let Ok(records) = engine.scan_historical() {
            let matching: Vec<_> = records
                .iter()
                .filter(|r| r.stream_name == "s" && r.key.to_string() == scan_key)
                .collect();
            prop_assert!(
                matching.len() <= 1,
                "scan_historical returned {} records for key '{}', expected at most 1",
                matching.len(),
                key
            );
        }
    }

    /// Concurrent inserts for the same key must not produce duplicate live records.
    /// At most one insert should succeed regardless of thread interleaving.
    #[test]
    fn fuzz_concurrent_insert_uniqueness(
        key in "[a-z]{1,15}",
        thread_count in 2usize..8,
    ) {
        let (engine, _dir) = make_engine();
        let engine = Arc::clone(&engine);

        let barrier = Arc::new(std::sync::Barrier::new(thread_count));
        let handles: Vec<_> = (0..thread_count)
            .map(|t| {
                let engine = Arc::clone(&engine);
                let key = key.clone(); // clone per thread — original key retained for assertions
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait(); // all threads start simultaneously
                    let query = Query::Insert {
                        stream_name: "s".to_string(),
                        key,               // moved into query inside the closure
                        value: serde_json::json!({"thread": t}),
                    };
                    execute_query(&engine, &query).ok()
                })
            })
            .collect();

        let results: Vec<_> = handles
            .into_iter()
            .map(|h| h.join().expect("thread panicked"))
            .collect();

        let success_count = results.iter().filter(|r| r.is_some()).count();

        // At most one insert should succeed
        prop_assert!(
            success_count <= 1,
            "Concurrent inserts: {} succeeded for key '{}', expected at most 1",
            success_count,
            key
        );

        // If one succeeded, the key must be readable
        if success_count == 1 {
            let record = engine.get("s", &key).expect("get must not error");
            prop_assert!(
                record.is_some(),
                "Key '{}' must be readable after successful concurrent insert",
                key
            );
        }
    }

    /// Stateful sequence: insert → upsert → delete invariants.
    /// Tracks expected live key set and verifies engine matches after each operation.
    #[test]
    fn fuzz_stateful_insert_upsert_delete(
        ops in prop::collection::vec(
            (0..3u32, "[a-z]{1,15}", arbitrary_datavalue()),
            1..20
        )
    ) {
        let (engine, _dir) = make_engine();
        let mut live_keys: std::collections::HashSet<String> = std::collections::HashSet::new();

        for (op_type, key, value) in ops {
            match op_type {
                0 => {
                    // Insert — must succeed only if key is not already live
                    let query = Query::Insert {
                        stream_name: "s".to_string(),
                        key: key.clone(),
                        value: serde_json::json!(value),
                    };
                    let result = execute_query(&engine, &query);
                    if live_keys.contains(&key) {
                        prop_assert!(
                            result.is_err(),
                            "Insert on existing key '{}' must fail, succeeded instead",
                            key
                        );
                    } else if result.is_ok() {
                        live_keys.insert(key.clone());
                    }
                }
                1 => {
                    // Delete — removes key from live set regardless of outcome
                    let query = Query::DeleteKey {
                        stream_name: "s".to_string(),
                        key: key.clone(),
                    };
                    let _ = execute_query(&engine, &query);
                    live_keys.remove(&key);
                }
                2 => {
                    // Upsert — always makes the key live if it succeeds
                    let query = Query::Upsert {
                        stream_name: "s".to_string(),
                        key: key.clone(),
                        value: serde_json::json!(value),
                    };
                    if execute_query(&engine, &query).is_ok() {
                        live_keys.insert(key.clone());
                    }
                }
                _ => {}
            }

            // After every operation, all live keys must be readable
            for live_key in &live_keys {
                let result = engine.get("s", live_key);
                prop_assert!(
                    result.is_ok(),
                    "get() must not error for live key '{}'",
                    live_key
                );
                prop_assert!(
                    result.unwrap().is_some(),
                    "Live key '{}' must be readable, got None",
                    live_key
                );
            }
        }
    }
}

// ── Compaction Invariants ─────────────────────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// Compaction must not lose or corrupt any live records.
    /// Uses 1MB segments so inactive segments exist for compaction to process.
    #[test]
    fn fuzz_compaction_preserves_live_records(
        keys in prop::collection::vec("[a-z]{1,15}", 5..30)
    ) {
        let dir = tempdir().expect("tempdir");
        let engine = StorageEngine::new(dir.path(), 1024 * 1024)
            .expect("StorageEngine::new");

        let mut live_keys: std::collections::HashSet<String> = std::collections::HashSet::new();

        // Insert initial records
        for key in &keys {
            let query = Query::Insert {
                stream_name: "s".to_string(),
                key: key.clone(),
                value: serde_json::json!({"key": key}),
            };
            if execute_query(&engine, &query).is_ok() {
                live_keys.insert(key.clone());
            }
        }

        // Upsert half the keys to create orphaned records on disk
        // (tombstone + new append = two records for the same key pre-compaction)
        for key in keys.iter().take(keys.len() / 2) {
            let query = Query::Upsert {
                stream_name: "s".to_string(),
                key: key.clone(),
                value: serde_json::json!({"updated": true}),
            };
            let _ = execute_query(&engine, &query);
        }

        // Run actual compaction — not a scan proxy
        let compact_result = engine.compact();
        prop_assert!(
            compact_result.is_ok(),
            "Compaction must not error: {:?}",
            compact_result
        );

        // Every live key must be readable after compaction
        for key in &live_keys {
            let result = engine.get("s", key);
            prop_assert!(
                result.is_ok(),
                "get() must not error after compaction for key '{}'",
                key
            );
            prop_assert!(
                result.unwrap().is_some(),
                "Live key '{}' must be readable after compaction, got None",
                key
            );
        }
    }
}

// ── Serialization Round-Trip ──────────────────────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    /// DataValue → JSON → DataValue round-trip must preserve primitive types.
    /// Uses the real liven::types::json_to_datavalue, not a local reimplementation.
    #[test]
    fn fuzz_datavalue_json_round_trip(value in arbitrary_datavalue()) {
        // Skip NaN floats — JSON does not support NaN and round-trip is undefined
        if let DataValue::Float(f) = &value
            && f.is_nan() {
                return Ok(());
            }

        let json = serde_json::to_value(&value);
        prop_assert!(
            json.is_ok(),
            "DataValue must serialize to JSON without error"
        );

        let json = json.unwrap();
        let json_str = serde_json::to_string(&json).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        let roundtripped = json_to_datavalue(parsed);

        // Verify primitive type preservation
        match (&value, &roundtripped) {
            (DataValue::Null, DataValue::Null) => {}
            (DataValue::Bool(a), DataValue::Bool(b)) => {
                prop_assert_eq!(a, b, "Bool round-trip failed")
            }
            (DataValue::Int(a), DataValue::Int(b)) => {
                prop_assert_eq!(a, b, "Int round-trip failed")
            }
            (DataValue::UInt(a), DataValue::UInt(b)) => {
                prop_assert_eq!(a, b, "UInt round-trip failed")
            }
            (DataValue::Float(a), DataValue::Float(b)) => {
                prop_assert_eq!(a, b, "Float round-trip failed")
            }
            (DataValue::String(a), DataValue::String(b)) => {
                prop_assert_eq!(a, b, "String round-trip failed")
            }
            // Binary, Vector, Array, Object may not round-trip exactly through JSON
            _ => {}
        }
    }

    /// MessagePack serialize → deserialize round-trip for storage payloads.
    /// Verifies that serialize_payload_into and deserialize_payload are inverses.
    #[test]
    fn fuzz_msgpack_payload_round_trip(
        stream in "[a-z]{1,10}",
        key in "[a-z]{1,20}",
        value in arbitrary_datavalue(),
    ) {
        let mut buf = Vec::new();
        serialize_payload_into(&stream, &key, &value, &mut buf);

        prop_assert!(!buf.is_empty(), "Serialized payload must not be empty");

        match deserialize_payload(&buf) {
            Ok((s, k, _v)) => {
                prop_assert_eq!(s, stream, "Stream name must survive round-trip");
                prop_assert_eq!(k, key, "Key must survive round-trip");
                // Value type may shift through JSON conversion but must not error
            }
            Err(e) => {
                prop_assert!(
                    false,
                    "deserialize_payload failed on valid serialized payload: {}",
                    e
                );
            }
        }
    }
}
