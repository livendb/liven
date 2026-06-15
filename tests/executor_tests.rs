use liven::executor::{
    apply_pipeline_stages_to_vec, compare_values, execute_query, extract_field, project_record,
};
use liven::parser::parse_query;
use liven::storage::StorageEngine;
use liven::types::{DataValue, FilterExpr, Op, PipelineStage, Query, Record};

#[test]
fn test_extract_field() {
    let value =
        DataValue::String(r#"{"status": "error", "code": 500, "healthy": false}"#.to_string());
    assert_eq!(
        extract_field(&value, "status"),
        Some(DataValue::String("error".to_string()))
    );
    assert_eq!(extract_field(&value, "code"), Some(DataValue::Int(500)));
    assert_eq!(
        extract_field(&value, "healthy"),
        Some(DataValue::Bool(false))
    );
    assert_eq!(extract_field(&value, "missing"), None);
}

#[test]
fn test_compare_values_coerced() {
    let int_val = DataValue::Int(100);
    let uint_val = DataValue::UInt(100);
    let float_val = DataValue::Float(ordered_float::OrderedFloat(100.0));
    let larger_float_val = DataValue::Float(ordered_float::OrderedFloat(100.1));

    assert!(compare_values(&int_val, Op::Eq, &uint_val));
    assert!(compare_values(&int_val, Op::Eq, &float_val));
    assert!(compare_values(&float_val, Op::Lt, &larger_float_val));
    assert!(compare_values(&int_val, Op::Lt, &larger_float_val));
}

#[test]
fn test_project_record() {
    let mut record = Record {
        sequence_id: 1,
        timestamp: 12345,
        type_tag: 5,
        flags: 1,
        stream_name: "logs".to_string(),
        key: liven::storage::key::StreamKey::from_str_truncated("key1"),
        value: DataValue::String(
            r#"{"user": "alice", "role": "admin", "ip": "1.1.1.1"}"#.to_string(),
        ),
    };

    project_record(
        &mut record,
        &["key".to_string(), "user".to_string(), "role".to_string()],
    );

    let value_str = match &record.value {
        DataValue::String(s) => s,
        _ => panic!("Expected DataValue::String"),
    };

    let parsed: serde_json::Value = serde_json::from_str(value_str).unwrap();
    assert_eq!(parsed.get("key").unwrap(), "key1");
    assert_eq!(parsed.get("user").unwrap(), "alice");
    assert_eq!(parsed.get("role").unwrap(), "admin");
    assert!(parsed.get("ip").is_none());
}

#[test]
fn test_delete_trash_execution() {
    let records = vec![
        Record {
            sequence_id: 1,
            timestamp: 100,
            type_tag: 1,
            flags: 0x01, // Active key
            stream_name: "logs".to_string(),
            key: liven::storage::key::StreamKey::from_str_truncated("k1"),
            value: DataValue::Int(10),
        },
        Record {
            sequence_id: 2,
            timestamp: 200,
            type_tag: 0,
            flags: 0x02, // Deleted value (key tombstone)
            stream_name: "logs".to_string(),
            key: liven::storage::key::StreamKey::from_str_truncated("k2"),
            value: DataValue::Null,
        },
        Record {
            sequence_id: 3,
            timestamp: 300,
            type_tag: 0,
            flags: 0x04, // Trashed stream & values
            stream_name: "logs".to_string(),
            key: liven::storage::key::StreamKey::from_str_truncated("*"),
            value: DataValue::Null,
        },
    ];

    let temp_dir = std::env::temp_dir().join(format!(
        "test_exec_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let engine = StorageEngine::new(&temp_dir, 1024 * 1024).unwrap();

    // Test Delete stage
    let mut d_records = records.clone();
    apply_pipeline_stages_to_vec(&mut d_records, &engine, &[PipelineStage::Delete]);
    assert_eq!(d_records.len(), 1);
    assert_eq!(d_records[0].key.as_str(), "k2");

    // Test Trash stage
    let mut t_records = records.clone();
    apply_pipeline_stages_to_vec(&mut t_records, &engine, &[PipelineStage::Trash]);
    assert_eq!(t_records.len(), 1);
    assert_eq!(t_records[0].key.as_str(), "*");

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_pipeline_delete_optimizations() {
    let temp_dir = std::env::temp_dir().join(format!(
        "test_exec_del_opt_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let engine = StorageEngine::new(&temp_dir, 1024 * 1024).unwrap();

    // Seed initial records
    let _ = engine
        .append(
            "users",
            "user_1",
            DataValue::String(r#"{"status": "active"}"#.to_string()),
            false,
        )
        .unwrap();
    let _ = engine
        .append(
            "users",
            "user_2",
            DataValue::String(r#"{"status": "inactive"}"#.to_string()),
            false,
        )
        .unwrap();
    let _ = engine
        .append(
            "users",
            "user_3",
            DataValue::String(r#"{"status": "inactive"}"#.to_string()),
            false,
        )
        .unwrap();

    // 1. Test short-circuiting unique-key deletion when key doesn't exist
    let q_missing = Query::PipelineDelete {
        pipeline: vec![
            PipelineStage::From {
                stream_name: "users".to_string(),
            },
            PipelineStage::Get {
                key: "user_99".to_string(),
            },
        ],
    };
    let res_missing = execute_query(&engine, &q_missing).unwrap();
    assert_eq!(res_missing.len(), 1);
    let status_val = match &res_missing[0].value {
        DataValue::String(s) => s,
        _ => panic!("Expected status json"),
    };
    assert!(status_val.contains("\"affected_rows\": 0"));

    // 2. Test unique-key deletion when key exists but filter doesn't match
    let q_exists_filtered_out = Query::PipelineDelete {
        pipeline: vec![
            PipelineStage::From {
                stream_name: "users".to_string(),
            },
            PipelineStage::Get {
                key: "user_1".to_string(),
            },
            PipelineStage::Filter {
                expr: FilterExpr::Simple {
                    field: "status".to_string(),
                    operator: Op::Eq,
                    value: DataValue::String("inactive".to_string()),
                },
            },
        ],
    };
    let res_filtered_out = execute_query(&engine, &q_exists_filtered_out).unwrap();
    let status_val = match &res_filtered_out[0].value {
        DataValue::String(s) => s,
        _ => panic!("Expected status json"),
    };
    assert!(status_val.contains("\"affected_rows\": 0"));
    assert!(engine.get("users", "user_1").unwrap().is_some());

    // 3. Test unique-key deletion when key exists and filter matches
    let q_exists_matched = Query::PipelineDelete {
        pipeline: vec![
            PipelineStage::From {
                stream_name: "users".to_string(),
            },
            PipelineStage::Get {
                key: "user_1".to_string(),
            },
            PipelineStage::Filter {
                expr: FilterExpr::Simple {
                    field: "status".to_string(),
                    operator: Op::Eq,
                    value: DataValue::String("active".to_string()),
                },
            },
        ],
    };
    let res_matched = execute_query(&engine, &q_exists_matched).unwrap();
    let status_val = match &res_matched[0].value {
        DataValue::String(s) => s,
        _ => panic!("Expected status json"),
    };
    assert!(status_val.contains("\"affected_rows\": 1"));
    assert!(engine.get("users", "user_1").unwrap().is_none());

    // 4. Test scan-based bulk tombstone delete
    let q_bulk_inactive = Query::PipelineDelete {
        pipeline: vec![
            PipelineStage::From {
                stream_name: "users".to_string(),
            },
            PipelineStage::Filter {
                expr: FilterExpr::Simple {
                    field: "status".to_string(),
                    operator: Op::Eq,
                    value: DataValue::String("inactive".to_string()),
                },
            },
        ],
    };
    let res_bulk = execute_query(&engine, &q_bulk_inactive).unwrap();
    let status_val = match &res_bulk[0].value {
        DataValue::String(s) => s,
        _ => panic!("Expected status json"),
    };
    assert!(status_val.contains("\"affected_rows\": 2"));
    assert!(engine.get("users", "user_2").unwrap().is_none());
    assert!(engine.get("users", "user_3").unwrap().is_none());

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_status_query() {
    let temp_dir = format!("./data_test_status_{}", std::process::id());
    let engine = StorageEngine::new(&temp_dir, 1024 * 1024)
        .unwrap()
        .with_max_connections(500)
        .with_broadcast_capacity(100);

    let res = execute_query(&engine, &Query::Status).unwrap();
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].stream_name, "status");

    let val_str = match &res[0].value {
        DataValue::String(s) => s,
        _ => panic!("Expected DataValue::String"),
    };

    let metrics: serde_json::Value = serde_json::from_str(val_str).unwrap();
    assert_eq!(metrics["max_connections"], 500);
    assert_eq!(metrics["broadcast_capacity"], 100);
    assert_eq!(metrics["active_connections"], 0);

    // If we acquire a permit, active_connections increases
    let permit = engine.conn_semaphore.clone().try_acquire_owned().unwrap();
    let res_with_permit = execute_query(&engine, &Query::Status).unwrap();
    let val_str_2 = match &res_with_permit[0].value {
        DataValue::String(s) => s,
        _ => panic!("Expected DataValue::String"),
    };
    let metrics_2: serde_json::Value = serde_json::from_str(val_str_2).unwrap();
    assert_eq!(metrics_2["active_connections"], 1);

    drop(permit);

    let _ = std::fs::remove_dir_all(&temp_dir);
}

// ============ CORRELATE TESTS ============

#[test]
fn test_correlate_matches_within_window() {
    let thread_id = std::thread::current().id();
    let temp_dir = std::env::temp_dir().join(format!(
        "test_correlate_window_{:?}_{}",
        thread_id,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let engine = StorageEngine::new(&temp_dir, 1024 * 1024).unwrap();

    // Setup: stream "transactions" with record at timestamp=1000, user_id="u1"
    let txn_val = r#"{"user_id": "u1", "amount": 50}"#;
    engine
        .append(
            "transactions",
            "txn_1",
            DataValue::String(txn_val.to_string()),
            false,
        )
        .unwrap();

    // Setup: stream "logins" with record at timestamp=800, user_id="u1"
    let login_val = r#"{"user_id": "u1", "ip": "192.168.1.1"}"#;
    engine
        .append(
            "logins",
            "login_1",
            DataValue::String(login_val.to_string()),
            false,
        )
        .unwrap();

    // Query: from("transactions") | correlate("logins", "user_id", within: 500)
    let query =
        parse_query("from(\"transactions\") | correlate(\"logins\", \"user_id\", within: 500)")
            .unwrap();
    let result = execute_query(&engine, &query).unwrap();

    // Assert: Result contains 1 record with correlated_count: 1
    assert_eq!(result.len(), 1);
    let val_str = match &result[0].value {
        DataValue::String(s) => s,
        _ => panic!("Expected DataValue::String"),
    };
    let parsed: serde_json::Value = serde_json::from_str(val_str).unwrap();
    assert_eq!(parsed["correlated_count"], 1);
    assert_eq!(parsed["user_id"], "u1");

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_correlate_discards_outside_window() {
    let thread_id = std::thread::current().id();
    let temp_dir = std::env::temp_dir().join(format!(
        "test_correlate_outside_window_{:?}_{}",
        thread_id,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let engine = StorageEngine::new(&temp_dir, 1024 * 1024).unwrap();

    // Use explicit timestamps via Record struct directly to test window boundary
    // Setup: stream "transactions" with record at timestamp=1000, user_id="u1"
    let txn = Record {
        sequence_id: 1,
        timestamp: 1000,
        type_tag: 5,
        flags: 0x01,
        stream_name: "transactions".to_string(),
        key: liven::storage::key::StreamKey::from_str_truncated("txn_1"),
        value: DataValue::String(r#"{"user_id": "u1"}"#.to_string()),
    };

    // Setup: stream "logins" with record at timestamp=200, user_id="u1" (outside 500ms window)

    // Apply Correlate stage via apply_pipeline_stages_to_vec with explicit timestamp records
    let mut records: Vec<Record> = vec![txn];
    // We also need the login record in the engine's historical scan, so append it
    engine
        .append(
            "logins",
            "login_1",
            DataValue::String(r#"{"user_id": "u1"}"#.to_string()),
            false,
        )
        .unwrap();

    let stages = vec![PipelineStage::Correlate {
        source_stream: "logins".to_string(),
        join_key: "user_id".to_string(),
        within_ms: 500,
    }];
    apply_pipeline_stages_to_vec(&mut records, &engine, &stages);

    // Assert: result is empty — login at timestamp 200 is outside the 500ms window of txn at 1000
    assert!(
        records.is_empty(),
        "Expected empty result, got {} records",
        records.len()
    );

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_correlate_no_key_match_discards_record() {
    let thread_id = std::thread::current().id();
    let temp_dir = std::env::temp_dir().join(format!(
        "test_correlate_no_key_match_{:?}_{}",
        thread_id,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let engine = StorageEngine::new(&temp_dir, 1024 * 1024).unwrap();

    // Setup: stream "transactions" with user_id="u1"
    engine
        .append(
            "transactions",
            "txn_1",
            DataValue::String(r#"{"user_id": "u1"}"#.to_string()),
            false,
        )
        .unwrap();

    // Setup: stream "logins" with user_id="u2" (different user)
    engine
        .append(
            "logins",
            "login_1",
            DataValue::String(r#"{"user_id": "u2"}"#.to_string()),
            false,
        )
        .unwrap();

    // Query: from("transactions") | correlate("logins", "user_id", within: 5000)
    let query =
        parse_query("from(\"transactions\") | correlate(\"logins\", \"user_id\", within: 5000)")
            .unwrap();
    let result = execute_query(&engine, &query).unwrap();

    // Assert: result is empty — no matching user_id
    assert!(result.is_empty());

    let _ = std::fs::remove_dir_all(&temp_dir);
}

// ============ SEQUENCE TESTS ============

#[test]
fn test_sequence_complete_match() {
    let thread_id = std::thread::current().id();
    let temp_dir = std::env::temp_dir().join(format!(
        "test_seq_complete_{:?}_{}",
        thread_id,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let engine = StorageEngine::new(&temp_dir, 1024 * 1024).unwrap();

    // Setup: stream "telemetry" with records
    let r1 = r#"{"event": "cpu_spike"}"#;
    let r2 = r#"{"event": "memory_leak"}"#;
    engine
        .append(
            "telemetry",
            "rec_1",
            DataValue::String(r1.to_string()),
            false,
        )
        .unwrap();
    engine
        .append(
            "telemetry",
            "rec_2",
            DataValue::String(r2.to_string()),
            false,
        )
        .unwrap();

    // Query: sequence(event == "cpu_spike", then: event == "memory_leak", within: 500)
    let query = parse_query(
        "from(\"telemetry\") | sequence(event == \"cpu_spike\", then: event == \"memory_leak\", within: 500)",
    )
    .unwrap();
    let result = execute_query(&engine, &query).unwrap();

    // Assert: result contains 1 record — the memory_leak record that completed the sequence
    assert_eq!(result.len(), 1);
    let val_str = match &result[0].value {
        DataValue::String(s) => s,
        _ => panic!("Expected DataValue::String"),
    };
    let parsed: serde_json::Value = serde_json::from_str(val_str).unwrap();
    assert_eq!(parsed["event"], "memory_leak");

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_sequence_window_expired() {
    let thread_id = std::thread::current().id();
    let temp_dir = std::env::temp_dir().join(format!(
        "test_seq_window_expired_{:?}_{}",
        thread_id,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let engine = StorageEngine::new(&temp_dir, 1024 * 1024).unwrap();

    // We inject records with explicit timestamps via the Record struct directly
    let rec1 = Record {
        sequence_id: 1,
        timestamp: 100,
        type_tag: 5,
        flags: 0x01,
        stream_name: "telemetry".to_string(),
        key: liven::storage::key::StreamKey::from_str_truncated("rec_1"),
        value: DataValue::String(r#"{"event": "cpu_spike"}"#.to_string()),
    };
    let rec2 = Record {
        sequence_id: 2,
        timestamp: 700,
        type_tag: 5,
        flags: 0x01,
        stream_name: "telemetry".to_string(),
        key: liven::storage::key::StreamKey::from_str_truncated("rec_2"),
        value: DataValue::String(r#"{"event": "memory_leak"}"#.to_string()),
    };

    let mut records: Vec<Record> = vec![rec1, rec2];
    let stages = vec![PipelineStage::Sequence {
        steps: vec![
            FilterExpr::Simple {
                field: "event".to_string(),
                operator: Op::Eq,
                value: DataValue::String("cpu_spike".to_string()),
            },
            FilterExpr::Simple {
                field: "event".to_string(),
                operator: Op::Eq,
                value: DataValue::String("memory_leak".to_string()),
            },
        ],
        within_ms: 500,
    }];
    apply_pipeline_stages_to_vec(&mut records, &engine, &stages);

    // Assert: result is empty — window of 500ms expired before memory_leak at 700
    assert!(records.is_empty());

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_sequence_out_of_order_no_false_match() {
    let thread_id = std::thread::current().id();
    let temp_dir = std::env::temp_dir().join(format!(
        "test_seq_out_of_order_{:?}_{}",
        thread_id,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let engine = StorageEngine::new(&temp_dir, 1024 * 1024).unwrap();

    // memory_leak appears before cpu_spike (wrong order)
    let rec1 = Record {
        sequence_id: 1,
        timestamp: 100,
        type_tag: 5,
        flags: 0x01,
        stream_name: "telemetry".to_string(),
        key: liven::storage::key::StreamKey::from_str_truncated("rec_1"),
        value: DataValue::String(r#"{"event": "memory_leak"}"#.to_string()),
    };
    let rec2 = Record {
        sequence_id: 2,
        timestamp: 200,
        type_tag: 5,
        flags: 0x01,
        stream_name: "telemetry".to_string(),
        key: liven::storage::key::StreamKey::from_str_truncated("rec_2"),
        value: DataValue::String(r#"{"event": "cpu_spike"}"#.to_string()),
    };

    let mut records: Vec<Record> = vec![rec1, rec2];
    let stages = vec![PipelineStage::Sequence {
        steps: vec![
            FilterExpr::Simple {
                field: "event".to_string(),
                operator: Op::Eq,
                value: DataValue::String("cpu_spike".to_string()),
            },
            FilterExpr::Simple {
                field: "event".to_string(),
                operator: Op::Eq,
                value: DataValue::String("memory_leak".to_string()),
            },
        ],
        within_ms: 500,
    }];
    apply_pipeline_stages_to_vec(&mut records, &engine, &stages);

    // Assert: result is empty — events are in wrong order
    assert!(records.is_empty());

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_sequence_too_many_steps_rejected() {
    // Build a sequence query string with 11 then: steps
    let mut query_str = String::from("from(\"t\") | sequence(a == 1");
    for i in 0..11 {
        query_str.push_str(&format!(", then: a == {}", i + 2));
    }
    query_str.push_str(", within: 1000)");

    let result = parse_query(&query_str);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.contains("Sequence pattern too complex: maximum 10 steps allowed"),
        "Expected max steps error, got: {}",
        err
    );
}

// ============ CHAIN TESTS ============

#[test]
fn test_chain_single_hop() {
    let thread_id = std::thread::current().id();
    let temp_dir = std::env::temp_dir().join(format!(
        "test_chain_single_hop_{:?}_{}",
        thread_id,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let engine = StorageEngine::new(&temp_dir, 1024 * 1024).unwrap();

    // Setup: stream "prompts" key="p1", value={prompt_id: "p1", text: "Hello"}
    engine
        .append(
            "prompts",
            "p1",
            DataValue::String(r#"{"prompt_id": "p1", "text": "Hello"}"#.to_string()),
            false,
        )
        .unwrap();

    // Setup: stream "responses" key="p1", value={response_id: "r1", text: "Hi"}
    engine
        .append(
            "responses",
            "p1",
            DataValue::String(r#"{"response_id": "r1", "text": "Hi"}"#.to_string()),
            false,
        )
        .unwrap();

    // Query: from("prompts") | chain("responses", "prompt_id")
    let query = parse_query("from(\"prompts\") | chain(\"responses\", prompt_id)").unwrap();
    let result = execute_query(&engine, &query).unwrap();

    // Assert: Result contains 1 record with fields from both streams
    assert_eq!(result.len(), 1);
    let val_str = match &result[0].value {
        DataValue::String(s) => s,
        _ => panic!("Expected DataValue::String"),
    };
    let parsed: serde_json::Value = serde_json::from_str(val_str).unwrap();
    assert_eq!(parsed["text"], "Hello");
    assert!(parsed.get("responses").is_some());
    assert_eq!(parsed["responses"]["text"], "Hi");

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_chain_two_hops() {
    let thread_id = std::thread::current().id();
    let temp_dir = std::env::temp_dir().join(format!(
        "test_chain_two_hops_{:?}_{}",
        thread_id,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let engine = StorageEngine::new(&temp_dir, 1024 * 1024).unwrap();

    // Setup: stream "prompts" key="p1", value={prompt_id: "p1"}
    engine
        .append(
            "prompts",
            "p1",
            DataValue::String(r#"{"prompt_id": "p1"}"#.to_string()),
            false,
        )
        .unwrap();

    // Setup: stream "responses" key="p1", value={response_id: "r1", prompt_id: "p1"}
    engine
        .append(
            "responses",
            "p1",
            DataValue::String(r#"{"response_id": "r1", "prompt_id": "p1"}"#.to_string()),
            false,
        )
        .unwrap();

    // Setup: stream "memory" key="r1", value={memory_id: "m1", response_id: "r1"}
    engine
        .append(
            "memory",
            "r1",
            DataValue::String(r#"{"memory_id": "m1", "response_id": "r1"}"#.to_string()),
            false,
        )
        .unwrap();

    // Query: from("prompts") | chain("responses", "prompt_id") | chain("memory", "response_id")
    let query = parse_query(
        "from(\"prompts\") | chain(\"responses\", prompt_id) | chain(\"memory\", response_id)",
    )
    .unwrap();
    let result = execute_query(&engine, &query).unwrap();

    // Assert: result contains fields from all three streams
    assert_eq!(result.len(), 1);
    let val_str = match &result[0].value {
        DataValue::String(s) => s,
        _ => panic!("Expected DataValue::String"),
    };
    let parsed: serde_json::Value = serde_json::from_str(val_str).unwrap();
    assert!(parsed.get("responses").is_some());
    assert!(parsed.get("memory").is_some());
    assert_eq!(parsed["prompt_id"], "p1");

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_chain_no_match_retains_record() {
    let thread_id = std::thread::current().id();
    let temp_dir = std::env::temp_dir().join(format!(
        "test_chain_no_match_{:?}_{}",
        thread_id,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let engine = StorageEngine::new(&temp_dir, 1024 * 1024).unwrap();

    // Setup: stream "prompts" key="p1", value={prompt_id: "p1"}
    engine
        .append(
            "prompts",
            "p1",
            DataValue::String(r#"{"prompt_id": "p1", "text": "Hello"}"#.to_string()),
            false,
        )
        .unwrap();

    // Stream "responses" is empty — no data inserted

    // Query: from("prompts") | chain("responses", "prompt_id")
    let query = parse_query("from(\"prompts\") | chain(\"responses\", prompt_id)").unwrap();
    let result = execute_query(&engine, &query).unwrap();

    // Assert: Result contains 1 record with unchanged value (left join behavior)
    assert_eq!(result.len(), 1);
    let val_str = match &result[0].value {
        DataValue::String(s) => s,
        _ => panic!("Expected DataValue::String"),
    };
    let parsed: serde_json::Value = serde_json::from_str(val_str).unwrap();
    assert_eq!(parsed["text"], "Hello");
    // No "responses" field should be present since there was no match
    assert!(
        parsed.get("responses").is_none(),
        "Left join should not add nested field when no match exists",
    );

    let _ = std::fs::remove_dir_all(&temp_dir);
}

// ── Deletion Fix 2: Drop removes stream from set ──

#[test]
fn test_drop_removes_stream_from_set() {
    use liven::executor::execute_query;
    use liven::parser::parse_query;
    use liven::storage::StorageEngine;
    use liven::types::DataValue;

    let path = std::env::temp_dir().join(format!(
        "liven_drop_stream_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&path);
    let engine = StorageEngine::new(&path, 1024 * 1024).unwrap();

    engine
        .append("logs", "k1", DataValue::String("hello".to_string()), false)
        .unwrap();
    engine
        .append("logs", "k2", DataValue::String("world".to_string()), false)
        .unwrap();

    // Confirm stream exists
    assert!(engine.list_streams().contains(&"logs".to_string()));

    // Drop the stream
    let q = parse_query(r#"drop("logs")"#).unwrap();
    execute_query(&engine, &q).unwrap();

    // Stream must no longer appear in streams()
    assert!(
        !engine.list_streams().contains(&"logs".to_string()),
        "Dropped stream must not appear in list_streams"
    );

    // streams() query must not return it
    let q2 = parse_query("streams()").unwrap();
    let results = execute_query(&engine, &q2).unwrap();
    assert!(
        !results.iter().any(|r| r.key.as_str() == "logs"),
        "streams() must not return dropped stream"
    );

    // empty() on the dropped stream must return an error
    let q3 = parse_query(r#"from("logs").empty()"#).unwrap();
    assert!(
        execute_query(&engine, &q3).is_err(),
        "empty() on dropped stream must fail"
    );

    // Re-inserting into the dropped stream must succeed and re-create it
    let q4 = parse_query(r#"from("logs").insert("k3", {msg: "reborn"})"#).unwrap();
    execute_query(&engine, &q4).unwrap();
    assert!(
        engine.list_streams().contains(&"logs".to_string()),
        "Stream must reappear after re-insert"
    );

    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn test_empty_retains_stream_in_set() {
    use liven::executor::execute_query;
    use liven::parser::parse_query;
    use liven::storage::StorageEngine;
    use liven::types::DataValue;

    let path = std::env::temp_dir().join(format!(
        "liven_empty_stream_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&path);
    let engine = StorageEngine::new(&path, 1024 * 1024).unwrap();

    engine
        .append("events", "e1", DataValue::Int(1), false)
        .unwrap();
    engine
        .append("events", "e2", DataValue::Int(2), false)
        .unwrap();

    let q = parse_query(r#"from("events").empty()"#).unwrap();
    execute_query(&engine, &q).unwrap();

    // Stream must still be in streams_set after empty()
    assert!(
        engine.list_streams().contains(&"events".to_string()),
        "Empty must retain stream in list"
    );

    // Keys must be gone
    assert!(
        engine.list_keys("events").is_empty(),
        "All keys must be removed after empty()"
    );

    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn test_drop_frees_stream_slot() {
    use liven::executor::execute_query;
    use liven::parser::parse_query;
    use liven::storage::StorageEngine;
    use liven::types::DataValue;

    let path = std::env::temp_dir().join(format!(
        "liven_drop_slot_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&path);
    let mut engine = StorageEngine::new(&path, 1024 * 1024).unwrap();
    engine.set_max_streams(2);

    // Fill stream slots
    engine.append("s1", "k1", DataValue::Int(1), false).unwrap();
    engine.append("s2", "k1", DataValue::Int(1), false).unwrap();

    // Third stream must fail — at capacity
    let err = engine.append("s3", "k1", DataValue::Int(1), false);
    assert!(err.is_err(), "Should be at stream capacity");

    // Drop s1
    let q = parse_query(r#"drop("s1")"#).unwrap();
    execute_query(&engine, &q).unwrap();

    // Now s3 must succeed — slot was freed
    engine
        .append("s3", "k1", DataValue::Int(1), false)
        .expect("Stream slot should be available after drop");

    let _ = std::fs::remove_dir_all(&path);
}

// ── Deletion Fix 4: Null value record ──

#[test]
fn test_null_value_record_not_treated_as_tombstone() {
    use liven::executor::execute_query;
    use liven::parser::parse_query;
    use liven::storage::StorageEngine;
    use liven::types::DataValue;

    let path = std::env::temp_dir().join(format!(
        "liven_null_value_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&path);
    let engine = StorageEngine::new(&path, 1024 * 1024).unwrap();

    // Insert a record with an explicit null value (active record, not tombstone)
    engine.append("data", "k1", DataValue::Null, false).unwrap();
    engine
        .append("data", "k2", DataValue::String("hello".to_string()), false)
        .unwrap();

    // Both records must be returned by a normal scan
    let q = parse_query(r#"from("data")"#).unwrap();
    let results = execute_query(&engine, &q).unwrap();
    assert_eq!(
        results.len(),
        2,
        "Null-value active record must appear in results"
    );

    // count() must return 2
    let q2 = parse_query(r#"from("data") | count()"#).unwrap();
    let results2 = execute_query(&engine, &q2).unwrap();
    assert_eq!(results2[0].value, DataValue::UInt(2));

    // Point lookup must find the null-value record
    let found = engine.get("data", "k1").unwrap();
    assert!(
        found.is_some(),
        "Null-value active record must be findable via get()"
    );
    assert_eq!(found.unwrap().value, DataValue::Null);

    let _ = std::fs::remove_dir_all(&path);
}

// ── Deletion Fix 5: write_lock serialization ──

#[test]
fn test_delete_key_write_lock_serialization() {
    use liven::executor::execute_query;
    use liven::parser::parse_query;
    use liven::storage::StorageEngine;
    use liven::types::DataValue;
    use std::sync::Arc;

    let path = std::env::temp_dir().join(format!(
        "liven_delete_race_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&path);
    let engine = Arc::new(StorageEngine::new(&path, 1024 * 1024).unwrap());

    // Insert initial record
    engine
        .append(
            "test",
            "k1",
            DataValue::String("initial".to_string()),
            false,
        )
        .unwrap();

    // Spawn a thread that repeatedly upserts k1
    let engine_write = engine.clone();
    let write_handle = std::thread::spawn(move || {
        for i in 0..100 {
            let _ = engine_write.append(
                "test",
                "k1",
                DataValue::String(format!("write_{}", i)),
                false,
            );
            std::thread::yield_now();
        }
    });

    // Concurrently delete k1 repeatedly
    let engine_delete = engine.clone();
    let delete_handle = std::thread::spawn(move || {
        for _ in 0..100 {
            let q = parse_query(r#"from("test").delete("k1")"#).unwrap();
            let _ = execute_query(&engine_delete, &q);
            std::thread::yield_now();
        }
    });

    write_handle.join().unwrap();
    delete_handle.join().unwrap();

    // Database must be in a consistent state — no panic, no corruption
    // The key may or may not exist depending on last operation
    let final_state = engine.get("test", "k1").unwrap();
    // Just verify no panic and the result is coherent
    let in_skipmap = engine.skipmap.contains_key("test:k1");
    match final_state {
        Some(_) => assert!(in_skipmap, "If get() returns Some, key must be in SkipMap"),
        None => assert!(
            !in_skipmap,
            "If get() returns None, key must not be in SkipMap"
        ),
    }

    let _ = std::fs::remove_dir_all(&path);
}

// ── New filter operator tests ──

#[test]
fn test_contains_operator_execution() {
    use liven::executor::compare_values;

    let haystack = DataValue::String("authentication failed".to_string());
    let needle = DataValue::String("failed".to_string());
    assert!(compare_values(&haystack, Op::Contains, &needle));

    let no_match = DataValue::String("success".to_string());
    assert!(!compare_values(&haystack, Op::Contains, &no_match));

    // non-string fallback
    assert!(!compare_values(&DataValue::Int(42), Op::Contains, &needle));
}

#[test]
fn test_endswith_operator_execution() {
    use liven::executor::compare_values;

    let val = DataValue::String("data.log".to_string());
    let suffix = DataValue::String(".log".to_string());
    assert!(compare_values(&val, Op::EndsWith, &suffix));

    let no_match = DataValue::String(".txt".to_string());
    assert!(!compare_values(&val, Op::EndsWith, &no_match));

    // non-string fallback
    assert!(!compare_values(
        &DataValue::Bool(true),
        Op::EndsWith,
        &suffix
    ));
}

#[test]
fn test_between_operator_execution() {
    use liven::executor::compare_values;

    let val = DataValue::Int(250);
    let bounds = DataValue::Array(vec![
        DataValue::Int(100),
        DataValue::Float(ordered_float::OrderedFloat(500.0)),
    ]);
    assert!(compare_values(&val, Op::Between, &bounds));

    // below lower bound
    let low_val = DataValue::Int(50);
    assert!(!compare_values(&low_val, Op::Between, &bounds));

    // above upper bound
    let high_val = DataValue::UInt(600);
    assert!(!compare_values(&high_val, Op::Between, &bounds));

    // non-numeric value
    let string_val = DataValue::String("hello".to_string());
    assert!(!compare_values(&string_val, Op::Between, &bounds));

    // bounds array not length 2
    let bad_bounds = DataValue::Array(vec![DataValue::Int(1)]);
    assert!(!compare_values(&val, Op::Between, &bad_bounds));
}

// ── Not filter expression ──

#[test]
fn test_not_filter_execution() {
    let temp_dir = std::env::temp_dir().join(format!(
        "liven_test_not_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let engine = StorageEngine::new(&temp_dir, 1024 * 1024).unwrap();

    engine
        .append(
            "users",
            "u1",
            DataValue::String(r#"{"status": "active"}"#.to_string()),
            false,
        )
        .unwrap();
    engine
        .append(
            "users",
            "u2",
            DataValue::String(r#"{"status": "inactive"}"#.to_string()),
            false,
        )
        .unwrap();
    engine
        .append(
            "users",
            "u3",
            DataValue::String(r#"{"status": "active"}"#.to_string()),
            false,
        )
        .unwrap();

    // not status == "inactive" should return 2 records (u1, u3)
    let results = execute_query(
        &engine,
        &parse_query(r#"from("users") | filter(not status == "inactive")"#).unwrap(),
    )
    .unwrap();
    assert_eq!(results.len(), 2);

    // double negative: not not (status == "inactive") should return 1 record (u2)
    let results = execute_query(
        &engine,
        &parse_query(r#"from("users") | filter(not not status == "inactive")"#).unwrap(),
    )
    .unwrap();
    assert_eq!(results.len(), 1);

    let _ = std::fs::remove_dir_all(&temp_dir);
}

// ── Distinct stage ──

#[test]
fn test_distinct_execution() {
    let temp_dir = std::env::temp_dir().join(format!(
        "liven_test_distinct_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let engine = StorageEngine::new(&temp_dir, 1024 * 1024).unwrap();

    engine
        .append(
            "events",
            "e1",
            DataValue::String(r#"{"type": "click", "user": "alice"}"#.to_string()),
            false,
        )
        .unwrap();
    engine
        .append(
            "events",
            "e2",
            DataValue::String(r#"{"type": "click", "user": "bob"}"#.to_string()),
            false,
        )
        .unwrap();
    engine
        .append(
            "events",
            "e3",
            DataValue::String(r#"{"type": "pageview", "user": "alice"}"#.to_string()),
            false,
        )
        .unwrap();
    engine
        .append(
            "events",
            "e4",
            DataValue::String(r#"{"type": "pageview", "user": "bob"}"#.to_string()),
            false,
        )
        .unwrap();

    // distinct(type) should return 2 records (first click, first pageview)
    let results = execute_query(
        &engine,
        &parse_query(r#"from("events") | distinct(type)"#).unwrap(),
    )
    .unwrap();
    assert_eq!(results.len(), 2);

    // distinct on a field that is the same for all records should return 1
    let results = execute_query(
        &engine,
        &parse_query(r#"from("events") | filter(type == "click") | distinct(type)"#).unwrap(),
    )
    .unwrap();
    assert_eq!(results.len(), 1);

    // distinct on stream name should deduplicate by stream (all from "events", so 1)
    let results = execute_query(
        &engine,
        &parse_query(r#"from("events") | distinct(stream)"#).unwrap(),
    )
    .unwrap();
    assert_eq!(results.len(), 1);

    let _ = std::fs::remove_dir_all(&temp_dir);
}

// ── Explain ──

#[test]
fn test_explain_execution() {
    let temp_dir = std::env::temp_dir().join(format!(
        "liven_test_explain_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let engine = StorageEngine::new(&temp_dir, 1024 * 1024).unwrap();

    engine
        .append(
            "orders",
            "k1",
            DataValue::String(r#"{"amount": 100}"#.to_string()),
            false,
        )
        .unwrap();
    engine
        .append(
            "orders",
            "k2",
            DataValue::String(r#"{"amount": 200}"#.to_string()),
            false,
        )
        .unwrap();

    // explain a pipeline query (use bare identifiers to avoid escape issues in inner query)
    let results = execute_query(
        &engine,
        &parse_query(r#"explain("from(orders) | filter(amount > 100)")"#).unwrap(),
    )
    .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].stream_name, "explain");

    // Parse the JSON plan
    if let DataValue::String(ref plan_str) = results[0].value {
        let plan: serde_json::Value =
            serde_json::from_str(plan_str).expect("Explain output should be valid JSON");
        let steps = plan["steps"]
            .as_array()
            .expect("Plan should have steps array");
        // step 0: scan, step 1: from, step 2: filter
        assert!(steps.len() >= 3, "Should have scan + from + filter steps");
        assert_eq!(steps[0]["stage"], "scan");
        assert_eq!(steps[1]["stage"], "from");
        assert_eq!(steps[2]["stage"], "filter");
    } else {
        panic!("Explain output should be DataValue::String");
    }

    // explain list streams
    let results = execute_query(&engine, &parse_query(r#"explain("streams")"#).unwrap()).unwrap();
    assert_eq!(results.len(), 1);
    if let DataValue::String(ref plan_str) = results[0].value {
        let plan: serde_json::Value = serde_json::from_str(plan_str).unwrap();
        assert_eq!(plan["steps"][0]["stage"], "streams_set");
    }

    // explain an insert
    let results = execute_query(
        &engine,
        &parse_query(r#"explain("from(orders).insert(k3, {amount: 300})")"#).unwrap(),
    )
    .unwrap();
    assert_eq!(results.len(), 1);
    if let DataValue::String(ref plan_str) = results[0].value {
        let plan: serde_json::Value = serde_json::from_str(plan_str).unwrap();
        assert_eq!(plan["steps"][0]["stage"], "existence_check");
        assert_eq!(plan["steps"][1]["stage"], "append");
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}
