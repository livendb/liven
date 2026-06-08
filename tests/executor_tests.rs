use liven::executor::{
    apply_pipeline_stages_to_vec, compare_values, execute_query, extract_field, project_record,
};
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
