// Data Safety Tests
// Tests for critical data safety, consistency, and correctness guarantees

use liven::executor::execute_query;
use liven::storage::StorageEngine;
use liven::types::Query;
use std::sync::Arc;
use tempfile::tempdir;

#[test]
fn test_flusher_shutdown_with_pending_writes() {
    let temp_dir = tempdir().unwrap();
    let data_dir = temp_dir.path();

    // Create engine and append data
    let engine = StorageEngine::new(data_dir, 16 * 1024 * 1024).unwrap();

    // Append multiple records to ensure flusher has work
    for i in 0..10 {
        let _ = engine.append(
            "test_stream",
            &format!("key_{}", i),
            liven::types::DataValue::String(format!("value_{}", i)),
            false,
        );
    }

    // Drop engine - this should trigger clean shutdown
    drop(engine);

    // Reopen engine and verify data persisted
    let engine2 = StorageEngine::new(data_dir, 16 * 1024 * 1024).unwrap();
    let records = engine2.scan_historical().unwrap();

    // Should have all 10 records (no data loss)
    assert_eq!(
        records.len(),
        10,
        "Data should persist after clean shutdown"
    );

    // Verify content
    for i in 0..10 {
        let key = format!("key_{}", i);
        let record = engine2.get("test_stream", &key).unwrap();
        assert!(record.is_some(), "Record {} should exist", i);
    }
}

#[test]
fn test_write_lock_prevents_duplicate_keys() {
    let temp_dir = tempdir().unwrap();
    let data_dir = temp_dir.path();
    let engine = Arc::new(StorageEngine::new(data_dir, 16 * 1024 * 1024).unwrap());

    // First insert should succeed
    let query1 = Query::Insert {
        stream_name: "test_stream".to_string(),
        key: "test_key".to_string(),
        value: serde_json::json!({"field": "value1"}),
    };

    let result1 = execute_query(&engine, &query1);
    assert!(result1.is_ok(), "First insert should succeed");
    assert_eq!(result1.unwrap().len(), 1);

    // Second insert with same key should fail
    let query2 = Query::Insert {
        stream_name: "test_stream".to_string(),
        key: "test_key".to_string(),
        value: serde_json::json!({"field": "value2"}),
    };

    let result2 = execute_query(&engine, &query2);
    assert!(result2.is_err(), "Second insert should fail");

    // Verify error message contains guidance
    if let Err(e) = result2 {
        let error_msg = e.to_string();
        assert!(error_msg.contains("already exists"));
        assert!(error_msg.contains("upsert"));
        assert!(error_msg.contains("update"));
    }

    // Verify only one record exists
    let records = engine.get("test_stream", "test_key").unwrap();
    assert!(records.is_some());
}

#[test]
fn test_upsert_tombstones_old_records() {
    let temp_dir = tempdir().unwrap();
    let data_dir = temp_dir.path();
    let engine = Arc::new(StorageEngine::new(data_dir, 16 * 1024 * 1024).unwrap());

    // Insert initial record
    let insert_query = Query::Insert {
        stream_name: "test_stream".to_string(),
        key: "test_key".to_string(),
        value: serde_json::json!({"version": 1}),
    };

    execute_query(&engine, &insert_query).unwrap();

    // Upsert should tombstone old record and create new one
    let upsert_query = Query::Upsert {
        stream_name: "test_stream".to_string(),
        key: "test_key".to_string(),
        value: serde_json::json!({"version": 2}),
    };

    execute_query(&engine, &upsert_query).unwrap();

    // Should have exactly one live record (the new one)
    let records = engine.get("test_stream", "test_key").unwrap();
    assert!(records.is_some());

    let record = records.unwrap();
    if let liven::types::DataValue::String(json_str) = &record.value {
        let value: serde_json::Value = serde_json::from_str(json_str).unwrap();
        assert_eq!(value["version"], 2, "Should have the new version");
    } else {
        panic!("Expected string JSON value");
    }
}

#[test]
fn test_update_tombstones_old_records() {
    let temp_dir = tempdir().unwrap();
    let data_dir = temp_dir.path();
    let engine = Arc::new(StorageEngine::new(data_dir, 16 * 1024 * 1024).unwrap());

    // Insert initial record
    let insert_query = Query::Insert {
        stream_name: "test_stream".to_string(),
        key: "test_key".to_string(),
        value: serde_json::json!({"name": "original", "count": 1}),
    };

    execute_query(&engine, &insert_query).unwrap();

    // Update should tombstone old record and merge
    let update_query = Query::Update {
        stream_name: "test_stream".to_string(),
        key: "test_key".to_string(),
        value: serde_json::json!({"count": 2, "new_field": "added"}),
    };

    execute_query(&engine, &update_query).unwrap();

    // Should have exactly one live record (the merged one)
    let records = engine.get("test_stream", "test_key").unwrap();
    assert!(records.is_some());

    let record = records.unwrap();
    if let liven::types::DataValue::String(json_str) = &record.value {
        let value: serde_json::Value = serde_json::from_str(json_str).unwrap();
        assert_eq!(value["name"], "original", "Should preserve original field");
        assert_eq!(value["count"], 2, "Should update count");
        assert_eq!(value["new_field"], "added", "Should add new field");
    } else {
        panic!("Expected string JSON value");
    }
}

#[test]
fn test_key_length_validation() {
    let temp_dir = tempdir().unwrap();
    let data_dir = temp_dir.path();
    let engine = Arc::new(StorageEngine::new(data_dir, 16 * 1024 * 1024).unwrap());

    // 33-byte key should fail
    let long_key = "a".repeat(33);
    let query = Query::Insert {
        stream_name: "test_stream".to_string(),
        key: long_key,
        value: serde_json::json!({"test": "data"}),
    };

    let result = execute_query(&engine, &query);
    assert!(result.is_err(), "33-byte key should be rejected");

    if let Err(e) = result {
        let error_msg = e.to_string();
        assert!(error_msg.contains("33 bytes"));
        assert!(error_msg.contains("maximum is 32 bytes"));
        assert!(error_msg.contains("Shorten the key"));
    }

    // 32-byte key should succeed
    let valid_key = "a".repeat(32);
    let query = Query::Insert {
        stream_name: "test_stream".to_string(),
        key: valid_key,
        value: serde_json::json!({"test": "data"}),
    };

    let result = execute_query(&engine, &query);
    assert!(result.is_ok(), "32-byte key should be accepted");
}
