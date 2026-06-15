// Resource Management Tests
// Tests for resource limits, stability, and input validation

use liven::executor::execute_query;
use liven::storage::StorageEngine;
use liven::types::Query;
use std::sync::Arc;
use tempfile::tempdir;

#[test]
fn test_scan_results_limit_enforced() {
    let temp_dir = tempdir().unwrap();
    let data_dir = temp_dir.path();

    // Create engine with low scan limit for testing
    let mut engine = StorageEngine::new(data_dir, 16 * 1024 * 1024).unwrap();
    engine.set_max_scan_results(5); // Very low limit for testing

    // Insert more records than the limit
    for i in 0..10 {
        let _ = engine.append(
            "test_stream",
            &format!("key_{}", i),
            liven::types::DataValue::String(format!("value_{}", i)),
            false,
        );
    }

    // Try to scan all records - should hit the limit
    let result = engine.scan_historical();

    // Should fail with specific error message
    assert!(result.is_err(), "Should fail when exceeding scan limit");

    if let Err(e) = result {
        let error_msg = e.to_string();
        assert!(error_msg.contains("Scan result limit reached (5 records)"));
        assert!(error_msg.contains("Refine your query"));
        assert!(error_msg.contains("max_scan_results"));
    }

    // Limited scan should work
    let result = engine.scan_historical_limited(3);
    assert!(result.is_ok(), "Limited scan should work within limit");
    assert_eq!(result.unwrap().len(), 3, "Should return exactly 3 records");
}

#[test]
fn test_scan_limit_not_applied_to_index_lookups() {
    let temp_dir = tempdir().unwrap();
    let data_dir = temp_dir.path();

    // Create engine with very low scan limit
    let mut engine = StorageEngine::new(data_dir, 16 * 1024 * 1024).unwrap();
    engine.set_max_scan_results(1); // Extremely low limit

    // Insert a record
    let _ = engine.append(
        "test_stream",
        "specific_key",
        liven::types::DataValue::String("test_value".to_string()),
        false,
    );

    // Key-based lookup should work despite low scan limit
    let result = engine.get("test_stream", "specific_key");
    assert!(
        result.is_ok(),
        "Key-based lookup should not be affected by scan limit"
    );
    assert!(result.unwrap().is_some(), "Should find the record");
}

#[test]
fn test_key_validation_on_all_write_operations() {
    let temp_dir = tempdir().unwrap();
    let data_dir = temp_dir.path();
    let engine = Arc::new(StorageEngine::new(data_dir, 16 * 1024 * 1024).unwrap());

    let long_key = "a".repeat(33); // 33 bytes - too long

    // Test Insert
    let insert_query = Query::Insert {
        stream_name: "test_stream".to_string(),
        key: long_key.clone(),
        value: serde_json::json!({"test": "data"}),
    };

    let result = execute_query(&engine, &insert_query);
    assert!(result.is_err(), "Insert should validate key length");

    // Test InsertBatch
    let batch = vec![(long_key.clone(), serde_json::json!({"test": "data"}))];

    let batch_query = Query::InsertBatch {
        stream_name: "test_stream".to_string(),
        batch: batch,
    };

    let result = execute_query(&engine, &batch_query);
    assert!(result.is_err(), "InsertBatch should validate key length");

    // Test Upsert
    let upsert_query = Query::Upsert {
        stream_name: "test_stream".to_string(),
        key: long_key.clone(),
        value: serde_json::json!({"test": "data"}),
    };

    let result = execute_query(&engine, &upsert_query);
    assert!(result.is_err(), "Upsert should validate key length");

    // Test UpsertBatch
    let batch = vec![(long_key.clone(), serde_json::json!({"test": "data"}))];

    let batch_query = Query::UpsertBatch {
        stream_name: "test_stream".to_string(),
        batch: batch,
    };

    let result = execute_query(&engine, &batch_query);
    assert!(result.is_err(), "UpsertBatch should validate key length");

    // Test Update
    let update_query = Query::Update {
        stream_name: "test_stream".to_string(),
        key: long_key.clone(),
        value: serde_json::json!({"test": "data"}),
    };

    let result = execute_query(&engine, &update_query);
    assert!(result.is_err(), "Update should validate key length");

    // Test DeleteKey
    let delete_query = Query::DeleteKey {
        stream_name: "test_stream".to_string(),
        key: long_key,
    };

    let result = execute_query(&engine, &delete_query);
    assert!(result.is_err(), "DeleteKey should validate key length");
}

#[test]
fn test_error_message_contains_key_and_limit() {
    let temp_dir = tempdir().unwrap();
    let data_dir = temp_dir.path();
    let engine = Arc::new(StorageEngine::new(data_dir, 16 * 1024 * 1024).unwrap());

    let long_key = "this_is_a_very_long_key_that_exceeds_thirty_two_bytes"; // 50 bytes

    let query = Query::Insert {
        stream_name: "test_stream".to_string(),
        key: long_key.to_string(),
        value: serde_json::json!({"test": "data"}),
    };

    let result = execute_query(&engine, &query);

    assert!(result.is_err());
    if let Err(e) = result {
        let error_msg = e.to_string();

        // Should contain helpful guidance
        assert!(error_msg.contains("Shorten the key"));
        assert!(error_msg.contains("hash of the original value"));
    }
}

#[test]
fn test_auto_ram_detection_in_config() {
    // This test verifies the auto-detection logic exists
    // Note: Actual detection may not work in test environment

    // Test the calculation formula directly
    assert_eq!(liven::sysinfo::calculate_auto_budget(128), 32); // Min clamp
    assert_eq!(liven::sysinfo::calculate_auto_budget(256), 64); // 25%
    assert_eq!(liven::sysinfo::calculate_auto_budget(1024), 256); // 25%
    assert_eq!(liven::sysinfo::calculate_auto_budget(4096), 1024); // 25%
    assert_eq!(liven::sysinfo::calculate_auto_budget(16384), 4096); // Max clamp

    // Test key capacity estimation
    let capacity_16mb = liven::sysinfo::estimate_key_capacity(16);
    assert!(capacity_16mb > 190_000, "16MB should hold ~190K keys");

    let capacity_256mb = liven::sysinfo::estimate_key_capacity(256);
    assert!(capacity_256mb > 3_000_000, "256MB should hold ~3M keys");
}
