use liven::storage::StorageEngine;
use liven::types::{DataValue, Record};
use std::fs;

#[test]
fn test_tombstone_compaction_lifecycle() {
    // 1. Generate guaranteed isolated directory path using time + thread ID to prevent CI collision
    let thread_id = std::thread::current().id();
    let path = std::env::temp_dir().join(format!(
        "liven_test_tombstone_compaction_lifecycle_{:?}_{}",
        thread_id,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    // Ensure clean state environment
    if path.exists() {
        fs::remove_dir_all(&path).unwrap();
    }
    fs::create_dir_all(&path).unwrap();

    // Initialize StorageEngine with a tight segment limit (150 bytes)
    // Wrapped in a block or scope if you need to test structural dropping later
    let engine = StorageEngine::new(&path, 150).unwrap();

    // 2. Append key "k1" and value "v1"
    engine
        .append(
            "test_stream",
            "k1",
            DataValue::String("v1".to_string()),
            false,
        )
        .unwrap();

    // 3. Verify get("test_stream", "k1") returns "v1"
    let r_init = engine
        .get("test_stream", "k1")
        .unwrap()
        .expect("Key k1 should exist initially");
    assert_eq!(r_init.value, DataValue::String("v1".to_string()));
    assert!(engine.skipmap.contains_key(&"test_stream:k1".to_string()));

    // 4. Append a tombstone for "k1" (marking it as deleted)
    engine
        .append("test_stream", "k1", DataValue::Null, true)
        .unwrap();

    // 5. Verify get("test_stream", "k1") immediately returns None (index-purged)
    assert!(engine.get("test_stream", "k1").unwrap().is_none());
    assert!(!engine.skipmap.contains_key(&"test_stream:k1".to_string()));

    // 6. Verify that scan_historical still contains both original and tombstone records
    let historical_1 = engine.scan_historical().unwrap();
    let k1_records: Vec<&Record> = historical_1
        .iter()
        .filter(|r| r.stream_name == "test_stream" && r.key.as_str() == "k1")
        .collect();
    assert_eq!(
        k1_records.len(),
        2,
        "Both regular record and tombstone must exist in active segment"
    );

    let normal_record_exists = k1_records.iter().any(|r| r.flags == 0x01);
    let tombstone_record_exists = k1_records.iter().any(|r| r.flags == 0x02);
    assert!(normal_record_exists, "Expected original record missing");
    assert!(tombstone_record_exists, "Expected tombstone marker missing");

    // 7. Compacting while the segment is active should leave the tombstone on disk
    engine.compact().unwrap();
    let historical_2 = engine.scan_historical().unwrap();
    let k1_records_after_active_compaction: Vec<&Record> = historical_2
        .iter()
        .filter(|r| r.stream_name == "test_stream" && r.key.as_str() == "k1")
        .collect();
    assert_eq!(
        k1_records_after_active_compaction.len(),
        2,
        "Active segment records must not be purged"
    );

    // 8. Roll over the active segment by forcing a payload size overflow
    engine
        .append(
            "test_stream",
            "k2",
            DataValue::String("x".repeat(100)),
            false,
        )
        .unwrap();

    // Verify that rollover occurred smoothly
    let (_ram_usage, _size, segment_count, _streams) = engine.metrics().unwrap();
    assert!(
        segment_count > 1,
        "Rollover should have generated multiple segment files"
    );

    // 9. Call compaction to consolidate historical inactive segments
    engine.compact().unwrap();

    // 10. Verify that scan_historical NO LONGER contains k1 or its tombstone
    let historical_3 = engine.scan_historical().unwrap();
    let k1_records_final: Vec<&Record> = historical_3
        .iter()
        .filter(|r| r.stream_name == "test_stream" && r.key.as_str() == "k1")
        .collect();
    assert!(
        k1_records_final.is_empty(),
        "Compaction failed to safely evict orphaned dead tombstones"
    );

    // 11. Explicit Clean up
    let _ = fs::remove_dir_all(&path);
}
