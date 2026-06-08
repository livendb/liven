use liven::storage::{StorageEngine, deserialize_payload, serialize_payload_into};
use liven::types::DataValue;
use std::fs;

#[test]
fn test_payload_serialization() {
    let stream = "logs";
    let key = "user_123";
    let val = DataValue::String("login_failed".to_string());

    let mut payload = Vec::new();
    serialize_payload_into(stream, key, &val, &mut payload);
    let (s_out, k_out, v_out) = deserialize_payload(&payload).unwrap();

    assert_eq!(s_out, stream);
    assert_eq!(k_out, key);
    assert_eq!(v_out, val);
}

#[test]
fn test_storage_engine_lifecycle() {
    let path = format!(
        "./data_liven_test_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );

    let _ = fs::remove_dir_all(&path).unwrap_err();

    // 1. Initialize
    let engine = StorageEngine::new(&path, 1024 * 1024).unwrap();

    // 2. Insert records
    let _rec1 = engine
        .append(
            "test_stream",
            "k1",
            DataValue::String("v1".to_string()),
            false,
        )
        .unwrap();

    let _rec2 = engine
        .append("test_stream", "k2", DataValue::Int(100), false)
        .unwrap();

    assert_eq!(engine.skipmap.len(), 2);

    // 3. Point lookup
    let get_r1 = engine.get("test_stream", "k1").unwrap().unwrap();
    assert_eq!(get_r1.value, DataValue::String("v1".to_string()));

    let get_r2 = engine.get("test_stream", "k2").unwrap().unwrap();
    assert_eq!(get_r2.value, DataValue::Int(100));

    // 4. Batch append
    let batch = vec![
        (
            "test_stream".to_string(),
            "k3".to_string(),
            DataValue::Bool(true),
        ),
        (
            "test_stream".to_string(),
            "k4".to_string(),
            DataValue::UInt(999),
        ),
    ];
    engine.append_batch(batch).unwrap();
    assert_eq!(engine.skipmap.len(), 4);

    // 5. Restart engine to verify recovery
    drop(engine);

    let engine_recovered = StorageEngine::new(&path, 1024 * 1024).unwrap();
    assert_eq!(engine_recovered.skipmap.len(), 4);

    let recovered_r1 = engine_recovered.get("test_stream", "k1").unwrap().unwrap();
    assert_eq!(recovered_r1.value, DataValue::String("v1".to_string()));

    let recovered_r4 = engine_recovered.get("test_stream", "k4").unwrap().unwrap();
    assert_eq!(recovered_r4.value, DataValue::UInt(999));

    // 6. Delete a key via Tombstone
    engine_recovered
        .append("test_stream", "k1", DataValue::Null, true)
        .unwrap();
    assert_eq!(engine_recovered.skipmap.len(), 3);
    assert!(engine_recovered.get("test_stream", "k1").unwrap().is_none());

    // 7. Verify list_streams and list_keys
    let streams = engine_recovered.list_streams();
    assert_eq!(streams, vec!["test_stream".to_string()]);

    let keys = engine_recovered.list_keys("test_stream");
    assert!(keys.contains(&"k2".to_string()));
    assert!(keys.contains(&"k3".to_string()));
    assert!(keys.contains(&"k4".to_string()));
    assert!(!keys.contains(&"k1".to_string()));

    // 8. Clean up
    let _ = fs::remove_dir_all(&path);
}

#[test]
fn test_compaction() {
    let path = format!(
        "./data_liven_test_compaction_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let _ = fs::remove_dir_all(&path);

    // Set very small segment size (120 bytes) to force rollovers!
    let engine = StorageEngine::new(&path, 120).unwrap();

    // Write several records to force rollovers and generate inactive segments
    engine
        .append("s1", "k1", DataValue::String("a".to_string()), false)
        .unwrap();
    engine
        .append("s1", "k1", DataValue::String("b".to_string()), false)
        .unwrap();
    engine
        .append("s1", "k2", DataValue::String("c".to_string()), false)
        .unwrap();
    engine
        .append("s1", "k2", DataValue::String("d".to_string()), false)
        .unwrap();
    engine
        .append("s1", "k3", DataValue::String("e".to_string()), false)
        .unwrap();
    engine
        .append("s1", "k3", DataValue::String("f".to_string()), false)
        .unwrap();
    engine
        .append("s1", "k4", DataValue::String("g".to_string()), false)
        .unwrap();
    engine
        .append("s1", "k4", DataValue::String("h".to_string()), false)
        .unwrap();

    let (_ram_usage, _size_before, segments_before, _streams) = engine.metrics().unwrap();

    assert!(segments_before > 1);

    // Compact
    engine.compact().unwrap();

    // Check that points-lookup still works flawlessly
    let r1 = engine.get("s1", "k1").unwrap().unwrap();
    assert_eq!(r1.value, DataValue::String("b".to_string()));

    let r2 = engine.get("s1", "k2").unwrap().unwrap();
    assert_eq!(r2.value, DataValue::String("d".to_string()));

    let r3 = engine.get("s1", "k3").unwrap().unwrap();
    assert_eq!(r3.value, DataValue::String("f".to_string()));

    let r4 = engine.get("s1", "k4").unwrap().unwrap();
    assert_eq!(r4.value, DataValue::String("h".to_string()));

    let (_ram_usage_, _size_after, segments_after, _streams_after) = engine.metrics().unwrap();
    // Since we consolidated everything and removed inactive ones, we should have less segments
    assert!(segments_after < segments_before);

    // Clean up
    let _ = fs::remove_dir_all(&path);
}

#[test]
fn test_stream_limits() {
    let path = std::env::temp_dir().join(format!(
        "liven_test_streams_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = fs::remove_dir_all(&path);

    // Configure engine with limit of 2 streams max
    let mut engine = StorageEngine::new(&path, 1024 * 1024).unwrap();
    engine.set_max_streams(2);

    // 1st stream -> should succeed
    engine.append("s1", "k1", DataValue::Int(1), false).unwrap();
    // 2nd stream -> should succeed
    engine.append("s2", "k1", DataValue::Int(1), false).unwrap();
    // 3rd stream -> should fail
    let err = engine.append("s3", "k1", DataValue::Int(1), false);
    assert!(err.is_err());
    assert_eq!(
        err.unwrap_err(),
        "Maximum concurrent streams limit exceeded"
    );

    // Clean up
    let _ = fs::remove_dir_all(&path);
}

#[test]
fn test_index_ram_limits() {
    let path = std::env::temp_dir().join(format!(
        "liven_test_ram_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = fs::remove_dir_all(&path);

    let mut engine = StorageEngine::new(&path, 1024 * 1024).unwrap();
    // Override RAM limits to be very small, e.g. 150 bytes
    // Each entry compound key: "s1:k1" is 5 chars. Node overhead is 5 + 64 = 69 bytes.
    // Two keys would be 138 bytes. Three keys would be 207 bytes (exceeds 150 bytes limit).
    engine.set_max_index_ram_bytes(150);

    // 1st key -> succeeds
    engine.append("s1", "k1", DataValue::Int(1), false).unwrap();
    // 2nd key -> succeeds
    engine.append("s1", "k2", DataValue::Int(1), false).unwrap();
    // 3rd key -> should fail due to Index RAM limit
    let err = engine.append("s1", "k3", DataValue::Int(1), false);
    assert!(err.is_err());
    assert_eq!(err.unwrap_err(), "Index RAM limit exceeded");

    // Tombstone of an existing key should succeed
    engine.append("s1", "k1", DataValue::Null, true).unwrap();

    // Since s1:k1 was removed, index size is reduced by 69 bytes. We can now insert s1:k3!
    engine.append("s1", "k3", DataValue::Int(1), false).unwrap();

    // Clean up
    let _ = fs::remove_dir_all(&path);
}

#[test]
fn test_file_descriptor_limits() {
    let path = std::env::temp_dir().join(format!(
        "liven_test_fds_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = fs::remove_dir_all(&path);

    // Configure with FD limit of 6 (which means max_fds.saturating_sub(5) is 1 cached handle max!)
    let mut engine = StorageEngine::new(&path, 10).unwrap(); // very small segment to force rolling
    engine.set_max_fds(6);

    // Append to trigger multiple rolling segments
    engine
        .append("s1", "k1", DataValue::String("a".to_string()), false)
        .unwrap(); // segment 1
    std::thread::sleep(std::time::Duration::from_millis(150));
    engine
        .append("s1", "k2", DataValue::String("b".to_string()), false)
        .unwrap(); // segment 2
    std::thread::sleep(std::time::Duration::from_millis(150));
    engine
        .append("s1", "k3", DataValue::String("c".to_string()), false)
        .unwrap(); // segment 3

    // Let's force load a file handle
    let ptr1 = engine
        .skipmap
        .get(&"s1:k1".to_string())
        .map(|e| *e.value())
        .unwrap();
    let ptr2 = engine
        .skipmap
        .get(&"s1:k2".to_string())
        .map(|e| *e.value())
        .unwrap();

    // Accessing ptr1 will load segment into cache
    let _ = engine.read_record(ptr1).unwrap();
    assert!(engine.active_handles_count() <= 1);

    // Accessing ptr2 will load another segment into cache and evict the previous one
    let _ = engine.read_record(ptr2).unwrap();
    assert!(engine.active_handles_count() <= 1);

    // Clean up
    let _ = fs::remove_dir_all(&path);
}
