#[cfg(feature = "server")]
#[tokio::test]
async fn test_object_storage_roundtrip() {
    use liven::storage::StorageEngine;
    use liven::types::{DataValue, parse_json_to_datavalue};
    use std::collections::BTreeMap;

    let test_dir = std::env::temp_dir().join(format!(
        "liven_object_test_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&test_dir);
    std::fs::create_dir_all(&test_dir).unwrap();

    // Create storage engine
    let engine = StorageEngine::new(&test_dir, 1024 * 1024).unwrap();

    // Test 1: Store a simple object
    let mut obj1 = BTreeMap::new();
    obj1.insert("name".to_string(), DataValue::String("test".to_string()));
    obj1.insert("value".to_string(), DataValue::Int(42));

    engine
        .append("test_stream", "key1", DataValue::Object(obj1), false)
        .unwrap();

    // Test 2: Store a nested object
    let json_str = r#"{"user": {"name": "alice", "age": 30}, "active": true}"#;
    let nested_obj = parse_json_to_datavalue(json_str);

    engine
        .append("test_stream", "key2", nested_obj, false)
        .unwrap();

    // Test 3: Retrieve and verify objects using get method
    let record1 = engine.get("test_stream", "key1").unwrap();
    let record2 = engine.get("test_stream", "key2").unwrap();

    assert!(record1.is_some());
    assert!(record2.is_some());

    // Verify first record
    if let DataValue::Object(obj) = &record1.unwrap().value {
        assert_eq!(
            obj.get("name").unwrap(),
            &DataValue::String("test".to_string())
        );
        assert_eq!(obj.get("value").unwrap(), &DataValue::Int(42));
    } else {
        panic!("Expected Object variant in first record");
    }

    // Verify second record (nested object)
    if let DataValue::Object(outer_obj) = &record2.unwrap().value {
        if let DataValue::Object(inner_obj) = outer_obj.get("user").unwrap() {
            assert_eq!(
                inner_obj.get("name").unwrap(),
                &DataValue::String("alice".to_string())
            );
            assert_eq!(inner_obj.get("age").unwrap(), &DataValue::Int(30));
        } else {
            panic!("Expected nested Object in second record");
        }
        assert_eq!(outer_obj.get("active").unwrap(), &DataValue::Bool(true));
    } else {
        panic!("Expected Object variant in second record");
    }

    // Clean up
    let _ = std::fs::remove_dir_all(&test_dir);
}
