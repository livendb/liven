use liven::import_export::*;
use liven::types::DataValue;
use std::io::Write;
use tempfile::NamedTempFile;

#[tokio::test]
async fn test_json_to_datavalue_with_base64() {
    // Test all data types including base64
    let test_cases = vec![
        (serde_json::json!(null), DataValue::Null),
        (serde_json::json!(true), DataValue::Bool(true)),
        (serde_json::json!(42), DataValue::Int(42)),
        (
            serde_json::json!(18446744073709551615u64),
            DataValue::UInt(18446744073709551615),
        ),
        (
            serde_json::json!(19.99),
            DataValue::Float(ordered_float::OrderedFloat(19.99)),
        ),
        (
            serde_json::json!("hello"),
            DataValue::String("hello".to_string()),
        ),
        (
            serde_json::json!("base64:SGVsbG8="),
            DataValue::Binary(b"Hello".to_vec()),
        ),
        (
            serde_json::json!([1, 2, 3]),
            DataValue::Array(vec![
                DataValue::Int(1),
                DataValue::Int(2),
                DataValue::Int(3),
            ]),
        ),
        (
            serde_json::json!({"k": "v"}),
            DataValue::Object({
                let mut map = std::collections::BTreeMap::new();
                map.insert("k".to_string(), DataValue::String("v".to_string()));
                map
            }),
        ),
    ];

    for (json, expected) in test_cases {
        let result = json_to_datavalue_with_base64(json).unwrap();
        assert_eq!(result, expected);
    }
}

#[tokio::test]
async fn test_datavalue_to_json() {
    // Test all data types including base64
    let test_cases = vec![
        (DataValue::Null, serde_json::json!(null)),
        (DataValue::Bool(true), serde_json::json!(true)),
        (DataValue::Int(42), serde_json::json!(42)),
        (DataValue::UInt(123), serde_json::json!(123)),
        (
            DataValue::Float(ordered_float::OrderedFloat(19.99)),
            serde_json::json!(19.99),
        ),
        (
            DataValue::String("hello".to_string()),
            serde_json::json!("hello"),
        ),
        (
            DataValue::Binary(b"Hello".to_vec()),
            serde_json::json!("base64:SGVsbG8="),
        ),
        (
            DataValue::Array(vec![DataValue::Int(1), DataValue::Int(2)]),
            serde_json::json!([1, 2]),
        ),
        (
            DataValue::Object({
                let mut map = std::collections::BTreeMap::new();
                map.insert("k".to_string(), DataValue::String("v".to_string()));
                map
            }),
            serde_json::json!({"k": "v"}),
        ),
        (
            DataValue::Vector(vec![1, 0, -1, 2]),
            serde_json::json!([1, 0, -1, 2]),
        ),
    ];

    for (value, expected) in test_cases {
        let result = datavalue_to_json(&value);
        assert_eq!(result, expected);
    }
}

#[tokio::test]
async fn test_jsonl_validation() {
    // Create a temporary JSONL file with valid data
    let mut temp_file = NamedTempFile::new().unwrap();
    writeln!(
        temp_file,
        r#"{{"stream": "users", "key": "user1", "value": {{"name": "Alice", "age": 30}}}}"#
    )
    .unwrap();
    writeln!(
        temp_file,
        r#"{{"stream": "users", "key": "user2", "value": {{"name": "Bob", "age": 25}}}}"#
    )
    .unwrap();
    writeln!(
        temp_file,
        r#"{{"stream": "events", "key": "event1", "value": {{"type": "click"}}}}"#
    )
    .unwrap();

    let path = temp_file.path();
    let validation = validate_jsonl_file(path).unwrap();

    assert_eq!(validation.record_count, 3);
    assert_eq!(validation.streams.len(), 2);
    assert!(validation.streams.contains(&"users".to_string()));
    assert!(validation.streams.contains(&"events".to_string()));
}

#[tokio::test]
async fn test_jsonl_validation_errors() {
    // Create a temporary JSONL file with invalid data
    let mut temp_file = NamedTempFile::new().unwrap();
    writeln!(
        temp_file,
        r#"{{"stream": "users", "key": "user1", "value": {{"name": "Alice"}}}}"#
    )
    .unwrap();
    writeln!(
        temp_file,
        r#"{{"key": "user2", "value": {{"name": "Bob"}}}}"#
    )
    .unwrap(); // Missing stream
    writeln!(
        temp_file,
        r#"{{"stream": "users", "value": {{"name": "Charlie"}}}}"#
    )
    .unwrap(); // Missing key

    let path = temp_file.path();
    let result = validate_jsonl_file(path);

    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.contains("missing field `stream`"));
    assert!(error.contains("missing field `key`"));
}

#[tokio::test]
async fn test_jsonl_with_timestamps() {
    // Create a temporary JSONL file with timestamps
    let mut temp_file = NamedTempFile::new().unwrap();
    let timestamp = 1700000000000i64;
    writeln!(
        temp_file,
        r#"{{"stream": "users", "key": "user1", "timestamp": {}, "value": {{"name": "Alice"}}}}"#,
        timestamp
    )
    .unwrap();

    let path = temp_file.path();
    let validation = validate_jsonl_file(path).unwrap();
    assert_eq!(validation.record_count, 1);
}

#[tokio::test]
async fn test_jsonl_with_base64() {
    // Create a temporary JSONL file with base64 encoded binary data
    let mut temp_file = NamedTempFile::new().unwrap();
    writeln!(
        temp_file,
        r#"{{"stream": "data", "key": "bin1", "value": "base64:SGVsbG8gV29ybGQ="}}"#
    )
    .unwrap();

    let path = temp_file.path();
    let validation = validate_jsonl_file(path).unwrap();
    assert_eq!(validation.record_count, 1);
}

#[tokio::test]
async fn test_binary_round_trip() {
    // Create some test records
    let records = vec![
        BinaryRecord {
            stream: "users".to_string(),
            key: "user1".to_string(),
            value: DataValue::Object({
                let mut map = std::collections::BTreeMap::new();
                map.insert("name".to_string(), DataValue::String("Alice".to_string()));
                map.insert("age".to_string(), DataValue::Int(30));
                map
            }),
            timestamp: 1700000000000,
            type_tag: 9, // Object
            flags: 0,
        },
        BinaryRecord {
            stream: "users".to_string(),
            key: "user2".to_string(),
            value: DataValue::String("Bob".to_string()),
            timestamp: 1700000001000,
            type_tag: 5, // String
            flags: 1,
        },
    ];

    let export = BinaryExport {
        version: 1,
        created_at: 1700000000000,
        records: records.clone(),
    };

    // Serialize to binary format
    let mut data = Vec::new();
    data.extend_from_slice(BINARY_MAGIC);
    data.extend_from_slice(&[0u8; 4]); // Checksum placeholder
    let serialized = rmp_serde::to_vec(&export).unwrap();
    data.extend_from_slice(&serialized);

    // Calculate and write checksum
    let checksum = crc32fast::hash(&data[BINARY_MAGIC.len() + 4..]);
    data[BINARY_MAGIC.len()..BINARY_MAGIC.len() + 4].copy_from_slice(&checksum.to_be_bytes());

    // Write to temporary file
    let mut temp_file = NamedTempFile::new().unwrap();
    temp_file.write_all(&data).unwrap();
    let path = temp_file.path();

    // Read back and verify
    let file_data = std::fs::read(path).unwrap();
    assert_eq!(file_data, data);

    // Verify magic bytes
    assert_eq!(&file_data[..BINARY_MAGIC.len()], BINARY_MAGIC);

    // Verify checksum
    let checksum_pos = BINARY_MAGIC.len();
    let expected_checksum = u32::from_be_bytes(
        file_data[checksum_pos..checksum_pos + 4]
            .try_into()
            .unwrap(),
    );
    let actual_checksum = crc32fast::hash(&file_data[checksum_pos + 4..]);
    assert_eq!(expected_checksum, actual_checksum);

    // Deserialize and verify content
    let deserialized: BinaryExport = rmp_serde::from_slice(&file_data[checksum_pos + 4..]).unwrap();
    assert_eq!(deserialized.version, 1);
    assert_eq!(deserialized.created_at, 1700000000000);
    assert_eq!(deserialized.records.len(), 2);
    assert_eq!(deserialized.records[0].stream, "users");
    assert_eq!(deserialized.records[0].key, "user1");
}
