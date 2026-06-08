use liven::codec::{LivenCodec, LivenFrame};
use liven::executor::datavalue_to_json;
use liven::storage::{serialize_payload_into, FrameReader};
use liven::types::DataValue;
use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

#[test]
fn test_vector_type_and_display() {
    let vec_data = vec![1, -2, 127, -128, 0];
    let val = DataValue::Vector(vec_data.clone());
    
    assert_eq!(val.type_tag(), 8);
    assert_eq!(format!("{}", val), "[1, -2, 127, -128, 0]");
}

#[test]
fn test_vector_to_json() {
    let vec_data = vec![10, -20, 30];
    let val = DataValue::Vector(vec_data);
    let json_val = datavalue_to_json(&val);
    
    assert!(json_val.is_array());
    let arr = json_val.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0].as_i64().unwrap(), 10);
    assert_eq!(arr[1].as_i64().unwrap(), -20);
    assert_eq!(arr[2].as_i64().unwrap(), 30);
}

#[test]
fn test_vector_codec_encode_decode() {
    let mut codec = LivenCodec::new(false);
    let mut dst = BytesMut::new();
    
    let original_vec = vec![1, 2, 3, 4, 5, -1, -2, -3, -4, -5];
    let frame = LivenFrame::Vector(original_vec.clone());
    
    codec.encode(frame, &mut dst).unwrap();
    
    // Header should be 4 bytes of length, and the first byte of payload should be 0x03
    assert!(dst.len() > 5);
    let len = u32::from_be_bytes([dst[0], dst[1], dst[2], dst[3]]) as usize;
    assert_eq!(len, dst.len() - 4);
    assert_eq!(dst[4], 0x03); // TCP discriminator prefix for raw quantized vector
    
    // Decode back
    let decoded = codec.decode(&mut dst).unwrap().unwrap();
    if let LivenFrame::Vector(decoded_vec) = decoded {
        assert_eq!(decoded_vec, original_vec);
    } else {
        panic!("Decoded frame is not LivenFrame::Vector");
    }
}

#[test]
fn test_vector_storage_serialization_and_framereader() {
    let stream_name = "test_stream";
    let key = "test_key";
    let vec_data = vec![1, -2, 3, -4, 5];
    let value = DataValue::Vector(vec_data.clone());
    
    let mut payload_buf = Vec::new();
    serialize_payload_into(stream_name, key, &value, &mut payload_buf);
    
    // Construct a complete on-disk 26-byte binary header followed by payload
    // Offsets:
    // 0..8: sequence_id
    // 8..16: timestamp
    // 16: type_tag (8)
    // 17: flags
    // 18..22: payload_len
    // 22..26: CRC32
    let mut frame = Vec::new();
    frame.extend_from_slice(&1u64.to_be_bytes()); // sequence_id
    frame.extend_from_slice(&123456i64.to_be_bytes()); // timestamp
    frame.push(8); // type_tag (8)
    frame.push(0); // flags
    frame.extend_from_slice(&(payload_buf.len() as u32).to_be_bytes()); // payload_len
    let crc = crc32fast::hash(&payload_buf);
    frame.extend_from_slice(&crc.to_be_bytes()); // CRC32
    frame.extend_from_slice(&payload_buf); // Payload
    
    // Verify FrameReader
    let reader = FrameReader::new(&frame);
    let slice = reader.as_vector_slice();
    assert!(slice.is_some());
    let unpacked_slice = slice.unwrap();
    assert_eq!(unpacked_slice, &vec_data[..]);
}

#[test]
fn test_vector_similarity_computation() {
    use liven::executor::cosine_similarity;

    // Identical vectors: similarity should be 1.0
    let a = vec![1, 2, 3];
    let b = vec![1, 2, 3];
    let sim1 = cosine_similarity(&a, &b).unwrap();
    assert!((sim1 - 1.0).abs() < 1e-6);

    // Orthogonal vectors: similarity should be 0.0
    let c = vec![1, 0];
    let d = vec![0, 1];
    let sim2 = cosine_similarity(&c, &d).unwrap();
    assert!((sim2 - 0.0).abs() < 1e-6);

    // Opposite vectors: similarity should be -1.0
    let e = vec![1, -1];
    let f = vec![-1, 1];
    let sim3 = cosine_similarity(&e, &f).unwrap();
    assert!((sim3 - (-1.0)).abs() < 1e-6);
}

#[test]
fn test_vector_filter_parsing() {
    use liven::parser::parse_pipeline;
    use liven::types::PipelineStage;

    let query_str = "from(\"vectors\") | vector_filter(value, [10, -20, 30], 0.85)";
    let stages = parse_pipeline(query_str).unwrap();

    assert_eq!(stages.len(), 2);
    if let PipelineStage::VectorFilter { field, query_vector, threshold } = &stages[1] {
        assert_eq!(field, "value");
        assert_eq!(query_vector, &vec![10, -20, 30]);
        assert!((threshold.into_inner() - 0.85).abs() < 1e-6);
    } else {
        panic!("Parsed stage is not PipelineStage::VectorFilter");
    }
}

#[test]
fn test_vector_filter_execution() {
    use liven::types::{Record, PipelineStage};
    use liven::executor::apply_pipeline_stages_to_vec;
    use liven::storage::StorageEngine;
    use liven::storage::key::StreamKey;
    use ordered_float::OrderedFloat;

    // Create a pool of records
    let mut records = vec![
        Record {
            sequence_id: 1,
            timestamp: 1000,
            type_tag: 8, // Vector
            flags: 0x01,
            stream_name: "vectors".to_string(),
            key: StreamKey::from_str_truncated("rec1"),
            value: DataValue::Vector(vec![1, 2, 3]), // exact match with query vector
        },
        Record {
            sequence_id: 2,
            timestamp: 1001,
            type_tag: 8,
            flags: 0x01,
            stream_name: "vectors".to_string(),
            key: StreamKey::from_str_truncated("rec2"),
            value: DataValue::Vector(vec![-1, -2, -3]), // opposite vector
        },
    ];

    // Pipeline Stage
    let stage = PipelineStage::VectorFilter {
        field: "value".to_string(),
        query_vector: vec![1, 2, 3],
        threshold: OrderedFloat(0.8),
    };

    let temp_dir = std::env::temp_dir().join(format!(
        "test_vec_exec_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let engine = StorageEngine::new(&temp_dir, 1024 * 1024).unwrap();

    apply_pipeline_stages_to_vec(&mut records, &engine, &[stage]);

    // Should retain only rec1
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].key.as_str(), "rec1");
}
