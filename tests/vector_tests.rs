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
