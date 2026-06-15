use crate::types::Record;
use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use std::io;
use tokio_util::codec::{Decoder, Encoder};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LivenFrame {
    Query(String),
    Records(Vec<Record>),
    Connect {
        client_id: String,
        #[serde(default)]
        protocol_version: Option<u8>,
    },
    Ok,
    Err(String),
    Vector(Vec<i8>),
}

#[derive(Default)]
pub struct LivenCodec {
    pub is_client: bool,
}

impl LivenCodec {
    pub fn new(is_client: bool) -> Self {
        Self { is_client }
    }
}

impl Decoder for LivenCodec {
    type Item = LivenFrame;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        // Distinguish protocol versions by the first byte:
        // - v0 (legacy): frame starts with a 4-byte big-endian length.
        //   A length byte of 0x00 or 0x01 with first byte < 4 means the length
        //   would be impossibly small, so this heuristic treats 0x00/0x01 as v1.
        // - v1: frame starts with [version: u8][length: u32 big-endian]...
        let is_v1 = src[0] == 0x00 || src[0] == 0x01;

        if is_v1 {
            // Version 1: [version: u8][length: u32][discriminator: u8][payload]
            if src.len() < 5 {
                return Ok(None);
            }
            let _version = src[0];
            let mut len_bytes = [0u8; 4];
            len_bytes.copy_from_slice(&src[1..5]);
            let len = u32::from_be_bytes(len_bytes) as usize;

            if src.len() < 5 + len {
                src.reserve(5 + len - src.len());
                return Ok(None);
            }

            src.advance(5);
            let data = src.split_to(len);
            Self::decode_frame(data)
        } else {
            // Version 0 (legacy): [length: u32][discriminator: u8][payload]
            if src.len() < 4 {
                return Ok(None);
            }
            let mut len_bytes = [0u8; 4];
            len_bytes.copy_from_slice(&src[..4]);
            let len = u32::from_be_bytes(len_bytes) as usize;

            if src.len() < 4 + len {
                src.reserve(4 + len - src.len());
                return Ok(None);
            }

            src.advance(4);
            let data = src.split_to(len);
            Self::decode_frame(data)
        }
    }
}

impl LivenCodec {
    /// Decode the frame payload after the length/version header has been stripped.
    fn decode_frame(data: BytesMut) -> Result<Option<LivenFrame>, io::Error> {
        if data.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Empty TCP frame payload",
            ));
        }

        let discriminator = data[0];

        if discriminator == 0x03 {
            // Convert &[u8] to Vec<i8> safely.
            // `as i8` is a defined numeric cast in Rust for all u8 values —
            // the bit pattern is preserved exactly, which is all we need for
            // the vector frame payload. LLVM optimises this to a memcpy in
            // release builds.
            let vec_i8: Vec<i8> = data[1..].iter().map(|&b| b as i8).collect();
            Ok(Some(LivenFrame::Vector(vec_i8)))
        } else if discriminator == 0x02 {
            let frame: LivenFrame = rmp_serde::from_slice(&data[1..])
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(Some(frame))
        } else {
            // Robust fallback for backward compatibility with v0 frames
            // that do not carry a discriminator byte.
            match rmp_serde::from_slice::<LivenFrame>(&data) {
                Ok(frame) => Ok(Some(frame)),
                Err(_) => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Unknown TCP frame discriminator: {}", discriminator),
                )),
            }
        }
    }
}

impl Encoder<LivenFrame> for LivenCodec {
    type Error = io::Error;

    fn encode(&mut self, item: LivenFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        // Frame format: [version: u8][length: u32][discriminator: u8][payload]
        // version byte is always 0x01 for current protocol.
        match item {
            LivenFrame::Vector(vec) => {
                // payload = discriminator byte (0x03) + one byte per element
                let payload_len = 1 + vec.len();
                dst.reserve(5 + payload_len);
                dst.extend_from_slice(&[0x01]);
                dst.extend_from_slice(&(payload_len as u32).to_be_bytes());
                dst.extend_from_slice(&[0x03]);
                // Write directly into dst — no intermediate allocation.
                // dst is already reserved so each put_u8 is a bounds-checked
                // write into pre-allocated memory. `as u8` is a defined cast
                // that preserves the bit pattern exactly.
                for b in &vec {
                    dst.put_u8(*b as u8);
                }
            }
            other => {
                let serialized = rmp_serde::to_vec(&other)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                let payload_len = 1 + serialized.len();
                dst.reserve(5 + payload_len);
                dst.extend_from_slice(&[0x01]);
                dst.extend_from_slice(&(payload_len as u32).to_be_bytes());
                dst.extend_from_slice(&[0x02]);
                dst.extend_from_slice(&serialized);
            }
        }
        Ok(())
    }
}

impl Encoder<Vec<Record>> for LivenCodec {
    type Error = io::Error;

    fn encode(&mut self, item: Vec<Record>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        self.encode(LivenFrame::Records(item), dst)
    }
}

impl Encoder<String> for LivenCodec {
    type Error = io::Error;

    fn encode(&mut self, item: String, dst: &mut BytesMut) -> Result<(), Self::Error> {
        self.encode(LivenFrame::Query(item), dst)
    }
}

impl Encoder<&str> for LivenCodec {
    type Error = io::Error;

    fn encode(&mut self, item: &str, dst: &mut BytesMut) -> Result<(), Self::Error> {
        self.encode(LivenFrame::Query(item.to_string()), dst)
    }
}
