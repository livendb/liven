use crate::types::Record;
use bytes::{Buf, BytesMut};
use serde::{Deserialize, Serialize};
use std::io;
use tokio_util::codec::{Decoder, Encoder};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LivenFrame {
    Query(String),
    Records(Vec<Record>),
    Connect { client_id: String },
    Challenge { nonce: Vec<u8> },
    Response { signature: Vec<u8> },
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

        if data.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Empty TCP frame payload",
            ));
        }

        let discriminator = data[0];
        if discriminator == 0x03 {
            let raw_bytes = &data[1..];
            let ptr = raw_bytes.as_ptr() as *const i8;
            let count = raw_bytes.len();
            // SAFETY: `raw_bytes` is a valid &[u8] slice obtained from `split_to(len)` on a
            // `BytesMut`. Reinterpreting u8 as i8 is sound because both types have the same
            // size (1 byte), alignment (1), and bit layout. The resulting slice is immediately
            // converted to an owned Vec<i8> via `.to_vec()`, which copies the bytes. The
            // original `data` bytes are dropped after this scope, but the Vec owns its memory.
            // If the discriminator encoding changes or `raw_bytes` could be aliased, this
            // safety justification would need to be revisited.
            let vec_i8 = unsafe { std::slice::from_raw_parts(ptr, count) }.to_vec();
            Ok(Some(LivenFrame::Vector(vec_i8)))
        } else if discriminator == 0x02 {
            let frame: LivenFrame = rmp_serde::from_slice(&data[1..])
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(Some(frame))
        } else {
            // Robust fallback for backward compatibility
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
        match item {
            LivenFrame::Vector(vec) => {
                let payload_len = 1 + vec.len();
                dst.reserve(4 + payload_len);
                dst.extend_from_slice(&(payload_len as u32).to_be_bytes());
                dst.extend_from_slice(&[0x03]);
                let ptr = vec.as_ptr() as *const u8;
                let len = vec.len();
                // SAFETY: `vec` is a valid Vec<i8> whose backing allocation is live for the
                // duration of this scope. Reinterpreting i8 as u8 is sound because both types
                // have the same size (1 byte), alignment (1), and bit layout. The resulting
                // slice is only used to feed `extend_from_slice`, which copies the bytes into
                // `dst`. No aliasing occurs, and the slice does not outlive `vec` or the
                // current scope. If this code is modified to store or return the slice, this
                // safety justification would need to be revisited.
                let u8_slice = unsafe { std::slice::from_raw_parts(ptr, len) };
                dst.extend_from_slice(u8_slice);
            }
            other => {
                let serialized = rmp_serde::to_vec(&other)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                let payload_len = 1 + serialized.len();
                dst.reserve(4 + payload_len);
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
