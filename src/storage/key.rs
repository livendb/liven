use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct StreamKey {
    bytes: [u8; 32],
    len: u8,
}

impl StreamKey {
    /// Create a new StreamKey from a string slice, validating it is <= 32 bytes.
    pub fn from_str(s: &str) -> crate::error::Result<Self> {
        let bytes = s.as_bytes();
        if bytes.len() > 32 {
            return Err(crate::error::LivenError::KeyTooLong {
                len: bytes.len(),
                key: s.to_string(),
            });
        }
        let mut arr = [0u8; 32];
        arr[..bytes.len()].copy_from_slice(bytes);
        Ok(Self {
            bytes: arr,
            len: bytes.len() as u8,
        })
    }

    /// Try to create a new StreamKey, returning a key too long error if length > 32 bytes.
    pub fn try_new(s: &str) -> crate::error::Result<Self> {
        Self::from_str(s)
    }

    /// Create a StreamKey from a string slice, truncating to 32 bytes safely (UTF-8 aware).
    pub fn from_str_truncated(s: &str) -> Self {
        let mut bytes = s.as_bytes();
        if bytes.len() > 32 {
            bytes = &bytes[..32];
            while std::str::from_utf8(bytes).is_err() && !bytes.is_empty() {
                bytes = &bytes[..bytes.len() - 1];
            }
        }
        let mut arr = [0u8; 32];
        arr[..bytes.len()].copy_from_slice(bytes);
        Self {
            bytes: arr,
            len: bytes.len() as u8,
        }
    }

    /// Generate a K-sortable unique 32-byte ASCII/UTF-8 stack key.
    /// Combines a 64-bit microsecond-precision timestamp in hex (16 chars)
    /// and 16 characters of random hex entropy.
    pub fn generate_k_sortable() -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        // Format microsecond timestamp to hex (16 chars)
        let ts_hex = format!("{:016x}", now);

        // Generate 16 chars of random hex entropy (8 bytes)
        let mut rand_bytes = [0u8; 8];
        use rand::RngCore;
        rand::thread_rng().fill_bytes(&mut rand_bytes);
        let rand_hex = format!("{:016x}", u64::from_be_bytes(rand_bytes));

        let mut arr = [0u8; 32];
        arr[..16].copy_from_slice(ts_hex.as_bytes());
        arr[16..32].copy_from_slice(rand_hex.as_bytes());

        Self {
            bytes: arr,
            len: 32,
        }
    }

    /// Get the string representation of the StreamKey.
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.bytes[..self.len as usize]).unwrap_or("")
    }

    /// Convert to a raw byte slice.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }
}

impl std::fmt::Display for StreamKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::fmt::Debug for StreamKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StreamKey({:?})", self.as_str())
    }
}

impl Serialize for StreamKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for StreamKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        StreamKey::from_str(&s).map_err(serde::de::Error::custom)
    }
}
