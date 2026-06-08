use rand::rngs::OsRng;

pub const CAP_NONE: u8 = 0x00;
pub const CAP_READ: u8 = 0x01; // historical scan, subscribes, list streams
pub const CAP_WRITE: u8 = 0x02; // inserts, updates, upserts, emtying
pub const CAP_ADMIN: u8 = 0x04; // drop streams, compactions
pub const CAP_ROOT: u8 = 0xFF; // unrestricted/full

pub fn generate_nonce() -> [u8; 32] {
    let mut nonce = [0u8; 32];
    let mut rng = OsRng;
    rand::RngCore::fill_bytes(&mut rng, &mut nonce);
    nonce
}

pub fn hex_decode(s: &str) -> Result<Vec<u8>, String> {
    let s = s.trim();
    if s.len() % 2 != 0 {
        return Err("Odd hex length".to_string());
    }
    let mut bytes = Vec::with_capacity(s.len() / 2);
    for i in (0..s.len()).step_by(2) {
        let res = u8::from_str_radix(&s[i..i + 2], 16)
            .map_err(|_| "Invalid hex character".to_string())?;
        bytes.push(res);
    }
    Ok(bytes)
}

pub fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}
