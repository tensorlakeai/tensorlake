//! Object identifiers.
//!
//! [`Oid`] is a git object id (SHA-1, 20 bytes). [`ChunkHash`] is a content-defined-chunk
//! address (SHA-256, 32 bytes) used by the large-object/CDC path, which is independent of git's
//! own object naming.

use serde::{Deserialize, Serialize};
use std::fmt;

use crate::CodecError;

/// A git object id: a 20-byte SHA-1 digest.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Oid([u8; 20]);

impl Oid {
    /// Number of raw bytes in an [`Oid`].
    pub const SIZE: usize = 20;
    /// Number of hex characters in a rendered [`Oid`].
    pub const HEX_SIZE: usize = 40;
    /// The all-zero oid, used by git to mean "no object" (e.g. ref creation/deletion).
    pub const ZERO: Oid = Oid([0u8; 20]);

    /// Construct from exactly 20 raw bytes.
    pub fn from_bytes(b: &[u8]) -> Result<Oid, CodecError> {
        if b.len() != Self::SIZE {
            return Err(CodecError::BadOidLen(b.len()));
        }
        let mut a = [0u8; 20];
        a.copy_from_slice(b);
        Ok(Oid(a))
    }

    /// Construct directly from a fixed array.
    #[inline]
    pub const fn from_array(a: [u8; 20]) -> Oid {
        Oid(a)
    }

    /// Parse from a 40-char lowercase/uppercase hex string.
    pub fn from_hex(s: &str) -> Result<Oid, CodecError> {
        if s.len() != Self::HEX_SIZE {
            return Err(CodecError::BadOidHex(s.to_string()));
        }
        let mut a = [0u8; 20];
        hex::decode_to_slice(s, &mut a).map_err(|_| CodecError::BadOidHex(s.to_string()))?;
        Ok(Oid(a))
    }

    /// Raw bytes.
    #[inline]
    pub fn as_bytes(&self) -> &[u8; 20] {
        &self.0
    }

    /// Lowercase hex rendering.
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// The first byte, used for the idx fanout table and S3 sharding prefix.
    #[inline]
    pub fn first_byte(&self) -> u8 {
        self.0[0]
    }

    /// Two-hex-char shard prefix (e.g. `s3://blobs/<prefix>/<oid>`).
    pub fn shard_prefix(&self) -> String {
        hex::encode([self.0[0]])
    }

    /// Whether this is the all-zero oid.
    #[inline]
    pub fn is_zero(&self) -> bool {
        self.0 == [0u8; 20]
    }
}

impl fmt::Display for Oid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl fmt::Debug for Oid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Oid({})", self.to_hex())
    }
}

/// A content-defined-chunk address: a 32-byte SHA-256 digest.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ChunkHash([u8; 32]);

impl ChunkHash {
    /// Number of raw bytes.
    pub const SIZE: usize = 32;

    /// Hash arbitrary content into a chunk address.
    pub fn of(data: &[u8]) -> ChunkHash {
        use sha2::{Digest, Sha256};
        let mut h = Sha256::new();
        h.update(data);
        ChunkHash(h.finalize().into())
    }

    /// Construct from raw bytes.
    pub fn from_bytes(b: &[u8]) -> Result<ChunkHash, CodecError> {
        if b.len() != Self::SIZE {
            return Err(CodecError::BadOidLen(b.len()));
        }
        let mut a = [0u8; 32];
        a.copy_from_slice(b);
        Ok(ChunkHash(a))
    }

    /// Parse from a 64-char lowercase hex string.
    pub fn from_hex(s: &str) -> Result<ChunkHash, CodecError> {
        let bytes = hex::decode(s).map_err(|_| CodecError::BadOidHex(s.to_string()))?;
        ChunkHash::from_bytes(&bytes)
    }

    /// Raw bytes.
    #[inline]
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Lowercase hex rendering.
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Two-hex-char shard prefix for S3 layout (`s3://chunks/<prefix>/<hash>`).
    pub fn shard_prefix(&self) -> String {
        hex::encode([self.0[0]])
    }
}

impl fmt::Display for ChunkHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl fmt::Debug for ChunkHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ChunkHash({})", self.to_hex())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oid_hex_roundtrip() {
        let h = "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391";
        let o = Oid::from_hex(h).unwrap();
        assert_eq!(o.to_hex(), h);
        assert_eq!(o.first_byte(), 0xe6);
        assert_eq!(o.shard_prefix(), "e6");
        assert!(!o.is_zero());
        assert!(Oid::ZERO.is_zero());
    }

    #[test]
    fn oid_bad_inputs() {
        assert!(Oid::from_hex("abc").is_err());
        assert!(Oid::from_bytes(&[0u8; 19]).is_err());
        assert!(Oid::from_hex("zz9de29bb2d1d6434b8b29ae775ad8c2e48c5391").is_err());
    }

    #[test]
    fn chunk_hash_stable() {
        let a = ChunkHash::of(b"hello");
        let b = ChunkHash::of(b"hello");
        let c = ChunkHash::of(b"world");
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_eq!(a.as_bytes().len(), 32);
    }
}
