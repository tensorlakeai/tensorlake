use crate::Oid;

/// Errors produced by the codec layer.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("oid must be 20 bytes, got {0}")]
    BadOidLen(usize),
    #[error("invalid oid hex: {0}")]
    BadOidHex(String),
    #[error("unknown object kind: {0}")]
    BadKind(String),
    #[error("malformed git object: {0}")]
    BadObject(String),
    #[error("invalid pack object type code: {0}")]
    BadPackType(u8),

    #[error("pack too short")]
    PackTooShort,
    #[error("bad pack magic (expected 'PACK')")]
    BadPackMagic,
    #[error("unsupported pack version: {0}")]
    BadPackVersion(u32),
    #[error("pack checksum (trailer SHA-1) mismatch")]
    PackChecksumMismatch,
    #[error("OFS_DELTA base offset underflows pack start")]
    BadDeltaBaseOffset,
    #[error("OFS_DELTA references missing base at offset {0}")]
    MissingDeltaBaseOffset(u64),
    #[error("REF_DELTA references missing base oid {0}")]
    MissingDeltaBaseOid(Oid),
    #[error("pack contains duplicate object oid {0}")]
    DuplicateObject(Oid),

    #[error("idx too short / truncated")]
    IdxTooShort,
    #[error("bad idx magic")]
    BadIdxMagic,
    #[error("unsupported idx version: {0}")]
    BadIdxVersion(u32),

    #[error("truncated delta stream")]
    TruncatedDelta,
    #[error("reserved delta opcode 0x00")]
    ReservedDeltaOpcode,
    #[error("delta base size mismatch: header says {expected}, base is {actual}")]
    DeltaBaseSizeMismatch { expected: u64, actual: u64 },
    #[error("delta target size mismatch: header says {expected}, produced {actual}")]
    DeltaTargetSizeMismatch { expected: u64, actual: u64 },
    #[error("delta copy out of range: [{start},{end}) of base len {base_len}")]
    DeltaCopyOutOfRange { start: u64, end: u64, base_len: u64 },

    #[error("zlib inflate error: {0}")]
    Inflate(String),
    #[error("io error: {0}")]
    Io(String),
}
