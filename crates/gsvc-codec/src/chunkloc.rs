//! [`ChunkLoc`] — where a content-addressed chunk physically lives.
//!
//! Defined here in the codec (the crate both `gsvc-store` and `gsvc-meta` depend on) so there is a
//! single shared type rather than two identical structs across the storage/metadata boundary.

use serde::{Deserialize, Serialize};

/// The storage location of a CDC chunk: the **zstd-compressed** chunk bytes occupy
/// `[offset, offset+length)` inside the chunk pack `chunkpacks/<pack_id>`.
///
/// Chunks are always zstd-compressed (zstd stores incompressible data as near-raw blocks, so there
/// is no expansion to guard against — no raw/compressed branch). `length` is the **compressed**
/// length. The content address (the chunk's `ChunkHash`) is taken over the **uncompressed** bytes,
/// so compression never affects dedup.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkLoc {
    /// Content-addressed id of the chunk pack holding this chunk.
    pub pack_id: String,
    /// Byte offset of the compressed chunk bytes within that pack.
    pub offset: u64,
    /// Length in bytes of the stored (compressed) representation.
    pub length: u32,
}

impl ChunkLoc {
    /// The byte range of this chunk within its pack.
    pub fn range(&self) -> std::ops::Range<u64> {
        self.offset..self.offset + self.length as u64
    }
}
