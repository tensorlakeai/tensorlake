//! Git object model and hashing.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};

use crate::{CodecError, Oid};

/// The four git object kinds.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash, Serialize, Deserialize)]
pub enum Kind {
    Commit,
    Tree,
    Blob,
    Tag,
}

impl Kind {
    /// The loose-header keyword (`commit`, `tree`, `blob`, `tag`).
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Commit => "commit",
            Kind::Tree => "tree",
            Kind::Blob => "blob",
            Kind::Tag => "tag",
        }
    }

    /// Parse from the loose-header keyword. (Inherent, not the `FromStr` trait: it takes the git
    /// keyword set specifically and returns our [`CodecError`].)
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Result<Kind, CodecError> {
        Ok(match s {
            "commit" => Kind::Commit,
            "tree" => Kind::Tree,
            "blob" => Kind::Blob,
            "tag" => Kind::Tag,
            other => return Err(CodecError::BadKind(other.to_string())),
        })
    }

    /// The packfile type code (commit=1, tree=2, blob=3, tag=4).
    pub fn pack_type(self) -> u8 {
        match self {
            Kind::Commit => 1,
            Kind::Tree => 2,
            Kind::Blob => 3,
            Kind::Tag => 4,
        }
    }

    /// Parse from a packfile non-delta type code.
    pub fn from_pack_type(t: u8) -> Result<Kind, CodecError> {
        Ok(match t {
            1 => Kind::Commit,
            2 => Kind::Tree,
            3 => Kind::Blob,
            4 => Kind::Tag,
            other => return Err(CodecError::BadPackType(other)),
        })
    }
}

/// Compute a git object id over `data` of the given `kind`.
///
/// The hashed pre-image is the canonical loose form: `"<kind> <len>\0<data>"`.
pub fn hash(kind: Kind, data: &[u8]) -> Oid {
    let mut h = Sha1::new();
    h.update(kind.as_str().as_bytes());
    h.update(b" ");
    h.update(data.len().to_string().as_bytes());
    h.update(b"\0");
    h.update(data);
    let digest: [u8; 20] = h.finalize().into();
    Oid::from_array(digest)
}

/// Incrementally computes a **blob**'s git oid from its bytes without holding them — for streaming
/// large-object ingest, where the blob is chunked to the store as it arrives and never buffered.
/// The git pre-image is `"blob <size>\0<data>"`, so the size must be known up front (it is: the
/// packfile entry header declares it).
pub struct BlobOidHasher {
    hasher: Sha1,
}

impl BlobOidHasher {
    /// Start hashing a blob of `size` bytes.
    pub fn new(size: u64) -> BlobOidHasher {
        let mut hasher = Sha1::new();
        hasher.update(b"blob ");
        hasher.update(size.to_string().as_bytes());
        hasher.update(b"\0");
        BlobOidHasher { hasher }
    }

    /// Feed the next run of blob bytes.
    pub fn update(&mut self, data: &[u8]) {
        self.hasher.update(data);
    }

    /// Finalize into the blob's oid.
    pub fn finalize(self) -> Oid {
        let digest: [u8; 20] = self.hasher.finalize().into();
        Oid::from_array(digest)
    }
}

/// A [`BlobOidHasher`] whose state can be exported and resumed — for verified-at-upload file
/// hashing, where one file's bytes arrive across several requests (and possibly several pods).
/// The state is the standard SHA-1 midstate (h0..h4), the processed pre-image length, and the
/// sub-block remainder; importing it on another machine continues the exact same hash.
///
/// Hand-rolled compression is justified only because resumability requires it (the `sha1` crate
/// does not expose midstates); correctness is pinned by differential tests against
/// [`BlobOidHasher`] across arbitrary split points.
pub struct ResumableBlobOidHasher {
    h: [u32; 5],
    /// Pre-image bytes fed so far (header + payload), including what sits in `buf`.
    len: u64,
    buf: [u8; 64],
    buf_len: usize,
}

/// Serialized size of an exported hasher state.
pub const RESUMABLE_HASHER_STATE_SIZE: usize = 20 + 8 + 1 + 64;

impl ResumableBlobOidHasher {
    /// Start hashing a blob of `size` payload bytes (the git pre-image is `"blob <size>\0"`).
    pub fn new(size: u64) -> ResumableBlobOidHasher {
        let mut h = ResumableBlobOidHasher {
            h: [0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0xC3D2E1F0],
            len: 0,
            buf: [0u8; 64],
            buf_len: 0,
        };
        h.update(b"blob ");
        h.update(size.to_string().as_bytes());
        h.update(b"\0");
        h
    }

    pub fn update(&mut self, mut data: &[u8]) {
        self.len += data.len() as u64;
        if self.buf_len > 0 {
            let take = (64 - self.buf_len).min(data.len());
            self.buf[self.buf_len..self.buf_len + take].copy_from_slice(&data[..take]);
            self.buf_len += take;
            data = &data[take..];
            if self.buf_len == 64 {
                let block = self.buf;
                self.compress(&block);
                self.buf_len = 0;
            }
        }
        while data.len() >= 64 {
            let (block, rest) = data.split_at(64);
            let arr: [u8; 64] = block.try_into().expect("length checked");
            self.compress(&arr);
            data = rest;
        }
        if !data.is_empty() {
            self.buf[..data.len()].copy_from_slice(data);
            self.buf_len = data.len();
        }
    }

    pub fn finalize(mut self) -> Oid {
        let total_bits = self.len * 8;
        self.update(&[0x80]);
        // update() bumped len; padding must not count. Track bits from before the pad.
        while self.buf_len != 56 {
            self.update(&[0]);
        }
        let mut block = self.buf;
        block[56..64].copy_from_slice(&total_bits.to_be_bytes());
        self.compress(&block);
        let mut out = [0u8; 20];
        for (i, w) in self.h.iter().enumerate() {
            out[i * 4..i * 4 + 4].copy_from_slice(&w.to_be_bytes());
        }
        Oid::from_array(out)
    }

    /// Export the midstate (fixed [`RESUMABLE_HASHER_STATE_SIZE`] bytes).
    pub fn export_state(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(RESUMABLE_HASHER_STATE_SIZE);
        for w in &self.h {
            out.extend_from_slice(&w.to_be_bytes());
        }
        out.extend_from_slice(&self.len.to_be_bytes());
        out.push(self.buf_len as u8);
        out.extend_from_slice(&self.buf);
        out
    }

    /// Resume from a state produced by [`Self::export_state`].
    pub fn import_state(state: &[u8]) -> Result<ResumableBlobOidHasher, CodecError> {
        if state.len() != RESUMABLE_HASHER_STATE_SIZE {
            return Err(CodecError::Io(format!(
                "hasher state must be {RESUMABLE_HASHER_STATE_SIZE} bytes, got {}",
                state.len()
            )));
        }
        let mut h = [0u32; 5];
        for (i, w) in h.iter_mut().enumerate() {
            *w = u32::from_be_bytes(state[i * 4..i * 4 + 4].try_into().expect("sized"));
        }
        let len = u64::from_be_bytes(state[20..28].try_into().expect("sized"));
        let buf_len = state[28] as usize;
        if buf_len >= 64 {
            return Err(CodecError::Io("hasher buffer length out of range".into()));
        }
        let mut buf = [0u8; 64];
        buf.copy_from_slice(&state[29..93]);
        Ok(ResumableBlobOidHasher {
            h,
            len,
            buf,
            buf_len,
        })
    }

    fn compress(&mut self, block: &[u8; 64]) {
        let mut w = [0u32; 80];
        for (i, chunk) in block.chunks_exact(4).enumerate() {
            w[i] = u32::from_be_bytes(chunk.try_into().expect("sized"));
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }
        let [mut a, mut b, mut c, mut d, mut e] = self.h;
        for (i, wi) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | ((!b) & d), 0x5A827999u32),
                20..=39 => (b ^ c ^ d, 0x6ED9EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1BBCDC),
                _ => (b ^ c ^ d, 0xCA62C1D6),
            };
            let tmp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(*wi);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = tmp;
        }
        self.h[0] = self.h[0].wrapping_add(a);
        self.h[1] = self.h[1].wrapping_add(b);
        self.h[2] = self.h[2].wrapping_add(c);
        self.h[3] = self.h[3].wrapping_add(d);
        self.h[4] = self.h[4].wrapping_add(e);
    }
}

/// A fully-materialized git object: its kind plus its raw (decompressed, un-deltified) bytes.
#[derive(Clone, PartialEq, Eq)]
pub struct Object {
    pub kind: Kind,
    pub data: Bytes,
}

impl Object {
    /// Build an object from owned bytes.
    pub fn new(kind: Kind, data: impl Into<Bytes>) -> Object {
        Object {
            kind,
            data: data.into(),
        }
    }

    /// The object's git id (computed on demand).
    pub fn id(&self) -> Oid {
        hash(self.kind, &self.data)
    }

    /// Object payload length.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Whether the payload is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl std::fmt::Debug for Object {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Object {{ kind: {:?}, id: {}, len: {} }}",
            self.kind,
            self.id(),
            self.data.len()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Ground-truth hashes from the reference git implementation.
    #[test]
    fn empty_blob_hash() {
        let o = Object::new(Kind::Blob, &b""[..]);
        assert_eq!(o.id().to_hex(), "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391");
    }

    #[test]
    fn empty_tree_hash() {
        let o = Object::new(Kind::Tree, &b""[..]);
        assert_eq!(o.id().to_hex(), "4b825dc642cb6eb9a060e54bf8d69288fbee4904");
    }

    #[test]
    fn hello_blob_hash() {
        // `printf 'hello\n' | git hash-object --stdin`
        let o = Object::new(Kind::Blob, &b"hello\n"[..]);
        assert_eq!(o.id().to_hex(), "ce013625030ba8dba906f756967f9e9ca394464a");
    }

    #[test]
    fn kind_roundtrip() {
        for k in [Kind::Commit, Kind::Tree, Kind::Blob, Kind::Tag] {
            assert_eq!(Kind::from_str(k.as_str()).unwrap(), k);
            assert_eq!(Kind::from_pack_type(k.pack_type()).unwrap(), k);
        }
        assert!(Kind::from_str("nope").is_err());
    }
}

#[cfg(test)]
mod resumable_hasher_tests {
    use super::*;

    /// Deterministic xorshift so failures reproduce.
    fn rng(seed: &mut u64) -> u64 {
        *seed ^= *seed << 13;
        *seed ^= *seed >> 7;
        *seed ^= *seed << 17;
        *seed
    }

    /// The resumable hasher must agree with [`BlobOidHasher`] (and thus git) for every size,
    /// under arbitrary update splits, and under export/import at every split point — including
    /// splits inside the header, at block boundaries, and mid-block.
    #[test]
    fn matches_blob_oid_hasher_across_random_splits_and_resumes() {
        let mut seed = 0x1234_5678_9abc_def0u64;
        let sizes = [
            0usize,
            1,
            55,
            56,
            63,
            64,
            65,
            127,
            128,
            1000,
            64 * 1024 + 17,
        ];
        for &size in &sizes {
            let data: Vec<u8> = (0..size).map(|_| (rng(&mut seed) & 0xff) as u8).collect();
            let mut reference = BlobOidHasher::new(size as u64);
            reference.update(&data);
            let expected = reference.finalize();

            for _round in 0..8 {
                let mut hasher = ResumableBlobOidHasher::new(size as u64);
                let mut off = 0usize;
                while off < data.len() {
                    let take = 1 + (rng(&mut seed) as usize) % (data.len() - off);
                    hasher.update(&data[off..off + take]);
                    off += take;
                    // Round-trip the midstate on every split: resume must be exact.
                    hasher = ResumableBlobOidHasher::import_state(&hasher.export_state()).unwrap();
                }
                assert_eq!(
                    hasher.finalize(),
                    expected,
                    "size {size}: resumable hash diverged"
                );
            }
        }
    }

    #[test]
    fn import_rejects_malformed_state() {
        assert!(ResumableBlobOidHasher::import_state(&[0u8; 10]).is_err());
        let mut bad = ResumableBlobOidHasher::new(4).export_state();
        bad[28] = 64; // buf_len out of range
        assert!(ResumableBlobOidHasher::import_state(&bad).is_err());
    }
}
