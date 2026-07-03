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
