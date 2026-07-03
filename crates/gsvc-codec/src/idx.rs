//! Pack index v2 (`.idx`) writing and lookup.
//!
//! The cold read path (§7) reads the small `.idx` from object storage, binary-searches it for a
//! wanted oid, and issues a byte-range GET into the `.pack` at the resulting offset — never
//! downloading the whole pack.

use sha1::{Digest, Sha1};

use crate::pack::PackedEntry;
use crate::{CodecError, Oid};

const IDX_MAGIC: [u8; 4] = [0xff, 0x74, 0x4f, 0x63]; // "\377tOc"
const IDX_VERSION: u32 = 2;
const FANOUT_LEN: usize = 256;
/// Offsets ≥ 2^31 are stored in a 64-bit secondary table; the primary slot holds an index into it.
const LARGE_OFFSET_FLAG: u32 = 0x8000_0000;

/// Serialize a v2 pack index for `entries` (any order; sorted internally), tied to `pack_hash`.
pub fn write_idx_v2(entries: &[PackedEntry], pack_hash: Oid) -> Vec<u8> {
    let mut sorted = entries.to_vec();
    sorted.sort_by(|a, b| a.oid.as_bytes().cmp(b.oid.as_bytes()));
    let n = sorted.len();

    let mut out = Vec::with_capacity(8 + 256 * 4 + n * (20 + 4 + 4) + 40);
    out.extend_from_slice(&IDX_MAGIC);
    out.extend_from_slice(&IDX_VERSION.to_be_bytes());

    // Fanout: cumulative count of oids whose first byte is ≤ i.
    let mut fanout = [0u32; FANOUT_LEN];
    for e in &sorted {
        fanout[e.oid.first_byte() as usize] += 1;
    }
    let mut acc = 0u32;
    for slot in fanout.iter_mut() {
        acc += *slot;
        *slot = acc;
    }
    for v in fanout {
        out.extend_from_slice(&v.to_be_bytes());
    }

    // Sorted oids.
    for e in &sorted {
        out.extend_from_slice(e.oid.as_bytes());
    }
    // CRC-32 of each packed object.
    for e in &sorted {
        out.extend_from_slice(&e.crc32.to_be_bytes());
    }

    // Offsets: small ones inline; large ones flagged with an index into the 64-bit table.
    let mut large: Vec<u64> = Vec::new();
    for e in &sorted {
        if e.offset < LARGE_OFFSET_FLAG as u64 {
            out.extend_from_slice(&(e.offset as u32).to_be_bytes());
        } else {
            let idx = large.len() as u32;
            out.extend_from_slice(&(LARGE_OFFSET_FLAG | idx).to_be_bytes());
            large.push(e.offset);
        }
    }
    for off in &large {
        out.extend_from_slice(&off.to_be_bytes());
    }

    // Trailers: pack hash, then idx self-hash.
    out.extend_from_slice(pack_hash.as_bytes());
    let mut h = Sha1::new();
    h.update(&out);
    let digest: [u8; 20] = h.finalize().into();
    out.extend_from_slice(&digest);
    out
}

/// A parsed, queryable view over v2 idx bytes. Borrows the buffer; cheap to construct.
pub struct IdxV2<'a> {
    buf: &'a [u8],
    count: usize,
    oids_off: usize,
    crc_off: usize,
    offsets_off: usize,
    large_off: usize,
}

impl<'a> IdxV2<'a> {
    /// Parse and validate the header of a v2 idx.
    pub fn parse(buf: &'a [u8]) -> Result<IdxV2<'a>, CodecError> {
        if buf.len() < 8 + 256 * 4 + 40 {
            return Err(CodecError::IdxTooShort);
        }
        if buf[0..4] != IDX_MAGIC {
            return Err(CodecError::BadIdxMagic);
        }
        let version = u32::from_be_bytes(buf[4..8].try_into().unwrap());
        if version != IDX_VERSION {
            return Err(CodecError::BadIdxVersion(version));
        }
        let fanout_off = 8;
        let count = u32::from_be_bytes(
            buf[fanout_off + 255 * 4..fanout_off + 256 * 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let oids_off = fanout_off + 256 * 4;
        let crc_off = oids_off + count * 20;
        let offsets_off = crc_off + count * 4;
        let large_off = offsets_off + count * 4;
        if large_off + 40 > buf.len() {
            return Err(CodecError::IdxTooShort);
        }
        Ok(IdxV2 {
            buf,
            count,
            oids_off,
            crc_off,
            offsets_off,
            large_off,
        })
    }

    /// Number of objects indexed.
    pub fn len(&self) -> usize {
        self.count
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn oid_at(&self, i: usize) -> Oid {
        let s = self.oids_off + i * 20;
        Oid::from_bytes(&self.buf[s..s + 20]).expect("20 bytes")
    }

    /// CRC-32 recorded for the object at sorted position `i`.
    pub fn crc_at(&self, i: usize) -> u32 {
        let s = self.crc_off + i * 4;
        u32::from_be_bytes(self.buf[s..s + 4].try_into().unwrap())
    }

    /// Byte offset within the pack for the object at sorted position `i` (resolves large offsets).
    pub fn offset_at(&self, i: usize) -> u64 {
        let s = self.offsets_off + i * 4;
        let raw = u32::from_be_bytes(self.buf[s..s + 4].try_into().unwrap());
        if raw & LARGE_OFFSET_FLAG == 0 {
            raw as u64
        } else {
            let idx = (raw & !LARGE_OFFSET_FLAG) as usize;
            let ls = self.large_off + idx * 8;
            u64::from_be_bytes(self.buf[ls..ls + 8].try_into().unwrap())
        }
    }

    /// Binary-search for `oid`, returning its byte offset within the pack if present.
    pub fn find_offset(&self, oid: &Oid) -> Option<u64> {
        let (mut lo, mut hi) = (0usize, self.count);
        while lo < hi {
            let mid = (lo + hi) / 2;
            match self.oid_at(mid).as_bytes().cmp(oid.as_bytes()) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(self.offset_at(mid)),
            }
        }
        None
    }

    /// Whether `oid` is present in this index.
    pub fn contains(&self, oid: &Oid) -> bool {
        self.find_offset(oid).is_some()
    }

    /// The oid at sorted position `i` — its **bitmap position** (reachability bitmaps index objects
    /// by their sorted rank in the index). Panics if `i >= len()`.
    pub fn oid_at_position(&self, i: usize) -> Oid {
        self.oid_at(i)
    }

    /// The sorted position (bitmap index) of `oid`, if present.
    pub fn position(&self, oid: &Oid) -> Option<usize> {
        let (mut lo, mut hi) = (0usize, self.count);
        while lo < hi {
            let mid = (lo + hi) / 2;
            match self.oid_at(mid).as_bytes().cmp(oid.as_bytes()) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(mid),
            }
        }
        None
    }

    /// Iterate all oids in sorted order.
    pub fn iter_oids(&self) -> impl Iterator<Item = Oid> + '_ {
        (0..self.count).map(move |i| self.oid_at(i))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::Kind;
    use crate::pack::build_pack;
    use crate::Object;

    #[test]
    fn idx_lookup_matches_pack_offsets() {
        let objects = vec![
            Object::new(Kind::Blob, &b"alpha"[..]),
            Object::new(Kind::Blob, &b"beta"[..]),
            Object::new(Kind::Blob, &b"gamma"[..]),
            Object::new(Kind::Commit, &b"some commit data"[..]),
        ];
        let built = build_pack(&objects).unwrap();
        let idx = write_idx_v2(&built.entries, built.pack_hash);
        let parsed = IdxV2::parse(&idx).unwrap();

        assert_eq!(parsed.len(), objects.len());
        for e in &built.entries {
            assert_eq!(parsed.find_offset(&e.oid), Some(e.offset));
            assert!(parsed.contains(&e.oid));
        }
        // A random oid is absent.
        assert_eq!(parsed.find_offset(&Oid::ZERO), None);
    }

    #[test]
    fn idx_oids_are_sorted() {
        let objects: Vec<Object> = (0..50u8)
            .map(|i| Object::new(Kind::Blob, vec![i; (i as usize) + 1]))
            .collect();
        let built = build_pack(&objects).unwrap();
        let idx = write_idx_v2(&built.entries, built.pack_hash);
        let parsed = IdxV2::parse(&idx).unwrap();
        let oids: Vec<Oid> = parsed.iter_oids().collect();
        let mut sorted = oids.clone();
        sorted.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        assert_eq!(oids, sorted);
    }
}
