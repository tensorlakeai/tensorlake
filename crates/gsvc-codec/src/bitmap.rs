//! Reachability bitmaps (design §7, §14.2) — the structure that makes clone/fetch fast.
//!
//! Walking the object graph to answer "what objects are reachable from these wants but not these
//! haves?" is `O(history)`. A reachability bitmap precomputes, for a chosen commit, the set of all
//! objects reachable from it as a bitset over object **positions**, so the same query becomes a few
//! bitwise `OR` / `AND-NOT` passes over machine words — independent of history depth.
//!
//! Position space (we own both writer and reader, so this is our own layout, not git's `.bitmap`):
//! * positions `0..P` are the objects of a packfile, in its index's **sorted-oid order** (so a
//!   position maps to an oid via [`IdxV2::oid_at_position`](crate::IdxV2::oid_at_position));
//! * positions `P..P+L` are **large blobs** referenced by the history but stored globally (outside
//!   the pack), listed explicitly in [`PackBitmaps::large_blobs`].
//!
//! [`PackBitmaps`] stores one [`Bitmap`] per selected commit (the ref tips). Generation walks the
//! graph once per tip at compaction time (off the hot path); serving then needs no walk at all.

use crate::{CodecError, Oid};

const BITMAP_MAGIC: &[u8; 4] = b"GBMP";
const BITMAP_VERSION: u32 = 1;

/// A growable bitset over object positions, stored as 64-bit words (little-endian on disk).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Bitmap {
    words: Vec<u64>,
}

impl Bitmap {
    /// An empty bitmap sized to hold at least `nbits` bits (it also grows on demand via [`set`]).
    pub fn with_capacity(nbits: usize) -> Bitmap {
        Bitmap {
            words: vec![0u64; nbits.div_ceil(64)],
        }
    }

    /// Set bit `i`, growing the backing store if needed.
    pub fn set(&mut self, i: usize) {
        let w = i / 64;
        if w >= self.words.len() {
            self.words.resize(w + 1, 0);
        }
        self.words[w] |= 1u64 << (i % 64);
    }

    /// Whether bit `i` is set.
    pub fn get(&self, i: usize) -> bool {
        let w = i / 64;
        w < self.words.len() && (self.words[w] >> (i % 64)) & 1 == 1
    }

    /// `self |= other` (union).
    pub fn or_assign(&mut self, other: &Bitmap) {
        if other.words.len() > self.words.len() {
            self.words.resize(other.words.len(), 0);
        }
        for (a, b) in self.words.iter_mut().zip(&other.words) {
            *a |= *b;
        }
    }

    /// `self &= !other` (set difference — remove everything in `other`).
    pub fn andnot_assign(&mut self, other: &Bitmap) {
        for (a, b) in self.words.iter_mut().zip(&other.words) {
            *a &= !*b;
        }
    }

    /// Number of set bits.
    pub fn count_ones(&self) -> usize {
        self.words.iter().map(|w| w.count_ones() as usize).sum()
    }

    /// Iterate the positions of all set bits, ascending.
    pub fn iter_ones(&self) -> impl Iterator<Item = usize> + '_ {
        self.words.iter().enumerate().flat_map(|(wi, &word)| {
            let base = wi * 64;
            BitIter { word, base }
        })
    }
}

/// Iterates set bits of one word by repeatedly clearing the lowest set bit.
struct BitIter {
    word: u64,
    base: usize,
}

impl Iterator for BitIter {
    type Item = usize;
    fn next(&mut self) -> Option<usize> {
        if self.word == 0 {
            return None;
        }
        let tz = self.word.trailing_zeros() as usize;
        self.word &= self.word - 1; // clear lowest set bit
        Some(self.base + tz)
    }
}

/// A set of reachability bitmaps for one packfile: the large-blob position table plus one bitmap
/// per selected tip commit. Serialized (zlib-compressed) alongside the pack as its `.bitmap`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PackBitmaps {
    /// Number of objects in the pack (positions `0..pack_object_count`).
    pub pack_object_count: u32,
    /// Large blobs referenced by the history but stored outside the pack (positions
    /// `pack_object_count..pack_object_count + large_blobs.len()`).
    pub large_blobs: Vec<Oid>,
    /// One reachability bitmap per **selected commit**, keyed by oid. The selection is the ref tips
    /// plus sampled interior commits (merges and a periodic sample), so fetch/clone negotiation
    /// points usually land on a commit that already has a bitmap.
    pub commits: Vec<(Oid, Bitmap)>,
}

impl PackBitmaps {
    /// Total number of positions covered (pack objects + large blobs).
    pub fn total_positions(&self) -> usize {
        self.pack_object_count as usize + self.large_blobs.len()
    }

    /// The reachability bitmap for commit `oid`, if one was selected for it.
    pub fn bitmap_for(&self, oid: &Oid) -> Option<&Bitmap> {
        self.commits.iter().find(|(o, _)| o == oid).map(|(_, b)| b)
    }

    /// The oid at `position`: a pack object (resolved via `idx`) for low positions, or a large blob
    /// for high ones. `None` if out of range.
    pub fn oid_at(&self, position: usize, idx: &crate::IdxV2) -> Option<Oid> {
        let p = self.pack_object_count as usize;
        if position < p {
            Some(idx.oid_at_position(position))
        } else {
            self.large_blobs.get(position - p).copied()
        }
    }

    /// Serialize to a compact, zlib-compressed byte blob.
    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        use std::io::Write;
        let mut raw = Vec::new();
        raw.extend_from_slice(BITMAP_MAGIC);
        raw.extend_from_slice(&BITMAP_VERSION.to_be_bytes());
        raw.extend_from_slice(&self.pack_object_count.to_be_bytes());
        raw.extend_from_slice(&(self.large_blobs.len() as u32).to_be_bytes());
        for oid in &self.large_blobs {
            raw.extend_from_slice(oid.as_bytes());
        }
        raw.extend_from_slice(&(self.commits.len() as u32).to_be_bytes());
        for (oid, bm) in &self.commits {
            raw.extend_from_slice(oid.as_bytes());
            raw.extend_from_slice(&(bm.words.len() as u32).to_be_bytes());
            for w in &bm.words {
                raw.extend_from_slice(&w.to_le_bytes());
            }
        }
        let mut enc = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        enc.write_all(&raw)
            .map_err(|e| CodecError::Io(e.to_string()))?;
        enc.finish().map_err(|e| CodecError::Io(e.to_string()))
    }

    /// Parse a blob produced by [`encode`](Self::encode).
    pub fn decode(bytes: &[u8]) -> Result<PackBitmaps, CodecError> {
        use std::io::Read;
        let mut raw = Vec::new();
        flate2::read::ZlibDecoder::new(bytes)
            .read_to_end(&mut raw)
            .map_err(|e| CodecError::Io(e.to_string()))?;

        let mut r = Cursor { buf: &raw, pos: 0 };
        if r.take(4)? != BITMAP_MAGIC {
            return Err(CodecError::BadObject("bad bitmap magic".into()));
        }
        let version = u32::from_be_bytes(r.take(4)?.try_into().unwrap());
        if version != BITMAP_VERSION {
            return Err(CodecError::BadObject(format!("bitmap version {version}")));
        }
        let pack_object_count = u32::from_be_bytes(r.take(4)?.try_into().unwrap());
        let l = u32::from_be_bytes(r.take(4)?.try_into().unwrap()) as usize;
        let mut large_blobs = Vec::with_capacity(l);
        for _ in 0..l {
            large_blobs.push(Oid::from_bytes(r.take(20)?)?);
        }
        let t = u32::from_be_bytes(r.take(4)?.try_into().unwrap()) as usize;
        let mut commits = Vec::with_capacity(t);
        for _ in 0..t {
            let oid = Oid::from_bytes(r.take(20)?)?;
            let nwords = u32::from_be_bytes(r.take(4)?.try_into().unwrap()) as usize;
            let mut words = Vec::with_capacity(nwords);
            for _ in 0..nwords {
                words.push(u64::from_le_bytes(r.take(8)?.try_into().unwrap()));
            }
            commits.push((oid, Bitmap { words }));
        }
        Ok(PackBitmaps {
            pack_object_count,
            large_blobs,
            commits,
        })
    }
}

/// Minimal forward byte cursor for decoding.
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], CodecError> {
        let s = self
            .buf
            .get(self.pos..self.pos + n)
            .ok_or_else(|| CodecError::BadObject("truncated bitmap".into()))?;
        self.pos += n;
        Ok(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oid(b: u8) -> Oid {
        Oid::from_array([b; 20])
    }

    #[test]
    fn bitmap_set_get_and_ops() {
        let mut a = Bitmap::with_capacity(10);
        a.set(1);
        a.set(64);
        a.set(200);
        assert!(a.get(1) && a.get(64) && a.get(200));
        assert!(!a.get(2));
        assert_eq!(a.count_ones(), 3);
        assert_eq!(a.iter_ones().collect::<Vec<_>>(), vec![1, 64, 200]);

        let mut b = Bitmap::with_capacity(10);
        b.set(64);
        b.set(5);

        let mut u = a.clone();
        u.or_assign(&b);
        assert_eq!(u.iter_ones().collect::<Vec<_>>(), vec![1, 5, 64, 200]);

        let mut d = a.clone();
        d.andnot_assign(&b); // remove 64
        assert_eq!(d.iter_ones().collect::<Vec<_>>(), vec![1, 200]);
    }

    #[test]
    fn pack_bitmaps_roundtrip() {
        let mut bm0 = Bitmap::with_capacity(5);
        bm0.set(0);
        bm0.set(3);
        let mut bm1 = Bitmap::with_capacity(5);
        bm1.set(1);
        bm1.set(4); // a large-blob position (pack_object_count = 4 → position 4 is large_blobs[0])

        let pb = PackBitmaps {
            pack_object_count: 4,
            large_blobs: vec![oid(0xaa)],
            commits: vec![(oid(0x01), bm0), (oid(0x02), bm1)],
        };
        let encoded = pb.encode().unwrap();
        let decoded = PackBitmaps::decode(&encoded).unwrap();
        assert_eq!(decoded, pb);
        assert_eq!(decoded.total_positions(), 5);
        assert!(decoded.bitmap_for(&oid(0x01)).unwrap().get(3));
        assert_eq!(decoded.large_blobs[0], oid(0xaa));
    }
}
