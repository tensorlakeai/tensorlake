//! Git delta encoding/decoding (the copy/insert instruction stream used by `OFS_DELTA` and
//! `REF_DELTA` packfile entries).

use crate::CodecError;

/// Read a little-endian base-128 varint (git's "size" encoding) from `buf` at `*pos`.
pub(crate) fn read_size_varint(buf: &[u8], pos: &mut usize) -> Result<u64, CodecError> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    loop {
        let c = *buf.get(*pos).ok_or(CodecError::TruncatedDelta)?;
        *pos += 1;
        result |= ((c & 0x7f) as u64) << shift;
        if c & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift >= 64 {
            return Err(CodecError::TruncatedDelta);
        }
    }
    Ok(result)
}

/// Apply a git delta stream to `base`, producing the target object bytes.
///
/// The stream begins with the (varint) source and target sizes, followed by a sequence of
/// copy (high bit set) and insert (high bit clear) instructions.
pub fn apply_delta(base: &[u8], delta: &[u8]) -> Result<Vec<u8>, CodecError> {
    let mut pos = 0usize;

    let src_size = read_size_varint(delta, &mut pos)?;
    if src_size as usize != base.len() {
        return Err(CodecError::DeltaBaseSizeMismatch {
            expected: src_size,
            actual: base.len() as u64,
        });
    }
    let tgt_size = read_size_varint(delta, &mut pos)? as usize;
    let mut out = Vec::with_capacity(tgt_size);

    while pos < delta.len() {
        let op = delta[pos];
        pos += 1;
        if op & 0x80 != 0 {
            // Copy from base: variable offset (4 optional bytes) and size (3 optional bytes).
            let mut cp_off: u64 = 0;
            let mut cp_size: u64 = 0;
            for (i, mask) in [0x01u8, 0x02, 0x04, 0x08].into_iter().enumerate() {
                if op & mask != 0 {
                    cp_off |=
                        (*delta.get(pos).ok_or(CodecError::TruncatedDelta)? as u64) << (8 * i);
                    pos += 1;
                }
            }
            for (i, mask) in [0x10u8, 0x20, 0x40].into_iter().enumerate() {
                if op & mask != 0 {
                    cp_size |=
                        (*delta.get(pos).ok_or(CodecError::TruncatedDelta)? as u64) << (8 * i);
                    pos += 1;
                }
            }
            if cp_size == 0 {
                cp_size = 0x10000;
            }
            let start = cp_off as usize;
            let end = start
                .checked_add(cp_size as usize)
                .ok_or(CodecError::TruncatedDelta)?;
            let slice = base
                .get(start..end)
                .ok_or(CodecError::DeltaCopyOutOfRange {
                    start: start as u64,
                    end: end as u64,
                    base_len: base.len() as u64,
                })?;
            out.extend_from_slice(slice);
        } else if op != 0 {
            // Insert: the opcode byte itself is the literal length.
            let len = op as usize;
            let end = pos.checked_add(len).ok_or(CodecError::TruncatedDelta)?;
            let slice = delta.get(pos..end).ok_or(CodecError::TruncatedDelta)?;
            out.extend_from_slice(slice);
            pos = end;
        } else {
            // 0x00 is reserved and must not appear.
            return Err(CodecError::ReservedDeltaOpcode);
        }
    }

    if out.len() != tgt_size {
        return Err(CodecError::DeltaTargetSizeMismatch {
            expected: tgt_size as u64,
            actual: out.len() as u64,
        });
    }
    Ok(out)
}

/// Produce a (non-optimized) delta encoding the transformation from `base` to `target`.
///
/// This emits the two size headers followed by a single insert of the whole target — i.e. a
/// valid, decodable delta that performs no copies. It exists so we can round-trip the
/// decoder/encoder and exercise `REF_DELTA`/`OFS_DELTA` write paths in tests; the production
/// delta-compressor (window matching) is a separate optimization.
pub fn encode_trivial_delta(base: &[u8], target: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    write_size_varint(&mut out, base.len() as u64);
    write_size_varint(&mut out, target.len() as u64);
    // Insert ops carry at most 127 bytes each.
    for chunk in target.chunks(0x7f) {
        out.push(chunk.len() as u8);
        out.extend_from_slice(chunk);
    }
    out
}

/// Block size that must match to start a copy. Bigger → fewer/cheaper index entries but misses
/// short common runs; 16 mirrors git's diff-delta granularity.
const DELTA_BLOCK: usize = 16;
/// Cap on candidate base positions examined per block hash (bounds worst-case time).
const DELTA_MAX_CANDIDATES: usize = 8;

/// A precomputed content-block index of a base object, reusable across many targets.
///
/// Building the index is the expensive part of delta encoding, so the pack repacker indexes each
/// candidate base **once** and encodes every object in its delta window against the cached index —
/// instead of rebuilding the base index per (base, target) pair (which was the dominant compaction
/// cost). Hash matches are byte-verified, so collisions only cost a missed copy, never corruption.
pub struct DeltaIndex {
    base: Vec<u8>,
    /// block content hash → base offsets (earliest first, so copies prefer small offsets).
    index: std::collections::HashMap<u64, Vec<u32>>,
}

impl DeltaIndex {
    /// Index every `DELTA_BLOCK`-byte window of `base`.
    pub fn build(base: &[u8]) -> DeltaIndex {
        let mut index: std::collections::HashMap<u64, Vec<u32>> = std::collections::HashMap::new();
        if base.len() >= DELTA_BLOCK {
            for i in 0..=base.len() - DELTA_BLOCK {
                index
                    .entry(block_hash(&base[i..i + DELTA_BLOCK]))
                    .or_default()
                    .push(i as u32);
            }
        }
        DeltaIndex {
            base: base.to_vec(),
            index,
        }
    }

    /// Length of the indexed base.
    pub fn base_len(&self) -> usize {
        self.base.len()
    }

    /// Encode the delta transforming this index's base into `target`.
    pub fn encode(&self, target: &[u8]) -> Vec<u8> {
        let base = &self.base;
        let mut out = Vec::new();
        write_size_varint(&mut out, base.len() as u64);
        write_size_varint(&mut out, target.len() as u64);

        let mut pending: Vec<u8> = Vec::new();
        let mut t = 0usize;
        while t < target.len() {
            let mut best_len = 0usize;
            let mut best_off = 0usize;
            if t + DELTA_BLOCK <= target.len() {
                if let Some(cands) = self.index.get(&block_hash(&target[t..t + DELTA_BLOCK])) {
                    for &b in cands.iter().take(DELTA_MAX_CANDIDATES) {
                        let b = b as usize;
                        if base[b..b + DELTA_BLOCK] != target[t..t + DELTA_BLOCK] {
                            continue; // hash collision
                        }
                        let mut len = DELTA_BLOCK;
                        while b + len < base.len()
                            && t + len < target.len()
                            && base[b + len] == target[t + len]
                        {
                            len += 1;
                        }
                        len = len.min(0xFF_FFFF); // copy size fits in 3 bytes
                        if len > best_len {
                            best_len = len;
                            best_off = b;
                        }
                    }
                }
            }
            if best_len >= DELTA_BLOCK {
                flush_insert(&mut out, &mut pending);
                emit_copy(&mut out, best_off as u64, best_len as u64);
                t += best_len;
            } else {
                pending.push(target[t]);
                t += 1;
                if pending.len() == 0x7f {
                    flush_insert(&mut out, &mut pending);
                }
            }
        }
        flush_insert(&mut out, &mut pending);
        out
    }
}

/// Produce an optimized git delta from `base` to `target` (convenience over [`DeltaIndex`]). For
/// repeated encodes against the same base, build a [`DeltaIndex`] once and reuse it.
pub fn encode_delta(base: &[u8], target: &[u8]) -> Vec<u8> {
    DeltaIndex::build(base).encode(target)
}

fn block_hash(b: &[u8]) -> u64 {
    // FNV-1a over the block (exact match is byte-verified by the caller).
    let mut h = 0xcbf29ce484222325u64;
    for &x in b {
        h ^= x as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Flush buffered literals as insert instructions (each ≤ 0x7f bytes).
fn flush_insert(out: &mut Vec<u8>, pending: &mut Vec<u8>) {
    for chunk in pending.chunks(0x7f) {
        out.push(chunk.len() as u8);
        out.extend_from_slice(chunk);
    }
    pending.clear();
}

/// Emit a copy instruction: opcode (high bit set) with a mask bit per present offset/size byte,
/// followed by those little-endian bytes.
fn emit_copy(out: &mut Vec<u8>, off: u64, size: u64) {
    let mut op = 0x80u8;
    let mut bytes = Vec::with_capacity(7);
    for i in 0..4 {
        let b = ((off >> (8 * i)) & 0xff) as u8;
        if b != 0 {
            op |= 1 << i;
            bytes.push(b);
        }
    }
    for i in 0..3 {
        let b = ((size >> (8 * i)) & 0xff) as u8;
        if b != 0 {
            op |= 0x10 << i;
            bytes.push(b);
        }
    }
    out.push(op);
    out.extend_from_slice(&bytes);
}

pub(crate) fn write_size_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let mut byte = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if v == 0 {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_varint_roundtrip() {
        for v in [0u64, 1, 127, 128, 300, 16384, 1 << 35] {
            let mut buf = Vec::new();
            write_size_varint(&mut buf, v);
            let mut pos = 0;
            assert_eq!(read_size_varint(&buf, &mut pos).unwrap(), v);
            assert_eq!(pos, buf.len());
        }
    }

    #[test]
    fn trivial_delta_roundtrips() {
        let base = b"the quick brown fox";
        let target = b"a completely different target string of some length";
        let d = encode_trivial_delta(base, target);
        let got = apply_delta(base, &d).unwrap();
        assert_eq!(&got, target);
    }

    #[test]
    fn optimized_delta_roundtrips_and_compresses() {
        // A target that shares long runs with the base (an edit in the middle) should round-trip
        // exactly and encode much smaller than the target itself.
        let base: Vec<u8> = (0..4096u32).map(|i| (i * 31 + 7) as u8).collect();
        let mut target = base.clone();
        target.splice(2000..2010, b"INSERTED!!".iter().copied());
        let d = encode_delta(&base, &target);
        assert_eq!(apply_delta(&base, &d).unwrap(), target);
        assert!(
            d.len() < target.len() / 4,
            "delta {} vs target {}",
            d.len(),
            target.len()
        );
    }

    #[test]
    fn optimized_delta_handles_unrelated_and_empty() {
        let base = b"the quick brown fox jumps over the lazy dog";
        let target = b"completely unrelated content here, no overlap at all really";
        assert_eq!(
            apply_delta(base, &encode_delta(base, target)).unwrap(),
            target
        );
        // Empty base / empty target edge cases.
        assert_eq!(
            apply_delta(b"", &encode_delta(b"", b"hello")).unwrap(),
            b"hello"
        );
        assert_eq!(apply_delta(base, &encode_delta(base, b"")).unwrap(), b"");
    }

    #[test]
    fn copy_instruction_applies() {
        // Hand-built delta: base len 5, target len 5, copy 5 bytes from offset 0.
        let base = b"hello";
        let mut delta = Vec::new();
        write_size_varint(&mut delta, 5);
        write_size_varint(&mut delta, 5);
        delta.push(0x80 | 0x01 | 0x10); // copy, offset byte + size byte
        delta.push(0x00); // offset 0
        delta.push(0x05); // size 5
        let got = apply_delta(base, &delta).unwrap();
        assert_eq!(&got, b"hello");
    }

    #[test]
    fn mixed_copy_and_insert() {
        // target = base[0..3] + "XYZ" + base[3..6]
        let base = b"ABCDEF";
        let mut delta = Vec::new();
        write_size_varint(&mut delta, 6);
        write_size_varint(&mut delta, 9);
        delta.push(0x80 | 0x01 | 0x10);
        delta.push(0); // off 0
        delta.push(3); // size 3 -> "ABC"
        delta.push(3); // insert 3
        delta.extend_from_slice(b"XYZ");
        delta.push(0x80 | 0x01 | 0x10);
        delta.push(3); // off 3
        delta.push(3); // size 3 -> "DEF"
        let got = apply_delta(base, &delta).unwrap();
        assert_eq!(&got, b"ABCXYZDEF");
    }

    #[test]
    fn rejects_base_size_mismatch() {
        let mut delta = Vec::new();
        write_size_varint(&mut delta, 99);
        write_size_varint(&mut delta, 0);
        assert!(apply_delta(b"short", &delta).is_err());
    }
}
