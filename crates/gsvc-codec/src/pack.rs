//! Packfile v2 reading and writing.
//!
//! The writer emits canonical, non-deltified v2 packs (binaries/large objects never reach this
//! path — see `gsvc-bigobj`). The reader resolves the full entry grammar including `OFS_DELTA`
//! and `REF_DELTA`, with an optional external base resolver for thin packs received over the wire.

use std::collections::{HashMap, VecDeque};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use bytes::Bytes;
use flate2::write::ZlibEncoder;
use flate2::{Compression, Decompress, FlushDecompress, Status};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};

use crate::delta::apply_delta;
use crate::{CodecError, Kind, Object, Oid};

const PACK_MAGIC: &[u8; 4] = b"PACK";
const PACK_VERSION: u32 = 2;
const PARALLEL_RESOLVE_BATCH: usize = 1024;

/// Chunk size for the parallel external-ref filter after resolve.
const EXTERNAL_REF_FILTER_CHUNK: usize = 64 * 1024;

/// Bounded queue depth (in per-object tree-entry batches) for the source-index writer thread.
const SOURCE_INDEX_WRITER_QUEUE: usize = 4096;

// Packfile entry type codes.
const T_COMMIT: u8 = 1;
const T_TREE: u8 = 2;
const T_BLOB: u8 = 3;
const T_TAG: u8 = 4;
const T_OFS_DELTA: u8 = 6;
const T_REF_DELTA: u8 = 7;

/// Cap for the up-front allocation hint taken from a pack's self-declared object count. That count
/// lives in the 12-byte header and is **attacker-controlled**, so a lying value (e.g. `u32::MAX`) fed
/// straight into `Vec::with_capacity(count)` would try to reserve hundreds of GB — and an allocation
/// failure *aborts the whole process* (it can't be caught). We therefore use the count only as a
/// *hint*, pre-sizing up to this cap and letting the collection grow naturally for genuinely huge
/// packs (a few reallocations, negligible beside parsing millions of objects). The parse loop still
/// uses the real count and fails cleanly via `?` the moment it runs past the available bytes.
pub(crate) const PREALLOC_HINT_CAP: usize = 1 << 20;

/// Index metadata for one object as written into a pack: its id, byte offset within the pack,
/// and the CRC-32 of its on-disk (header + compressed) representation. Feeds idx v2 generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PackedEntry {
    pub oid: Oid,
    pub offset: u64,
    pub crc32: u32,
}

/// The result of building a pack: the bytes, the per-object index entries, and the pack trailer
/// SHA-1 (also the pack's name in git).
pub struct BuiltPack {
    pub data: Vec<u8>,
    pub entries: Vec<PackedEntry>,
    pub object_entries: Vec<PackObjectEntry>,
    pub pack_hash: Oid,
}

/// Encode `objects` into a non-deltified v2 packfile.
pub fn build_pack(objects: &[Object]) -> Result<BuiltPack, CodecError> {
    build_pack_at_level(objects, PUSH_COMPRESSION)
}

/// Compression level for the **push-time** pack. Deliberately fast (level 1): a freshly-received
/// pack is transient — background compaction soon repacks it delta-compressed at a higher ratio — so
/// minimizing CPU on the push hot path beats squeezing out bytes that get rewritten minutes later.
const PUSH_COMPRESSION: u32 = 1;
type SortedObjectIds = Vec<Oid>;

/// Persistable metadata for one object entry in a pack. Unlike the legacy `(oid, offset)` location
/// rows, this includes enough information for background workers to reason about pack contents and
/// range-read non-delta entries without replaying the full resolver.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PackObjectEntry {
    pub oid: Oid,
    pub kind: PackObjectKind,
    pub resolved_kind: Kind,
    pub offset: u64,
    pub compressed_offset: u64,
    pub compressed_len: u64,
    pub declared_size: u64,
    pub resolved_size: u64,
    pub crc32: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PackObjectKind {
    Full(Kind),
    OfsDelta {
        base_offset: u64,
        base_oid: Oid,
        depth: u32,
    },
    RefDelta {
        base_oid: Oid,
        depth: u32,
    },
}

const OBJECT_INDEX_MAGIC: &[u8; 8] = b"GSOBIDX1";
const SOURCE_INDEX_MAGIC: &[u8; 8] = b"GSSRCIX1";

pub fn write_pack_object_index_sidecar<'a, W, I>(
    mut writer: W,
    object_count: usize,
    entries: I,
) -> Result<(), CodecError>
where
    W: Write,
    I: IntoIterator<Item = &'a PackObjectEntry>,
{
    writer
        .write_all(OBJECT_INDEX_MAGIC)
        .map_err(|e| CodecError::Io(e.to_string()))?;
    write_u64(&mut writer, object_count as u64)?;
    for entry in entries {
        write_pack_object_index_entry(&mut writer, entry)?;
    }
    writer.flush().map_err(|e| CodecError::Io(e.to_string()))?;
    Ok(())
}

fn write_pack_object_index_entry<W: Write>(
    writer: &mut W,
    entry: &PackObjectEntry,
) -> Result<(), CodecError> {
    write_oid(writer, &entry.oid)?;
    match &entry.kind {
        PackObjectKind::Full(kind) => {
            write_u8(writer, 0)?;
            write_kind(writer, *kind)?;
        }
        PackObjectKind::OfsDelta {
            base_offset,
            base_oid,
            depth,
        } => {
            write_u8(writer, 1)?;
            write_u64(writer, *base_offset)?;
            write_oid(writer, base_oid)?;
            write_u32(writer, *depth)?;
        }
        PackObjectKind::RefDelta { base_oid, depth } => {
            write_u8(writer, 2)?;
            write_oid(writer, base_oid)?;
            write_u32(writer, *depth)?;
        }
    }
    write_kind(writer, entry.resolved_kind)?;
    write_u64(writer, entry.offset)?;
    write_u64(writer, entry.compressed_offset)?;
    write_u64(writer, entry.compressed_len)?;
    write_u64(writer, entry.declared_size)?;
    write_u64(writer, entry.resolved_size)?;
    write_u32(writer, entry.crc32)?;
    Ok(())
}

pub fn encode_pack_object_index_sidecar(
    entries: &[PackObjectEntry],
) -> Result<Vec<u8>, CodecError> {
    let mut out = Vec::with_capacity(entries.len().saturating_mul(80).saturating_add(16));
    write_pack_object_index_sidecar(&mut out, entries.len(), entries.iter())?;
    Ok(out)
}

pub fn decode_pack_object_index_sidecar(
    sidecar: &[u8],
) -> Result<Vec<PackObjectEntry>, CodecError> {
    let mut cursor = std::io::Cursor::new(sidecar);
    let mut magic = [0u8; 8];
    cursor
        .read_exact(&mut magic)
        .map_err(|e| CodecError::Io(e.to_string()))?;
    if &magic != OBJECT_INDEX_MAGIC {
        return Err(CodecError::Io("invalid object-index sidecar magic".into()));
    }
    let count = read_u64(&mut cursor)?;
    let count_usize = usize::try_from(count)
        .map_err(|_| CodecError::Io("object-index sidecar count overflows usize".into()))?;
    let mut entries = Vec::with_capacity(count_usize.min(PREALLOC_HINT_CAP));
    for _ in 0..count_usize {
        let oid = read_oid(&mut cursor)?;
        let kind = match read_u8(&mut cursor)? {
            0 => PackObjectKind::Full(read_kind(&mut cursor)?),
            1 => PackObjectKind::OfsDelta {
                base_offset: read_u64(&mut cursor)?,
                base_oid: read_oid(&mut cursor)?,
                depth: read_u32(&mut cursor)?,
            },
            2 => PackObjectKind::RefDelta {
                base_oid: read_oid(&mut cursor)?,
                depth: read_u32(&mut cursor)?,
            },
            other => {
                return Err(CodecError::Io(format!(
                    "invalid object-index sidecar kind tag {other}"
                )))
            }
        };
        entries.push(PackObjectEntry {
            oid,
            kind,
            resolved_kind: read_kind(&mut cursor)?,
            offset: read_u64(&mut cursor)?,
            compressed_offset: read_u64(&mut cursor)?,
            compressed_len: read_u64(&mut cursor)?,
            declared_size: read_u64(&mut cursor)?,
            resolved_size: read_u64(&mut cursor)?,
            crc32: read_u32(&mut cursor)?,
        });
    }
    if cursor.position() != sidecar.len() as u64 {
        return Err(CodecError::Io(
            "object-index sidecar has trailing bytes".into(),
        ));
    }
    Ok(entries)
}

/// Encode `objects` into a non-deltified v2 packfile at zlib `level`, deflating and hashing every
/// object **in parallel** (the per-object work is independent), then assembling the pack serially.
/// This is the dominant cost of a large first push, so the parallelism is what keeps a half-million-
/// object push in single-digit seconds rather than minutes.
pub fn build_pack_at_level(objects: &[Object], level: u32) -> Result<BuiltPack, CodecError> {
    use rayon::prelude::*;

    // Phase 1 (parallel): each object → (entry bytes [header + zlib], oid, crc32). All independent.
    let prepared: Vec<(Vec<u8>, Oid, u32, Kind, u64, usize)> = objects
        .par_iter()
        .map(|obj| {
            let mut entry_bytes = Vec::with_capacity(obj.data.len() / 2 + 16);
            write_entry_header(
                &mut entry_bytes,
                obj.kind.pack_type(),
                obj.data.len() as u64,
            );
            let compressed_header_len = entry_bytes.len();
            let mut enc = ZlibEncoder::new(Vec::new(), Compression::new(level));
            enc.write_all(&obj.data)
                .map_err(|e| CodecError::Io(e.to_string()))?;
            let compressed = enc.finish().map_err(|e| CodecError::Io(e.to_string()))?;
            entry_bytes.extend_from_slice(&compressed);
            let crc = crc32fast::hash(&entry_bytes);
            Ok((
                entry_bytes,
                obj.id(),
                crc,
                obj.kind,
                obj.data.len() as u64,
                compressed_header_len,
            ))
        })
        .collect::<Result<Vec<_>, CodecError>>()?;

    // Phase 2 (serial): lay the entries out at their offsets and assemble the pack.
    let total: usize = prepared.iter().map(|(b, ..)| b.len()).sum();
    let mut out = Vec::with_capacity(12 + total + 20);
    out.extend_from_slice(PACK_MAGIC);
    out.extend_from_slice(&PACK_VERSION.to_be_bytes());
    out.extend_from_slice(&(objects.len() as u32).to_be_bytes());
    let mut entries = Vec::with_capacity(objects.len());
    let mut object_entries = Vec::with_capacity(objects.len());
    for (entry_bytes, oid, crc32, kind, declared_size, compressed_header_len) in prepared {
        let offset = out.len() as u64;
        let compressed_offset = offset + compressed_header_len as u64;
        let compressed_len = (entry_bytes.len() - compressed_header_len) as u64;
        entries.push(PackedEntry { oid, offset, crc32 });
        object_entries.push(PackObjectEntry {
            oid,
            kind: PackObjectKind::Full(kind),
            resolved_kind: kind,
            offset,
            compressed_offset,
            compressed_len,
            declared_size,
            resolved_size: declared_size,
            crc32,
        });
        out.extend_from_slice(&entry_bytes);
    }

    // Trailer: SHA-1 over everything written so far.
    let mut h = Sha1::new();
    h.update(&out);
    let digest: [u8; 20] = h.finalize().into();
    out.extend_from_slice(&digest);

    Ok(BuiltPack {
        data: out,
        entries,
        object_entries,
        pack_hash: Oid::from_array(digest),
    })
}

/// Tuning for [`build_pack_delta`].
const DELTA_WINDOW: usize = 10;
const DELTA_MAX_DEPTH: usize = 50;
/// Serving packs trade a little ratio for materially cheaper client `index-pack`.
const SERVING_DELTA_MAX_DEPTH: usize = 12;
const DELTA_MAX_SIZE: usize = 1 << 20; // don't deltify objects above 1 MiB

#[derive(Clone, Copy, Debug)]
struct DeltaPackOptions {
    window: usize,
    max_depth: usize,
    max_size: usize,
    search_deltas: bool,
}

impl DeltaPackOptions {
    fn default_compaction() -> DeltaPackOptions {
        DeltaPackOptions {
            window: DELTA_WINDOW,
            max_depth: DELTA_MAX_DEPTH,
            max_size: DELTA_MAX_SIZE,
            search_deltas: true,
        }
    }

    fn serving(search_deltas: bool) -> DeltaPackOptions {
        DeltaPackOptions {
            window: DELTA_WINDOW,
            max_depth: SERVING_DELTA_MAX_DEPTH,
            max_size: DELTA_MAX_SIZE,
            search_deltas,
        }
    }
}

/// Encode `objects` into a **delta-compressed** v2 packfile. Each object is tried against a sliding
/// window of recently-emitted same-type objects; if a delta against one is smaller than the object
/// itself (and within the chain-depth limit), it is stored as an `OFS_DELTA`, otherwise full. Bases
/// are always emitted before the deltas that reference them, so the result is self-contained and
/// resolvable by [`parse_pack`]. Used by compaction to shrink repacked packs (design §10, §14.1).
pub fn build_pack_delta(objects: &[Object]) -> Result<BuiltPack, CodecError> {
    build_pack_delta_reuse(objects, &HashMap::new())
}

/// Encode `objects` into a delta-compressed pack tuned for clone serving rather than maximum ratio.
///
/// Git clients pay delta-chain cost during `index-pack`; keeping the chain ceiling well below git's
/// default 50 improves full-clone latency on large repos while retaining the bulk of delta savings.
pub fn build_pack_delta_for_serving(objects: &[Object]) -> Result<BuiltPack, CodecError> {
    build_pack_delta_reuse_for_serving(objects, &HashMap::new())
}

/// An object's existing delta from a source pack, ready to re-emit verbatim while testing pack
/// reuse behavior. Runtime GC paths use the file-backed/indexed pack model instead of this heap
/// parser/rebuilder path.
#[derive(Clone, Debug)]
struct ReusableDelta {
    base_oid: Oid,
    /// Inflated delta length (the entry header's size field).
    delta_len: u64,
    /// The raw zlib stream of the delta instructions, copyable straight into the new pack.
    zdelta: Vec<u8>,
}

fn build_pack_delta_reuse(
    objects: &[Object],
    reuse: &HashMap<Oid, ReusableDelta>,
) -> Result<BuiltPack, CodecError> {
    build_pack_delta_reuse_with_options(objects, reuse, DeltaPackOptions::default_compaction())
}

fn build_pack_delta_reuse_for_serving(
    objects: &[Object],
    reuse: &HashMap<Oid, ReusableDelta>,
) -> Result<BuiltPack, CodecError> {
    build_pack_delta_reuse_with_options(objects, reuse, DeltaPackOptions::serving(reuse.is_empty()))
}

fn build_pack_delta_reuse_with_options(
    objects: &[Object],
    reuse: &HashMap<Oid, ReusableDelta>,
    opts: DeltaPackOptions,
) -> Result<BuiltPack, CodecError> {
    let n = objects.len();
    // Hash every object once (used for the index, reuse lookup, and the entry record).
    let ids: Vec<Oid> = objects.iter().map(|o| o.id()).collect();
    let oid_to_idx: HashMap<Oid, usize> = ids.iter().enumerate().map(|(i, o)| (*o, i)).collect();

    // Resolve reuse hints to in-set base indices (a hint whose base isn't in this pack is dropped).
    let mut reuse_base: Vec<Option<usize>> = vec![None; n];
    let mut reuse_rd: Vec<Option<&ReusableDelta>> = vec![None; n];
    if !reuse.is_empty() {
        for (i, id) in ids.iter().enumerate() {
            if let Some(rd) = reuse.get(id) {
                if let Some(&bi) = oid_to_idx.get(&rd.base_oid) {
                    if bi != i {
                        reuse_base[i] = Some(bi);
                        reuse_rd[i] = Some(rd);
                    }
                }
            }
        }
    }

    // Primary order: group by type, then size descending (git's heuristic), so a smaller object
    // tends to deltify against a larger, similar predecessor in the window.
    let mut order: Vec<usize> = (0..n).collect();
    order.sort_by(|&a, &b| {
        objects[a]
            .kind
            .pack_type()
            .cmp(&objects[b].kind.pack_type())
            .then(objects[b].data.len().cmp(&objects[a].data.len()))
    });

    // Emit order: the primary order, but every reused-delta base is emitted before its target
    // (iterative DFS over reuse edges). A reuse edge that would form a cycle (conflicting source
    // packs) is dropped so the order is always well-defined.
    let mut emit_order: Vec<usize> = Vec::with_capacity(n);
    let mut visited = vec![false; n];
    let mut on_stack = vec![false; n];
    for &start in &order {
        if visited[start] {
            continue;
        }
        let mut stack = vec![start];
        while let Some(&oi) = stack.last() {
            if visited[oi] {
                stack.pop();
                continue;
            }
            match reuse_base[oi] {
                Some(bi) if !visited[bi] && !on_stack[bi] => {
                    on_stack[oi] = true;
                    stack.push(bi);
                }
                Some(bi) if on_stack[bi] => {
                    // Cycle: break it by dropping this object's reuse.
                    reuse_base[oi] = None;
                    reuse_rd[oi] = None;
                    visited[oi] = true;
                    on_stack[oi] = false;
                    emit_order.push(oi);
                    stack.pop();
                }
                _ => {
                    visited[oi] = true;
                    on_stack[oi] = false;
                    emit_order.push(oi);
                    stack.pop();
                }
            }
        }
    }

    let mut out = Vec::with_capacity(64 + n * 32);
    out.extend_from_slice(PACK_MAGIC);
    out.extend_from_slice(&PACK_VERSION.to_be_bytes());
    out.extend_from_slice(&(n as u32).to_be_bytes());

    let mut entries = Vec::with_capacity(n);
    let mut object_entries = Vec::with_capacity(n);
    let mut emitted_offset: Vec<Option<u64>> = vec![None; n];
    let mut emitted_depth: Vec<usize> = vec![0; n];
    let mut window: VecDeque<(usize, crate::delta::DeltaIndex)> =
        VecDeque::with_capacity(opts.window);

    for &oi in &emit_order {
        let obj = &objects[oi];
        let offset = out.len() as u64;
        let mut entry_bytes = Vec::new();
        let mut object_kind = PackObjectKind::Full(obj.kind);
        let mut declared_size = obj.data.len() as u64;

        // Reuse path: copy the existing delta verbatim if its base is already emitted and the chain
        // depth stays within bounds.
        let reused = match (reuse_base[oi], reuse_rd[oi]) {
            (Some(bi), Some(rd))
                if emitted_offset[bi].is_some() && emitted_depth[bi] < opts.max_depth =>
            {
                let rel = offset - emitted_offset[bi].unwrap();
                write_entry_header(&mut entry_bytes, T_OFS_DELTA, rd.delta_len);
                write_offset_varint(&mut entry_bytes, rel);
                let compressed_prefix_len = entry_bytes.len();
                entry_bytes.extend_from_slice(&rd.zdelta);
                object_kind = PackObjectKind::OfsDelta {
                    base_offset: emitted_offset[bi].unwrap(),
                    base_oid: rd.base_oid,
                    depth: emitted_depth[bi] as u32 + 1,
                };
                declared_size = rd.delta_len;
                Some((emitted_depth[bi] + 1, compressed_prefix_len))
            }
            _ => None,
        };

        let (depth, compressed_prefix_len) = if let Some(reused) = reused {
            reused
        } else {
            // Sliding-window delta search (same as the from-scratch builder), disabled for
            // serving-pack rebuilds that already have source-pack deltas to reuse.
            let mut best: Option<(usize, Vec<u8>)> = None;
            if opts.search_deltas && obj.data.len() <= opts.max_size {
                for (bi, index) in &window {
                    let base = &objects[*bi];
                    if base.kind != obj.kind || index.base_len() > opts.max_size {
                        continue;
                    }
                    if emitted_depth[*bi] + 1 > opts.max_depth {
                        continue;
                    }
                    let delta = index.encode(&obj.data);
                    if delta.len() < obj.data.len()
                        && best.as_ref().is_none_or(|(_, d)| delta.len() < d.len())
                    {
                        best = Some((*bi, delta));
                    }
                }
            }
            match &best {
                Some((bi, delta)) => {
                    let base_offset = emitted_offset[*bi].expect("base emitted before delta");
                    let rel = offset - base_offset;
                    declared_size = delta.len() as u64;
                    write_entry_header(&mut entry_bytes, T_OFS_DELTA, declared_size);
                    write_offset_varint(&mut entry_bytes, rel);
                    let compressed_prefix_len = entry_bytes.len();
                    append_zlib(&mut entry_bytes, delta)?;
                    object_kind = PackObjectKind::OfsDelta {
                        base_offset,
                        base_oid: ids[*bi],
                        depth: emitted_depth[*bi] as u32 + 1,
                    };
                    (emitted_depth[*bi] + 1, compressed_prefix_len)
                }
                None => {
                    declared_size = obj.data.len() as u64;
                    write_entry_header(&mut entry_bytes, obj.kind.pack_type(), declared_size);
                    let compressed_prefix_len = entry_bytes.len();
                    append_zlib(&mut entry_bytes, &obj.data)?;
                    object_kind = PackObjectKind::Full(obj.kind);
                    (0, compressed_prefix_len)
                }
            }
        };

        let mut crc = crc32fast::Hasher::new();
        crc.update(&entry_bytes);
        let crc32 = crc.finalize();
        let compressed_offset = offset + compressed_prefix_len as u64;
        let compressed_len = (entry_bytes.len() - compressed_prefix_len) as u64;
        entries.push(PackedEntry {
            oid: ids[oi],
            offset,
            crc32,
        });
        object_entries.push(PackObjectEntry {
            oid: ids[oi],
            kind: object_kind,
            resolved_kind: obj.kind,
            offset,
            compressed_offset,
            compressed_len,
            declared_size,
            resolved_size: obj.data.len() as u64,
            crc32,
        });
        out.extend_from_slice(&entry_bytes);

        emitted_offset[oi] = Some(offset);
        emitted_depth[oi] = depth;
        if opts.search_deltas {
            // Cache this object's delta index so later objects in the window can deltify against it.
            window.push_back((oi, crate::delta::DeltaIndex::build(&obj.data)));
            if window.len() > opts.window {
                window.pop_front();
            }
        }
    }

    let mut h = Sha1::new();
    h.update(&out);
    let digest: [u8; 20] = h.finalize().into();
    out.extend_from_slice(&digest);

    Ok(BuiltPack {
        data: out,
        entries,
        object_entries,
        pack_hash: Oid::from_array(digest),
    })
}

/// Zlib-compress `data` and append it to `out`.
fn append_zlib(out: &mut Vec<u8>, data: &[u8]) -> Result<(), CodecError> {
    let mut enc = ZlibEncoder::new(Vec::new(), Compression::default());
    enc.write_all(data)
        .map_err(|e| CodecError::Io(e.to_string()))?;
    let compressed = enc.finish().map_err(|e| CodecError::Io(e.to_string()))?;
    out.extend_from_slice(&compressed);
    Ok(())
}

/// Write git's `OFS_DELTA` base-offset encoding (the exact inverse of [`read_offset_varint`]).
fn write_offset_varint(out: &mut Vec<u8>, value: u64) {
    let mut tmp = [0u8; 10];
    let mut i = tmp.len() - 1;
    tmp[i] = (value & 0x7f) as u8;
    let mut v = value >> 7;
    while v != 0 {
        v -= 1;
        i -= 1;
        tmp[i] = 0x80 | (v & 0x7f) as u8;
        v >>= 7;
    }
    out.extend_from_slice(&tmp[i..]);
}

/// Write a packfile object's type/size header (3-bit type, base-128 size, low 4 bits first).
fn write_entry_header(out: &mut Vec<u8>, ty: u8, mut size: u64) {
    let mut byte = (ty << 4) | (size as u8 & 0x0f);
    size >>= 4;
    while size != 0 {
        out.push(byte | 0x80);
        byte = (size & 0x7f) as u8;
        size >>= 7;
    }
    out.push(byte);
}

/// A fully-parsed pack: objects in pack order, plus an offset→oid map for idx generation.
pub struct ParsedPack {
    pub objects: Vec<(Oid, Object)>,
    pub offsets: Vec<PackedEntry>,
}

/// Parse a v2 packfile, resolving all deltas. `resolver` supplies bases for `REF_DELTA` entries
/// whose base is not contained in this pack (thin packs); pass `|_| None` for self-contained packs.
pub fn parse_pack<R>(data: &[u8], mut resolver: R) -> Result<ParsedPack, CodecError>
where
    R: FnMut(&Oid) -> Option<Object>,
{
    if data.len() < 12 + 20 {
        return Err(CodecError::PackTooShort);
    }
    if &data[0..4] != PACK_MAGIC {
        return Err(CodecError::BadPackMagic);
    }
    let version = u32::from_be_bytes(data[4..8].try_into().unwrap());
    if version != PACK_VERSION {
        return Err(CodecError::BadPackVersion(version));
    }
    let count = u32::from_be_bytes(data[8..12].try_into().unwrap()) as usize;

    // Verify the trailer hash before trusting any bytes.
    let body_end = data.len() - 20;
    let mut h = Sha1::new();
    h.update(&data[..body_end]);
    let computed: [u8; 20] = h.finalize().into();
    if computed != data[body_end..] {
        return Err(CodecError::PackChecksumMismatch);
    }

    let mut pos = 12usize;
    let mut by_offset: HashMap<u64, (Kind, Vec<u8>)> =
        HashMap::with_capacity(count.min(PREALLOC_HINT_CAP));
    let mut by_oid: HashMap<Oid, (Kind, Vec<u8>)> =
        HashMap::with_capacity(count.min(PREALLOC_HINT_CAP));
    let mut objects = Vec::with_capacity(count.min(PREALLOC_HINT_CAP));
    let mut offsets = Vec::with_capacity(count.min(PREALLOC_HINT_CAP));

    for _ in 0..count {
        let entry_start = pos as u64;
        let (ty, size) = read_entry_header(data, &mut pos)?;

        let (kind, raw) = match ty {
            T_COMMIT | T_TREE | T_BLOB | T_TAG => {
                let (raw, _consumed) = inflate(&data[..body_end], &mut pos, size as usize)?;
                (Kind::from_pack_type(ty)?, raw)
            }
            T_OFS_DELTA => {
                let rel = read_offset_varint(data, &mut pos)?;
                let base_off = entry_start
                    .checked_sub(rel)
                    .ok_or(CodecError::BadDeltaBaseOffset)?;
                let (delta, _consumed) = inflate(&data[..body_end], &mut pos, size as usize)?;
                let (bkind, bdata) = by_offset
                    .get(&base_off)
                    .ok_or(CodecError::MissingDeltaBaseOffset(base_off))?;
                let target = apply_delta(bdata, &delta)?;
                (*bkind, target)
            }
            T_REF_DELTA => {
                let base_oid =
                    Oid::from_bytes(data.get(pos..pos + 20).ok_or(CodecError::PackTooShort)?)?;
                pos += 20;
                let (delta, _consumed) = inflate(&data[..body_end], &mut pos, size as usize)?;
                let (bkind, bdata) = if let Some((k, d)) = by_oid.get(&base_oid) {
                    (*k, d.clone())
                } else if let Some(obj) = resolver(&base_oid) {
                    (obj.kind, obj.data.to_vec())
                } else {
                    return Err(CodecError::MissingDeltaBaseOid(base_oid));
                };
                let target = apply_delta(&bdata, &delta)?;
                (bkind, target)
            }
            other => return Err(CodecError::BadPackType(other)),
        };

        let oid = crate::object::hash(kind, &raw);
        let crc = {
            let mut c = crc32fast::Hasher::new();
            c.update(&data[entry_start as usize..pos]);
            c.finalize()
        };
        by_offset.insert(entry_start, (kind, raw.clone()));
        by_oid.insert(oid, (kind, raw.clone()));
        offsets.push(PackedEntry {
            oid,
            offset: entry_start,
            crc32: crc,
        });
        objects.push((oid, Object::new(kind, Bytes::from(raw))));
    }

    Ok(ParsedPack { objects, offsets })
}

#[cfg(test)]
fn parse_pack_reuse(data: &[u8]) -> Result<Vec<(Oid, Object, Option<ReusableDelta>)>, CodecError> {
    if data.len() < 12 + 20 {
        return Err(CodecError::PackTooShort);
    }
    if &data[0..4] != PACK_MAGIC {
        return Err(CodecError::BadPackMagic);
    }
    let version = u32::from_be_bytes(data[4..8].try_into().unwrap());
    if version != PACK_VERSION {
        return Err(CodecError::BadPackVersion(version));
    }
    let count = u32::from_be_bytes(data[8..12].try_into().unwrap()) as usize;

    let body_end = data.len() - 20;
    let mut h = Sha1::new();
    h.update(&data[..body_end]);
    let computed: [u8; 20] = h.finalize().into();
    if computed != data[body_end..] {
        return Err(CodecError::PackChecksumMismatch);
    }

    let mut pos = 12usize;
    let mut by_offset: HashMap<u64, (Kind, Vec<u8>)> =
        HashMap::with_capacity(count.min(PREALLOC_HINT_CAP));
    let mut by_oid: HashMap<Oid, (Kind, Vec<u8>)> =
        HashMap::with_capacity(count.min(PREALLOC_HINT_CAP));
    let mut offset_to_oid: HashMap<u64, Oid> = HashMap::with_capacity(count.min(PREALLOC_HINT_CAP));
    let mut out = Vec::with_capacity(count.min(PREALLOC_HINT_CAP));

    for _ in 0..count {
        let entry_start = pos as u64;
        let (ty, size) = read_entry_header(data, &mut pos)?;

        let (kind, raw, reuse) = match ty {
            T_COMMIT | T_TREE | T_BLOB | T_TAG => {
                let (raw, _) = inflate(&data[..body_end], &mut pos, size as usize)?;
                (Kind::from_pack_type(ty)?, raw, None)
            }
            T_OFS_DELTA => {
                let rel = read_offset_varint(data, &mut pos)?;
                let base_off = entry_start
                    .checked_sub(rel)
                    .ok_or(CodecError::BadDeltaBaseOffset)?;
                let cstart = pos;
                let (delta, consumed) = inflate(&data[..body_end], &mut pos, size as usize)?;
                let (bkind, bdata) = by_offset
                    .get(&base_off)
                    .ok_or(CodecError::MissingDeltaBaseOffset(base_off))?;
                let target = apply_delta(bdata, &delta)?;
                let base_oid = *offset_to_oid
                    .get(&base_off)
                    .ok_or(CodecError::MissingDeltaBaseOffset(base_off))?;
                let rd = ReusableDelta {
                    base_oid,
                    delta_len: size,
                    zdelta: data[cstart..cstart + consumed].to_vec(),
                };
                (*bkind, target, Some(rd))
            }
            T_REF_DELTA => {
                let base_oid =
                    Oid::from_bytes(data.get(pos..pos + 20).ok_or(CodecError::PackTooShort)?)?;
                pos += 20;
                let cstart = pos;
                let (delta, consumed) = inflate(&data[..body_end], &mut pos, size as usize)?;
                let (bkind, bdata) = by_oid
                    .get(&base_oid)
                    .map(|(k, d)| (*k, d.clone()))
                    .ok_or(CodecError::MissingDeltaBaseOid(base_oid))?;
                let target = apply_delta(&bdata, &delta)?;
                let rd = ReusableDelta {
                    base_oid,
                    delta_len: size,
                    zdelta: data[cstart..cstart + consumed].to_vec(),
                };
                (bkind, target, Some(rd))
            }
            other => return Err(CodecError::BadPackType(other)),
        };

        let oid = crate::object::hash(kind, &raw);
        by_offset.insert(entry_start, (kind, raw.clone()));
        by_oid.insert(oid, (kind, raw.clone()));
        offset_to_oid.insert(entry_start, oid);
        out.push((oid, Object::new(kind, Bytes::from(raw)), reuse));
    }

    Ok(out)
}

/// The result of a parallel parse: every object resolved, the source pack's trailer id (its content
/// address, used as the stored pack id), and — computed in the same multi-core pass — the in-pack oid
/// set and the set of oids the pack **references but does not contain** (its external dependencies).
/// The latter two turn push connectivity validation from a full re-walk of history into a couple of
/// set checks against the existing pool.
pub struct ResolvedPack {
    pub object_count: usize,
    pub pack_hash: Oid,
    /// Present when the received pack was thin: the self-contained replacement whose bytes must
    /// be stored instead of the received ones (same storage id). `pack_hash` is its trailer.
    pub fixed_thin_pack: Option<FixedThinPack>,
    pub replayed_pack_entries: bool,
    pub idx_file: tempfile::NamedTempFile,
    pub idx_bytes: usize,
    pub object_index_file: tempfile::NamedTempFile,
    pub source_index_file: tempfile::NamedTempFile,
    pub source_commit_entries: Vec<PackCommitIndexEntry>,
    pub oids: Vec<Oid>,
    pub external_refs: std::collections::HashSet<Oid>,
    pub tag_targets: std::collections::HashMap<Oid, Oid>,
    pub largest: Option<(Oid, u64)>,
}

/// A fully scanned receive-pack file: trailer verified and compressed entry ranges recorded.
/// Delta and source-metadata resolution happens from the seekable temp file so the scanner does not
/// retain inflated object payloads while request bytes are still arriving.
pub struct ScannedPack {
    pack_hash: Oid,
    entries: Vec<ReceiveEntryMeta>,
    pack_len: u64,
    scan_ms: f64,
    retained_payload_bytes: u64,
    spilled_payload_bytes: u64,
    predecode_budget_exhausted: bool,
}

#[derive(Clone, Copy, Default)]
struct ScanResolveStats {
    scan_ms: f64,
    retained_payload_bytes: u64,
    spilled_payload_bytes: u64,
    predecode_budget_exhausted: bool,
}

impl ScannedPack {
    pub fn pack_hash(&self) -> Oid {
        self.pack_hash
    }

    pub fn pack_len(&self) -> u64 {
        self.pack_len
    }

    pub fn retained_payload_bytes(&self) -> u64 {
        self.retained_payload_bytes
    }

    pub fn spilled_payload_bytes(&self) -> u64 {
        self.spilled_payload_bytes
    }

    pub fn predecode_budget_exhausted(&self) -> bool {
        self.predecode_budget_exhausted
    }

    pub fn ref_delta_bases(&self) -> Vec<Oid> {
        let mut bases = Vec::new();
        for entry in &self.entries {
            if let ReceiveEntryKind::RefDelta { base_oid } = entry.kind {
                bases.push(base_oid);
            }
        }
        bases.sort_unstable();
        bases.dedup();
        bases
    }

    /// Whether the pack carries any `REF_DELTA` entry — i.e. it is thin and its received bytes
    /// can never be manifested as-is (the resolver must produce a self-contained replacement).
    pub fn is_thin(&self) -> bool {
        self.entries
            .iter()
            .any(|entry| matches!(entry.kind, ReceiveEntryKind::RefDelta { .. }))
    }
}

/// A large blob resolved from a pack into a local temporary file.
pub struct FileBackedBlob {
    pub oid: Oid,
    pub size: u64,
    file: tempfile::NamedTempFile,
}

impl FileBackedBlob {
    pub fn from_temp(oid: Oid, size: u64, file: tempfile::NamedTempFile) -> Self {
        Self { oid, size, file }
    }

    pub fn path(&self) -> &Path {
        self.file.path()
    }
}

/// Bounded artifact extraction plan for a self-contained pack.
///
/// This never materializes all small objects for repacking. It only resolves the pack far enough to
/// identify large blobs that should be extracted to the global artifact store, spilling those blobs
/// to temporary files. This is the optimizer hot path for large first-push packs: keep the
/// client-authored pack as the serving pack and publish extracted artifact side data without
/// building a second full pack in memory.
pub struct PackOptimizationPlan {
    pub object_count: usize,
    pub large_blobs: Vec<FileBackedBlob>,
}

/// One tree entry extracted from a pack for derived metadata indexing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackTreeIndexEntry {
    pub tree_oid: Oid,
    pub mode: u32,
    pub name: Vec<u8>,
    pub oid: Oid,
}

/// One commit entry extracted from a pack for derived metadata indexing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackCommitIndexEntry {
    pub commit_oid: Oid,
    pub root_tree: Oid,
    pub parents: Vec<Oid>,
}

/// Bounded derived-index plan for source metadata.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackTreeIndexPlan {
    pub object_count: usize,
    pub entries: Vec<PackTreeIndexEntry>,
    pub commits: Vec<PackCommitIndexEntry>,
}

pub fn write_pack_source_index_sidecar<W: Write>(
    mut writer: W,
    plan: &PackTreeIndexPlan,
) -> Result<(), CodecError> {
    writer
        .write_all(SOURCE_INDEX_MAGIC)
        .map_err(|e| CodecError::Io(e.to_string()))?;
    write_u64(&mut writer, plan.object_count as u64)?;
    write_u64(&mut writer, plan.entries.len() as u64)?;
    write_u64(&mut writer, plan.commits.len() as u64)?;
    for entry in &plan.entries {
        write_pack_tree_index_entry(&mut writer, entry)?;
    }
    for commit in &plan.commits {
        write_pack_commit_index_entry(&mut writer, commit)?;
    }
    writer.flush().map_err(|e| CodecError::Io(e.to_string()))?;
    Ok(())
}

fn write_pack_tree_index_entry<W: Write>(
    writer: &mut W,
    entry: &PackTreeIndexEntry,
) -> Result<(), CodecError> {
    write_oid(writer, &entry.tree_oid)?;
    write_u32(writer, entry.mode)?;
    write_u64(writer, entry.name.len() as u64)?;
    writer
        .write_all(&entry.name)
        .map_err(|e| CodecError::Io(e.to_string()))?;
    write_oid(writer, &entry.oid)?;
    Ok(())
}

fn write_pack_commit_index_entry<W: Write>(
    writer: &mut W,
    commit: &PackCommitIndexEntry,
) -> Result<(), CodecError> {
    write_oid(writer, &commit.commit_oid)?;
    write_oid(writer, &commit.root_tree)?;
    write_u64(writer, commit.parents.len() as u64)?;
    for parent in &commit.parents {
        write_oid(writer, parent)?;
    }
    Ok(())
}

pub fn encode_pack_source_index_sidecar(plan: &PackTreeIndexPlan) -> Result<Vec<u8>, CodecError> {
    let mut out = Vec::new();
    write_pack_source_index_sidecar(&mut out, plan)?;
    Ok(out)
}

pub fn decode_pack_source_index_sidecar(sidecar: &[u8]) -> Result<PackTreeIndexPlan, CodecError> {
    let mut cursor = std::io::Cursor::new(sidecar);
    let mut magic = [0u8; 8];
    cursor
        .read_exact(&mut magic)
        .map_err(|e| CodecError::Io(e.to_string()))?;
    if &magic != SOURCE_INDEX_MAGIC {
        return Err(CodecError::Io("invalid source-index sidecar magic".into()));
    }
    let object_count = usize::try_from(read_u64(&mut cursor)?)
        .map_err(|_| CodecError::Io("source-index object count overflows usize".into()))?;
    let tree_count = usize::try_from(read_u64(&mut cursor)?)
        .map_err(|_| CodecError::Io("source-index tree count overflows usize".into()))?;
    let commit_count = usize::try_from(read_u64(&mut cursor)?)
        .map_err(|_| CodecError::Io("source-index commit count overflows usize".into()))?;
    let mut entries = Vec::with_capacity(tree_count.min(PREALLOC_HINT_CAP));
    for _ in 0..tree_count {
        let tree_oid = read_oid(&mut cursor)?;
        let mode = read_u32(&mut cursor)?;
        let name_len = usize::try_from(read_u64(&mut cursor)?)
            .map_err(|_| CodecError::Io("source-index name length overflows usize".into()))?;
        let mut name = vec![0u8; name_len];
        cursor
            .read_exact(&mut name)
            .map_err(|e| CodecError::Io(e.to_string()))?;
        entries.push(PackTreeIndexEntry {
            tree_oid,
            mode,
            name,
            oid: read_oid(&mut cursor)?,
        });
    }
    let mut commits = Vec::with_capacity(commit_count.min(PREALLOC_HINT_CAP));
    for _ in 0..commit_count {
        let commit_oid = read_oid(&mut cursor)?;
        let root_tree = read_oid(&mut cursor)?;
        let parent_count = usize::try_from(read_u64(&mut cursor)?)
            .map_err(|_| CodecError::Io("source-index parent count overflows usize".into()))?;
        let mut parents = Vec::with_capacity(parent_count);
        for _ in 0..parent_count {
            parents.push(read_oid(&mut cursor)?);
        }
        commits.push(PackCommitIndexEntry {
            commit_oid,
            root_tree,
            parents,
        });
    }
    if cursor.position() != sidecar.len() as u64 {
        return Err(CodecError::Io(
            "source-index sidecar has trailing bytes".into(),
        ));
    }
    Ok(PackTreeIndexPlan {
        object_count,
        entries,
        commits,
    })
}

fn write_u8<W: Write>(writer: &mut W, value: u8) -> Result<(), CodecError> {
    writer
        .write_all(&[value])
        .map_err(|e| CodecError::Io(e.to_string()))
}

fn write_u32<W: Write>(writer: &mut W, value: u32) -> Result<(), CodecError> {
    writer
        .write_all(&value.to_be_bytes())
        .map_err(|e| CodecError::Io(e.to_string()))
}

fn write_u64<W: Write>(writer: &mut W, value: u64) -> Result<(), CodecError> {
    writer
        .write_all(&value.to_be_bytes())
        .map_err(|e| CodecError::Io(e.to_string()))
}

fn write_oid<W: Write>(writer: &mut W, oid: &Oid) -> Result<(), CodecError> {
    writer
        .write_all(oid.as_bytes())
        .map_err(|e| CodecError::Io(e.to_string()))
}

fn write_kind<W: Write>(writer: &mut W, kind: Kind) -> Result<(), CodecError> {
    write_u8(writer, kind.pack_type())
}

fn read_u8<R: Read>(reader: &mut R) -> Result<u8, CodecError> {
    let mut buf = [0u8; 1];
    reader
        .read_exact(&mut buf)
        .map_err(|e| CodecError::Io(e.to_string()))?;
    Ok(buf[0])
}

fn read_u32<R: Read>(reader: &mut R) -> Result<u32, CodecError> {
    let mut buf = [0u8; 4];
    reader
        .read_exact(&mut buf)
        .map_err(|e| CodecError::Io(e.to_string()))?;
    Ok(u32::from_be_bytes(buf))
}

fn read_u64<R: Read>(reader: &mut R) -> Result<u64, CodecError> {
    let mut buf = [0u8; 8];
    reader
        .read_exact(&mut buf)
        .map_err(|e| CodecError::Io(e.to_string()))?;
    Ok(u64::from_be_bytes(buf))
}

fn read_oid<R: Read>(reader: &mut R) -> Result<Oid, CodecError> {
    let mut bytes = [0u8; 20];
    reader
        .read_exact(&mut bytes)
        .map_err(|e| CodecError::Io(e.to_string()))?;
    Ok(Oid::from_array(bytes))
}

fn read_kind<R: Read>(reader: &mut R) -> Result<Kind, CodecError> {
    Kind::from_pack_type(read_u8(reader)?)
}

#[derive(Clone, Copy)]
enum ReceiveEntryKind {
    Full(Kind),
    OfsDelta { base_offset: u64 },
    RefDelta { base_oid: Oid },
}

struct ReceiveEntryMeta {
    offset: u64,
    compressed_offset: u64,
    compressed_len: usize,
    declared_size: u64,
    kind: ReceiveEntryKind,
    crc32: u32,
    base_ref_count: u32,
    oid: Option<Oid>,
    resolved_kind: Option<Kind>,
    resolved_size: Option<u64>,
    predecoded_raw: Option<Vec<u8>>,
    base_oid: Option<Oid>,
    depth: u32,
}

struct StoredBase {
    oid: Oid,
    kind: Kind,
    size: u64,
    depth: u32,
    remaining_refs: u32,
    data: StoredBaseData,
}

/// Incrementally scans a received pack while the server spools the same bytes to disk.
pub struct ReceivePackSpooler {
    state: SpoolState,
    buf: Vec<u8>,
    pos: usize,
    absolute_pos: u64,
    hasher: Sha1,
    entries: Vec<ReceiveEntryMeta>,
    count: Option<usize>,
    allow_ref_delta: bool,
    seen_ref_delta: bool,
    started: std::time::Instant,
}

enum SpoolState {
    Header,
    Entry,
    OfsDelta {
        entry_offset: u64,
        ty: u8,
        declared_size: u64,
        crc: crc32fast::Hasher,
        rel: u64,
        started: bool,
    },
    RefDelta {
        entry_offset: u64,
        ty: u8,
        declared_size: u64,
        crc: crc32fast::Hasher,
        base: [u8; 20],
        read: usize,
    },
    Inflate(Box<InflightEntry>),
    Trailer {
        trailer: [u8; 20],
        read: usize,
    },
    Done,
    Thin,
}

struct InflightEntry {
    offset: u64,
    compressed_offset: u64,
    declared_size: u64,
    kind: ReceiveEntryKind,
    crc: crc32fast::Hasher,
    dec: Decompress,
    scratch: [u8; 16 * 1024],
    object_hasher: Option<Sha1>,
}

struct ScannedResolution {
    oid: Option<Oid>,
    kind: Option<Kind>,
    size: Option<u64>,
    raw: Option<Vec<u8>>,
    base_oid: Option<Oid>,
    depth: u32,
}

impl ReceivePackSpooler {
    pub fn new(allow_ref_delta: bool) -> Self {
        Self {
            state: SpoolState::Header,
            buf: Vec::new(),
            pos: 0,
            absolute_pos: 0,
            hasher: Sha1::new(),
            entries: Vec::new(),
            count: None,
            allow_ref_delta,
            seen_ref_delta: false,
            started: std::time::Instant::now(),
        }
    }

    pub fn push(&mut self, chunk: &[u8]) -> Result<(), CodecError> {
        self.buf.extend_from_slice(chunk);
        self.process()
    }

    /// Whether any `REF_DELTA` entry header has been consumed so far. Available while bytes are
    /// still streaming in, so a caller uploading the raw bytes in parallel can stop as soon as
    /// the pack turns out to be thin (its bytes must never be manifested verbatim).
    pub fn seen_ref_delta(&self) -> bool {
        self.seen_ref_delta
    }

    pub fn finish(mut self) -> Result<Option<ScannedPack>, CodecError> {
        self.process()?;
        if matches!(self.state, SpoolState::Thin) {
            return Ok(None);
        }
        if !matches!(self.state, SpoolState::Done) {
            return Err(CodecError::PackTooShort);
        }
        if self.pos != self.buf.len() {
            return Err(CodecError::PackTooShort);
        }
        let pack_len = self.absolute_pos;
        Ok(Some(ScannedPack {
            pack_hash: Oid::from_array(finalize_sha1(self.hasher)),
            entries: self.entries,
            pack_len,
            scan_ms: elapsed_ms(self.started),
            retained_payload_bytes: 0,
            spilled_payload_bytes: 0,
            predecode_budget_exhausted: false,
        }))
    }

    fn process(&mut self) -> Result<(), CodecError> {
        loop {
            self.compact_buffer();
            match std::mem::replace(&mut self.state, SpoolState::Done) {
                SpoolState::Header => {
                    if self.available() < 12 {
                        self.state = SpoolState::Header;
                        return Ok(());
                    }
                    let header = self.peek_exact(12)?;
                    if &header[0..4] != PACK_MAGIC {
                        return Err(CodecError::BadPackMagic);
                    }
                    let version = u32::from_be_bytes(header[4..8].try_into().unwrap());
                    if version != PACK_VERSION {
                        return Err(CodecError::BadPackVersion(version));
                    }
                    let count = u32::from_be_bytes(header[8..12].try_into().unwrap()) as usize;
                    self.count = Some(count);
                    self.entries = Vec::with_capacity(count.min(PREALLOC_HINT_CAP));
                    self.consume_body(12, None)?;
                    self.state = if count == 0 {
                        SpoolState::Trailer {
                            trailer: [0; 20],
                            read: 0,
                        }
                    } else {
                        SpoolState::Entry
                    };
                }
                SpoolState::Entry => {
                    let Some(count) = self.count else {
                        return Err(CodecError::BadPackMagic);
                    };
                    if self.entries.len() == count {
                        self.state = SpoolState::Trailer {
                            trailer: [0; 20],
                            read: 0,
                        };
                        continue;
                    }
                    let entry_offset = self.absolute_pos;
                    let mut crc = crc32fast::Hasher::new();
                    let Some((ty, declared_size, header_len)) = self.try_peek_entry_header()?
                    else {
                        self.state = SpoolState::Entry;
                        return Ok(());
                    };
                    self.consume_body(header_len, Some(&mut crc))?;
                    match ty {
                        T_COMMIT | T_TREE | T_BLOB | T_TAG => {
                            let kind = ReceiveEntryKind::Full(Kind::from_pack_type(ty)?);
                            self.state = SpoolState::Inflate(Box::new(InflightEntry::new(
                                entry_offset,
                                self.absolute_pos,
                                declared_size,
                                kind,
                                crc,
                            )?));
                        }
                        T_OFS_DELTA => {
                            self.state = SpoolState::OfsDelta {
                                entry_offset,
                                ty,
                                declared_size,
                                crc,
                                rel: 0,
                                started: false,
                            };
                        }
                        T_REF_DELTA => {
                            self.seen_ref_delta = true;
                            self.state = SpoolState::RefDelta {
                                entry_offset,
                                ty,
                                declared_size,
                                crc,
                                base: [0; 20],
                                read: 0,
                            };
                        }
                        other => return Err(CodecError::BadPackType(other)),
                    }
                }
                SpoolState::OfsDelta {
                    entry_offset,
                    ty,
                    declared_size,
                    mut crc,
                    mut rel,
                    mut started,
                } => loop {
                    let Some(b) = self.try_read_body_byte(Some(&mut crc))? else {
                        self.state = SpoolState::OfsDelta {
                            entry_offset,
                            ty,
                            declared_size,
                            crc,
                            rel,
                            started,
                        };
                        return Ok(());
                    };
                    if started {
                        rel = ((rel + 1) << 7) | (b & 0x7f) as u64;
                    } else {
                        rel = (b & 0x7f) as u64;
                        started = true;
                    }
                    if b & 0x80 == 0 {
                        let base_offset = entry_offset
                            .checked_sub(rel)
                            .ok_or(CodecError::BadDeltaBaseOffset)?;
                        let base_idx = self
                            .entries
                            .binary_search_by_key(&base_offset, |entry| entry.offset)
                            .map_err(|_| CodecError::MissingDeltaBaseOffset(base_offset))?;
                        self.entries[base_idx].base_ref_count = self.entries[base_idx]
                            .base_ref_count
                            .checked_add(1)
                            .ok_or_else(|| CodecError::Io("too many delta children".into()))?;
                        self.state = SpoolState::Inflate(Box::new(InflightEntry::new(
                            entry_offset,
                            self.absolute_pos,
                            declared_size,
                            ReceiveEntryKind::OfsDelta { base_offset },
                            crc,
                        )?));
                        break;
                    }
                },
                SpoolState::RefDelta {
                    entry_offset,
                    ty,
                    declared_size,
                    mut crc,
                    mut base,
                    mut read,
                } => {
                    while read < 20 {
                        let Some(b) = self.try_read_body_byte(Some(&mut crc))? else {
                            self.state = SpoolState::RefDelta {
                                entry_offset,
                                ty,
                                declared_size,
                                crc,
                                base,
                                read,
                            };
                            return Ok(());
                        };
                        base[read] = b;
                        read += 1;
                    }
                    if !self.allow_ref_delta {
                        self.state = SpoolState::Thin;
                        return Ok(());
                    }
                    self.state = SpoolState::Inflate(Box::new(InflightEntry::new(
                        entry_offset,
                        self.absolute_pos,
                        declared_size,
                        ReceiveEntryKind::RefDelta {
                            base_oid: Oid::from_array(base),
                        },
                        crc,
                    )?));
                }
                SpoolState::Inflate(mut entry) => {
                    if !self.process_inflate(&mut entry)? {
                        self.state = SpoolState::Inflate(entry);
                        return Ok(());
                    }
                    self.push_scanned_entry(*entry)?;
                    self.state = SpoolState::Entry;
                }
                SpoolState::Trailer {
                    mut trailer,
                    mut read,
                } => {
                    while read < 20 {
                        let Some(b) = self.try_read_raw_byte()? else {
                            self.state = SpoolState::Trailer { trailer, read };
                            return Ok(());
                        };
                        trailer[read] = b;
                        read += 1;
                    }
                    let computed = finalize_sha1(self.hasher.clone());
                    if computed != trailer {
                        return Err(CodecError::PackChecksumMismatch);
                    }
                    self.state = SpoolState::Done;
                }
                SpoolState::Done => {
                    self.state = SpoolState::Done;
                    return Ok(());
                }
                SpoolState::Thin => {
                    self.state = SpoolState::Thin;
                    return Ok(());
                }
            }
        }
    }

    fn push_scanned_entry(&mut self, entry: InflightEntry) -> Result<(), CodecError> {
        let compressed_len = (self.absolute_pos - entry.compressed_offset)
            .try_into()
            .map_err(|_| CodecError::PackTooShort)?;
        let predecoded_oid = entry.object_hasher.clone().map(finalize_object_hash);
        let InflightEntry {
            offset,
            compressed_offset,
            declared_size,
            kind,
            crc,
            ..
        } = entry;
        let resolution = self.try_resolve_scanned_entry(kind, declared_size, predecoded_oid)?;
        self.entries.push(ReceiveEntryMeta {
            offset,
            compressed_offset,
            compressed_len,
            declared_size,
            kind,
            crc32: crc.finalize(),
            base_ref_count: 0,
            oid: resolution.oid,
            resolved_kind: resolution.kind,
            resolved_size: resolution.size,
            predecoded_raw: resolution.raw,
            base_oid: resolution.base_oid,
            depth: resolution.depth,
        });
        Ok(())
    }

    fn try_resolve_scanned_entry(
        &self,
        kind: ReceiveEntryKind,
        declared_size: u64,
        full_oid: Option<Oid>,
    ) -> Result<ScannedResolution, CodecError> {
        match kind {
            ReceiveEntryKind::Full(kind) => Ok(ScannedResolution {
                oid: full_oid,
                kind: full_oid.map(|_| kind),
                size: full_oid.map(|_| declared_size),
                raw: None,
                base_oid: None,
                depth: 0,
            }),
            ReceiveEntryKind::OfsDelta { base_offset } => {
                let base_oid = self
                    .entries
                    .binary_search_by_key(&base_offset, |entry| entry.offset)
                    .ok()
                    .and_then(|base_idx| self.entries[base_idx].oid);
                Ok(ScannedResolution {
                    oid: None,
                    kind: None,
                    size: None,
                    raw: None,
                    base_oid,
                    depth: 0,
                })
            }
            ReceiveEntryKind::RefDelta { base_oid } => Ok(ScannedResolution {
                oid: None,
                kind: None,
                size: None,
                raw: None,
                base_oid: Some(base_oid),
                depth: 1,
            }),
        }
    }

    fn process_inflate(&mut self, entry: &mut InflightEntry) -> Result<bool, CodecError> {
        loop {
            if self.available() == 0 {
                return Ok(false);
            }
            let input = &self.buf[self.pos..];
            let before_in = entry.dec.total_in();
            let before_out = entry.dec.total_out();
            let status = entry
                .dec
                .decompress(input, &mut entry.scratch, FlushDecompress::None)
                .map_err(|e| CodecError::Inflate(e.to_string()))?;
            let consumed = (entry.dec.total_in() - before_in) as usize;
            let produced = (entry.dec.total_out() - before_out) as usize;
            self.consume_body(consumed, Some(&mut entry.crc))?;
            if produced > 0 {
                let bytes = &entry.scratch[..produced];
                if let Some(hasher) = entry.object_hasher.as_mut() {
                    hasher.update(bytes);
                }
            }
            if matches!(status, Status::StreamEnd) {
                let produced = entry.dec.total_out() as usize;
                if produced != entry.declared_size as usize {
                    return Err(CodecError::Inflate(format!(
                        "expected {} bytes, produced {produced}",
                        entry.declared_size
                    )));
                }
                return Ok(true);
            }
            if entry.dec.total_in() == before_in && entry.dec.total_out() == before_out {
                return Err(CodecError::Inflate("inflate made no progress".into()));
            }
        }
    }

    fn try_peek_entry_header(&self) -> Result<Option<(u8, u64, usize)>, CodecError> {
        let Some(&first) = self.buf.get(self.pos) else {
            return Ok(None);
        };
        let ty = (first >> 4) & 0x07;
        let mut size = (first & 0x0f) as u64;
        let mut shift = 4u32;
        let mut b = first;
        let mut read = 1usize;
        while b & 0x80 != 0 {
            let Some(&next) = self.buf.get(self.pos + read) else {
                return Ok(None);
            };
            b = next;
            size |= ((b & 0x7f) as u64) << shift;
            shift += 7;
            read += 1;
        }
        Ok(Some((ty, size, read)))
    }

    fn peek_exact(&self, n: usize) -> Result<&[u8], CodecError> {
        self.buf
            .get(self.pos..self.pos + n)
            .ok_or(CodecError::PackTooShort)
    }

    fn try_read_body_byte(
        &mut self,
        crc: Option<&mut crc32fast::Hasher>,
    ) -> Result<Option<u8>, CodecError> {
        if self.available() == 0 {
            return Ok(None);
        }
        let b = self.buf[self.pos];
        self.consume_body(1, crc)?;
        Ok(Some(b))
    }

    fn try_read_raw_byte(&mut self) -> Result<Option<u8>, CodecError> {
        if self.available() == 0 {
            return Ok(None);
        }
        let b = self.buf[self.pos];
        self.pos += 1;
        self.absolute_pos += 1;
        Ok(Some(b))
    }

    fn consume_body(
        &mut self,
        n: usize,
        crc: Option<&mut crc32fast::Hasher>,
    ) -> Result<(), CodecError> {
        let bytes = self
            .buf
            .get(self.pos..self.pos + n)
            .ok_or(CodecError::PackTooShort)?;
        self.hasher.update(bytes);
        if let Some(crc) = crc {
            crc.update(bytes);
        }
        self.pos += n;
        self.absolute_pos += n as u64;
        Ok(())
    }

    fn available(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    fn compact_buffer(&mut self) {
        if self.pos > 64 * 1024 && self.pos * 2 > self.buf.len() {
            self.buf.drain(..self.pos);
            self.pos = 0;
        }
    }
}

impl InflightEntry {
    fn new(
        offset: u64,
        compressed_offset: u64,
        declared_size: u64,
        kind: ReceiveEntryKind,
        crc: crc32fast::Hasher,
    ) -> Result<Self, CodecError> {
        let full_kind = match kind {
            ReceiveEntryKind::Full(kind) => Some(kind),
            ReceiveEntryKind::OfsDelta { .. } | ReceiveEntryKind::RefDelta { .. } => None,
        };
        #[cfg(test)]
        if full_kind == Some(Kind::Blob) {
            STREAMED_FULL_BLOBS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(Self {
            offset,
            compressed_offset,
            declared_size,
            kind,
            crc,
            dec: Decompress::new(true),
            scratch: [0; 16 * 1024],
            object_hasher: full_kind.map(|kind| object_hasher(kind, declared_size)),
        })
    }
}

enum StoredBaseData {
    Memory(Vec<u8>),
    Spilled(tempfile::NamedTempFile),
    ExternalPath(PathBuf),
}

#[cfg(test)]
const LARGE_DELTA_BASE_SPILL_BYTES: usize = 1024;
#[cfg(not(test))]
const LARGE_DELTA_BASE_SPILL_BYTES: usize = 16 * 1024 * 1024;
#[cfg(test)]
const DELTA_BASE_MEMORY_BUDGET_BYTES: usize = 16 * 1024;
#[cfg(not(test))]
const DELTA_BASE_MEMORY_BUDGET_BYTES: usize = 256 * 1024 * 1024;
#[cfg(test)]
static SPILLED_DELTA_BASES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
#[cfg(test)]
static STREAMED_DELTA_TARGETS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);
#[cfg(test)]
static STREAMED_FULL_BLOBS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

struct ResolvedData {
    oid: Oid,
    size: u64,
    raw: Option<Vec<u8>>,
    spilled: Option<tempfile::NamedTempFile>,
    external_path: Option<PathBuf>,
}

/// A resolved object payload stored outside the current pack, used as a `REF_DELTA` base without
/// materializing the payload in the codec heap.
#[derive(Clone, Debug)]
pub struct ExternalBase {
    pub kind: Kind,
    pub size: u64,
    pub path: PathBuf,
}

fn store_delta_base(
    oid: Oid,
    kind: Kind,
    depth: u32,
    raw: Vec<u8>,
    remaining_refs: u32,
) -> Result<StoredBase, CodecError> {
    let size = raw.len() as u64;
    if raw.len() < LARGE_DELTA_BASE_SPILL_BYTES {
        return Ok(StoredBase {
            oid,
            kind,
            size,
            depth,
            remaining_refs,
            data: StoredBaseData::Memory(raw),
        });
    }
    let mut file = tempfile::NamedTempFile::new().map_err(|e| CodecError::Io(e.to_string()))?;
    file.write_all(&raw)
        .map_err(|e| CodecError::Io(e.to_string()))?;
    file.as_file_mut()
        .flush()
        .map_err(|e| CodecError::Io(e.to_string()))?;
    #[cfg(test)]
    SPILLED_DELTA_BASES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Ok(StoredBase {
        oid,
        kind,
        size,
        depth,
        remaining_refs,
        data: StoredBaseData::Spilled(file),
    })
}

fn store_delta_base_with_memory_budget(
    oid: Oid,
    kind: Kind,
    depth: u32,
    raw: Vec<u8>,
    remaining_refs: u32,
    resident_base_bytes: &mut usize,
) -> Result<StoredBase, CodecError> {
    let size = raw.len();
    if size < LARGE_DELTA_BASE_SPILL_BYTES
        && resident_base_bytes
            .checked_add(size)
            .is_some_and(|next| next <= DELTA_BASE_MEMORY_BUDGET_BYTES)
    {
        *resident_base_bytes += size;
        return Ok(StoredBase {
            oid,
            kind,
            size: size as u64,
            depth,
            remaining_refs,
            data: StoredBaseData::Memory(raw),
        });
    }

    let mut file = tempfile::NamedTempFile::new().map_err(|e| CodecError::Io(e.to_string()))?;
    file.write_all(&raw)
        .map_err(|e| CodecError::Io(e.to_string()))?;
    file.as_file_mut()
        .flush()
        .map_err(|e| CodecError::Io(e.to_string()))?;
    #[cfg(test)]
    SPILLED_DELTA_BASES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Ok(StoredBase {
        oid,
        kind,
        size: size as u64,
        depth,
        remaining_refs,
        data: StoredBaseData::Spilled(file),
    })
}

fn stored_base_resident_bytes(base: &StoredBase) -> usize {
    match &base.data {
        StoredBaseData::Memory(data) => data.len(),
        StoredBaseData::Spilled(_) | StoredBaseData::ExternalPath(_) => 0,
    }
}

fn store_spilled_delta_base(
    oid: Oid,
    kind: Kind,
    size: u64,
    depth: u32,
    file: tempfile::NamedTempFile,
    remaining_refs: u32,
) -> StoredBase {
    StoredBase {
        oid,
        kind,
        size,
        depth,
        remaining_refs,
        data: StoredBaseData::Spilled(file),
    }
}

fn resolve_delta_from_base(
    base: &StoredBase,
    delta: &[u8],
    target_size: usize,
) -> Result<ResolvedData, CodecError> {
    resolve_delta_from_base_with_spill_threshold(
        base,
        delta,
        target_size,
        LARGE_DELTA_BASE_SPILL_BYTES,
    )
}

fn resolve_delta_from_base_with_spill_threshold(
    base: &StoredBase,
    delta: &[u8],
    target_size: usize,
    spill_threshold: usize,
) -> Result<ResolvedData, CodecError> {
    let should_stream_blob = base.kind == Kind::Blob
        && (target_size >= spill_threshold || matches!(base.data, StoredBaseData::Spilled(_)));
    if should_stream_blob {
        let (oid, file) = apply_delta_to_spill(base, delta, target_size)?;
        return Ok(ResolvedData {
            oid,
            size: target_size as u64,
            raw: None,
            spilled: Some(file),
            external_path: None,
        });
    }

    let base_data = materialize_base(base)?;
    let raw = apply_delta(&base_data, delta)?;
    if raw.len() != target_size {
        return Err(CodecError::DeltaTargetSizeMismatch {
            expected: target_size as u64,
            actual: raw.len() as u64,
        });
    }
    let oid = crate::object::hash(base.kind, &raw);
    Ok(ResolvedData {
        oid,
        size: raw.len() as u64,
        raw: Some(raw),
        spilled: None,
        external_path: None,
    })
}

fn materialize_base(base: &StoredBase) -> Result<Vec<u8>, CodecError> {
    match &base.data {
        StoredBaseData::Memory(data) => Ok(data.clone()),
        StoredBaseData::Spilled(file) => {
            std::fs::read(file.path()).map_err(|e| CodecError::Io(e.to_string()))
        }
        StoredBaseData::ExternalPath(path) => {
            std::fs::read(path).map_err(|e| CodecError::Io(e.to_string()))
        }
    }
}

fn apply_delta_to_spill(
    base: &StoredBase,
    delta: &[u8],
    target_size: usize,
) -> Result<(Oid, tempfile::NamedTempFile), CodecError> {
    let mut pos = 0usize;
    let src_size = crate::delta::read_size_varint(delta, &mut pos)?;
    if src_size != base.size {
        return Err(CodecError::DeltaBaseSizeMismatch {
            expected: src_size,
            actual: base.size,
        });
    }
    let declared_target = crate::delta::read_size_varint(delta, &mut pos)? as usize;
    if declared_target != target_size {
        return Err(CodecError::DeltaTargetSizeMismatch {
            expected: declared_target as u64,
            actual: target_size as u64,
        });
    }

    let mut out = tempfile::NamedTempFile::new().map_err(|e| CodecError::Io(e.to_string()))?;
    let mut hasher = object_hasher(base.kind, target_size as u64);
    let mut written = 0usize;
    while pos < delta.len() {
        let op = delta[pos];
        pos += 1;
        if op & 0x80 != 0 {
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
            let end = cp_off
                .checked_add(cp_size)
                .ok_or(CodecError::TruncatedDelta)?;
            if end > base.size {
                return Err(CodecError::DeltaCopyOutOfRange {
                    start: cp_off,
                    end,
                    base_len: base.size,
                });
            }
            copy_base_range_to_output(base, cp_off, cp_size, &mut out, &mut hasher)?;
            written = written
                .checked_add(cp_size as usize)
                .ok_or(CodecError::TruncatedDelta)?;
        } else if op != 0 {
            let len = op as usize;
            let end = pos.checked_add(len).ok_or(CodecError::TruncatedDelta)?;
            let bytes = delta.get(pos..end).ok_or(CodecError::TruncatedDelta)?;
            out.write_all(bytes)
                .map_err(|e| CodecError::Io(e.to_string()))?;
            hasher.update(bytes);
            written = written.checked_add(len).ok_or(CodecError::TruncatedDelta)?;
            pos = end;
        } else {
            return Err(CodecError::ReservedDeltaOpcode);
        }
    }
    if written != target_size {
        return Err(CodecError::DeltaTargetSizeMismatch {
            expected: target_size as u64,
            actual: written as u64,
        });
    }
    out.as_file_mut()
        .flush()
        .map_err(|e| CodecError::Io(e.to_string()))?;
    #[cfg(test)]
    STREAMED_DELTA_TARGETS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Ok((finalize_object_hash(hasher), out))
}

fn copy_base_range_to_output(
    base: &StoredBase,
    offset: u64,
    mut len: u64,
    out: &mut tempfile::NamedTempFile,
    hasher: &mut Sha1,
) -> Result<(), CodecError> {
    const CHUNK: usize = 64 * 1024;
    match &base.data {
        StoredBaseData::Memory(data) => {
            let start = offset as usize;
            let end = start
                .checked_add(len as usize)
                .ok_or(CodecError::TruncatedDelta)?;
            let bytes = data
                .get(start..end)
                .ok_or(CodecError::DeltaCopyOutOfRange {
                    start: offset,
                    end: offset + len,
                    base_len: data.len() as u64,
                })?;
            out.write_all(bytes)
                .map_err(|e| CodecError::Io(e.to_string()))?;
            hasher.update(bytes);
        }
        StoredBaseData::Spilled(file) => {
            let input =
                std::fs::File::open(file.path()).map_err(|e| CodecError::Io(e.to_string()))?;
            let mut buf = [0u8; CHUNK];
            let mut read_offset = offset;
            while len > 0 {
                let n = (len as usize).min(buf.len());
                read_exact_at(&input, read_offset, &mut buf[..n])?;
                out.write_all(&buf[..n])
                    .map_err(|e| CodecError::Io(e.to_string()))?;
                hasher.update(&buf[..n]);
                read_offset += n as u64;
                len -= n as u64;
            }
        }
        StoredBaseData::ExternalPath(path) => {
            let input = std::fs::File::open(path).map_err(|e| CodecError::Io(e.to_string()))?;
            let mut buf = [0u8; CHUNK];
            let mut read_offset = offset;
            while len > 0 {
                let n = (len as usize).min(buf.len());
                read_exact_at(&input, read_offset, &mut buf[..n])?;
                out.write_all(&buf[..n])
                    .map_err(|e| CodecError::Io(e.to_string()))?;
                hasher.update(&buf[..n]);
                read_offset += n as u64;
                len -= n as u64;
            }
        }
    }
    Ok(())
}

fn object_hasher(kind: Kind, size: u64) -> Sha1 {
    let mut h = Sha1::new();
    h.update(kind.as_str().as_bytes());
    h.update(b" ");
    h.update(size.to_string().as_bytes());
    h.update(b"\0");
    h
}

fn finalize_object_hash(hasher: Sha1) -> Oid {
    Oid::from_array(finalize_sha1(hasher))
}

fn finalize_sha1(hasher: Sha1) -> [u8; 20] {
    hasher.finalize().into()
}

fn write_idx_v2_from_receive_entries(
    entries: &[ReceiveEntryMeta],
    pack_hash: Oid,
) -> Result<Vec<u8>, CodecError> {
    const IDX_MAGIC: [u8; 4] = [0xff, 0x74, 0x4f, 0x63];
    const IDX_VERSION: u32 = 2;
    const FANOUT_LEN: usize = 256;
    const LARGE_OFFSET_FLAG: u32 = 0x8000_0000;

    let mut sorted: Vec<(Oid, usize)> = entries
        .iter()
        .enumerate()
        .map(|(idx, entry)| {
            entry
                .oid
                .map(|oid| (oid, idx))
                .ok_or_else(|| CodecError::Io("missing resolved oid".into()))
        })
        .collect::<Result<_, _>>()?;
    sorted.sort_by(|(a, _), (b, _)| a.as_bytes().cmp(b.as_bytes()));
    let n = entries.len();
    let mut out = Vec::with_capacity(8 + 256 * 4 + n * (20 + 4 + 4) + 40);
    out.extend_from_slice(&IDX_MAGIC);
    out.extend_from_slice(&IDX_VERSION.to_be_bytes());

    let mut fanout = [0u32; FANOUT_LEN];
    for &(oid, _) in &sorted {
        fanout[oid.first_byte() as usize] += 1;
    }
    let mut acc = 0u32;
    for slot in fanout.iter_mut() {
        acc += *slot;
        *slot = acc;
    }
    for v in fanout {
        out.extend_from_slice(&v.to_be_bytes());
    }

    for &(oid, _) in &sorted {
        out.extend_from_slice(oid.as_bytes());
    }
    for &(_, idx) in &sorted {
        out.extend_from_slice(&entries[idx].crc32.to_be_bytes());
    }

    let mut large: Vec<u64> = Vec::new();
    for &(_, idx) in &sorted {
        let offset = entries[idx].offset;
        if offset < LARGE_OFFSET_FLAG as u64 {
            out.extend_from_slice(&(offset as u32).to_be_bytes());
        } else {
            let large_idx = large.len() as u32;
            out.extend_from_slice(&(LARGE_OFFSET_FLAG | large_idx).to_be_bytes());
            large.push(offset);
        }
    }
    for off in &large {
        out.extend_from_slice(&off.to_be_bytes());
    }

    out.extend_from_slice(pack_hash.as_bytes());
    let mut h = Sha1::new();
    h.update(&out);
    let digest: [u8; 20] = h.finalize().into();
    out.extend_from_slice(&digest);
    Ok(out)
}

fn pack_object_entry_from_receive_entry(
    entry: &ReceiveEntryMeta,
) -> Result<PackObjectEntry, CodecError> {
    let oid = entry
        .oid
        .ok_or_else(|| CodecError::Io("missing resolved oid".into()))?;
    let resolved_kind = entry
        .resolved_kind
        .ok_or_else(|| CodecError::Io("missing resolved kind".into()))?;
    let resolved_size = entry
        .resolved_size
        .ok_or_else(|| CodecError::Io("missing resolved size".into()))?;
    let kind = match entry.kind {
        ReceiveEntryKind::Full(kind) => PackObjectKind::Full(kind),
        ReceiveEntryKind::OfsDelta { base_offset } => PackObjectKind::OfsDelta {
            base_offset,
            base_oid: entry
                .base_oid
                .ok_or_else(|| CodecError::Io("missing OFS_DELTA base oid".into()))?,
            depth: entry.depth,
        },
        ReceiveEntryKind::RefDelta { base_oid } => PackObjectKind::RefDelta {
            base_oid,
            depth: entry.depth,
        },
    };
    Ok(PackObjectEntry {
        oid,
        kind,
        resolved_kind,
        offset: entry.offset,
        compressed_offset: entry.compressed_offset,
        compressed_len: entry.compressed_len as u64,
        declared_size: entry.declared_size,
        resolved_size,
        crc32: entry.crc32,
    })
}

fn write_resolved_pack_object_index_file(
    entries: &[ReceiveEntryMeta],
) -> Result<(tempfile::NamedTempFile, SortedObjectIds), CodecError> {
    let mut file = tempfile::NamedTempFile::new().map_err(|e| CodecError::Io(e.to_string()))?;
    let mut oids = Vec::with_capacity(entries.len().min(PREALLOC_HINT_CAP));
    {
        let mut writer = BufWriter::new(file.as_file_mut());
        writer
            .write_all(OBJECT_INDEX_MAGIC)
            .map_err(|e| CodecError::Io(e.to_string()))?;
        write_u64(&mut writer, entries.len() as u64)?;
        for entry in entries {
            let object_entry = pack_object_entry_from_receive_entry(entry)?;
            oids.push(object_entry.oid);
            write_pack_object_index_entry(&mut writer, &object_entry)?;
        }
        writer.flush().map_err(|e| CodecError::Io(e.to_string()))?;
    }
    oids.sort_unstable();
    if let Some(duplicate) = oids
        .windows(2)
        .find_map(|pair| (pair[0] == pair[1]).then_some(pair[0]))
    {
        return Err(CodecError::DuplicateObject(duplicate));
    }
    Ok((file, oids))
}

fn write_temp_bytes(data: Vec<u8>) -> Result<tempfile::NamedTempFile, CodecError> {
    let mut file = tempfile::NamedTempFile::new().map_err(|e| CodecError::Io(e.to_string()))?;
    file.write_all(&data)
        .map_err(|e| CodecError::Io(e.to_string()))?;
    file.as_file_mut()
        .flush()
        .map_err(|e| CodecError::Io(e.to_string()))?;
    Ok(file)
}

struct SourceIndexFileWriter {
    file: tempfile::NamedTempFile,
    writer: BufWriter<std::fs::File>,
    tree_count: u64,
}

impl SourceIndexFileWriter {
    fn new(object_count: usize) -> Result<Self, CodecError> {
        let file = tempfile::NamedTempFile::new().map_err(|e| CodecError::Io(e.to_string()))?;
        let mut writer = BufWriter::new(
            file.as_file()
                .try_clone()
                .map_err(|e| CodecError::Io(e.to_string()))?,
        );
        writer
            .write_all(SOURCE_INDEX_MAGIC)
            .map_err(|e| CodecError::Io(e.to_string()))?;
        write_u64(&mut writer, object_count as u64)?;
        write_u64(&mut writer, 0)?;
        write_u64(&mut writer, 0)?;
        Ok(Self {
            file,
            writer,
            tree_count: 0,
        })
    }

    fn append_tree_entries(
        &mut self,
        entries: impl IntoIterator<Item = PackTreeIndexEntry>,
    ) -> Result<(), CodecError> {
        for entry in entries {
            write_pack_tree_index_entry(&mut self.writer, &entry)?;
            self.tree_count = self
                .tree_count
                .checked_add(1)
                .ok_or_else(|| CodecError::Io("source-index tree count overflow".into()))?;
        }
        Ok(())
    }

    fn finish(
        mut self,
        commits: &[PackCommitIndexEntry],
        object_count: u64,
    ) -> Result<tempfile::NamedTempFile, CodecError> {
        for commit in commits {
            write_pack_commit_index_entry(&mut self.writer, commit)?;
        }
        self.writer
            .flush()
            .map_err(|e| CodecError::Io(e.to_string()))?;
        drop(self.writer);

        let file = self.file.as_file_mut();
        // Patch every header count. `object_count` is passed at finish time because fix-thin
        // appends external-base entries after the writer was created with the scanned count; the
        // GC derived-index materializer validates this header against the manifest obj_count and
        // a mismatch defers tree/commit indexing forever.
        file.seek(SeekFrom::Start(SOURCE_INDEX_MAGIC.len() as u64))
            .map_err(|e| CodecError::Io(e.to_string()))?;
        write_u64(file, object_count)?;
        write_u64(file, self.tree_count)?;
        write_u64(file, commits.len() as u64)?;
        file.seek(SeekFrom::End(0))
            .map_err(|e| CodecError::Io(e.to_string()))?;
        file.flush().map_err(|e| CodecError::Io(e.to_string()))?;

        Ok(self.file)
    }
}

#[derive(Default)]
struct ResolveEntryMetrics {
    full_blob_ms: f64,
    full_non_blob_ms: f64,
    delta_ms: f64,
    link_ms: f64,
    base_store_ms: f64,
    source_index_write_ms: f64,
    full_objects: usize,
    ofs_deltas: usize,
    streamed_blobs: usize,
    spilled_delta_bases: usize,
    peak_delta_base_resident_bytes: usize,
}

struct ResolveEntryOutput {
    oid: Oid,
    kind: Kind,
    size: u64,
    base_oid: Option<Oid>,
    depth: u32,
    raw: Option<Vec<u8>>,
    spilled: Option<tempfile::NamedTempFile>,
    external_path: Option<PathBuf>,
    refs: Vec<Oid>,
    tag_target: Option<Oid>,
    tree_entries: Vec<PackTreeIndexEntry>,
    commit: Option<PackCommitIndexEntry>,
    metrics: ResolveEntryMetrics,
}

struct ResolveEntriesOutput {
    refs: Vec<Oid>,
    tag_targets: HashMap<Oid, Oid>,
    source_index_file: tempfile::NamedTempFile,
    commit_entries: Vec<PackCommitIndexEntry>,
    largest: Option<(Oid, u64)>,
    metrics: ResolveEntryMetrics,
    fixed_thin_pack: Option<FixedThinPack>,
}

/// A self-contained replacement for a received **thin** pack: the original bytes with every
/// external `REF_DELTA` base appended as a full object, the entry count patched, and the trailer
/// recomputed. Thin packs must never be manifested verbatim — their `REF_DELTA` records point at
/// objects outside the pack, which breaks stock clients on clone/fetch (`unresolved deltas`).
pub struct FixedThinPack {
    pub file: tempfile::NamedTempFile,
    pub len: u64,
    pub pack_hash: Oid,
}

fn merge_metrics(dst: &mut ResolveEntryMetrics, src: ResolveEntryMetrics) {
    dst.full_blob_ms += src.full_blob_ms;
    dst.full_non_blob_ms += src.full_non_blob_ms;
    dst.delta_ms += src.delta_ms;
    dst.link_ms += src.link_ms;
    dst.base_store_ms += src.base_store_ms;
    dst.source_index_write_ms += src.source_index_write_ms;
    dst.full_objects += src.full_objects;
    dst.ofs_deltas += src.ofs_deltas;
    dst.streamed_blobs += src.streamed_blobs;
    dst.spilled_delta_bases += src.spilled_delta_bases;
    dst.peak_delta_base_resident_bytes = dst
        .peak_delta_base_resident_bytes
        .max(src.peak_delta_base_resident_bytes);
}

fn resolve_entries_from_scanned_metadata(
    entries: &[ReceiveEntryMeta],
) -> Result<Option<ResolveEntriesOutput>, CodecError> {
    let mut refs = Vec::new();
    let mut tag_targets = HashMap::new();
    let mut source_index = SourceIndexFileWriter::new(entries.len())?;
    let mut commit_entries = Vec::new();
    let mut largest = None;
    let mut metrics = ResolveEntryMetrics::default();

    for entry in entries {
        let (Some(oid), Some(kind), Some(size)) =
            (entry.oid, entry.resolved_kind, entry.resolved_size)
        else {
            return Ok(None);
        };

        match entry.kind {
            ReceiveEntryKind::Full(_) => metrics.full_objects += 1,
            ReceiveEntryKind::OfsDelta { .. } => metrics.ofs_deltas += 1,
            ReceiveEntryKind::RefDelta { .. } => return Ok(None),
        }

        if largest.is_none_or(|(_, max)| size > max) {
            largest = Some((oid, size));
        }

        match kind {
            Kind::Blob => {
                if entry.predecoded_raw.is_none() {
                    metrics.streamed_blobs += 1;
                }
            }
            Kind::Commit | Kind::Tree | Kind::Tag => {
                let Some(raw) = entry.predecoded_raw.as_ref() else {
                    return Ok(None);
                };
                let link_started = std::time::Instant::now();
                let links = collect_links(oid, kind, raw, &mut refs, &mut tag_targets)?;
                if let Some((root_tree, parents)) = links.commit {
                    commit_entries.push(PackCommitIndexEntry {
                        commit_oid: oid,
                        root_tree,
                        parents,
                    });
                }
                metrics.link_ms += elapsed_ms(link_started);
                let source_index_started = std::time::Instant::now();
                source_index.append_tree_entries(links.tree_entries)?;
                metrics.source_index_write_ms += elapsed_ms(source_index_started);
            }
        }
    }
    let source_index_started = std::time::Instant::now();
    let source_index_file = source_index.finish(&commit_entries, entries.len() as u64)?;
    metrics.source_index_write_ms += elapsed_ms(source_index_started);

    Ok(Some(ResolveEntriesOutput {
        refs,
        tag_targets,
        source_index_file,
        commit_entries,
        largest,
        metrics,
        // Fast path is only reachable for delta-free packs, which are never thin.
        fixed_thin_pack: None,
    }))
}

fn resolve_receive_entry(
    file: &std::fs::File,
    entry: &ReceiveEntryMeta,
    base: Option<&StoredBase>,
    external_bases: &HashMap<Oid, ExternalBase>,
) -> Result<ResolveEntryOutput, CodecError> {
    let mut metrics = ResolveEntryMetrics::default();
    let predecoded = match (
        entry.resolved_kind,
        entry.oid,
        entry.resolved_size,
        entry.predecoded_raw.as_ref(),
    ) {
        (Some(kind), Some(oid), Some(size), Some(raw)) => Some((
            kind,
            ResolvedData {
                oid,
                size,
                raw: Some(raw.clone()),
                spilled: None,
                external_path: None,
            },
            entry.base_oid,
            entry.depth,
        )),
        (Some(kind @ Kind::Blob), Some(oid), Some(size), None)
            if matches!(entry.kind, ReceiveEntryKind::Full(_)) && entry.base_ref_count == 0 =>
        {
            Some((
                kind,
                ResolvedData {
                    oid,
                    size,
                    raw: None,
                    spilled: None,
                    external_path: None,
                },
                None,
                0,
            ))
        }
        _ => None,
    };
    let (kind, resolved, base_oid, depth) = if let Some(predecoded) = predecoded {
        predecoded
    } else {
        match entry.kind {
            ReceiveEntryKind::Full(kind) => {
                metrics.full_objects += 1;
                let full_started = std::time::Instant::now();
                let resolved = if let Some(raw) = entry.predecoded_raw.as_ref() {
                    let oid = entry
                        .oid
                        .ok_or_else(|| CodecError::Io("missing predecoded oid".into()))?;
                    ResolvedData {
                        oid,
                        size: raw.len() as u64,
                        raw: Some(raw.clone()),
                        spilled: None,
                        external_path: None,
                    }
                } else if let (Kind::Blob, Some(oid), Some(size), 0) =
                    (kind, entry.oid, entry.resolved_size, entry.base_ref_count)
                {
                    ResolvedData {
                        oid,
                        size,
                        raw: None,
                        spilled: None,
                        external_path: None,
                    }
                } else if kind == Kind::Blob {
                    let needs_base = entry.base_ref_count > 0;
                    let should_spill =
                        needs_base && entry.declared_size >= LARGE_DELTA_BASE_SPILL_BYTES as u64;
                    let should_stream = !needs_base || should_spill;
                    if should_stream {
                        metrics.streamed_blobs += 1;
                        stream_full_blob_at(file, entry, should_spill)?
                    } else {
                        let raw = inflate_entry_at(file, entry)?;
                        let size = raw.len() as u64;
                        let oid = crate::object::hash(kind, &raw);
                        ResolvedData {
                            oid,
                            size,
                            raw: Some(raw),
                            spilled: None,
                            external_path: None,
                        }
                    }
                } else {
                    let raw = inflate_entry_at(file, entry)?;
                    let size = raw.len() as u64;
                    let oid = crate::object::hash(kind, &raw);
                    ResolvedData {
                        oid,
                        size,
                        raw: Some(raw),
                        spilled: None,
                        external_path: None,
                    }
                };
                let full_elapsed = elapsed_ms(full_started);
                if kind == Kind::Blob {
                    metrics.full_blob_ms += full_elapsed;
                } else {
                    metrics.full_non_blob_ms += full_elapsed;
                }
                (kind, resolved, None, 0)
            }
            ReceiveEntryKind::OfsDelta { base_offset } => {
                metrics.ofs_deltas += 1;
                let delta_started = std::time::Instant::now();
                let delta = inflate_entry_at(file, entry)?;
                let base = base.ok_or(CodecError::MissingDeltaBaseOffset(base_offset))?;
                let target_size = delta_target_size(&delta, base.size)?;
                let resolved = resolve_delta_from_base(base, &delta, target_size as usize)?;
                metrics.delta_ms += elapsed_ms(delta_started);
                (base.kind, resolved, Some(base.oid), base.depth + 1)
            }
            ReceiveEntryKind::RefDelta { base_oid } => {
                metrics.ofs_deltas += 1;
                let delta_started = std::time::Instant::now();
                let delta = inflate_entry_at(file, entry)?;
                let Some(external) = external_bases.get(&base_oid) else {
                    return Err(CodecError::MissingDeltaBaseOid(base_oid));
                };
                let base = StoredBase {
                    oid: base_oid,
                    kind: external.kind,
                    size: external.size,
                    depth: 0,
                    remaining_refs: 1,
                    data: StoredBaseData::ExternalPath(external.path.clone()),
                };
                let target_size = delta_target_size(&delta, base.size)?;
                let resolved = resolve_delta_from_base(&base, &delta, target_size as usize)?;
                metrics.delta_ms += elapsed_ms(delta_started);
                (external.kind, resolved, Some(base_oid), 1)
            }
        }
    };

    let mut refs = Vec::new();
    let mut tag_targets = HashMap::new();
    let mut tree_entries = Vec::new();
    let mut commit = None;
    if let Some(raw) = resolved.raw.as_ref() {
        let link_started = std::time::Instant::now();
        let links = collect_links(resolved.oid, kind, raw, &mut refs, &mut tag_targets)?;
        tree_entries = links.tree_entries;
        if let Some((root_tree, parents)) = links.commit {
            commit = Some(PackCommitIndexEntry {
                commit_oid: resolved.oid,
                root_tree,
                parents,
            });
        }
        metrics.link_ms += elapsed_ms(link_started);
    }

    Ok(ResolveEntryOutput {
        oid: resolved.oid,
        kind,
        size: resolved.size,
        base_oid,
        depth,
        raw: resolved.raw,
        spilled: resolved.spilled,
        external_path: resolved.external_path,
        refs,
        tag_target: tag_targets.remove(&resolved.oid),
        tree_entries,
        commit,
        metrics,
    })
}

fn pack_type_of(kind: Kind) -> u8 {
    match kind {
        Kind::Commit => T_COMMIT,
        Kind::Tree => T_TREE,
        Kind::Blob => T_BLOB,
        Kind::Tag => T_TAG,
    }
}

/// Git pack entry header, delegating to the shared varint encoder.
fn encode_pack_entry_header(kind: Kind, size: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(10);
    write_entry_header(&mut out, pack_type_of(kind), size);
    out
}

/// Writer tee for fix-thin appends: bytes go to the output file while the pack trailer SHA-1 and
/// the per-entry CRC-32 accumulate.
struct FixThinWriter<'a> {
    out: &'a mut std::io::BufWriter<std::fs::File>,
    sha: &'a mut Sha1,
    crc: &'a mut crc32fast::Hasher,
    written: u64,
}

impl std::io::Write for FixThinWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.out.write_all(buf)?;
        self.sha.update(buf);
        self.crc.update(buf);
        self.written += buf.len() as u64;
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.out.flush()
    }
}

/// Exact, concurrency-safe OID de-duplication for outgoing pack references. Kernel-scale packs
/// emit hundreds of millions of tree-entry refs but only about one unique OID per pack object;
/// deduping inside the parallel resolve workers keeps duplicate refs out of the serial merge
/// loop and bounds resident ref memory to the unique set.
struct ShardedOidSet {
    shards: Vec<std::sync::Mutex<std::collections::HashSet<Oid>>>,
}

impl ShardedOidSet {
    const SHARDS: usize = 256;

    fn new() -> Self {
        Self {
            shards: (0..Self::SHARDS).map(|_| Default::default()).collect(),
        }
    }

    /// Insert, returning `true` when the OID was not already present. OIDs are cryptographic
    /// hashes, so the first byte is a uniform shard key.
    fn insert(&self, oid: &Oid) -> bool {
        self.shards[oid.first_byte() as usize % Self::SHARDS]
            .lock()
            .expect("oid shard poisoned")
            .insert(*oid)
    }
}

fn resolve_receive_entries_parallel(
    file: &std::fs::File,
    pack_len: u64,
    entries: &mut Vec<ReceiveEntryMeta>,
    external_bases: &HashMap<Oid, ExternalBase>,
) -> Result<ResolveEntriesOutput, CodecError> {
    let offset_to_idx: HashMap<u64, usize> = entries
        .iter()
        .enumerate()
        .map(|(idx, entry)| (entry.offset, idx))
        .collect();
    let mut children: Vec<Vec<usize>> = (0..entries.len()).map(|_| Vec::new()).collect();
    let mut base_idx_for_entry = vec![None; entries.len()];
    let mut ready = VecDeque::new();
    for (idx, entry) in entries.iter().enumerate() {
        match entry.kind {
            ReceiveEntryKind::Full(_) | ReceiveEntryKind::RefDelta { .. } => ready.push_back(idx),
            ReceiveEntryKind::OfsDelta { base_offset } => {
                let base_idx = *offset_to_idx
                    .get(&base_offset)
                    .ok_or(CodecError::MissingDeltaBaseOffset(base_offset))?;
                children[base_idx].push(idx);
                base_idx_for_entry[idx] = Some(base_idx);
            }
        }
    }

    let mut bases: Vec<Option<StoredBase>> = (0..entries.len()).map(|_| None).collect();
    let seen_refs = ShardedOidSet::new();
    let mut refs = Vec::new();
    let mut tag_targets = HashMap::new();
    // Source-index rows are written by a dedicated thread so serializing tens of GiB of tree
    // entries does not stall the merge loop between rayon batches.
    let (tree_entry_tx, tree_entry_rx) =
        std::sync::mpsc::sync_channel::<Vec<PackTreeIndexEntry>>(SOURCE_INDEX_WRITER_QUEUE);
    let object_count = entries.len();
    let source_index_writer =
        std::thread::spawn(move || -> Result<SourceIndexFileWriter, CodecError> {
            let mut source_index = SourceIndexFileWriter::new(object_count)?;
            while let Ok(batch) = tree_entry_rx.recv() {
                source_index.append_tree_entries(batch)?;
            }
            Ok(source_index)
        });
    let mut commit_entries = Vec::new();
    let mut largest = None;
    let mut metrics = ResolveEntryMetrics::default();
    let mut resolved_count = 0usize;
    let mut resident_base_bytes = 0usize;
    let mut peak_resident_base_bytes = 0usize;

    while !ready.is_empty() {
        let take = ready.len().min(PARALLEL_RESOLVE_BATCH);
        let mut batch = Vec::with_capacity(take);
        for _ in 0..take {
            if let Some(idx) = ready.pop_front() {
                batch.push(idx);
            }
        }
        let results: Vec<(usize, Result<ResolveEntryOutput, CodecError>)> = batch
            .par_iter()
            .map(|&idx| {
                let base = base_idx_for_entry[idx].and_then(|base_idx| bases[base_idx].as_ref());
                let mut result = resolve_receive_entry(file, &entries[idx], base, external_bases);
                if let Ok(resolved) = result.as_mut() {
                    // Drop already-seen outgoing refs here, on the worker, so the serial merge
                    // below only appends novel OIDs.
                    resolved.refs.retain(|oid| seen_refs.insert(oid));
                }
                (idx, result)
            })
            .collect();

        let mut newly_ready = Vec::new();
        for (idx, result) in results {
            let mut resolved = result?;
            entries[idx].oid = Some(resolved.oid);
            entries[idx].resolved_kind = Some(resolved.kind);
            entries[idx].resolved_size = Some(resolved.size);
            entries[idx].base_oid = resolved.base_oid;
            entries[idx].depth = resolved.depth;
            if largest.is_none_or(|(_, max)| resolved.size > max) {
                largest = Some((resolved.oid, resolved.size));
            }
            refs.append(&mut resolved.refs);
            if let Some(tag_target) = resolved.tag_target {
                tag_targets.insert(resolved.oid, tag_target);
            }
            if !resolved.tree_entries.is_empty() {
                let source_index_started = std::time::Instant::now();
                if tree_entry_tx.send(resolved.tree_entries).is_err() {
                    // Writer thread failed; surface its error below by breaking out through join.
                    drop(tree_entry_tx);
                    return Err(source_index_writer
                        .join()
                        .map_err(|_| CodecError::Io("source-index writer panicked".into()))?
                        .err()
                        .unwrap_or_else(|| {
                            CodecError::Io("source-index writer stopped unexpectedly".into())
                        }));
                }
                resolved.metrics.source_index_write_ms += elapsed_ms(source_index_started);
            }
            if let Some(commit) = resolved.commit {
                commit_entries.push(commit);
            }
            if entries[idx].base_ref_count > 0 {
                let base_store_started = std::time::Instant::now();
                let resolved_oid = resolved.oid;
                let resolved_size = resolved.size;
                let base = match (resolved.raw, resolved.spilled, resolved.external_path) {
                    (Some(raw), None, None) => {
                        let base = store_delta_base_with_memory_budget(
                            resolved_oid,
                            resolved.kind,
                            resolved.depth,
                            raw,
                            entries[idx].base_ref_count,
                            &mut resident_base_bytes,
                        )?;
                        if matches!(base.data, StoredBaseData::Spilled(_)) {
                            resolved.metrics.spilled_delta_bases += 1;
                        }
                        peak_resident_base_bytes =
                            peak_resident_base_bytes.max(resident_base_bytes);
                        base
                    }
                    (None, Some(file), None) => {
                        resolved.metrics.spilled_delta_bases += 1;
                        store_spilled_delta_base(
                            resolved_oid,
                            resolved.kind,
                            resolved_size,
                            resolved.depth,
                            file,
                            entries[idx].base_ref_count,
                        )
                    }
                    (None, None, Some(path)) => {
                        resolved.metrics.spilled_delta_bases += 1;
                        StoredBase {
                            oid: resolved_oid,
                            kind: resolved.kind,
                            size: resolved_size,
                            depth: resolved.depth,
                            remaining_refs: entries[idx].base_ref_count,
                            data: StoredBaseData::ExternalPath(path),
                        }
                    }
                    (None, None, None) if entries[idx].base_ref_count == 0 => unreachable!(),
                    _ => return Err(CodecError::Io("invalid resolved base state".into())),
                };
                bases[idx] = Some(base);
                resolved.metrics.base_store_ms += elapsed_ms(base_store_started);
            }
            for child in &children[idx] {
                newly_ready.push(*child);
            }
            if let Some(base_idx) = base_idx_for_entry[idx] {
                if let Some(base) = bases[base_idx].as_mut() {
                    base.remaining_refs = base.remaining_refs.saturating_sub(1);
                    if base.remaining_refs == 0 {
                        resident_base_bytes =
                            resident_base_bytes.saturating_sub(stored_base_resident_bytes(base));
                        bases[base_idx] = None;
                    }
                }
            }
            merge_metrics(&mut metrics, resolved.metrics);
            resolved_count += 1;
        }
        // Keep delta-base residency bounded: resolve children before unrelated roots.
        for child in newly_ready.into_iter().rev() {
            ready.push_front(child);
        }
    }

    if resolved_count != entries.len() {
        return Err(CodecError::Io(format!(
            "resolved {resolved_count} of {} pack entries",
            entries.len()
        )));
    }
    metrics.peak_delta_base_resident_bytes = peak_resident_base_bytes;

    // Thin pack: append every external REF_DELTA base as a full object so the manifested pack is
    // self-contained. Appended entries flow into the idx and `.objects`; their links flow into
    // `.sources` (via the still-live writer thread), the commit index, and the connectivity ref
    // set, exactly as if they had been part of the received pack.
    let fixed_thin_pack = if external_bases.is_empty() {
        None
    } else {
        let mut appended_refs: Vec<Oid> = Vec::new();
        let fixed = append_external_bases(AppendExternalBases {
            journal: file,
            pack_len,
            entries,
            external_bases,
            tree_entry_tx: &tree_entry_tx,
            commit_entries: &mut commit_entries,
            refs: &mut appended_refs,
            tag_targets: &mut tag_targets,
        })?;
        for oid in appended_refs {
            if seen_refs.insert(&oid) {
                refs.push(oid);
            }
        }
        Some(fixed)
    };

    let source_index_started = std::time::Instant::now();
    drop(tree_entry_tx);
    let source_index = source_index_writer
        .join()
        .map_err(|_| CodecError::Io("source-index writer panicked".into()))??;
    let source_index_file = source_index.finish(&commit_entries, entries.len() as u64)?;
    metrics.source_index_write_ms += elapsed_ms(source_index_started);

    Ok(ResolveEntriesOutput {
        refs,
        tag_targets,
        source_index_file,
        commit_entries,
        largest,
        metrics,
        fixed_thin_pack,
    })
}

struct AppendExternalBases<'a> {
    journal: &'a std::fs::File,
    pack_len: u64,
    entries: &'a mut Vec<ReceiveEntryMeta>,
    external_bases: &'a HashMap<Oid, ExternalBase>,
    tree_entry_tx: &'a std::sync::mpsc::SyncSender<Vec<PackTreeIndexEntry>>,
    commit_entries: &'a mut Vec<PackCommitIndexEntry>,
    refs: &'a mut Vec<Oid>,
    tag_targets: &'a mut HashMap<Oid, Oid>,
}

/// Build the fix-thin replacement pack: copy the received bytes minus the trailer with the entry
/// count patched, append each external base as a full (zlib) object streamed from its spill file,
/// then write the recomputed SHA-1 trailer. Appended entry metadata is pushed onto `entries` and
/// non-blob bases contribute their links.
fn append_external_bases(input: AppendExternalBases<'_>) -> Result<FixedThinPack, CodecError> {
    let AppendExternalBases {
        journal,
        pack_len,
        entries,
        external_bases,
        tree_entry_tx,
        commit_entries,
        refs,
        tag_targets,
    } = input;
    let io_err = |e: std::io::Error| CodecError::Io(e.to_string());
    let body_len = pack_len
        .checked_sub(20)
        .ok_or_else(|| CodecError::Io("thin pack shorter than its trailer".into()))?;

    let out_file = tempfile::NamedTempFile::new().map_err(|e| CodecError::Io(e.to_string()))?;
    let mut out = std::io::BufWriter::new(
        out_file
            .as_file()
            .try_clone()
            .map_err(|e| CodecError::Io(e.to_string()))?,
    );
    let mut sha = Sha1::new();
    use std::io::Write as _;

    // Header with the entry count patched to include the appended bases.
    let mut header = [0u8; 12];
    read_exact_at(journal, 0, &mut header)?;
    let count = u32::from_be_bytes(header[8..12].try_into().unwrap())
        .checked_add(external_bases.len() as u32)
        .ok_or_else(|| CodecError::Io("fix-thin entry count overflow".into()))?;
    header[8..12].copy_from_slice(&count.to_be_bytes());
    out.write_all(&header).map_err(io_err)?;
    sha.update(header);

    // Original entry bytes (past the header, minus the trailer).
    let mut offset = 12u64;
    let mut buf = vec![0u8; 1024 * 1024];
    while offset < body_len {
        let n = ((body_len - offset) as usize).min(buf.len());
        read_exact_at(journal, offset, &mut buf[..n])?;
        out.write_all(&buf[..n]).map_err(io_err)?;
        sha.update(&buf[..n]);
        offset += n as u64;
    }

    // Append each base, in oid order for determinism.
    let mut bases: Vec<(&Oid, &ExternalBase)> = external_bases.iter().collect();
    bases.sort_by_key(|(oid, _)| *(*oid));
    let mut out_len = body_len;
    for (oid, base) in bases {
        let entry_offset = out_len;
        let mut crc = crc32fast::Hasher::new();
        let header = encode_pack_entry_header(base.kind, base.size);
        out.write_all(&header).map_err(io_err)?;
        sha.update(&header);
        crc.update(&header);
        out_len += header.len() as u64;
        let compressed_offset = out_len;

        let payload = std::fs::File::open(&base.path).map_err(io_err)?;
        let mut reader = std::io::BufReader::new(payload);
        let mut tee = FixThinWriter {
            out: &mut out,
            sha: &mut sha,
            crc: &mut crc,
            written: 0,
        };
        let mut enc = flate2::write::ZlibEncoder::new(&mut tee, flate2::Compression::new(6));
        std::io::copy(&mut reader, &mut enc).map_err(io_err)?;
        enc.finish().map_err(io_err)?;
        let compressed_len = tee.written;
        out_len += compressed_len;

        // Links for non-blob bases: their children must count for connectivity and the derived
        // tree/commit indexes, exactly as for in-pack objects.
        if base.kind != Kind::Blob {
            let raw = std::fs::read(&base.path).map_err(io_err)?;
            let links = collect_links(*oid, base.kind, &raw, refs, tag_targets)?;
            if !links.tree_entries.is_empty() {
                tree_entry_tx.send(links.tree_entries).map_err(|_| {
                    CodecError::Io("source-index writer stopped during fix-thin".into())
                })?;
            }
            if let Some((root_tree, parents)) = links.commit {
                commit_entries.push(PackCommitIndexEntry {
                    commit_oid: *oid,
                    root_tree,
                    parents,
                });
            }
        }

        entries.push(ReceiveEntryMeta {
            offset: entry_offset,
            compressed_offset,
            compressed_len: compressed_len as usize,
            declared_size: base.size,
            kind: ReceiveEntryKind::Full(base.kind),
            crc32: crc.finalize(),
            base_ref_count: 0,
            oid: Some(*oid),
            resolved_kind: Some(base.kind),
            resolved_size: Some(base.size),
            predecoded_raw: None,
            base_oid: None,
            depth: 0,
        });
    }

    let trailer = finalize_sha1(sha);
    out.write_all(&trailer).map_err(io_err)?;
    out.flush().map_err(io_err)?;
    drop(out);
    Ok(FixedThinPack {
        file: out_file,
        len: out_len + 20,
        pack_hash: Oid::from_array(trailer),
    })
}

/// Resolve and index a received pack from a local temp file without memory-mapping the whole pack.
///
/// The first pass walks the file sequentially to validate the pack, compute per-entry CRCs, and record
/// each compressed entry range. The second pass inflates one compressed entry at a time, resolving
/// offset-deltas while retaining only objects still needed as future delta bases. That keeps memory
/// proportional to object metadata plus live delta bases, not to the compressed pack size.
pub fn resolve_pack_file(path: impl AsRef<Path>) -> Result<Option<ResolvedPack>, CodecError> {
    resolve_pack_file_with_log_context(path, PackResolveLogContext::default())
}

/// Request context attached to resolver instrumentation.
#[derive(Clone, Copy, Default)]
pub struct PackResolveLogContext<'a> {
    pub project: &'a str,
    pub repo: &'a str,
    pub actor: &'a str,
}

/// Resolve and index a received pack, attaching caller context to instrumentation logs.
pub fn resolve_pack_file_with_log_context(
    path: impl AsRef<Path>,
    log_context: PackResolveLogContext<'_>,
) -> Result<Option<ResolvedPack>, CodecError> {
    let path = path.as_ref();
    let started = std::time::Instant::now();
    let mut file = std::fs::File::open(path).map_err(|e| CodecError::Io(e.to_string()))?;
    let pack_len = file
        .metadata()
        .map_err(|e| CodecError::Io(e.to_string()))?
        .len();
    let scan_started = std::time::Instant::now();
    let (pack_hash, mut entries) = match scan_received_pack(&mut file, pack_len, false)? {
        Some(v) => v,
        None => return Ok(None),
    };
    let scan_ms = elapsed_ms(scan_started);
    resolve_scanned_pack_inner(ResolveScannedPackInner {
        file: &file,
        pack_len,
        pack_hash,
        entries: &mut entries,
        scan_stats: ScanResolveStats {
            scan_ms,
            spilled_payload_bytes: 0,
            ..ScanResolveStats::default()
        },
        started,
        external_bases: &HashMap::new(),
        log_context,
    })
}

pub fn resolve_scanned_pack_with_log_context(
    file: &std::fs::File,
    scanned: ScannedPack,
    log_context: PackResolveLogContext<'_>,
) -> Result<Option<ResolvedPack>, CodecError> {
    resolve_scanned_pack_with_external_bases(file, scanned, &HashMap::new(), log_context)
}

pub fn resolve_scanned_pack_with_external_bases(
    file: &std::fs::File,
    mut scanned: ScannedPack,
    external_bases: &HashMap<Oid, ExternalBase>,
    log_context: PackResolveLogContext<'_>,
) -> Result<Option<ResolvedPack>, CodecError> {
    let started = std::time::Instant::now();
    let retained_payload_bytes = scanned.retained_payload_bytes;
    let spilled_payload_bytes = scanned.spilled_payload_bytes;
    let predecode_budget_exhausted = scanned.predecode_budget_exhausted;
    resolve_scanned_pack_inner(ResolveScannedPackInner {
        file,
        pack_len: scanned.pack_len,
        pack_hash: scanned.pack_hash,
        entries: &mut scanned.entries,
        scan_stats: ScanResolveStats {
            scan_ms: scanned.scan_ms,
            retained_payload_bytes,
            spilled_payload_bytes,
            predecode_budget_exhausted,
        },
        started,
        external_bases,
        log_context,
    })
}

struct ResolveScannedPackInner<'a, 'ctx> {
    file: &'a std::fs::File,
    pack_len: u64,
    pack_hash: Oid,
    entries: &'a mut Vec<ReceiveEntryMeta>,
    scan_stats: ScanResolveStats,
    started: std::time::Instant,
    external_bases: &'a HashMap<Oid, ExternalBase>,
    log_context: PackResolveLogContext<'ctx>,
}

fn resolve_scanned_pack_inner(
    input: ResolveScannedPackInner<'_, '_>,
) -> Result<Option<ResolvedPack>, CodecError> {
    let ResolveScannedPackInner {
        file,
        pack_len,
        pack_hash,
        entries,
        scan_stats,
        started,
        external_bases,
        log_context,
    } = input;
    let active_base_count = entries
        .iter()
        .filter(|entry| entry.base_ref_count > 0)
        .count();
    let resolve_started = std::time::Instant::now();
    let (resolved, replayed_pack_entries) =
        if let Some(resolved) = resolve_entries_from_scanned_metadata(entries)? {
            (resolved, false)
        } else {
            (
                resolve_receive_entries_parallel(file, pack_len, entries, external_bases)?,
                true,
            )
        };
    let resolve_entries_ms = elapsed_ms(resolve_started);
    // A thin pack resolves against a fixed (self-contained) replacement: its length and trailer
    // hash describe the bytes that will actually be stored and indexed.
    if !external_bases.is_empty() && resolved.fixed_thin_pack.is_none() {
        // Never manifest thin bytes: whichever resolve path ran must have produced the
        // self-contained replacement when external bases exist.
        return Err(CodecError::Io(
            "thin pack resolved without a fix-thin replacement".into(),
        ));
    }
    let (pack_len, pack_hash) = match &resolved.fixed_thin_pack {
        Some(fixed) => (fixed.len, fixed.pack_hash),
        None => (pack_len, pack_hash),
    };
    finish_resolved_pack(ResolvedPackFinish {
        pack_len,
        pack_hash,
        entries,
        scan_stats,
        started,
        log_context,
        active_base_count,
        resolved,
        replayed_pack_entries,
        resolve_entries_ms,
    })
}

struct ResolvedPackFinish<'entries, 'ctx> {
    pack_len: u64,
    pack_hash: Oid,
    entries: &'entries [ReceiveEntryMeta],
    scan_stats: ScanResolveStats,
    started: std::time::Instant,
    log_context: PackResolveLogContext<'ctx>,
    active_base_count: usize,
    resolved: ResolveEntriesOutput,
    replayed_pack_entries: bool,
    resolve_entries_ms: f64,
}

fn finish_resolved_pack(
    input: ResolvedPackFinish<'_, '_>,
) -> Result<Option<ResolvedPack>, CodecError> {
    let ResolvedPackFinish {
        pack_len,
        pack_hash,
        entries,
        scan_stats,
        started,
        log_context,
        active_base_count,
        resolved,
        replayed_pack_entries,
        resolve_entries_ms,
    } = input;
    let index_metadata_started = std::time::Instant::now();
    let (object_index_file, oids) = write_resolved_pack_object_index_file(entries)?;
    let index_metadata_ms = elapsed_ms(index_metadata_started);
    let idx_started = std::time::Instant::now();
    let idx_data = write_idx_v2_from_receive_entries(entries, pack_hash)?;
    let idx_bytes = idx_data.len();
    let idx_file = write_temp_bytes(idx_data)?;
    let idx_ms = elapsed_ms(idx_started);
    let external_refs_started = std::time::Instant::now();
    // Refs were deduplicated during resolve; filter the unique set against the pack's own OIDs in
    // parallel — kernel-scale packs carry tens of millions of unique refs.
    let refs = resolved.refs;
    let external_refs: std::collections::HashSet<Oid> = refs
        .par_chunks(EXTERNAL_REF_FILTER_CHUNK)
        .fold(std::collections::HashSet::new, |mut acc, chunk| {
            acc.extend(chunk.iter().filter(|oid| oids.binary_search(oid).is_err()));
            acc
        })
        .reduce(std::collections::HashSet::new, |mut a, mut b| {
            if a.len() < b.len() {
                std::mem::swap(&mut a, &mut b);
            }
            a.extend(b);
            a
        });
    let external_refs_ms = elapsed_ms(external_refs_started);

    tracing::info!(
        project = log_context.project,
        repo = log_context.repo,
        actor = log_context.actor,
        pack_id = %pack_hash.to_hex(),
        pack_bytes = pack_len,
        objects = entries.len(),
        full_objects = resolved.metrics.full_objects,
        ofs_deltas = resolved.metrics.ofs_deltas,
        streamed_blobs = resolved.metrics.streamed_blobs,
        active_delta_bases = active_base_count,
        spilled_delta_bases = resolved.metrics.spilled_delta_bases,
        peak_delta_base_resident_bytes = resolved.metrics.peak_delta_base_resident_bytes,
        external_refs = external_refs.len(),
        idx_bytes,
        replayed_pack_entries,
        scanner_retained_payload_bytes = scan_stats.retained_payload_bytes,
        scanner_spilled_payload_bytes = scan_stats.spilled_payload_bytes,
        scanner_predecode_budget_exhausted = scan_stats.predecode_budget_exhausted,
        scan_ms = scan_stats.scan_ms,
        resolve_entries_ms,
        index_metadata_ms,
        idx_ms,
        source_index_ms = resolved.metrics.source_index_write_ms,
        external_refs_ms,
        full_blob_cpu_ms = resolved.metrics.full_blob_ms,
        full_non_blob_cpu_ms = resolved.metrics.full_non_blob_ms,
        delta_cpu_ms = resolved.metrics.delta_ms,
        link_cpu_ms = resolved.metrics.link_ms,
        base_store_cpu_ms = resolved.metrics.base_store_ms,
        total_ms = elapsed_ms(started),
        "pack resolver: complete"
    );

    Ok(Some(ResolvedPack {
        object_count: entries.len(),
        pack_hash,
        fixed_thin_pack: resolved.fixed_thin_pack,
        replayed_pack_entries,
        idx_file,
        idx_bytes,
        object_index_file,
        source_index_file: resolved.source_index_file,
        source_commit_entries: resolved.commit_entries,
        oids,
        external_refs,
        tag_targets: resolved.tag_targets,
        largest: resolved.largest,
    }))
}

fn elapsed_ms(started: std::time::Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}

/// Inspect a self-contained pack for artifact extraction without materializing small objects.
///
/// Returns `Ok(None)` for packs with `REF_DELTA` entries because those packs are thin and cannot be
/// interpreted without external bases. The caller should mark such packs optimized in place.
pub fn plan_pack_file_optimization(
    path: impl AsRef<Path>,
    large_threshold: u64,
) -> Result<Option<PackOptimizationPlan>, CodecError> {
    let path = path.as_ref();
    let mut file = std::fs::File::open(path).map_err(|e| CodecError::Io(e.to_string()))?;
    let pack_len = file
        .metadata()
        .map_err(|e| CodecError::Io(e.to_string()))?
        .len();
    let (_, mut entries) = match scan_received_pack(&mut file, pack_len, false)? {
        Some(v) => v,
        None => return Ok(None),
    };

    let active_base_count = entries
        .iter()
        .filter(|entry| entry.base_ref_count > 0)
        .count();
    let mut resolved_bases: HashMap<u64, StoredBase> =
        HashMap::with_capacity(active_base_count.min(PREALLOC_HINT_CAP));
    let mut large_blobs = Vec::new();
    let spill_threshold = large_threshold.min(usize::MAX as u64) as usize;

    for entry in &mut entries {
        let offset = entry.offset;
        let entry_kind = entry.kind;
        let base_ref_count = entry.base_ref_count;
        let (kind, resolved, base_oid, depth) = match entry_kind {
            ReceiveEntryKind::Full(kind) => {
                let resolved = if kind == Kind::Blob && entry.declared_size >= large_threshold {
                    stream_full_blob_at(&file, entry, true)?
                } else if base_ref_count > 0 {
                    let raw = inflate_entry_at(&file, entry)?;
                    let size = raw.len() as u64;
                    let oid = crate::object::hash(kind, &raw);
                    ResolvedData {
                        oid,
                        size,
                        raw: Some(raw),
                        spilled: None,
                        external_path: None,
                    }
                } else {
                    stream_full_object_at(&file, entry, kind, false)?
                };
                (kind, resolved, None, 0)
            }
            ReceiveEntryKind::OfsDelta { base_offset } => {
                let delta = inflate_entry_at(&file, entry)?;
                let base = resolved_bases
                    .get(&base_offset)
                    .ok_or(CodecError::MissingDeltaBaseOffset(base_offset))?;
                let target_size = delta_target_size(&delta, base.size)?;
                let resolved = resolve_delta_from_base_with_spill_threshold(
                    base,
                    &delta,
                    target_size as usize,
                    spill_threshold,
                )?;
                (base.kind, resolved, Some(base.oid), base.depth + 1)
            }
            ReceiveEntryKind::RefDelta { base_oid } => {
                return Err(CodecError::MissingDeltaBaseOid(base_oid));
            }
        };

        entry.oid = Some(resolved.oid);
        entry.resolved_kind = Some(kind);
        entry.resolved_size = Some(resolved.size);
        entry.base_oid = base_oid;
        entry.depth = depth;
        let is_large_blob = kind == Kind::Blob && resolved.size >= large_threshold;

        let mut base_raw = None;
        let mut base_spilled = None;
        if is_large_blob {
            let blob_file = match (&resolved.raw, &resolved.spilled) {
                (Some(raw), None) => write_temp(raw)?,
                (None, Some(file)) => copy_temp(file.path())?,
                _ => return Err(CodecError::Io("invalid resolved blob state".into())),
            };
            large_blobs.push(FileBackedBlob {
                oid: resolved.oid,
                size: resolved.size,
                file: blob_file,
            });
        }

        if base_ref_count > 0 {
            let resolved_oid = resolved.oid;
            let resolved_size = resolved.size;
            if is_large_blob {
                match (resolved.raw, resolved.spilled) {
                    (Some(raw), None) => base_raw = Some(raw),
                    (None, Some(file)) => base_spilled = Some(file),
                    _ => return Err(CodecError::Io("invalid resolved base state".into())),
                }
            } else {
                base_raw = resolved.raw;
                base_spilled = resolved.spilled;
            }
            let base = match (base_raw, base_spilled) {
                (Some(raw), None) => {
                    store_delta_base(resolved_oid, kind, depth, raw, base_ref_count)?
                }
                (None, Some(file)) => store_spilled_delta_base(
                    resolved_oid,
                    kind,
                    resolved_size,
                    depth,
                    file,
                    base_ref_count,
                ),
                _ => return Err(CodecError::Io("invalid resolved base state".into())),
            };
            resolved_bases.insert(offset, base);
        }
        if let ReceiveEntryKind::OfsDelta { base_offset } = entry_kind {
            if let Some(base) = resolved_bases.get_mut(&base_offset) {
                base.remaining_refs = base.remaining_refs.saturating_sub(1);
                if base.remaining_refs == 0 {
                    resolved_bases.remove(&base_offset);
                }
            }
        }
    }

    Ok(Some(PackOptimizationPlan {
        object_count: entries.len(),
        large_blobs,
    }))
}

/// Inspect a self-contained pack and extract tree-entry metadata without retaining blob payloads.
///
/// Returns `Ok(None)` for thin packs with `REF_DELTA` entries. The caller can retry after a later
/// derived-index path has an external-base resolver.
pub fn plan_pack_tree_index(
    path: impl AsRef<Path>,
    spill_threshold: u64,
) -> Result<Option<PackTreeIndexPlan>, CodecError> {
    plan_pack_tree_index_with_external_bases(path, spill_threshold, &HashMap::new())
}

/// Return the external base oids referenced by `REF_DELTA` entries in a pack.
pub fn pack_ref_delta_bases(path: impl AsRef<Path>) -> Result<Vec<Oid>, CodecError> {
    let path = path.as_ref();
    let mut file = std::fs::File::open(path).map_err(|e| CodecError::Io(e.to_string()))?;
    let pack_len = file
        .metadata()
        .map_err(|e| CodecError::Io(e.to_string()))?
        .len();
    let (_, entries) = match scan_received_pack(&mut file, pack_len, true)? {
        Some(v) => v,
        None => return Ok(Vec::new()),
    };
    let mut out = Vec::new();
    for entry in entries {
        if let ReceiveEntryKind::RefDelta { base_oid } = entry.kind {
            out.push(base_oid);
        }
    }
    Ok(out)
}

/// Build a derived tree/commit index plan, resolving `REF_DELTA` entries through `external_bases`.
/// Returns `Ok(None)` when the pack is thin and one or more external bases are unavailable.
pub fn plan_pack_tree_index_with_external_bases(
    path: impl AsRef<Path>,
    spill_threshold: u64,
    external_bases: &HashMap<Oid, ExternalBase>,
) -> Result<Option<PackTreeIndexPlan>, CodecError> {
    let path = path.as_ref();
    let mut file = std::fs::File::open(path).map_err(|e| CodecError::Io(e.to_string()))?;
    let pack_len = file
        .metadata()
        .map_err(|e| CodecError::Io(e.to_string()))?
        .len();
    let (_, mut entries) = match scan_received_pack(&mut file, pack_len, true)? {
        Some(v) => v,
        None => return Ok(None),
    };

    let active_base_count = entries
        .iter()
        .filter(|entry| entry.base_ref_count > 0)
        .count();
    let mut resolved_bases: HashMap<u64, StoredBase> =
        HashMap::with_capacity(active_base_count.min(PREALLOC_HINT_CAP));
    let mut tree_entries_out = Vec::new();
    let mut commit_entries_out = Vec::new();
    let spill_threshold = spill_threshold.min(usize::MAX as u64) as usize;

    for entry in &mut entries {
        let offset = entry.offset;
        let entry_kind = entry.kind;
        let base_ref_count = entry.base_ref_count;
        let (kind, resolved, base_oid, depth) = match entry_kind {
            ReceiveEntryKind::Full(kind) => {
                let resolved =
                    if kind == Kind::Blob && entry.declared_size >= spill_threshold as u64 {
                        stream_full_blob_at(&file, entry, true)?
                    } else if kind == Kind::Tree || kind == Kind::Commit || base_ref_count > 0 {
                        let raw = inflate_entry_at(&file, entry)?;
                        let size = raw.len() as u64;
                        let oid = crate::object::hash(kind, &raw);
                        ResolvedData {
                            oid,
                            size,
                            raw: Some(raw),
                            spilled: None,
                            external_path: None,
                        }
                    } else {
                        continue;
                    };
                (kind, resolved, None, 0)
            }
            ReceiveEntryKind::OfsDelta { base_offset } => {
                let delta = inflate_entry_at(&file, entry)?;
                let base = resolved_bases
                    .get(&base_offset)
                    .ok_or(CodecError::MissingDeltaBaseOffset(base_offset))?;
                let target_size = delta_target_size(&delta, base.size)?;
                let resolved = resolve_delta_from_base_with_spill_threshold(
                    base,
                    &delta,
                    target_size as usize,
                    spill_threshold,
                )?;
                (base.kind, resolved, Some(base.oid), base.depth + 1)
            }
            ReceiveEntryKind::RefDelta { base_oid } => {
                let Some(base) = external_bases.get(&base_oid) else {
                    return Ok(None);
                };
                let delta = inflate_entry_at(&file, entry)?;
                let target_size = delta_target_size(&delta, base.size)?;
                let stored_base = StoredBase {
                    oid: base_oid,
                    kind: base.kind,
                    size: base.size,
                    depth: 0,
                    remaining_refs: 1,
                    data: StoredBaseData::ExternalPath(base.path.clone()),
                };
                let resolved = resolve_delta_from_base_with_spill_threshold(
                    &stored_base,
                    &delta,
                    target_size as usize,
                    spill_threshold,
                )?;
                let kind = base.kind;
                (kind, resolved, Some(base_oid), 1)
            }
        };

        entry.oid = Some(resolved.oid);
        entry.resolved_kind = Some(kind);
        entry.resolved_size = Some(resolved.size);
        entry.base_oid = base_oid;
        entry.depth = depth;
        if kind == Kind::Tree {
            if let Some(raw) = resolved.raw.as_ref() {
                for e in crate::graph::tree_entries(raw)? {
                    tree_entries_out.push(PackTreeIndexEntry {
                        tree_oid: resolved.oid,
                        mode: e.mode,
                        name: e.name,
                        oid: e.oid,
                    });
                }
            }
        } else if kind == Kind::Commit {
            if let Some(raw) = resolved.raw.as_ref() {
                let (root_tree, parents) = crate::graph::commit_links(raw)?;
                commit_entries_out.push(PackCommitIndexEntry {
                    commit_oid: resolved.oid,
                    root_tree,
                    parents,
                });
            }
        }

        if base_ref_count > 0 {
            let resolved_oid = resolved.oid;
            let resolved_size = resolved.size;
            let base = match (resolved.raw, resolved.spilled) {
                (Some(raw), None) => {
                    store_delta_base(resolved_oid, kind, depth, raw, base_ref_count)?
                }
                (None, Some(file)) => store_spilled_delta_base(
                    resolved_oid,
                    kind,
                    resolved_size,
                    depth,
                    file,
                    base_ref_count,
                ),
                _ => return Err(CodecError::Io("invalid resolved base state".into())),
            };
            resolved_bases.insert(offset, base);
        }
        if let ReceiveEntryKind::OfsDelta { base_offset } = entry_kind {
            if let Some(base) = resolved_bases.get_mut(&base_offset) {
                base.remaining_refs = base.remaining_refs.saturating_sub(1);
                if base.remaining_refs == 0 {
                    resolved_bases.remove(&base_offset);
                }
            }
        }
    }

    Ok(Some(PackTreeIndexPlan {
        object_count: entries.len(),
        entries: tree_entries_out,
        commits: commit_entries_out,
    }))
}

fn write_temp(bytes: &[u8]) -> Result<tempfile::NamedTempFile, CodecError> {
    let mut file = tempfile::NamedTempFile::new().map_err(|e| CodecError::Io(e.to_string()))?;
    file.write_all(bytes)
        .map_err(|e| CodecError::Io(e.to_string()))?;
    file.as_file_mut()
        .flush()
        .map_err(|e| CodecError::Io(e.to_string()))?;
    Ok(file)
}

fn copy_temp(path: &Path) -> Result<tempfile::NamedTempFile, CodecError> {
    let mut input = std::fs::File::open(path).map_err(|e| CodecError::Io(e.to_string()))?;
    let mut output = tempfile::NamedTempFile::new().map_err(|e| CodecError::Io(e.to_string()))?;
    std::io::copy(&mut input, &mut output).map_err(|e| CodecError::Io(e.to_string()))?;
    output
        .as_file_mut()
        .flush()
        .map_err(|e| CodecError::Io(e.to_string()))?;
    Ok(output)
}

/// Resolve every object in a **self-contained** pack by spilling `data` to a temp file and using the
/// same file-backed indexer as receive-pack.
///
/// Returns `Ok(None)` if the pack carries any `REF_DELTA` entry — that's a thin pack whose bases live
/// outside it (an incremental fetch/push), which the streaming parser resolves on demand against the
/// repo. Those packs are small, so the streaming path is already fast.
pub fn resolve_pack_parallel(data: &[u8]) -> Result<Option<ResolvedPack>, CodecError> {
    let mut tmp = tempfile::NamedTempFile::new().map_err(|e| CodecError::Io(e.to_string()))?;
    tmp.write_all(data)
        .map_err(|e| CodecError::Io(e.to_string()))?;
    tmp.as_file_mut()
        .flush()
        .map_err(|e| CodecError::Io(e.to_string()))?;
    resolve_pack_file(tmp.path())
}

struct LinkOutput {
    commit: Option<(Oid, Vec<Oid>)>,
    tree_entries: Vec<PackTreeIndexEntry>,
}

fn collect_links(
    oid: Oid,
    kind: Kind,
    bytes: &[u8],
    refs: &mut Vec<Oid>,
    tag_targets: &mut std::collections::HashMap<Oid, Oid>,
) -> Result<LinkOutput, CodecError> {
    match kind {
        Kind::Blob => Ok(LinkOutput {
            commit: None,
            tree_entries: Vec::new(),
        }),
        Kind::Commit => {
            let (tree, parents) = crate::graph::commit_links(bytes)?;
            refs.push(tree);
            refs.extend(parents.iter().copied());
            Ok(LinkOutput {
                commit: Some((tree, parents)),
                tree_entries: Vec::new(),
            })
        }
        Kind::Tree => {
            let mut tree_entries = Vec::new();
            for entry in crate::graph::tree_entries(bytes)? {
                if entry.is_tree() || entry.is_blob() {
                    refs.push(entry.oid);
                }
                tree_entries.push(PackTreeIndexEntry {
                    tree_oid: oid,
                    mode: entry.mode,
                    name: entry.name,
                    oid: entry.oid,
                });
            }
            Ok(LinkOutput {
                commit: None,
                tree_entries,
            })
        }
        Kind::Tag => {
            let (target, target_kind) = crate::graph::tag_target(bytes)?;
            tag_targets.insert(oid, target);
            match target_kind {
                Some(Kind::Commit | Kind::Tree | Kind::Blob | Kind::Tag) | None => {
                    refs.push(target);
                }
            }
            Ok(LinkOutput {
                commit: None,
                tree_entries: Vec::new(),
            })
        }
    }
}

fn delta_target_size(delta: &[u8], expected_base_size: u64) -> Result<u64, CodecError> {
    let mut pos = 0usize;
    let base_size = crate::delta::read_size_varint(delta, &mut pos)?;
    if base_size != expected_base_size {
        return Err(CodecError::DeltaBaseSizeMismatch {
            expected: base_size,
            actual: expected_base_size,
        });
    }
    crate::delta::read_size_varint(delta, &mut pos)
}

fn scan_received_pack(
    file: &mut std::fs::File,
    pack_len: u64,
    allow_ref_delta: bool,
) -> Result<Option<(Oid, Vec<ReceiveEntryMeta>)>, CodecError> {
    if pack_len < 12 + 20 {
        return Err(CodecError::PackTooShort);
    }
    let body_end = pack_len - 20;
    let mut reader = PackBodyReader::new(file, body_end);
    let mut header = [0u8; 12];
    reader.read_body_exact(&mut header, None)?;
    if &header[0..4] != PACK_MAGIC {
        return Err(CodecError::BadPackMagic);
    }
    let version = u32::from_be_bytes(header[4..8].try_into().unwrap());
    if version != PACK_VERSION {
        return Err(CodecError::BadPackVersion(version));
    }
    let count = u32::from_be_bytes(header[8..12].try_into().unwrap()) as usize;
    let mut entries = Vec::with_capacity(count.min(PREALLOC_HINT_CAP));

    for _ in 0..count {
        let offset = reader.position();
        let mut crc = crc32fast::Hasher::new();
        let (ty, declared_size) = read_entry_header_stream(&mut reader, &mut crc)?;
        let kind = match ty {
            T_COMMIT | T_TREE | T_BLOB | T_TAG => ReceiveEntryKind::Full(Kind::from_pack_type(ty)?),
            T_OFS_DELTA => {
                let rel = read_offset_varint_stream(&mut reader, &mut crc)?;
                let base_offset = offset
                    .checked_sub(rel)
                    .ok_or(CodecError::BadDeltaBaseOffset)?;
                let base_idx = entries
                    .binary_search_by_key(&base_offset, |entry: &ReceiveEntryMeta| entry.offset)
                    .map_err(|_| CodecError::MissingDeltaBaseOffset(base_offset))?;
                entries[base_idx].base_ref_count = entries[base_idx]
                    .base_ref_count
                    .checked_add(1)
                    .ok_or_else(|| CodecError::Io("too many delta children".into()))?;
                ReceiveEntryKind::OfsDelta { base_offset }
            }
            T_REF_DELTA => {
                let mut base = [0u8; 20];
                reader.read_body_exact(&mut base, Some(&mut crc))?;
                if !allow_ref_delta {
                    return Ok(None);
                }
                ReceiveEntryKind::RefDelta {
                    base_oid: Oid::from_array(base),
                }
            }
            other => return Err(CodecError::BadPackType(other)),
        };
        let compressed_offset = reader.position();
        let (predecoded_oid, predecoded_raw) = match kind {
            ReceiveEntryKind::Full(kind) => {
                let decoded =
                    inflate_full_entry_stream(&mut reader, &mut crc, kind, declared_size as usize)?;
                (Some(decoded.oid), decoded.raw)
            }
            ReceiveEntryKind::OfsDelta { .. } | ReceiveEntryKind::RefDelta { .. } => {
                skip_inflate_stream(&mut reader, &mut crc, declared_size as usize)?;
                (None, None)
            }
        };
        let compressed_len = (reader.position() - compressed_offset)
            .try_into()
            .map_err(|_| CodecError::PackTooShort)?;
        entries.push(ReceiveEntryMeta {
            offset,
            compressed_offset,
            compressed_len,
            declared_size,
            kind,
            crc32: crc.finalize(),
            base_ref_count: 0,
            oid: predecoded_oid,
            resolved_kind: predecoded_oid.map(|_| match kind {
                ReceiveEntryKind::Full(kind) => kind,
                ReceiveEntryKind::OfsDelta { .. } | ReceiveEntryKind::RefDelta { .. } => {
                    unreachable!()
                }
            }),
            resolved_size: predecoded_oid.map(|_| declared_size),
            predecoded_raw,
            base_oid: None,
            depth: 0,
        });
    }

    if reader.position() != body_end {
        return Err(CodecError::PackTooShort);
    }
    let mut trailer = [0u8; 20];
    reader.read_raw_exact(&mut trailer)?;
    let computed: [u8; 20] = reader.finalize_hash();
    if computed != trailer {
        return Err(CodecError::PackChecksumMismatch);
    }
    Ok(Some((Oid::from_array(computed), entries)))
}

struct PackBodyReader<'a> {
    file: &'a mut std::fs::File,
    body_end: u64,
    pos: u64,
    buf: Vec<u8>,
    buf_pos: usize,
    buf_len: usize,
    hasher: Sha1,
}

impl<'a> PackBodyReader<'a> {
    fn new(file: &'a mut std::fs::File, body_end: u64) -> Self {
        Self {
            file,
            body_end,
            pos: 0,
            buf: vec![0; 64 * 1024],
            buf_pos: 0,
            buf_len: 0,
            hasher: Sha1::new(),
        }
    }

    fn position(&self) -> u64 {
        self.pos
    }

    fn fill_body(&mut self) -> Result<(), CodecError> {
        if self.buf_pos < self.buf_len {
            return Ok(());
        }
        if self.pos >= self.body_end {
            return Err(CodecError::PackTooShort);
        }
        let remaining = (self.body_end - self.pos) as usize;
        let to_read = remaining.min(self.buf.len());
        let n = self
            .file
            .read(&mut self.buf[..to_read])
            .map_err(|e| CodecError::Io(e.to_string()))?;
        if n == 0 {
            return Err(CodecError::PackTooShort);
        }
        self.buf_pos = 0;
        self.buf_len = n;
        Ok(())
    }

    fn available_body(&mut self) -> Result<&[u8], CodecError> {
        self.fill_body()?;
        Ok(&self.buf[self.buf_pos..self.buf_len])
    }

    fn consume_body(
        &mut self,
        n: usize,
        crc: Option<&mut crc32fast::Hasher>,
    ) -> Result<(), CodecError> {
        if self.buf_pos + n > self.buf_len {
            return Err(CodecError::PackTooShort);
        }
        let bytes = &self.buf[self.buf_pos..self.buf_pos + n];
        self.hasher.update(bytes);
        if let Some(crc) = crc {
            crc.update(bytes);
        }
        self.buf_pos += n;
        self.pos += n as u64;
        Ok(())
    }

    fn read_body_byte(&mut self, crc: Option<&mut crc32fast::Hasher>) -> Result<u8, CodecError> {
        self.fill_body()?;
        let b = self.buf[self.buf_pos];
        self.consume_body(1, crc)?;
        Ok(b)
    }

    fn read_body_exact(
        &mut self,
        mut out: &mut [u8],
        mut crc: Option<&mut crc32fast::Hasher>,
    ) -> Result<(), CodecError> {
        while !out.is_empty() {
            self.fill_body()?;
            let n = out.len().min(self.buf_len - self.buf_pos);
            out[..n].copy_from_slice(&self.buf[self.buf_pos..self.buf_pos + n]);
            match crc.as_deref_mut() {
                Some(c) => self.consume_body(n, Some(c))?,
                None => self.consume_body(n, None)?,
            }
            out = &mut out[n..];
        }
        Ok(())
    }

    fn read_raw_exact(&mut self, mut out: &mut [u8]) -> Result<(), CodecError> {
        while self.buf_pos < self.buf_len && !out.is_empty() {
            let n = out.len().min(self.buf_len - self.buf_pos);
            out[..n].copy_from_slice(&self.buf[self.buf_pos..self.buf_pos + n]);
            self.buf_pos += n;
            out = &mut out[n..];
        }
        self.file
            .read_exact(out)
            .map_err(|e| CodecError::Io(e.to_string()))
    }

    fn finalize_hash(self) -> [u8; 20] {
        self.hasher.finalize().into()
    }
}

fn read_entry_header_stream(
    reader: &mut PackBodyReader<'_>,
    crc: &mut crc32fast::Hasher,
) -> Result<(u8, u64), CodecError> {
    let c = reader.read_body_byte(Some(crc))?;
    let ty = (c >> 4) & 0x07;
    let mut size = (c & 0x0f) as u64;
    let mut shift = 4u32;
    let mut cont = c & 0x80 != 0;
    while cont {
        let c = reader.read_body_byte(Some(crc))?;
        size |= ((c & 0x7f) as u64) << shift;
        shift += 7;
        cont = c & 0x80 != 0;
    }
    Ok((ty, size))
}

fn read_offset_varint_stream(
    reader: &mut PackBodyReader<'_>,
    crc: &mut crc32fast::Hasher,
) -> Result<u64, CodecError> {
    let mut c = reader.read_body_byte(Some(crc))?;
    let mut off = (c & 0x7f) as u64;
    while c & 0x80 != 0 {
        c = reader.read_body_byte(Some(crc))?;
        off = ((off + 1) << 7) | (c & 0x7f) as u64;
    }
    Ok(off)
}

fn skip_inflate_stream(
    reader: &mut PackBodyReader<'_>,
    crc: &mut crc32fast::Hasher,
    expected: usize,
) -> Result<usize, CodecError> {
    let mut dec = Decompress::new(true);
    let mut scratch = [0u8; 16 * 1024];
    loop {
        let input = reader.available_body()?;
        let before_in = dec.total_in();
        let before_out = dec.total_out();
        let status = dec
            .decompress(input, &mut scratch, FlushDecompress::None)
            .map_err(|e| CodecError::Inflate(e.to_string()))?;
        let consumed = (dec.total_in() - before_in) as usize;
        reader.consume_body(consumed, Some(crc))?;
        if matches!(status, Status::StreamEnd) {
            break;
        }
        if dec.total_in() == before_in && dec.total_out() == before_out {
            return Err(CodecError::Inflate("inflate made no progress".into()));
        }
    }
    let consumed = dec.total_in() as usize;
    let produced = dec.total_out() as usize;
    if produced != expected {
        return Err(CodecError::Inflate(format!(
            "expected {expected} bytes, produced {produced}"
        )));
    }
    Ok(consumed)
}

struct PredecodedFullEntry {
    oid: Oid,
    raw: Option<Vec<u8>>,
}

fn inflate_full_entry_stream(
    reader: &mut PackBodyReader<'_>,
    crc: &mut crc32fast::Hasher,
    kind: Kind,
    expected: usize,
) -> Result<PredecodedFullEntry, CodecError> {
    #[cfg(test)]
    if kind == Kind::Blob {
        STREAMED_FULL_BLOBS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    let mut dec = Decompress::new(true);
    let mut scratch = [0u8; 16 * 1024];
    let mut hasher = object_hasher(kind, expected as u64);
    loop {
        let input = reader.available_body()?;
        let before_in = dec.total_in();
        let before_out = dec.total_out();
        let status = dec
            .decompress(input, &mut scratch, FlushDecompress::None)
            .map_err(|e| CodecError::Inflate(e.to_string()))?;
        let consumed = (dec.total_in() - before_in) as usize;
        let produced = (dec.total_out() - before_out) as usize;
        reader.consume_body(consumed, Some(crc))?;
        if produced > 0 {
            let bytes = &scratch[..produced];
            hasher.update(bytes);
        }
        if matches!(status, Status::StreamEnd) {
            break;
        }
        if dec.total_in() == before_in && dec.total_out() == before_out {
            return Err(CodecError::Inflate("inflate made no progress".into()));
        }
    }
    let produced = dec.total_out() as usize;
    if produced != expected {
        return Err(CodecError::Inflate(format!(
            "expected {expected} bytes, produced {produced}"
        )));
    }
    Ok(PredecodedFullEntry {
        oid: finalize_object_hash(hasher),
        raw: None,
    })
}

fn inflate_entry_at(file: &std::fs::File, entry: &ReceiveEntryMeta) -> Result<Vec<u8>, CodecError> {
    let mut compressed = vec![0u8; entry.compressed_len];
    read_exact_at(file, entry.compressed_offset, &mut compressed)?;
    let mut pos = 0usize;
    let (raw, consumed) = inflate(&compressed, &mut pos, entry.declared_size as usize)?;
    if consumed != entry.compressed_len {
        return Err(CodecError::Inflate(format!(
            "expected {} compressed bytes, consumed {consumed}",
            entry.compressed_len
        )));
    }
    Ok(raw)
}

fn stream_full_blob_at(
    file: &std::fs::File,
    entry: &ReceiveEntryMeta,
    spill_to_file: bool,
) -> Result<ResolvedData, CodecError> {
    stream_full_object_at(file, entry, Kind::Blob, spill_to_file)
}

/// Stream one known full blob entry from `path` into a temporary file using persisted pack-entry
/// metadata. This is the optimizer's indexed fast path: it range-reads and inflates the one object
/// instead of replaying the pack resolver to rediscover it.
pub fn stream_indexed_full_blob_to_temp(
    path: impl AsRef<Path>,
    entry: &PackObjectEntry,
) -> Result<FileBackedBlob, CodecError> {
    if entry.kind != PackObjectKind::Full(Kind::Blob) {
        return Err(CodecError::Io("indexed entry is not a full blob".into()));
    }
    let file = std::fs::File::open(path.as_ref()).map_err(|e| CodecError::Io(e.to_string()))?;
    let meta = ReceiveEntryMeta {
        offset: entry.offset,
        compressed_offset: entry.compressed_offset,
        compressed_len: entry
            .compressed_len
            .try_into()
            .map_err(|_| CodecError::Io("indexed compressed length exceeds usize".into()))?,
        declared_size: entry.declared_size,
        kind: ReceiveEntryKind::Full(Kind::Blob),
        crc32: entry.crc32,
        base_ref_count: 0,
        oid: Some(entry.oid),
        resolved_kind: Some(entry.resolved_kind),
        resolved_size: Some(entry.resolved_size),
        predecoded_raw: None,
        base_oid: None,
        depth: 0,
    };
    let resolved = stream_full_blob_at(&file, &meta, true)?;
    if resolved.oid != entry.oid {
        return Err(CodecError::Io(format!(
            "indexed blob oid mismatch: expected {}, got {}",
            entry.oid.to_hex(),
            resolved.oid.to_hex()
        )));
    }
    let file = resolved
        .spilled
        .ok_or_else(|| CodecError::Io("indexed blob was not spilled".into()))?;
    Ok(FileBackedBlob {
        oid: resolved.oid,
        size: resolved.size,
        file,
    })
}

/// Inflate one indexed full blob from its compressed byte range. The caller supplies exactly
/// `entry.compressed_len` bytes starting at `entry.compressed_offset`.
pub fn stream_indexed_full_blob_range_to_temp(
    compressed: &[u8],
    entry: &PackObjectEntry,
) -> Result<FileBackedBlob, CodecError> {
    if entry.kind != PackObjectKind::Full(Kind::Blob) {
        return Err(CodecError::Io("indexed entry is not a full blob".into()));
    }
    if compressed.len() as u64 != entry.compressed_len {
        return Err(CodecError::Inflate(format!(
            "expected {} compressed bytes, got {}",
            entry.compressed_len,
            compressed.len()
        )));
    }
    let mut file = tempfile::NamedTempFile::new().map_err(|e| CodecError::Io(e.to_string()))?;
    let mut hasher = object_hasher(Kind::Blob, entry.declared_size);
    let mut dec = Decompress::new(true);
    let mut output = [0u8; 64 * 1024];
    let mut pos = 0usize;
    let mut produced_total = 0u64;
    let mut ended = false;
    while pos < compressed.len() && !ended {
        let before_in = dec.total_in();
        let before_out = dec.total_out();
        let status = dec
            .decompress(&compressed[pos..], &mut output, FlushDecompress::None)
            .map_err(|e| CodecError::Inflate(e.to_string()))?;
        let consumed = (dec.total_in() - before_in) as usize;
        let produced = (dec.total_out() - before_out) as usize;
        if produced > 0 {
            file.write_all(&output[..produced])
                .map_err(|e| CodecError::Io(e.to_string()))?;
            hasher.update(&output[..produced]);
            produced_total += produced as u64;
        }
        pos += consumed;
        if matches!(status, Status::StreamEnd) {
            ended = true;
        }
        if consumed == 0 && produced == 0 {
            return Err(CodecError::Inflate(
                "indexed blob inflate made no progress".into(),
            ));
        }
    }
    if !ended || pos != compressed.len() || produced_total != entry.declared_size {
        return Err(CodecError::Inflate(format!(
            "indexed blob inflate mismatch: consumed {}/{}, produced {}/{}",
            pos,
            compressed.len(),
            produced_total,
            entry.declared_size
        )));
    }
    let oid = finalize_object_hash(hasher);
    if oid != entry.oid {
        return Err(CodecError::Io(format!(
            "indexed blob oid mismatch: expected {}, got {}",
            entry.oid.to_hex(),
            oid.to_hex()
        )));
    }
    Ok(FileBackedBlob {
        oid,
        size: produced_total,
        file,
    })
}

pub fn inflate_full_payload_range_to_temp(
    compressed: &[u8],
    kind: Kind,
    expected_oid: Oid,
    expected_size: u64,
) -> Result<tempfile::TempPath, CodecError> {
    let mut file = tempfile::NamedTempFile::new().map_err(|e| CodecError::Io(e.to_string()))?;
    inflate_full_payload_range_to_writer(compressed, kind, expected_oid, expected_size, &mut file)?;
    file.as_file_mut()
        .flush()
        .map_err(|e| CodecError::Io(e.to_string()))?;
    Ok(file.into_temp_path())
}

fn inflate_full_payload_range_to_writer<W: Write>(
    compressed: &[u8],
    kind: Kind,
    expected_oid: Oid,
    expected_size: u64,
    writer: &mut W,
) -> Result<u64, CodecError> {
    let mut hasher = object_hasher(kind, expected_size);
    let mut dec = Decompress::new(true);
    let mut output = [0u8; 64 * 1024];
    let mut pos = 0usize;
    let mut produced_total = 0u64;
    let mut ended = false;
    while pos < compressed.len() && !ended {
        let before_in = dec.total_in();
        let before_out = dec.total_out();
        let status = dec
            .decompress(&compressed[pos..], &mut output, FlushDecompress::None)
            .map_err(|e| CodecError::Inflate(e.to_string()))?;
        let consumed = (dec.total_in() - before_in) as usize;
        let produced = (dec.total_out() - before_out) as usize;
        if produced > 0 {
            writer
                .write_all(&output[..produced])
                .map_err(|e| CodecError::Io(e.to_string()))?;
            hasher.update(&output[..produced]);
            produced_total += produced as u64;
        }
        pos += consumed;
        if matches!(status, Status::StreamEnd) {
            ended = true;
        }
        if consumed == 0 && produced == 0 {
            return Err(CodecError::Inflate(
                "full payload range inflate made no progress".into(),
            ));
        }
    }
    if !ended || pos != compressed.len() || produced_total != expected_size {
        return Err(CodecError::Inflate(format!(
            "full payload range inflate mismatch: consumed {}/{}, produced {}/{}",
            pos,
            compressed.len(),
            produced_total,
            expected_size
        )));
    }
    let oid = finalize_object_hash(hasher);
    if oid != expected_oid {
        return Err(CodecError::Io(format!(
            "full payload range oid mismatch: expected {}, got {}",
            expected_oid.to_hex(),
            oid.to_hex()
        )));
    }
    Ok(produced_total)
}

fn stream_full_object_at(
    file: &std::fs::File,
    entry: &ReceiveEntryMeta,
    kind: Kind,
    spill_to_file: bool,
) -> Result<ResolvedData, CodecError> {
    const IN_CHUNK: usize = 64 * 1024;
    const OUT_CHUNK: usize = 64 * 1024;

    let mut out_file = if spill_to_file {
        Some(tempfile::NamedTempFile::new().map_err(|e| CodecError::Io(e.to_string()))?)
    } else {
        None
    };
    let mut hasher = object_hasher(kind, entry.declared_size);
    let mut dec = Decompress::new(true);
    let mut input = [0u8; IN_CHUNK];
    let mut output = [0u8; OUT_CHUNK];
    let mut input_offset = entry.compressed_offset;
    let mut remaining = entry.compressed_len;
    let mut consumed_total = 0usize;
    let mut produced_total = 0u64;
    let mut ended = false;

    while remaining > 0 && !ended {
        let n = remaining.min(input.len());
        read_exact_at(file, input_offset, &mut input[..n])?;
        input_offset += n as u64;
        remaining -= n;

        let mut chunk_pos = 0usize;
        while chunk_pos < n {
            let before_in = dec.total_in();
            let before_out = dec.total_out();
            let status = dec
                .decompress(&input[chunk_pos..n], &mut output, FlushDecompress::None)
                .map_err(|e| CodecError::Inflate(e.to_string()))?;
            let consumed = (dec.total_in() - before_in) as usize;
            let produced = (dec.total_out() - before_out) as usize;
            if produced > 0 {
                let bytes = &output[..produced];
                hasher.update(bytes);
                if let Some(file) = out_file.as_mut() {
                    file.write_all(bytes)
                        .map_err(|e| CodecError::Io(e.to_string()))?;
                }
                produced_total = produced_total
                    .checked_add(produced as u64)
                    .ok_or(CodecError::TruncatedDelta)?;
            }
            chunk_pos += consumed;
            consumed_total += consumed;
            if matches!(status, Status::StreamEnd) {
                ended = true;
                break;
            }
            if consumed == 0 && produced == 0 {
                return Err(CodecError::Inflate("inflate made no progress".into()));
            }
        }
    }

    if !ended || consumed_total != entry.compressed_len || produced_total != entry.declared_size {
        return Err(CodecError::Inflate(format!(
            "expected {} compressed / {} produced bytes, consumed {consumed_total} / produced {produced_total}",
            entry.compressed_len, entry.declared_size
        )));
    }

    if let Some(file) = out_file.as_mut() {
        file.as_file_mut()
            .flush()
            .map_err(|e| CodecError::Io(e.to_string()))?;
        #[cfg(test)]
        SPILLED_DELTA_BASES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    #[cfg(test)]
    STREAMED_FULL_BLOBS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    Ok(ResolvedData {
        oid: finalize_object_hash(hasher),
        size: produced_total,
        raw: None,
        spilled: out_file,
        external_path: None,
    })
}

#[cfg(unix)]
fn read_exact_at(file: &std::fs::File, offset: u64, mut out: &mut [u8]) -> Result<(), CodecError> {
    use std::os::unix::fs::FileExt;
    let mut read_offset = offset;
    while !out.is_empty() {
        let n = file
            .read_at(out, read_offset)
            .map_err(|e| CodecError::Io(e.to_string()))?;
        if n == 0 {
            return Err(CodecError::PackTooShort);
        }
        read_offset += n as u64;
        out = &mut out[n..];
    }
    Ok(())
}

#[cfg(not(unix))]
fn read_exact_at(file: &std::fs::File, offset: u64, out: &mut [u8]) -> Result<(), CodecError> {
    use std::io::{Seek, SeekFrom};
    let mut file = file
        .try_clone()
        .map_err(|e| CodecError::Io(e.to_string()))?;
    file.seek(SeekFrom::Start(offset))
        .map_err(|e| CodecError::Io(e.to_string()))?;
    file.read_exact(out)
        .map_err(|e| CodecError::Io(e.to_string()))
}

/// A single packfile entry, read at a byte offset without parsing the rest of the pack. Deltas are
/// returned **unresolved** (with a reference to their base) so the caller can resolve the base
/// on demand — possibly from another pack — rather than requiring the whole pack in memory.
#[derive(Clone, Debug)]
pub enum PackEntry {
    /// A self-contained object.
    Object { kind: Kind, data: Vec<u8> },
    /// A delta against a base earlier in the same pack, identified by its absolute byte offset.
    OfsDelta { base_offset: u64, delta: Vec<u8> },
    /// A delta against a base identified by object id (may live in another pack).
    RefDelta { base_oid: Oid, delta: Vec<u8> },
}

/// Read the single entry that begins at `offset` in `pack` (a complete packfile, trailer included).
///
/// This is the on-demand counterpart to [`parse_pack`]: it inflates exactly one entry's bytes —
/// the zlib stream is self-terminating, so the trailing pack bytes are harmless — and returns
/// deltas unresolved. It lets an object be materialized from its idx offset without inflating every
/// other object in the pack. `offset` must be a genuine entry boundary (e.g. from the `.idx`).
pub fn read_pack_entry(pack: &[u8], offset: u64) -> Result<PackEntry, CodecError> {
    let entry_start = offset;
    let mut pos = offset as usize;
    let (ty, size) = read_entry_header(pack, &mut pos)?;
    match ty {
        T_COMMIT | T_TREE | T_BLOB | T_TAG => {
            let (data, _) = inflate(pack, &mut pos, size as usize)?;
            Ok(PackEntry::Object {
                kind: Kind::from_pack_type(ty)?,
                data,
            })
        }
        T_OFS_DELTA => {
            let rel = read_offset_varint(pack, &mut pos)?;
            let base_offset = entry_start
                .checked_sub(rel)
                .ok_or(CodecError::BadDeltaBaseOffset)?;
            let (delta, _) = inflate(pack, &mut pos, size as usize)?;
            Ok(PackEntry::OfsDelta { base_offset, delta })
        }
        T_REF_DELTA => {
            let base_oid =
                Oid::from_bytes(pack.get(pos..pos + 20).ok_or(CodecError::PackTooShort)?)?;
            pos += 20;
            let (delta, _) = inflate(pack, &mut pos, size as usize)?;
            Ok(PackEntry::RefDelta { base_oid, delta })
        }
        other => Err(CodecError::BadPackType(other)),
    }
}

/// Parse a packfile object type/size header.
fn read_entry_header(buf: &[u8], pos: &mut usize) -> Result<(u8, u64), CodecError> {
    let c = *buf.get(*pos).ok_or(CodecError::PackTooShort)?;
    *pos += 1;
    let ty = (c >> 4) & 0x07;
    let mut size = (c & 0x0f) as u64;
    let mut shift = 4u32;
    let mut cont = c & 0x80 != 0;
    while cont {
        let c = *buf.get(*pos).ok_or(CodecError::PackTooShort)?;
        *pos += 1;
        size |= ((c & 0x7f) as u64) << shift;
        shift += 7;
        cont = c & 0x80 != 0;
    }
    Ok((ty, size))
}

/// Parse the `OFS_DELTA` negative base-offset encoding.
fn read_offset_varint(buf: &[u8], pos: &mut usize) -> Result<u64, CodecError> {
    let mut c = *buf.get(*pos).ok_or(CodecError::PackTooShort)?;
    *pos += 1;
    let mut off = (c & 0x7f) as u64;
    while c & 0x80 != 0 {
        c = *buf.get(*pos).ok_or(CodecError::PackTooShort)?;
        *pos += 1;
        off = ((off + 1) << 7) | (c & 0x7f) as u64;
    }
    Ok(off)
}

/// Inflate exactly `expected` decompressed bytes starting at `buf[*pos]`, advancing `*pos` past
/// the consumed compressed bytes.
fn inflate(buf: &[u8], pos: &mut usize, expected: usize) -> Result<(Vec<u8>, usize), CodecError> {
    let mut dec = Decompress::new(true);
    let mut out = vec![0u8; expected];
    let input = buf.get(*pos..).ok_or(CodecError::PackTooShort)?;
    let status = dec
        .decompress(input, &mut out, FlushDecompress::Finish)
        .map_err(|e| CodecError::Inflate(e.to_string()))?;
    let consumed = dec.total_in() as usize;
    let produced = dec.total_out() as usize;
    if produced != expected || !matches!(status, Status::StreamEnd | Status::Ok) {
        return Err(CodecError::Inflate(format!(
            "expected {expected} bytes, produced {produced}"
        )));
    }
    out.truncate(produced);
    *pos += consumed;
    Ok((out, consumed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta::encode_trivial_delta;

    #[test]
    fn malicious_object_count_does_not_oom() {
        // A 32-byte pack whose header lies about holding u32::MAX objects must be rejected promptly,
        // not pre-allocate for billions of objects — that allocation would fail and *abort the
        // process*. With the unclamped `with_capacity(count)` this aborted; the cap makes every parser
        // return an error instead. (We can only assert the error, since an abort can't be caught.)
        let mut data = Vec::new();
        data.extend_from_slice(b"PACK");
        data.extend_from_slice(&2u32.to_be_bytes());
        data.extend_from_slice(&u32::MAX.to_be_bytes()); // count = 4.29 billion, but only 32 bytes
        data.extend_from_slice(&[0u8; 20]); // trailer (won't verify)
        assert!(resolve_pack_parallel(&data).is_err());
        assert!(parse_pack(&data, |_| None).is_err());
        assert!(parse_pack_reuse(&data).is_err());
    }

    fn objs() -> Vec<Object> {
        vec![
            Object::new(Kind::Blob, &b"hello\n"[..]),
            Object::new(Kind::Blob, &b""[..]),
            Object::new(
                Kind::Commit,
                &b"tree 4b825dc642cb6eb9a060e54bf8d69288fbee4904\n"[..],
            ),
            Object::new(Kind::Blob, vec![0xABu8; 5000]), // forces multi-byte size header
        ]
    }

    fn resolve_file(data: &[u8]) -> Result<Option<ResolvedPack>, CodecError> {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(data).unwrap();
        tmp.as_file_mut().flush().unwrap();
        resolve_pack_file(tmp.path())
    }

    fn resolved_object_entries(resolved: &ResolvedPack) -> Vec<PackObjectEntry> {
        let sidecar = std::fs::read(resolved.object_index_file.path()).unwrap();
        decode_pack_object_index_sidecar(&sidecar).unwrap()
    }

    fn resolved_idx_bytes(resolved: &ResolvedPack) -> Vec<u8> {
        std::fs::read(resolved.idx_file.path()).unwrap()
    }

    fn resolved_source_index(resolved: &ResolvedPack) -> PackTreeIndexPlan {
        let sidecar = std::fs::read(resolved.source_index_file.path()).unwrap();
        decode_pack_source_index_sidecar(&sidecar).unwrap()
    }

    fn assert_file_resolver_matches_parse(data: &[u8]) -> ResolvedPack {
        let resolved = resolve_file(data)
            .unwrap()
            .expect("self-contained pack resolves verbatim");
        let parsed = parse_pack(data, |_| None).unwrap();
        assert_eq!(resolved.object_count, parsed.objects.len());
        let object_entries = resolved_object_entries(&resolved);
        assert_eq!(object_entries.len(), parsed.objects.len());
        assert_eq!(
            crate::idx::IdxV2::parse(&resolved_idx_bytes(&resolved))
                .unwrap()
                .len(),
            parsed.objects.len()
        );

        let mut want_oids: Vec<Oid> = parsed.objects.iter().map(|(oid, _)| *oid).collect();
        want_oids.sort_unstable();
        want_oids.dedup();
        assert_eq!(resolved.oids, want_oids);

        let max_object_size = parsed
            .objects
            .iter()
            .map(|(_, obj)| obj.data.len() as u64)
            .max()
            .unwrap_or(0);
        assert_eq!(
            resolved.largest.map(|(_, size)| size),
            Some(max_object_size)
        );
        for (oid, _) in parsed.objects {
            assert!(object_entries
                .iter()
                .any(|entry| entry.oid == oid && entry.offset > 0));
        }
        resolved
    }

    #[test]
    fn build_then_parse_roundtrips() {
        let objects = objs();
        let built = build_pack(&objects).unwrap();
        assert_eq!(&built.data[0..4], b"PACK");
        assert_eq!(built.entries.len(), objects.len());

        let parsed = parse_pack(&built.data, |_| None).unwrap();
        assert_eq!(parsed.objects.len(), objects.len());
        for (orig, (oid, got)) in objects.iter().zip(parsed.objects.iter()) {
            assert_eq!(*oid, orig.id());
            assert_eq!(got.kind, orig.kind);
            assert_eq!(got.data, orig.data);
        }
    }

    #[test]
    fn file_resolver_matches_parse_for_full_object_pack() {
        let built = build_pack(&objs()).unwrap();
        let resolved = assert_file_resolver_matches_parse(&built.data);
        assert!(
            resolved.replayed_pack_entries,
            "source-bearing full-object packs should resolve from the file-backed resolver"
        );
    }

    #[test]
    fn file_resolver_streams_large_full_blobs_without_materializing_raw() {
        let streamed_before = STREAMED_FULL_BLOBS.load(std::sync::atomic::Ordering::Relaxed);

        let blob = Object::new(Kind::Blob, vec![0x53u8; LARGE_DELTA_BASE_SPILL_BYTES * 8]);
        let blob_id = blob.id();
        let built = build_pack(&[blob]).unwrap();
        let resolved = assert_file_resolver_matches_parse(&built.data);

        assert!(resolved.oids.binary_search(&blob_id).is_ok());
        assert_eq!(
            resolved.largest,
            Some((blob_id, (LARGE_DELTA_BASE_SPILL_BYTES * 8) as u64))
        );
        assert!(
            STREAMED_FULL_BLOBS.load(std::sync::atomic::Ordering::Relaxed) > streamed_before,
            "large full blob should stream through the hasher"
        );
    }

    #[test]
    fn file_resolver_matches_parse_for_ofs_delta_pack() {
        let base = Object::new(Kind::Blob, vec![0x42u8; 8000]);
        let mut changed = vec![0x42u8; 8000];
        changed[17] = 0x11;
        changed[4097] = 0x22;
        changed.extend_from_slice(b"tail");
        let target = Object::new(Kind::Blob, changed);
        let built = build_pack_delta(&[base, target]).unwrap();
        assert!(built.entries.iter().any(|e| matches!(
            read_pack_entry(&built.data, e.offset).unwrap(),
            PackEntry::OfsDelta { .. }
        )));
        assert_file_resolver_matches_parse(&built.data);
    }

    #[test]
    fn receive_spooler_records_ofs_delta_without_retaining_payload() {
        let base = Object::new(Kind::Blob, vec![0x42u8; 8000]);
        let mut changed = vec![0x42u8; 8000];
        changed[17] = 0x11;
        changed[4097] = 0x22;
        changed.extend_from_slice(b"tail");
        let target = Object::new(Kind::Blob, changed);
        let built = build_pack_delta(&[base, target]).unwrap();

        let mut spooler = ReceivePackSpooler::new(true);
        for chunk in built.data.chunks(97) {
            spooler.push(chunk).unwrap();
        }
        let scanned = spooler.finish().unwrap().expect("self-contained pack");
        let delta_entry = scanned
            .entries
            .iter()
            .find(|entry| matches!(entry.kind, ReceiveEntryKind::OfsDelta { .. }))
            .expect("expected OFS_DELTA entry");

        assert!(delta_entry.oid.is_none());
        assert!(delta_entry.resolved_kind.is_none());
        assert!(delta_entry.resolved_size.is_none());
        assert!(delta_entry.predecoded_raw.is_none());
        assert_eq!(scanned.retained_payload_bytes(), 0);
        assert!(!scanned.predecode_budget_exhausted());
    }

    #[test]
    fn receive_spooler_waits_for_split_entry_header_varint() {
        let blob = Object::new(Kind::Blob, vec![0x42u8; 200]);
        let built = build_pack(&[blob]).unwrap();

        let mut spooler = ReceivePackSpooler::new(true);
        spooler.push(&built.data[..13]).unwrap();
        spooler.push(&built.data[13..]).unwrap();
        let scanned = spooler.finish().unwrap().expect("self-contained pack");

        assert_eq!(scanned.entries.len(), 1);
    }

    #[test]
    fn scanned_small_delta_pack_resolves_from_file_backed_entries() {
        let base = Object::new(Kind::Blob, vec![0x42u8; 8000]);
        let mut changed = vec![0x42u8; 8000];
        changed[17] = 0x11;
        changed[4097] = 0x22;
        changed.extend_from_slice(b"tail");
        let target = Object::new(Kind::Blob, changed);
        let built = build_pack_delta(&[base, target]).unwrap();

        let mut spooler = ReceivePackSpooler::new(true);
        spooler.push(&built.data).unwrap();
        let scanned = spooler.finish().unwrap().expect("self-contained pack");
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(&built.data).unwrap();
        let resolved = resolve_scanned_pack_with_log_context(
            file.as_file(),
            scanned,
            PackResolveLogContext::default(),
        )
        .unwrap()
        .expect("self-contained pack");

        assert!(resolved.replayed_pack_entries);
    }

    #[test]
    fn scanner_metadata_does_not_retain_source_payloads() {
        let blob = Object::new(Kind::Blob, vec![0x33u8; 300 * 1024]);
        let tree_data = {
            let mut data = Vec::new();
            data.extend_from_slice(b"100644 large.bin\0");
            data.extend_from_slice(blob.id().as_bytes());
            data
        };
        let tree = Object::new(Kind::Tree, tree_data);
        let commit = Object::new(
            Kind::Commit,
            format!(
                "tree {}\nauthor A <a@example.com> 0 +0000\ncommitter A <a@example.com> 0 +0000\n\nmsg\n",
                tree.id().to_hex()
            )
            .into_bytes(),
        );
        let blob_id = blob.id();
        let tree_id = tree.id();
        let commit_id = commit.id();
        let built = build_pack(&[blob, tree, commit]).unwrap();

        let mut spooler = ReceivePackSpooler::new(true);
        spooler.push(&built.data).unwrap();
        let scanned = spooler.finish().unwrap().expect("self-contained pack");

        assert_eq!(scanned.retained_payload_bytes(), 0);
        assert!(!scanned.predecode_budget_exhausted());
        assert!(resolve_entries_from_scanned_metadata(&scanned.entries)
            .unwrap()
            .is_none());
        assert!(scanned
            .entries
            .iter()
            .find(|entry| entry.oid == Some(blob_id))
            .expect("blob entry")
            .predecoded_raw
            .is_none());

        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(&built.data).unwrap();
        let resolved = resolve_scanned_pack_with_log_context(
            file.as_file(),
            scanned,
            PackResolveLogContext::default(),
        )
        .unwrap()
        .expect("self-contained pack");
        assert!(resolved.replayed_pack_entries);
        assert_eq!(resolved.largest, Some((blob_id, 300 * 1024)));
        assert!(resolved.external_refs.is_empty());
        let source_index = resolved_source_index(&resolved);
        assert!(source_index
            .entries
            .iter()
            .any(|entry| entry.tree_oid == tree_id && entry.oid == blob_id));
        assert_eq!(
            resolved.source_commit_entries,
            vec![PackCommitIndexEntry {
                commit_oid: commit_id,
                root_tree: tree_id,
                parents: Vec::new(),
            }]
        );
    }

    #[test]
    fn local_journal_resolver_resolves_ref_delta_from_external_base() {
        let base = Object::new(Kind::Blob, &b"the original base content here"[..]);
        let target = Object::new(
            Kind::Blob,
            &b"the original base content here, now extended"[..],
        );
        let delta = encode_trivial_delta(&base.data, &target.data);

        let mut data = Vec::new();
        data.extend_from_slice(PACK_MAGIC);
        data.extend_from_slice(&PACK_VERSION.to_be_bytes());
        data.extend_from_slice(&1u32.to_be_bytes());
        write_entry_header(&mut data, T_REF_DELTA, delta.len() as u64);
        data.extend_from_slice(base.id().as_bytes());
        let mut enc = ZlibEncoder::new(Vec::new(), Compression::default());
        enc.write_all(&delta).unwrap();
        data.extend_from_slice(&enc.finish().unwrap());
        let mut h = Sha1::new();
        h.update(&data);
        let digest: [u8; 20] = h.finalize().into();
        data.extend_from_slice(&digest);

        let mut spooler = ReceivePackSpooler::new(true);
        for chunk in data.chunks(13) {
            spooler.push(chunk).unwrap();
        }
        let scanned = spooler.finish().unwrap().expect("local thin scan");
        assert_eq!(scanned.ref_delta_bases(), vec![base.id()]);
        assert_eq!(scanned.retained_payload_bytes(), 0);

        let mut journal = tempfile::NamedTempFile::new().unwrap();
        journal.write_all(&data).unwrap();
        journal.as_file_mut().flush().unwrap();
        let mut base_file = tempfile::NamedTempFile::new().unwrap();
        base_file.write_all(&base.data).unwrap();
        base_file.as_file_mut().flush().unwrap();
        let mut external = HashMap::new();
        external.insert(
            base.id(),
            ExternalBase {
                kind: base.kind,
                size: base.data.len() as u64,
                path: base_file.path().to_path_buf(),
            },
        );

        let resolved = resolve_scanned_pack_with_external_bases(
            journal.as_file(),
            scanned,
            &external,
            PackResolveLogContext::default(),
        )
        .unwrap()
        .expect("local journal resolver result");
        // Fix-thin appends the external base, so the resolved pack indexes both objects and is
        // self-contained.
        let mut expect = vec![base.id(), target.id()];
        expect.sort();
        assert_eq!(resolved.oids, expect);
        assert!(resolved.external_refs.is_empty());
        let fixed = resolved
            .fixed_thin_pack
            .as_ref()
            .expect("thin pack must produce a fix-thin replacement");
        assert_eq!(fixed.pack_hash, resolved.pack_hash);
        // The .sources sidecar header must record the post-append object count: the GC
        // derived-index materializer validates it against the manifest obj_count and would
        // otherwise defer tree/commit indexing forever for every thin push.
        let sidecar = std::fs::read(resolved.source_index_file.path()).unwrap();
        let plan = decode_pack_source_index_sidecar(&sidecar).unwrap();
        assert_eq!(
            plan.object_count, resolved.object_count,
            "sidecar object_count must match the manifest count including appended bases"
        );
        let entries = resolved_object_entries(&resolved);
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().any(|entry| matches!(
            entry.kind,
            PackObjectKind::RefDelta { base_oid, depth } if base_oid == base.id() && depth == 1
        )));
        assert!(entries.iter().any(|entry| {
            entry.oid == base.id() && matches!(entry.kind, PackObjectKind::Full(Kind::Blob))
        }));
    }

    #[test]
    fn file_resolver_extracts_annotated_tag_targets() {
        let commit = Object::new(
            Kind::Commit,
            &b"tree 4b825dc642cb6eb9a060e54bf8d69288fbee4904\nauthor A <a@example.com> 0 +0000\ncommitter A <a@example.com> 0 +0000\n\nmsg\n"[..],
        );
        let tag_data = format!(
            "object {}\ntype commit\ntag v1\ntagger A <a@example.com> 0 +0000\n\nrelease\n",
            commit.id().to_hex()
        );
        let tag = Object::new(Kind::Tag, tag_data.into_bytes());
        let tag_id = tag.id();
        let commit_id = commit.id();
        let built = build_pack(&[commit, tag]).unwrap();

        let resolved = assert_file_resolver_matches_parse(&built.data);
        assert_eq!(resolved.tag_targets.get(&tag_id), Some(&commit_id));
        assert!(resolved
            .external_refs
            .contains(&Oid::from_hex("4b825dc642cb6eb9a060e54bf8d69288fbee4904").unwrap()));
    }

    #[test]
    fn file_resolver_rejects_corrupt_trailer() {
        let mut data = build_pack(&objs()).unwrap().data;
        let last = data.len() - 1;
        data[last] ^= 0xff;
        assert!(matches!(
            resolve_file(&data),
            Err(CodecError::PackChecksumMismatch)
        ));
    }

    #[test]
    fn file_resolver_rejects_duplicate_object_ids() {
        let obj = Object::new(Kind::Blob, &b"same object twice"[..]);
        let built = build_pack(&[obj.clone(), obj.clone()]).unwrap();
        assert!(matches!(
            resolve_file(&built.data),
            Err(CodecError::DuplicateObject(oid)) if oid == obj.id()
        ));
    }

    #[test]
    fn file_resolver_declines_ref_delta_thin_pack() {
        let base = Object::new(Kind::Blob, vec![7u8; 4000]);
        let target = Object::new(Kind::Blob, {
            let mut v = vec![7u8; 4000];
            v[10] = 9;
            v
        });
        let delta = encode_trivial_delta(&base.data, &target.data);
        let zdelta = {
            let mut enc = ZlibEncoder::new(Vec::new(), Compression::new(1));
            enc.write_all(&delta).unwrap();
            enc.finish().unwrap()
        };
        let mut pack = Vec::new();
        pack.extend_from_slice(PACK_MAGIC);
        pack.extend_from_slice(&PACK_VERSION.to_be_bytes());
        pack.extend_from_slice(&1u32.to_be_bytes());
        write_entry_header(&mut pack, T_REF_DELTA, delta.len() as u64);
        pack.extend_from_slice(base.id().as_bytes());
        pack.extend_from_slice(&zdelta);
        let mut h = Sha1::new();
        h.update(&pack);
        let digest: [u8; 20] = h.finalize().into();
        pack.extend_from_slice(&digest);

        assert!(resolve_file(&pack).unwrap().is_none());
    }

    #[test]
    fn file_resolver_spills_large_delta_bases_to_scratch() {
        let spilled_before = SPILLED_DELTA_BASES.load(std::sync::atomic::Ordering::Relaxed);
        let streamed_delta_before =
            STREAMED_DELTA_TARGETS.load(std::sync::atomic::Ordering::Relaxed);
        let streamed_full_before = STREAMED_FULL_BLOBS.load(std::sync::atomic::Ordering::Relaxed);
        let base = Object::new(Kind::Blob, vec![0x7bu8; LARGE_DELTA_BASE_SPILL_BYTES * 4]);
        let mut changed = base.data.to_vec();
        changed[3] = 0x01;
        changed[LARGE_DELTA_BASE_SPILL_BYTES + 11] = 0x02;
        changed.extend_from_slice(b"suffix");
        let target = Object::new(Kind::Blob, changed);
        let built = build_pack_delta(&[base, target]).unwrap();
        assert!(built.entries.iter().any(|e| matches!(
            read_pack_entry(&built.data, e.offset).unwrap(),
            PackEntry::OfsDelta { .. }
        )));

        assert_file_resolver_matches_parse(&built.data);
        assert!(
            SPILLED_DELTA_BASES.load(std::sync::atomic::Ordering::Relaxed) > spilled_before,
            "expected large delta base to spill to scratch"
        );
        assert!(
            STREAMED_DELTA_TARGETS.load(std::sync::atomic::Ordering::Relaxed)
                > streamed_delta_before,
            "expected large blob delta target to stream to scratch"
        );
        assert!(
            STREAMED_FULL_BLOBS.load(std::sync::atomic::Ordering::Relaxed) > streamed_full_before,
            "expected large full blob base to stream directly to scratch"
        );
    }

    #[test]
    fn parallel_resolve_matches_sequential() {
        // A delta-heavy pack (near-identical large blobs → OFS deltas) plus assorted small objects.
        let base: Vec<u8> = (0..20_000u32)
            .map(|i| (i.wrapping_mul(2654435761) >> 16) as u8)
            .collect();
        let mut objects = vec![
            Object::new(
                Kind::Commit,
                &b"tree 4b825dc642cb6eb9a060e54bf8d69288fbee4904\n"[..],
            ),
            Object::new(Kind::Blob, base.clone()),
        ];
        for k in 0..8u8 {
            let mut v = base.clone();
            let len = v.len();
            for j in 0..40 {
                v[(j * 311) % len] = k.wrapping_add(j as u8);
            }
            objects.push(Object::new(Kind::Blob, v));
        }
        let delta = build_pack_delta(&objects).unwrap();

        // Parallel resolution returns Some (self-contained pack) with the same object metadata as the
        // sequential parser, without retaining inflated payloads for the receive-pack verbatim path.
        let resolved = resolve_pack_parallel(&delta.data)
            .unwrap()
            .expect("self-contained → Some");
        let seq = parse_pack(&delta.data, |_| None).unwrap();
        assert_eq!(resolved.object_count, objects.len());
        let object_entries = resolved_object_entries(&resolved);
        assert_eq!(object_entries.len(), objects.len());
        // The idx built from the verbatim pack round-trips (sorted, correct count).
        assert!(resolved.idx_bytes > 8 + 256 * 4);
        let mut seq_oids: Vec<Oid> = seq.objects.iter().map(|(oid, _)| *oid).collect();
        seq_oids.sort_unstable();
        seq_oids.dedup();
        assert_eq!(resolved.oids, seq_oids);
        for (oid, obj) in &seq.objects {
            assert!(resolved.oids.binary_search(oid).is_ok());
            assert!(object_entries.iter().any(|entry| &entry.oid == oid));
            assert!(obj.data.len() as u64 <= resolved.largest.unwrap().1);
        }
        assert!(resolved
            .external_refs
            .contains(&Oid::from_hex("4b825dc642cb6eb9a060e54bf8d69288fbee4904").unwrap()));
    }

    #[test]
    fn indexed_full_blob_entry_streams_without_full_optimization_plan() {
        let large = vec![42u8; 64 * 1024];
        let objects = vec![
            Object::new(Kind::Blob, &b"small"[..]),
            Object::new(Kind::Blob, large.clone()),
        ];
        let built = build_pack(&objects).unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(&built.data).unwrap();

        let resolved = resolve_pack_file(file.path())
            .unwrap()
            .expect("self-contained pack");
        let large_oid = objects[1].id();
        let object_entries = resolved_object_entries(&resolved);
        let entry = object_entries
            .iter()
            .find(|entry| entry.oid == large_oid)
            .expect("large blob indexed");
        assert_eq!(entry.kind, PackObjectKind::Full(Kind::Blob));
        assert_eq!(entry.declared_size, large.len() as u64);

        let streamed = stream_indexed_full_blob_to_temp(file.path(), entry).unwrap();
        assert_eq!(streamed.oid, large_oid);
        assert_eq!(streamed.size, large.len() as u64);
        assert_eq!(std::fs::read(streamed.path()).unwrap(), large);
    }

    #[test]
    fn parallel_resolve_declines_thin_packs() {
        // A REF_DELTA (thin) pack must return None so the caller falls back to the streaming parser.
        let base = Object::new(Kind::Blob, vec![7u8; 4000]);
        let target = Object::new(Kind::Blob, {
            let mut v = vec![7u8; 4000];
            v[10] = 9;
            v
        });
        // Hand-build a 1-entry pack whose single object is a REF_DELTA against `base` (not in pack).
        let zdelta = {
            let d = encode_trivial_delta(&base.data, &target.data);
            let mut enc = ZlibEncoder::new(Vec::new(), Compression::new(1));
            enc.write_all(&d).unwrap();
            enc.finish().unwrap()
        };
        let mut pack = Vec::new();
        pack.extend_from_slice(PACK_MAGIC);
        pack.extend_from_slice(&PACK_VERSION.to_be_bytes());
        pack.extend_from_slice(&1u32.to_be_bytes());
        // entry header: type=REF_DELTA(7), size = delta length
        let dlen = encode_trivial_delta(&base.data, &target.data).len() as u64;
        write_entry_header(&mut pack, 7, dlen);
        pack.extend_from_slice(base.id().as_bytes());
        pack.extend_from_slice(&zdelta);
        let mut h = Sha1::new();
        h.update(&pack);
        let digest: [u8; 20] = h.finalize().into();
        pack.extend_from_slice(&digest);

        assert!(
            resolve_pack_parallel(&pack).unwrap().is_none(),
            "thin pack → None"
        );
    }

    #[test]
    fn entries_have_increasing_offsets() {
        let built = build_pack(&objs()).unwrap();
        let mut last = 0u64;
        for e in &built.entries {
            assert!(e.offset >= last);
            last = e.offset;
        }
    }

    #[test]
    fn ref_delta_resolved_via_external_resolver() {
        // Build a pack containing only a REF_DELTA whose base is supplied externally (thin pack).
        let base = Object::new(Kind::Blob, &b"the original base content here"[..]);
        let target_data = b"the original base content here, now extended".to_vec();
        let delta = encode_trivial_delta(&base.data, &target_data);

        let mut data = Vec::new();
        data.extend_from_slice(PACK_MAGIC);
        data.extend_from_slice(&PACK_VERSION.to_be_bytes());
        data.extend_from_slice(&1u32.to_be_bytes());
        write_entry_header(&mut data, T_REF_DELTA, delta.len() as u64);
        data.extend_from_slice(base.id().as_bytes());
        let mut enc = ZlibEncoder::new(Vec::new(), Compression::default());
        enc.write_all(&delta).unwrap();
        data.extend_from_slice(&enc.finish().unwrap());
        let mut h = Sha1::new();
        h.update(&data);
        let digest: [u8; 20] = h.finalize().into();
        data.extend_from_slice(&digest);

        let base_clone = base.clone();
        let parsed = parse_pack(&data, move |oid| {
            if *oid == base_clone.id() {
                Some(base_clone.clone())
            } else {
                None
            }
        })
        .unwrap();
        assert_eq!(parsed.objects.len(), 1);
        assert_eq!(parsed.objects[0].1.data.as_ref(), target_data.as_slice());
    }

    #[test]
    fn read_pack_entry_materializes_one_object() {
        // Every non-delta entry can be read directly from its idx offset, no whole-pack parse.
        let objects = objs();
        let built = build_pack(&objects).unwrap();
        for (orig, e) in objects.iter().zip(built.entries.iter()) {
            match read_pack_entry(&built.data, e.offset).unwrap() {
                PackEntry::Object { kind, data } => {
                    assert_eq!(kind, orig.kind);
                    assert_eq!(data, orig.data.as_ref());
                }
                other => panic!("expected Object, got {other:?}"),
            }
        }
    }

    #[test]
    fn read_pack_entry_returns_unresolved_ofs_delta() {
        // Two near-identical blobs → the second is emitted as an OFS_DELTA against the first.
        let base = Object::new(Kind::Blob, vec![0x42u8; 8000]);
        let mut tweaked = vec![0x42u8; 8000];
        tweaked.extend_from_slice(b" and a little more");
        let target = Object::new(Kind::Blob, tweaked);
        let built = build_pack_delta(&[base.clone(), target.clone()]).unwrap();

        // Find the delta entry by reading each entry; resolve it against its base offset and confirm
        // it reconstructs the target object.
        let mut saw_delta = false;
        for e in &built.entries {
            if let PackEntry::OfsDelta { base_offset, delta } =
                read_pack_entry(&built.data, e.offset).unwrap()
            {
                saw_delta = true;
                let base_entry = read_pack_entry(&built.data, base_offset).unwrap();
                let base_data = match base_entry {
                    PackEntry::Object { data, .. } => data,
                    _ => panic!("base of an OFS_DELTA should be a plain object here"),
                };
                let resolved = crate::delta::apply_delta(&base_data, &delta).unwrap();
                let oid = crate::object::hash(Kind::Blob, &resolved);
                assert!(oid == base.id() || oid == target.id());
            }
        }
        assert!(saw_delta, "expected build_pack_delta to emit an OFS_DELTA");
    }

    #[test]
    fn parse_pack_reuse_then_rebuild_preserves_objects_and_reuses_deltas() {
        // Several similar blobs (so build_pack_delta produces OFS_DELTAs) plus a couple of others.
        let mut objects = Vec::new();
        for i in 0..6u8 {
            let mut data = vec![0xA5u8; 4000];
            data.extend_from_slice(format!(" variant {i}").as_bytes());
            objects.push(Object::new(Kind::Blob, data));
        }
        objects.push(Object::new(Kind::Commit, &b"tree 0\n"[..]));

        // Build with delta compression, then extract reusable deltas from it.
        let built = build_pack_delta(&objects).unwrap();
        let parsed = parse_pack_reuse(&built.data).unwrap();
        let reuse_count = parsed.iter().filter(|(_, _, r)| r.is_some()).count();
        assert!(
            reuse_count > 0,
            "the delta pack should contain reusable deltas"
        );

        let reuse: std::collections::HashMap<Oid, ReusableDelta> = parsed
            .iter()
            .filter_map(|(oid, _, r)| r.as_ref().map(|rd| (*oid, rd.clone())))
            .collect();

        // Rebuild reusing those deltas. The result must decode to exactly the same objects.
        let rebuilt = build_pack_delta_reuse(&objects, &reuse).unwrap();
        let out = parse_pack(&rebuilt.data, |_| None).unwrap();
        assert_eq!(out.objects.len(), objects.len());
        let mut got: Vec<(Oid, Vec<u8>)> = out
            .objects
            .iter()
            .map(|(oid, o)| (*oid, o.data.to_vec()))
            .collect();
        got.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        let mut want: Vec<(Oid, Vec<u8>)> =
            objects.iter().map(|o| (o.id(), o.data.to_vec())).collect();
        want.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        assert_eq!(
            got, want,
            "rebuilt-with-reuse pack must reproduce every object"
        );

        // The rebuilt pack must actually contain delta entries (reuse fired, not all full).
        let deltas = rebuilt
            .entries
            .iter()
            .filter(|e| {
                matches!(
                    read_pack_entry(&rebuilt.data, e.offset).unwrap(),
                    PackEntry::OfsDelta { .. } | PackEntry::RefDelta { .. }
                )
            })
            .count();
        assert!(deltas > 0, "rebuilt pack should retain delta entries");
    }

    #[test]
    fn parse_pack_reuse_on_full_pack_has_no_reuse() {
        let built = build_pack(&objs()).unwrap(); // build_pack = no deltas
        let parsed = parse_pack_reuse(&built.data).unwrap();
        assert!(parsed.iter().all(|(_, _, r)| r.is_none()));
        assert_eq!(parsed.len(), objs().len());
    }

    #[test]
    fn corrupt_trailer_is_rejected() {
        let mut built = build_pack(&objs()).unwrap();
        let n = built.data.len();
        built.data[n - 1] ^= 0xff;
        assert!(matches!(
            parse_pack(&built.data, |_| None),
            Err(CodecError::PackChecksumMismatch)
        ));
    }

    #[test]
    fn delta_pack_roundtrips_and_shrinks() {
        // Several near-identical large blobs (each a small edit of the first) → the delta builder
        // should store them as deltas, yielding a much smaller pack that still round-trips exactly.
        let base: Vec<u8> = (0..20_000u32)
            .map(|i| (i.wrapping_mul(2654435761) >> 16) as u8)
            .collect();
        let mut objects = vec![Object::new(Kind::Blob, base.clone())];
        for k in 0..6u8 {
            let mut v = base.clone();
            let len = v.len();
            for j in 0..50 {
                v[j * 307 % len] = k.wrapping_add(j as u8);
            }
            objects.push(Object::new(Kind::Blob, v));
        }

        let full = build_pack(&objects).unwrap();
        let delta = build_pack_delta(&objects).unwrap();
        assert!(
            delta.data.len() * 2 < full.data.len(),
            "delta pack {} should be far smaller than full pack {}",
            delta.data.len(),
            full.data.len()
        );

        // Round-trips: every object resolves to its original bytes, ids match.
        let parsed = parse_pack(&delta.data, |_| None).unwrap();
        assert_eq!(parsed.objects.len(), objects.len());
        let mut by_oid: std::collections::HashMap<Oid, &Object> =
            objects.iter().map(|o| (o.id(), o)).collect();
        for (oid, got) in &parsed.objects {
            let orig = by_oid.remove(oid).expect("known oid");
            assert_eq!(got.data, orig.data);
        }
        assert!(by_oid.is_empty(), "all objects present exactly once");
    }

    #[test]
    fn delta_builder_object_entries_match_resolver() {
        let base: Vec<u8> = (0..20_000u32)
            .map(|i| (i.wrapping_mul(2654435761) >> 16) as u8)
            .collect();
        let mut objects = vec![Object::new(Kind::Blob, base.clone())];
        for k in 0..6u8 {
            let mut v = base.clone();
            let len = v.len();
            for j in 0..50 {
                v[j * 307 % len] = k.wrapping_add(j as u8);
            }
            objects.push(Object::new(Kind::Blob, v));
        }

        let built = build_pack_delta_for_serving(&objects).unwrap();
        assert_eq!(built.object_entries.len(), built.entries.len());
        assert!(
            built
                .object_entries
                .iter()
                .any(|entry| matches!(entry.kind, PackObjectKind::OfsDelta { .. })),
            "fixture should exercise delta entries"
        );

        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(&built.data).unwrap();
        let resolved = resolve_pack_file(file.path())
            .unwrap()
            .expect("self-contained pack");
        let got: std::collections::HashMap<Oid, PackObjectEntry> = built
            .object_entries
            .iter()
            .cloned()
            .map(|entry| (entry.oid, entry))
            .collect();
        let want: std::collections::HashMap<Oid, PackObjectEntry> =
            resolved_object_entries(&resolved)
                .into_iter()
                .map(|entry| (entry.oid, entry))
                .collect();
        assert_eq!(got, want);
    }

    #[test]
    fn serving_delta_pack_caps_chain_depth() {
        let base: Vec<u8> = (0..20_000u32)
            .map(|i| (i.wrapping_mul(2654435761) >> 16) as u8)
            .collect();
        let mut objects = Vec::new();
        for k in 0..40u8 {
            let mut v = base.clone();
            let len = v.len();
            for j in 0..40 {
                v[j * 307 % len] = k.wrapping_add(j as u8);
            }
            objects.push(Object::new(Kind::Blob, v));
        }

        let built = build_pack_delta_for_serving(&objects).unwrap();
        let parsed = parse_pack(&built.data, |_| None).unwrap();
        assert_eq!(parsed.objects.len(), objects.len());

        let mut depths = std::collections::HashMap::<u64, usize>::new();
        let mut max_depth = 0usize;
        let mut saw_delta = false;
        for entry in &built.entries {
            let depth = match read_pack_entry(&built.data, entry.offset).unwrap() {
                PackEntry::Object { .. } => 0,
                PackEntry::OfsDelta { base_offset, .. } => {
                    saw_delta = true;
                    depths[&base_offset] + 1
                }
                PackEntry::RefDelta { .. } => {
                    panic!("builder should emit OFS_DELTA, not REF_DELTA")
                }
            };
            max_depth = max_depth.max(depth);
            depths.insert(entry.offset, depth);
        }
        assert!(
            saw_delta,
            "serving pack should still delta-compress similar objects"
        );
        assert!(
            max_depth <= SERVING_DELTA_MAX_DEPTH,
            "serving pack depth {max_depth} exceeded cap {SERVING_DELTA_MAX_DEPTH}"
        );
    }

    #[test]
    fn serving_reuse_pack_caps_chain_depth_without_losing_deltas() {
        let base: Vec<u8> = (0..20_000u32)
            .map(|i| (i.wrapping_mul(2654435761) >> 16) as u8)
            .collect();
        let mut objects = Vec::new();
        for k in 0..40u8 {
            let mut v = base.clone();
            let len = v.len();
            for j in 0..40 {
                v[j * 307 % len] = k.wrapping_add(j as u8);
            }
            objects.push(Object::new(Kind::Blob, v));
        }

        let source = build_pack_delta(&objects).unwrap();
        let parsed = parse_pack_reuse(&source.data).unwrap();
        let reuse: std::collections::HashMap<Oid, ReusableDelta> = parsed
            .iter()
            .filter_map(|(oid, _, r)| r.as_ref().map(|rd| (*oid, rd.clone())))
            .collect();
        assert!(
            !reuse.is_empty(),
            "source pack should provide reusable deltas"
        );

        let rebuilt = build_pack_delta_reuse_for_serving(&objects, &reuse).unwrap();
        let out = parse_pack(&rebuilt.data, |_| None).unwrap();
        assert_eq!(out.objects.len(), objects.len());

        let mut depths = std::collections::HashMap::<u64, usize>::new();
        let mut max_depth = 0usize;
        let mut saw_delta = false;
        for entry in &rebuilt.entries {
            let depth = match read_pack_entry(&rebuilt.data, entry.offset).unwrap() {
                PackEntry::Object { .. } => 0,
                PackEntry::OfsDelta { base_offset, .. } => {
                    saw_delta = true;
                    depths[&base_offset] + 1
                }
                PackEntry::RefDelta { .. } => {
                    panic!("builder should emit OFS_DELTA, not REF_DELTA")
                }
            };
            max_depth = max_depth.max(depth);
            depths.insert(entry.offset, depth);
        }
        assert!(saw_delta, "serving rebuild should reuse safe source deltas");
        assert!(
            max_depth <= SERVING_DELTA_MAX_DEPTH,
            "serving reuse pack depth {max_depth} exceeded cap {SERVING_DELTA_MAX_DEPTH}"
        );
    }

    #[test]
    fn delta_pack_handles_singletons_and_mixed_types() {
        // No two objects are similar; the builder must still produce a valid (mostly-full) pack.
        let objects = objs();
        let delta = build_pack_delta(&objects).unwrap();
        let parsed = parse_pack(&delta.data, |_| None).unwrap();
        assert_eq!(parsed.objects.len(), objects.len());
        for obj in &objects {
            assert!(parsed.objects.iter().any(|(oid, _)| *oid == obj.id()));
        }
    }
}
