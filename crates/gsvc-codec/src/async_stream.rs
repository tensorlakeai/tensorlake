//! Async-native streaming packfile I/O.
//!
//! Unlike the whole-buffer [`build_pack`](crate::build_pack)/[`parse_pack`](crate::parse_pack), and
//! unlike a `spawn_blocking` bridge, this drives the pack codec from `async` directly: every I/O
//! wait (reading the request body, writing the response, fetching/storing objects) is `.await`ed, so
//! a transfer occupies a cheap task — **not** an OS thread — for its lifetime. Concurrency is then
//! bounded by memory and connections, not by a fixed blocking-thread pool. Only the bounded CPU
//! bursts (per-object inflate/deflate) run inline between awaits.
//!
//! The codec stays I/O-agnostic via four small traits the caller implements:
//! * [`ByteSource`] — pull the next chunk of input (the request body);
//! * [`ByteSink`] — push a chunk of output (the response body);
//! * [`AsyncPackSink`] — receive parsed objects; large blobs are *pushed* as plaintext so the caller
//!   can chunk them straight to the store without a pull-based reader;
//! * [`ChunkSource`] — supply a large blob's bytes when writing it into an outgoing pack.

use std::future::Future;
use std::io;

use bytes::Bytes;
use flate2::{Compress, Compression, Decompress, FlushCompress, FlushDecompress, Status};
use sha1::{Digest, Sha1};

use crate::delta::apply_delta;
use crate::{CodecError, Kind, Object, Oid};

const PACK_MAGIC: &[u8; 4] = b"PACK";
const PACK_VERSION: u32 = 2;

const T_COMMIT: u8 = 1;
const T_TREE: u8 = 2;
const T_BLOB: u8 = 3;
const T_TAG: u8 = 4;
const T_OFS_DELTA: u8 = 6;
const T_REF_DELTA: u8 = 7;

/// Blobs at or below this size must be retained in memory while parsing a pack, because git may
/// delta-compress later objects against them. It matches git's default `core.bigFileThreshold`
/// (512 MiB): git never deltifies an object larger than this, so a bigger blob can never be a delta
/// base and is safe to stream straight through without keeping its bytes. (A non-default client that
/// *raises* `bigFileThreshold` could defeat this; the practical effect is only a rejected push, never
/// corruption.)
const DELTA_BASE_MAX: u64 = 512 * 1024 * 1024;

// ── caller-implemented async I/O traits ──────────────────────────────────────

/// An async source of input bytes (e.g. an HTTP request body). `next` yields the next chunk, or
/// `None` at end of input.
pub trait ByteSource {
    fn next(&mut self) -> impl Future<Output = io::Result<Option<Bytes>>> + Send;
}

/// An async sink for output bytes (e.g. an HTTP response body).
pub trait ByteSink {
    fn send(&mut self, bytes: Bytes) -> impl Future<Output = io::Result<()>> + Send;
}

/// An async source of a single large object's plaintext, chunk by chunk (e.g. a chunked blob read
/// from the store). `None` ends the object.
pub trait ChunkSource {
    fn next(&mut self) -> impl Future<Output = Result<Option<Bytes>, CodecError>> + Send;
}

/// Where a streamed pack's objects go. Small/delta-resolved objects arrive whole via [`object`];
/// a large blob arrives as a `begin` → repeated `data` (plaintext) → `end` sequence so the caller
/// can content-chunk it to the store as it streams, never buffering the whole blob.
pub trait AsyncPackSink {
    fn object(
        &mut self,
        oid: Oid,
        kind: Kind,
        data: Bytes,
    ) -> impl Future<Output = Result<(), CodecError>> + Send;

    fn large_blob_begin(
        &mut self,
        size: u64,
    ) -> impl Future<Output = Result<(), CodecError>> + Send;
    fn large_blob_data(
        &mut self,
        data: &[u8],
    ) -> impl Future<Output = Result<(), CodecError>> + Send;
    fn large_blob_end(&mut self) -> impl Future<Output = Result<(), CodecError>> + Send;
}

// ── reader ───────────────────────────────────────────────────────────────────

/// A buffered, SHA-1-hashing async reader over a [`ByteSource`]. Every consumed body byte is folded
/// into the running pack trailer hash; the trailer itself is read raw and excluded.
struct AsyncHashingSource<Src> {
    src: Src,
    cur: Bytes,
    off: usize,
    hasher: Sha1,
    consumed: u64,
    eof: bool,
    /// Reused inflate output buffer, so a half-million-object pack doesn't allocate (and free) a
    /// fresh scratch buffer for every single object.
    scratch: Vec<u8>,
}

impl<Src: ByteSource> AsyncHashingSource<Src> {
    fn new(src: Src) -> Self {
        AsyncHashingSource {
            src,
            cur: Bytes::new(),
            off: 0,
            hasher: Sha1::new(),
            consumed: 0,
            eof: false,
            scratch: vec![0u8; 64 * 1024],
        }
    }

    /// Ensure at least one byte is buffered. Returns `false` at end of input.
    async fn fill(&mut self) -> io::Result<bool> {
        while self.off >= self.cur.len() {
            match self.src.next().await? {
                Some(b) => {
                    self.cur = b;
                    self.off = 0;
                }
                None => {
                    self.eof = true;
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    fn available(&self) -> &[u8] {
        &self.cur[self.off..]
    }

    fn consume(&mut self, n: usize) {
        let n = n.min(self.cur.len() - self.off);
        self.hasher.update(&self.cur[self.off..self.off + n]);
        self.off += n;
        self.consumed += n as u64;
    }

    async fn read_byte(&mut self) -> Result<u8, CodecError> {
        if !self.fill().await.map_err(io_err)? {
            return Err(CodecError::PackTooShort);
        }
        let b = self.cur[self.off];
        self.consume(1);
        Ok(b)
    }

    async fn read_be_u32(&mut self) -> Result<u32, CodecError> {
        let mut b = [0u8; 4];
        for x in &mut b {
            *x = self.read_byte().await?;
        }
        Ok(u32::from_be_bytes(b))
    }

    /// Read `n` raw bytes **without** hashing (only the trailer uses this).
    async fn read_raw(&mut self, n: usize) -> Result<Vec<u8>, CodecError> {
        let mut out = Vec::with_capacity(n);
        while out.len() < n {
            if !self.fill().await.map_err(io_err)? {
                return Err(CodecError::PackTooShort);
            }
            let take = (n - out.len()).min(self.cur.len() - self.off);
            out.extend_from_slice(&self.cur[self.off..self.off + take]);
            self.off += take; // raw: not hashed
        }
        Ok(out)
    }

    /// Inflate one object's `expected` plaintext bytes into a `Vec`.
    async fn inflate_object(&mut self, expected: usize) -> Result<Vec<u8>, CodecError> {
        let mut dec = Decompress::new(true);
        let mut out = Vec::with_capacity(expected);
        // Borrow the shared scratch buffer for the duration (also sidesteps borrowing `self` both
        // mutably for the buffer and immutably for `available()`); restored on the success path.
        let mut scratch = std::mem::take(&mut self.scratch);
        loop {
            if !self.fill().await.map_err(io_err)? {
                return Err(CodecError::Inflate("truncated zlib stream".into()));
            }
            let (bi, bo) = (dec.total_in(), dec.total_out());
            let status = dec
                .decompress(self.available(), &mut scratch, FlushDecompress::None)
                .map_err(|e| CodecError::Inflate(e.to_string()))?;
            let cin = (dec.total_in() - bi) as usize;
            let cout = (dec.total_out() - bo) as usize;
            self.consume(cin);
            out.extend_from_slice(&scratch[..cout]);
            if status == Status::StreamEnd {
                break;
            }
        }
        self.scratch = scratch;
        if out.len() != expected {
            return Err(CodecError::Inflate(format!(
                "expected {expected} bytes, produced {}",
                out.len()
            )));
        }
        Ok(out)
    }

    /// Inflate one large blob, pushing its plaintext to `sink` as it decompresses (never buffered).
    /// Like [`inflate_large`](Self::inflate_large), but also returns the full inflated bytes so the
    /// blob can be kept as a delta base. Used for large blobs small enough that git might
    /// delta-compress against them. The bytes still flow through the sink's large-blob (chunked
    /// storage) path; we just also retain them.
    async fn inflate_large_retained<S: AsyncPackSink>(
        &mut self,
        sink: &mut S,
        expected: u64,
    ) -> Result<Vec<u8>, CodecError> {
        let data = self.inflate_object(expected as usize).await?;
        sink.large_blob_begin(expected).await?;
        sink.large_blob_data(&data).await?;
        sink.large_blob_end().await?;
        Ok(data)
    }

    async fn inflate_large<S: AsyncPackSink>(
        &mut self,
        sink: &mut S,
        expected: u64,
    ) -> Result<(), CodecError> {
        sink.large_blob_begin(expected).await?;
        let mut dec = Decompress::new(true);
        let mut scratch = std::mem::take(&mut self.scratch);
        let mut produced = 0u64;
        loop {
            if !self.fill().await.map_err(io_err)? {
                return Err(CodecError::Inflate("truncated zlib stream".into()));
            }
            let (bi, bo) = (dec.total_in(), dec.total_out());
            let status = dec
                .decompress(self.available(), &mut scratch, FlushDecompress::None)
                .map_err(|e| CodecError::Inflate(e.to_string()))?;
            let cin = (dec.total_in() - bi) as usize;
            let cout = (dec.total_out() - bo) as usize;
            self.consume(cin);
            if cout > 0 {
                sink.large_blob_data(&scratch[..cout]).await?;
                produced += cout as u64;
            }
            if status == Status::StreamEnd {
                break;
            }
        }
        self.scratch = scratch;
        if produced != expected {
            return Err(CodecError::Inflate(format!(
                "large blob: expected {expected} bytes, produced {produced}"
            )));
        }
        sink.large_blob_end().await?;
        Ok(())
    }
}

/// Resolves a thin-pack `REF_DELTA` base that isn't contained in the pack itself. The server backs
/// this with its **on-demand** object store, so a push resolves only the handful of bases it
/// references rather than loading the whole repo into memory. `Ok(None)` means "no such base".
pub trait BaseResolver {
    fn resolve_base(
        &mut self,
        oid: &Oid,
    ) -> impl std::future::Future<Output = Result<Option<Object>, CodecError>> + Send;
}

/// A [`BaseResolver`] for self-contained packs (no external thin-pack bases) — every `REF_DELTA`
/// base must appear earlier in the pack.
pub struct NoBases;

impl BaseResolver for NoBases {
    async fn resolve_base(&mut self, _oid: &Oid) -> Result<Option<Object>, CodecError> {
        Ok(None)
    }
}

/// Parse a v2 packfile from an async [`ByteSource`], resolving deltas, streaming blobs of at least
/// `large_threshold` bytes to the sink and materializing everything else. `resolver` supplies
/// external bases for thin-pack `REF_DELTA`s on demand. Returns the pack trailer oid.
pub async fn parse_pack_streaming_async<Src, R, Sink>(
    src: Src,
    large_threshold: u64,
    resolver: &mut R,
    sink: &mut Sink,
) -> Result<Oid, CodecError>
where
    Src: ByteSource,
    R: BaseResolver,
    Sink: AsyncPackSink,
{
    let mut s = AsyncHashingSource::new(src);

    let mut magic = [0u8; 4];
    for b in &mut magic {
        *b = s.read_byte().await?;
    }
    if &magic != PACK_MAGIC {
        return Err(CodecError::BadPackMagic);
    }
    let version = s.read_be_u32().await?;
    if version != PACK_VERSION {
        return Err(CodecError::BadPackVersion(version));
    }
    let count = s.read_be_u32().await?;

    // Resolved objects are kept here only so later entries can delta against them. The bytes are
    // `Bytes` (refcounted), so the copy handed to the sink shares this one allocation rather than
    // duplicating every object — roughly halving peak memory on a large push.
    let mut bases: std::collections::HashMap<Oid, (Kind, Bytes)> =
        std::collections::HashMap::with_capacity(
            (count as usize).min(crate::pack::PREALLOC_HINT_CAP),
        );
    let mut by_offset: std::collections::HashMap<u64, Oid> =
        std::collections::HashMap::with_capacity(
            (count as usize).min(crate::pack::PREALLOC_HINT_CAP),
        );
    let mut offset: u64 = 12;

    for _ in 0..count {
        let entry_start = offset;
        let (ty, size, header_len) = read_entry_header(&mut s).await?;
        offset += header_len;

        match ty {
            T_BLOB if size >= large_threshold => {
                let before = s.consumed;
                if size <= DELTA_BASE_MAX {
                    // Storage-wise this is a large blob (streamed to the chunk store), but git may
                    // still delta-compress later objects against it — so we materialize it once and
                    // keep it as a resolvable delta base for the rest of the pack, while feeding the
                    // same bytes through the sink's large-blob path for chunked storage.
                    let data = s.inflate_large_retained(sink, size).await?;
                    offset += s.consumed - before;
                    let oid = crate::object::hash(Kind::Blob, &data);
                    by_offset.insert(entry_start, oid);
                    bases.insert(oid, (Kind::Blob, Bytes::from(data)));
                } else {
                    // Above git's delta threshold: never a delta base, so stream without retaining.
                    s.inflate_large(sink, size).await?;
                    offset += s.consumed - before;
                }
            }
            T_COMMIT | T_TREE | T_BLOB | T_TAG => {
                let kind = Kind::from_pack_type(ty)?;
                let before = s.consumed;
                let data = Bytes::from(s.inflate_object(size as usize).await?);
                offset += s.consumed - before;
                let oid = crate::object::hash(kind, &data);
                by_offset.insert(entry_start, oid);
                bases.insert(oid, (kind, data.clone())); // refcount bump, shares the allocation
                sink.object(oid, kind, data).await?;
            }
            T_OFS_DELTA => {
                let rel = read_offset_varint(&mut s, &mut offset).await?;
                let base_off = entry_start
                    .checked_sub(rel)
                    .ok_or(CodecError::BadDeltaBaseOffset)?;
                let before = s.consumed;
                let delta = s.inflate_object(size as usize).await?;
                offset += s.consumed - before;
                let base_oid = *by_offset
                    .get(&base_off)
                    .ok_or(CodecError::MissingDeltaBaseOffset(base_off))?;
                let (bkind, bdata) = bases
                    .get(&base_oid)
                    .ok_or(CodecError::MissingDeltaBaseOffset(base_off))?;
                let result = Bytes::from(apply_delta(bdata, &delta)?);
                let kind = *bkind;
                let oid = crate::object::hash(kind, &result);
                by_offset.insert(entry_start, oid);
                bases.insert(oid, (kind, result.clone()));
                emit_resolved_object(sink, large_threshold, oid, kind, result).await?;
            }
            T_REF_DELTA => {
                let mut base = [0u8; 20];
                for b in &mut base {
                    *b = s.read_byte().await?;
                }
                offset += 20;
                let base_oid = Oid::from_bytes(&base)?;
                let before = s.consumed;
                let delta = s.inflate_object(size as usize).await?;
                offset += s.consumed - before;
                let (bkind, bdata): (Kind, Bytes) = if let Some((k, d)) = bases.get(&base_oid) {
                    (*k, d.clone())
                } else if let Some(obj) = resolver.resolve_base(&base_oid).await? {
                    (obj.kind, obj.data.clone())
                } else {
                    return Err(CodecError::MissingDeltaBaseOid(base_oid));
                };
                let result = Bytes::from(apply_delta(&bdata, &delta)?);
                let oid = crate::object::hash(bkind, &result);
                by_offset.insert(entry_start, oid);
                bases.insert(oid, (bkind, result.clone()));
                emit_resolved_object(sink, large_threshold, oid, bkind, result).await?;
            }
            other => return Err(CodecError::BadPackType(other)),
        }
    }

    let computed: [u8; 20] = s.hasher.clone().finalize().into();
    let trailer = s.read_raw(20).await?;
    if trailer != computed {
        return Err(CodecError::PackChecksumMismatch);
    }
    Ok(Oid::from_array(computed))
}

async fn emit_resolved_object<S: AsyncPackSink>(
    sink: &mut S,
    large_threshold: u64,
    oid: Oid,
    kind: Kind,
    data: Bytes,
) -> Result<(), CodecError> {
    if kind == Kind::Blob && data.len() as u64 >= large_threshold {
        sink.large_blob_begin(data.len() as u64).await?;
        sink.large_blob_data(&data).await?;
        sink.large_blob_end().await?;
    } else {
        sink.object(oid, kind, data).await?;
    }
    Ok(())
}

async fn read_entry_header<Src: ByteSource>(
    s: &mut AsyncHashingSource<Src>,
) -> Result<(u8, u64, u64), CodecError> {
    let c = s.read_byte().await?;
    let ty = (c >> 4) & 0x07;
    let mut size = (c & 0x0f) as u64;
    let mut shift = 4u32;
    let mut cont = c & 0x80 != 0;
    let mut len = 1u64;
    while cont {
        let c = s.read_byte().await?;
        len += 1;
        size |= ((c & 0x7f) as u64) << shift;
        shift += 7;
        cont = c & 0x80 != 0;
    }
    Ok((ty, size, len))
}

async fn read_offset_varint<Src: ByteSource>(
    s: &mut AsyncHashingSource<Src>,
    offset: &mut u64,
) -> Result<u64, CodecError> {
    let mut c = s.read_byte().await?;
    *offset += 1;
    let mut off = (c & 0x7f) as u64;
    while c & 0x80 != 0 {
        c = s.read_byte().await?;
        *offset += 1;
        off = ((off + 1) << 7) | (c & 0x7f) as u64;
    }
    Ok(off)
}

// ── writer ───────────────────────────────────────────────────────────────────

/// Streams a v2 packfile to an async [`ByteSink`], compressing each object as it goes and keeping
/// the running trailer hash. Large blobs are pulled from a [`ChunkSource`] and compressed
/// incrementally, so the response is produced without materializing the pack or any large object.
pub struct AsyncPackWriter<S> {
    sink: S,
    hasher: Sha1,
    remaining: u32,
    /// zlib level for the objects written into this pack. A fetch builds an **ephemeral wire pack**
    /// that is recompressed on every request and discarded, so it favors a fast level (deflate is the
    /// single-stream throughput ceiling); stored packs that are written once and read many keep a
    /// higher level.
    level: Compression,
    /// Output buffer: pack bytes accumulate here and flush to the sink in ~`BATCH`-sized pieces, so
    /// downstream framing (e.g. side-band pkt-lines) carries useful payloads, not one per object.
    buf: Vec<u8>,
}

impl<S: ByteSink> AsyncPackWriter<S> {
    /// Flush threshold for the output batch buffer.
    const BATCH: usize = 32 * 1024;

    /// Begin a pack of `object_count` objects compressed at zlib `level` (0–9; buffers the 12-byte
    /// header). Serving passes a low level for speed; storage passes a higher one for ratio.
    pub async fn new(sink: S, object_count: u32, level: u32) -> Result<Self, CodecError> {
        let mut w = AsyncPackWriter {
            sink,
            hasher: Sha1::new(),
            remaining: object_count,
            level: Compression::new(level.min(9)),
            buf: Vec::with_capacity(Self::BATCH + 4096),
        };
        let mut header = Vec::with_capacity(12);
        header.extend_from_slice(PACK_MAGIC);
        header.extend_from_slice(&PACK_VERSION.to_be_bytes());
        header.extend_from_slice(&object_count.to_be_bytes());
        w.emit(&header).await?;
        Ok(w)
    }

    /// Hash + buffer pack bytes, flushing to the sink when the batch fills.
    async fn emit(&mut self, bytes: &[u8]) -> Result<(), CodecError> {
        self.hasher.update(bytes);
        self.buf.extend_from_slice(bytes);
        if self.buf.len() >= Self::BATCH {
            let batch = std::mem::take(&mut self.buf);
            self.buf.reserve(Self::BATCH + 4096);
            self.sink.send(Bytes::from(batch)).await.map_err(io_err)?;
        }
        Ok(())
    }

    /// Append a fully-in-memory object.
    pub async fn write_object(&mut self, kind: Kind, data: &[u8]) -> Result<(), CodecError> {
        let mut header = Vec::with_capacity(8);
        write_entry_header(&mut header, kind.pack_type(), data.len() as u64);
        self.emit(&header).await?;
        let compressed = deflate(data, self.level)?;
        self.emit(&compressed).await?;
        self.remaining = self.remaining.saturating_sub(1);
        Ok(())
    }

    /// Append a pre-built object frame (entry header + compressed body) produced by [`object_frame`].
    /// The bytes are hashed and buffered exactly like a [`write_object`](Self::write_object) result,
    /// so a caller can deflate many objects off-thread in parallel and feed the frames back **in pack
    /// order** — moving the per-object CPU burst off the single async task and across cores.
    pub async fn write_frame(&mut self, frame: &[u8]) -> Result<(), CodecError> {
        self.emit(frame).await?;
        self.remaining = self.remaining.saturating_sub(1);
        Ok(())
    }

    /// Append raw pack bytes verbatim — already-compressed object entries copied from a stored pack
    /// (its 12-byte header and 20-byte trailer stripped) — without re-deflating. The bytes are still
    /// hashed into this pack's trailer. The caller must follow the span with [`mark_written`] to
    /// account for the objects it contains. Used to splice a base pack's body into a larger pack so
    /// the bulk of a clone is reused, not recompressed.
    ///
    /// [`mark_written`]: Self::mark_written
    pub async fn write_raw(&mut self, bytes: &[u8]) -> Result<(), CodecError> {
        self.emit(bytes).await
    }

    /// Account for `n` objects written via [`write_raw`](Self::write_raw) verbatim spans, so the
    /// final object-count check in [`finish`](Self::finish) balances.
    pub fn mark_written(&mut self, n: u32) {
        self.remaining = self.remaining.saturating_sub(n);
    }

    /// Append a blob by streaming and compressing its `size` plaintext bytes pulled from `src`.
    pub async fn write_blob_streaming<C: ChunkSource>(
        &mut self,
        size: u64,
        mut src: C,
    ) -> Result<(), CodecError> {
        let mut header = Vec::with_capacity(8);
        write_entry_header(&mut header, T_BLOB, size);
        self.emit(&header).await?;

        let mut comp = Compress::new(self.level, true);
        let mut scratch = vec![0u8; 64 * 1024];
        let mut plain = 0u64;
        while let Some(chunk) = src.next().await? {
            plain += chunk.len() as u64;
            let mut in_off = 0;
            while in_off < chunk.len() {
                let (bi, bo) = (comp.total_in(), comp.total_out());
                comp.compress(&chunk[in_off..], &mut scratch, FlushCompress::None)
                    .map_err(|e| CodecError::Io(e.to_string()))?;
                in_off += (comp.total_in() - bi) as usize;
                let cout = (comp.total_out() - bo) as usize;
                if cout > 0 {
                    self.emit(&scratch[..cout]).await?;
                }
                if comp.total_in() - bi == 0 && cout == 0 {
                    break;
                }
            }
        }
        // Flush the deflate stream.
        loop {
            let bo = comp.total_out();
            let status = comp
                .compress(&[], &mut scratch, FlushCompress::Finish)
                .map_err(|e| CodecError::Io(e.to_string()))?;
            let cout = (comp.total_out() - bo) as usize;
            if cout > 0 {
                self.emit(&scratch[..cout]).await?;
            }
            if status == Status::StreamEnd {
                break;
            }
        }
        if plain != size {
            return Err(CodecError::Io(format!(
                "blob stream produced {plain} bytes, declared {size}"
            )));
        }
        self.remaining = self.remaining.saturating_sub(1);
        Ok(())
    }

    /// Finish the pack: flush the buffer, send the SHA-1 trailer, and return the pack id.
    pub async fn finish(mut self) -> Result<Oid, CodecError> {
        if self.remaining != 0 {
            return Err(CodecError::Io(format!(
                "pack declared more objects than written ({} missing)",
                self.remaining
            )));
        }
        if !self.buf.is_empty() {
            let batch = std::mem::take(&mut self.buf);
            self.sink.send(Bytes::from(batch)).await.map_err(io_err)?;
        }
        let digest: [u8; 20] = self.hasher.finalize().into();
        self.sink
            .send(Bytes::copy_from_slice(&digest))
            .await
            .map_err(io_err)?;
        Ok(Oid::from_array(digest))
    }
}

/// Build one complete packfile object entry — varint type/size header followed by the zlib-deflated
/// body — for a fully-in-memory object at zlib `level` (0–9). Pure and allocation-local, so frames
/// can be produced concurrently off-thread and later written in order via
/// [`AsyncPackWriter::write_frame`].
pub fn object_frame(kind: Kind, data: &[u8], level: u32) -> Result<Vec<u8>, CodecError> {
    let mut frame = Vec::with_capacity(8 + data.len() / 2 + 16);
    write_entry_header(&mut frame, kind.pack_type(), data.len() as u64);
    let compressed = deflate(data, Compression::new(level.min(9)))?;
    frame.extend_from_slice(&compressed);
    Ok(frame)
}

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

fn deflate(data: &[u8], level: Compression) -> Result<Vec<u8>, CodecError> {
    use std::io::Write;
    let mut enc = flate2::write::ZlibEncoder::new(Vec::new(), level);
    enc.write_all(data)
        .map_err(|e| CodecError::Io(e.to_string()))?;
    enc.finish().map_err(|e| CodecError::Io(e.to_string()))
}

fn io_err(e: io::Error) -> CodecError {
    CodecError::Io(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{build_pack, parse_pack};

    /// A `ByteSource` that hands out a buffer in fixed-size pieces.
    struct VecSource {
        data: Vec<u8>,
        pos: usize,
        step: usize,
    }
    impl ByteSource for VecSource {
        async fn next(&mut self) -> io::Result<Option<Bytes>> {
            if self.pos >= self.data.len() {
                return Ok(None);
            }
            let end = (self.pos + self.step).min(self.data.len());
            let b = Bytes::copy_from_slice(&self.data[self.pos..end]);
            self.pos = end;
            Ok(Some(b))
        }
    }

    struct OneShot(Option<Bytes>);
    impl ChunkSource for OneShot {
        async fn next(&mut self) -> Result<Option<Bytes>, CodecError> {
            Ok(self.0.take())
        }
    }

    #[derive(Default)]
    struct CollectSink {
        objects: Vec<(Oid, Kind, Vec<u8>)>,
        large: Vec<(Oid, Vec<u8>)>,
        cur: Vec<u8>,
        cur_size: u64,
    }
    impl AsyncPackSink for CollectSink {
        async fn object(&mut self, oid: Oid, kind: Kind, data: Bytes) -> Result<(), CodecError> {
            self.objects.push((oid, kind, data.to_vec()));
            Ok(())
        }
        async fn large_blob_begin(&mut self, size: u64) -> Result<(), CodecError> {
            self.cur = Vec::new();
            self.cur_size = size;
            Ok(())
        }
        async fn large_blob_data(&mut self, data: &[u8]) -> Result<(), CodecError> {
            self.cur.extend_from_slice(data);
            Ok(())
        }
        async fn large_blob_end(&mut self) -> Result<(), CodecError> {
            assert_eq!(self.cur.len() as u64, self.cur_size);
            let oid = crate::object::hash(Kind::Blob, &self.cur);
            self.large.push((oid, std::mem::take(&mut self.cur)));
            Ok(())
        }
    }

    fn sample() -> Vec<Object> {
        vec![
            Object::new(Kind::Blob, &b"hello async\n"[..]),
            Object::new(
                Kind::Commit,
                &b"tree 4b825dc642cb6eb9a060e54bf8d69288fbee4904\n"[..],
            ),
            Object::new(Kind::Blob, vec![0x33u8; 250_000]),
            Object::new(Kind::Tree, &b""[..]),
        ]
    }

    #[test]
    fn async_parse_matches_build_pack() {
        let objects = sample();
        let built = build_pack(&objects).unwrap();
        let mut sink = CollectSink::default();
        let id = futures::executor::block_on(parse_pack_streaming_async(
            VecSource {
                data: built.data.clone(),
                pos: 0,
                step: 1000,
            },
            u64::MAX,
            &mut NoBases,
            &mut sink,
        ))
        .unwrap();
        assert_eq!(id, built.pack_hash);
        assert_eq!(sink.objects.len(), objects.len());
        for (orig, (oid, kind, data)) in objects.iter().zip(&sink.objects) {
            assert_eq!(*oid, orig.id());
            assert_eq!(*kind, orig.kind);
            assert_eq!(data.as_slice(), orig.data.as_ref());
        }
    }

    #[test]
    fn async_parse_streams_large_blobs() {
        let objects = sample();
        let built = build_pack(&objects).unwrap();
        let mut sink = CollectSink::default();
        // Odd-sized input pieces + a low threshold to route the 250k blob through the stream path.
        futures::executor::block_on(parse_pack_streaming_async(
            VecSource {
                data: built.data,
                pos: 0,
                step: 333,
            },
            100_000,
            &mut NoBases,
            &mut sink,
        ))
        .unwrap();
        assert_eq!(sink.large.len(), 1);
        assert_eq!(sink.large[0].0, objects[2].id());
        assert_eq!(sink.objects.len(), 3);
    }

    /// A `Send` sink that appends to a shared buffer (so we can read it back after the writer moves
    /// it in).
    #[derive(Clone)]
    struct SharedSink(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
    impl ByteSink for SharedSink {
        async fn send(&mut self, bytes: Bytes) -> io::Result<()> {
            self.0.lock().unwrap().extend_from_slice(&bytes);
            Ok(())
        }
    }

    #[test]
    fn write_frame_matches_write_object_byte_for_byte() {
        // The parallel-compression path (object_frame + write_frame) must produce exactly the same
        // pack bytes — including the trailer hash — as the serial write_object path.
        let objs = sample();
        let serial = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let framed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        futures::executor::block_on(async {
            let mut w = AsyncPackWriter::new(SharedSink(serial.clone()), objs.len() as u32, 6)
                .await
                .unwrap();
            for o in &objs {
                w.write_object(o.kind, &o.data).await.unwrap();
            }
            w.finish().await.unwrap();

            let mut w = AsyncPackWriter::new(SharedSink(framed.clone()), objs.len() as u32, 6)
                .await
                .unwrap();
            for o in &objs {
                let frame = object_frame(o.kind, &o.data, 6).unwrap();
                w.write_frame(&frame).await.unwrap();
            }
            w.finish().await.unwrap();
        });
        assert_eq!(*serial.lock().unwrap(), *framed.lock().unwrap());
    }

    #[test]
    fn async_writer_roundtrips_through_parser() {
        let big = vec![0x9eu8; 300_000];
        let out = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        futures::executor::block_on(async {
            let mut w = AsyncPackWriter::new(SharedSink(out.clone()), 2, 6)
                .await
                .unwrap();
            w.write_object(Kind::Blob, b"a small object\n")
                .await
                .unwrap();
            w.write_blob_streaming(big.len() as u64, OneShot(Some(Bytes::from(big.clone()))))
                .await
                .unwrap();
            w.finish().await.unwrap();
        });

        let packed = out.lock().unwrap().clone();
        let parsed = parse_pack(&packed, |_| None).unwrap();
        assert_eq!(parsed.objects.len(), 2);
        assert_eq!(parsed.objects[0].1.data.as_ref(), b"a small object\n");
        assert_eq!(parsed.objects[1].1.data.as_ref(), big.as_slice());
    }
}
