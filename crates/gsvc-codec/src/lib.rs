//! `gsvc-codec` — the git object & packfile codec for the fast git service.
//!
//! This crate is a pure, dependency-light implementation of the parts of git's on-disk format we
//! need on the hot path:
//!
//! * [`Oid`] / [`ChunkHash`] — object and chunk addresses.
//! * [`Object`] / [`Kind`] / [`hash`] — the git object model and content hashing.
//! * [`pack`] — packfile v2 read (full `OFS_DELTA`/`REF_DELTA` resolution) and write.
//! * [`idx`] — pack index v2 generation and oid→offset lookup for byte-range serving.
//! * [`delta`] — the delta instruction codec shared by the pack reader and (eventually) repacker.
//!
//! It deliberately has no I/O, async, or storage concerns — those live in `gsvc-store` and above —
//! so it stays trivially testable and reusable.

pub mod async_stream;
mod bitmap;
mod chunkloc;
mod commitgraph;
mod delta;
mod error;
pub mod graph;
mod idx;
pub mod latency;
mod object;
mod oid;
mod pack;

pub use async_stream::{
    object_frame, parse_pack_streaming_async, AsyncPackSink, AsyncPackWriter, BaseResolver,
    ByteSink, ByteSource, ChunkSource, NoBases,
};
pub use bitmap::{Bitmap, PackBitmaps};
pub use chunkloc::ChunkLoc;
pub use commitgraph::{CommitGraph, CommitNode};
pub use delta::{apply_delta, encode_delta, encode_trivial_delta, DeltaIndex};
pub use error::CodecError;
pub use graph::{
    commit_links, encode_commit, encode_tree, links_of, tag_target, tree_entries, Links, TreeEntry,
};
pub use idx::{write_idx_v2, IdxV2};
pub use object::{hash, BlobOidHasher, Kind, Object};
pub use oid::{ChunkHash, Oid};
pub use pack::{
    build_pack, build_pack_at_level, build_pack_delta, build_pack_delta_for_serving,
    decode_pack_object_index_sidecar, decode_pack_source_index_sidecar,
    encode_pack_object_index_sidecar, encode_pack_source_index_sidecar,
    inflate_full_payload_range_to_temp, pack_ref_delta_bases, parse_pack,
    plan_pack_file_optimization, plan_pack_tree_index, plan_pack_tree_index_with_external_bases,
    read_pack_entry, resolve_pack_file, resolve_pack_file_with_log_context, resolve_pack_parallel,
    resolve_scanned_pack_with_external_bases, resolve_scanned_pack_with_log_context,
    stream_indexed_full_blob_range_to_temp, stream_indexed_full_blob_to_temp,
    write_pack_object_index_sidecar, write_pack_source_index_sidecar, BuiltPack, ExternalBase,
    FileBackedBlob, PackCommitIndexEntry, PackEntry, PackObjectEntry, PackObjectKind,
    PackOptimizationPlan, PackResolveLogContext, PackTreeIndexEntry, PackTreeIndexPlan,
    PackedEntry, ParsedPack, ReceivePackSpooler, ResolvedPack, ScannedPack,
};
