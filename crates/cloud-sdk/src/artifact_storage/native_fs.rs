//! Native, non-Git filesystem snapshots and their upload protocol.
//!
//! These wire types intentionally mirror `gsvc-fs-format` in artifact storage. The public
//! Tensorlake tree cannot depend on that private crate, so golden vectors below fail if the
//! canonical postcard encoding or domain-separated identities drift between the two repos.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::io::{Read, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::StreamExt;
use ignore::WalkBuilder;
use rayon::prelude::*;
use reqwest::{Body, Method, StatusCode};
use serde::{Deserialize, Serialize};
use sha2::Digest;

use crate::error::SdkError;

use super::ArtifactStorageClient;

pub const FORMAT_VERSION: u16 = 1;
pub const MAX_INLINE_BYTES: usize = 4 * 1024;
pub const MAX_CANONICAL_OBJECT_BYTES: usize = 90 * 1024;
pub const MAX_DIRECTORY_PAGE_ENTRIES: usize = 4096;
pub const MAX_RECIPE_PAGE_ENTRIES: usize = 1000;
pub const MAX_TREE_DEPTH: u8 = 8;
pub const MAX_SEGMENT_BYTES: u64 = 256 * 1024 * 1024;
pub const MAX_SEGMENT_SLICE_LOGICAL_BYTES: u64 = 64 * 1024 * 1024;
pub const ENTRY_MODE_MASK: u32 = 0o7777;

// The server accepts at most 16 MiB and 4096 objects on the metadata route. Keep the client target
// comfortably below the transport ceiling so request headers and future additive wire fields do not
// turn a valid large tree into a 413 response.
const NATIVE_METADATA_REQUEST_TARGET_BYTES: usize = 8 * 1024 * 1024;
const NATIVE_METADATA_REQUEST_MAX_OBJECTS: usize = 4096;
const NATIVE_METADATA_UPLOAD_CONCURRENCY: usize = 8;
// Keep cold-snapshot segments small enough to remain in bounded memory while their single-part PUT
// is in flight. This preserves exact full-object SHA-256 validation without a local tempfile or a
// multipart completion protocol.
const NATIVE_MEMORY_SEGMENT_TARGET_BYTES: u64 = 32 * 1024 * 1024;
const NATIVE_RECORD_LOGICAL_BYTES: u64 = 16 * 1024 * 1024;
const NATIVE_SEGMENT_UPLOAD_QUEUE: usize = 4;
const NATIVE_SEGMENT_UPLOAD_CONCURRENCY: usize = 8;
// Registration publishes record facts in one bounded FDB transaction and carries declarations in a
// bounded HTTP body. Tiny-file workloads therefore seal on record count as well as byte count.
const NATIVE_RECORDS_PER_SEGMENT: usize = 4_096;
// Content preparation owns one open 32 MiB segment per worker. Keep that memory bound independent
// of host core count so a large build machine cannot exhaust an agent sandbox.
const NATIVE_CONTENT_PREPARE_CONCURRENCY: usize = 8;
// Stable path-ordered grouping is part of segment identity. It must never depend on CPU count.
const NATIVE_CONTENT_GROUP_TARGET_BYTES: u64 = 512 * 1024 * 1024;
// Snapshots optimize for wall time: immutable segment dedup amortizes the modest ratio difference,
// while level 2 keeps compression from becoming the bottleneck ahead of direct blob uploads.
const NATIVE_SEGMENT_ZSTD_LEVEL: i32 = 2;

const FILE_CONTENT_DOMAIN: &[u8] = b"tensorlake.fs.file-content.v1\0";
const DIRECTORY_PAGE_DOMAIN: &[u8] = b"tensorlake.fs.directory-page.v1\0";
const CHUNK_RECIPE_DOMAIN: &[u8] = b"tensorlake.fs.chunk-recipe.v1\0";

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum FormatError {
    #[error("invalid object id {0:?}")]
    InvalidObjectId(String),
    #[error("unsupported format version {0}")]
    UnsupportedVersion(u16),
    #[error("invalid filesystem entry name")]
    InvalidName,
    #[error("entry mode contains Unix file-type or unsupported bits: {0:#o}")]
    InvalidMode(u32),
    #[error("directory entries must be strictly sorted by raw name bytes")]
    UnsortedDirectoryEntries,
    #[error("directory index must have at least two children strictly sorted by first name")]
    InvalidDirectoryIndex,
    #[error("tree index level must be in 1..={MAX_TREE_DEPTH}: {0}")]
    InvalidTreeLevel(u8),
    #[error("directory page contains too many entries: {0}")]
    DirectoryPageTooLarge(usize),
    #[error("inline content exceeds {MAX_INLINE_BYTES} bytes: {0}")]
    InlineContentTooLarge(usize),
    #[error("file content length {actual} does not match declared size {declared}")]
    FileSizeMismatch { declared: u64, actual: u64 },
    #[error("segment range is empty or overflows u64")]
    InvalidSegmentRange,
    #[error("segment slice logical length exceeds {MAX_SEGMENT_SLICE_LOGICAL_BYTES} bytes: {0}")]
    SegmentSliceTooLarge(u64),
    #[error("raw segment range stores {stored} bytes but declares {logical} logical bytes")]
    RawSegmentLengthMismatch { stored: u32, logical: u64 },
    #[error("chunk recipe page contains an invalid number of entries: {0}")]
    InvalidRecipeParts(usize),
    #[error("chunk recipe page entries total {actual} bytes, expected {declared}")]
    RecipeSizeMismatch { declared: u64, actual: u64 },
    #[error("invalid symlink target")]
    InvalidSymlinkTarget,
    #[error("xattrs must have non-empty, NUL-free, strictly sorted names")]
    InvalidXattrs,
    #[error(
        "canonical object is {actual} bytes, exceeding the {MAX_CANONICAL_OBJECT_BYTES}-byte limit"
    )]
    CanonicalObjectTooLarge { actual: usize },
    #[error("canonical serialization failed: {0}")]
    Encode(String),
}

#[derive(Clone, Copy, Default, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ObjectId([u8; 32]);

impl ObjectId {
    pub const fn from_array(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn from_hex(value: &str) -> Result<Self, FormatError> {
        let bytes = hex::decode(value).map_err(|_| FormatError::InvalidObjectId(value.into()))?;
        let bytes = bytes
            .try_into()
            .map_err(|_| FormatError::InvalidObjectId(value.into()))?;
        Ok(Self(bytes))
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(self) -> String {
        hex::encode(self.0)
    }

    pub fn segment(bytes: &[u8]) -> Self {
        let mut hasher = Self::segment_hasher();
        hasher.update(bytes);
        hasher.finalize()
    }

    pub fn file_content(bytes: &[u8]) -> Self {
        let mut hasher = Self::file_content_hasher();
        hasher.update(bytes);
        hasher.finalize()
    }

    pub fn segment_hasher() -> SegmentIdHasher {
        SegmentIdHasher::new()
    }

    pub fn file_content_hasher() -> ObjectIdHasher {
        ObjectIdHasher::new(FILE_CONTENT_DOMAIN)
    }
}

impl fmt::Debug for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ObjectId").field(&self.to_hex()).finish()
    }
}

impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

#[derive(Clone, Debug)]
pub struct ObjectIdHasher {
    hasher: blake3::Hasher,
}

impl ObjectIdHasher {
    fn new(domain: &[u8]) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(domain);
        Self { hasher }
    }

    pub fn update(&mut self, bytes: &[u8]) -> &mut Self {
        self.hasher.update(bytes);
        self
    }

    pub fn finalize(&self) -> ObjectId {
        ObjectId::from_array(*self.hasher.finalize().as_bytes())
    }
}

#[derive(Clone, Debug)]
pub struct SegmentIdHasher {
    hasher: sha2::Sha256,
}

impl SegmentIdHasher {
    fn new() -> Self {
        Self {
            hasher: sha2::Sha256::new(),
        }
    }

    pub fn update(&mut self, bytes: &[u8]) -> &mut Self {
        self.hasher.update(bytes);
        self
    }

    pub fn finalize(&self) -> ObjectId {
        ObjectId::from_array(self.hasher.clone().finalize().into())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntryMetadata {
    pub mode: u32,
    pub mtime_ns: i64,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub xattrs: Vec<Xattr>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Xattr {
    pub name: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectoryEntry {
    pub name: Vec<u8>,
    pub metadata: EntryMetadata,
    pub data: EntryData,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryData {
    File {
        size: u64,
        content: FileContent,
        hardlink_group: Option<[u8; 16]>,
    },
    Directory {
        root: ObjectId,
    },
    Symlink {
        target: Vec<u8>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileContent {
    Inline(Vec<u8>),
    Segment(SegmentSlice),
    Recipe {
        recipe: ObjectId,
        logical_len: u64,
        content_id: ObjectId,
    },
}

impl FileContent {
    pub fn logical_len(&self) -> u64 {
        match self {
            Self::Inline(bytes) => bytes.len() as u64,
            Self::Segment(slice) => slice.logical_len,
            Self::Recipe { logical_len, .. } => *logical_len,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentSlice {
    pub segment: ObjectId,
    pub offset: u64,
    pub stored_len: u32,
    pub logical_len: u64,
    pub compression: Compression,
    pub content_id: ObjectId,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum Compression {
    Raw,
    Zstd,
}

impl SegmentSlice {
    fn validate(&self) -> Result<(), FormatError> {
        if self.stored_len == 0
            || self.logical_len == 0
            || self.offset.checked_add(self.stored_len as u64).is_none()
        {
            return Err(FormatError::InvalidSegmentRange);
        }
        if self.logical_len > MAX_SEGMENT_SLICE_LOGICAL_BYTES {
            return Err(FormatError::SegmentSliceTooLarge(self.logical_len));
        }
        if self.compression == Compression::Raw && self.stored_len as u64 != self.logical_len {
            return Err(FormatError::RawSegmentLengthMismatch {
                stored: self.stored_len,
                logical: self.logical_len,
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChunkRecipe {
    Leaf {
        version: u16,
        logical_len: u64,
        content_id: ObjectId,
        parts: Vec<SegmentSlice>,
    },
    Index {
        version: u16,
        level: u8,
        logical_len: u64,
        content_id: ObjectId,
        children: Vec<RecipeChild>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecipeChild {
    pub recipe: ObjectId,
    pub logical_len: u64,
}

impl ChunkRecipe {
    pub fn level(&self) -> u8 {
        match self {
            Self::Leaf { .. } => 0,
            Self::Index { level, .. } => *level,
        }
    }

    pub fn logical_len(&self) -> u64 {
        match self {
            Self::Leaf { logical_len, .. } | Self::Index { logical_len, .. } => *logical_len,
        }
    }

    pub fn content_id(&self) -> ObjectId {
        match self {
            Self::Leaf { content_id, .. } | Self::Index { content_id, .. } => *content_id,
        }
    }

    pub fn validate(&self) -> Result<(), FormatError> {
        let (declared, actual) = match self {
            Self::Leaf {
                version,
                logical_len,
                parts,
                ..
            } => {
                require_version(*version)?;
                if parts.is_empty() || parts.len() > MAX_RECIPE_PAGE_ENTRIES {
                    return Err(FormatError::InvalidRecipeParts(parts.len()));
                }
                let mut actual = 0u64;
                for part in parts {
                    part.validate()?;
                    actual = checked_sum(*logical_len, actual, part.logical_len)?;
                }
                (*logical_len, actual)
            }
            Self::Index {
                version,
                level,
                logical_len,
                children,
                ..
            } => {
                require_version(*version)?;
                validate_tree_level(*level)?;
                if children.len() < 2 || children.len() > MAX_RECIPE_PAGE_ENTRIES {
                    return Err(FormatError::InvalidRecipeParts(children.len()));
                }
                let mut actual = 0u64;
                for child in children {
                    if child.logical_len == 0 {
                        return Err(FormatError::InvalidRecipeParts(children.len()));
                    }
                    actual = checked_sum(*logical_len, actual, child.logical_len)?;
                }
                (*logical_len, actual)
            }
        };
        if declared != actual {
            return Err(FormatError::RecipeSizeMismatch { declared, actual });
        }
        validate_canonical_size(self)
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>, FormatError> {
        self.validate()?;
        encode(self)
    }

    pub fn id(&self) -> Result<ObjectId, FormatError> {
        Ok(hash_domain(CHUNK_RECIPE_DOMAIN, &self.canonical_bytes()?))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DirectoryPage {
    Leaf {
        version: u16,
        entries: Vec<DirectoryEntry>,
    },
    Index {
        version: u16,
        level: u8,
        children: Vec<DirectoryChild>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectoryChild {
    pub first_name: Vec<u8>,
    pub page: ObjectId,
}

impl DirectoryPage {
    pub fn empty() -> Self {
        Self::Leaf {
            version: FORMAT_VERSION,
            entries: Vec::new(),
        }
    }

    pub fn level(&self) -> u8 {
        match self {
            Self::Leaf { .. } => 0,
            Self::Index { level, .. } => *level,
        }
    }

    pub fn validate(&self) -> Result<(), FormatError> {
        match self {
            Self::Leaf { version, entries } => {
                require_version(*version)?;
                if entries.len() > MAX_DIRECTORY_PAGE_ENTRIES {
                    return Err(FormatError::DirectoryPageTooLarge(entries.len()));
                }
                validate_strict_names(entries.iter().map(|entry| entry.name.as_slice()))?;
                for entry in entries {
                    entry.validate()?;
                }
            }
            Self::Index {
                version,
                level,
                children,
            } => {
                require_version(*version)?;
                validate_tree_level(*level)?;
                if children.len() < 2 || children.len() > MAX_DIRECTORY_PAGE_ENTRIES {
                    return Err(FormatError::InvalidDirectoryIndex);
                }
                validate_strict_names(children.iter().map(|child| child.first_name.as_slice()))
                    .map_err(|_| FormatError::InvalidDirectoryIndex)?;
            }
        }
        validate_canonical_size(self)
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>, FormatError> {
        self.validate()?;
        encode(self)
    }

    pub fn id(&self) -> Result<ObjectId, FormatError> {
        Ok(hash_domain(DIRECTORY_PAGE_DOMAIN, &self.canonical_bytes()?))
    }
}

impl DirectoryEntry {
    fn validate(&self) -> Result<(), FormatError> {
        validate_name(&self.name)?;
        if self.metadata.mode & !ENTRY_MODE_MASK != 0 {
            return Err(FormatError::InvalidMode(self.metadata.mode));
        }
        validate_xattrs(&self.metadata.xattrs)?;
        match &self.data {
            EntryData::File { size, content, .. } => {
                let actual = content.logical_len();
                match content {
                    FileContent::Inline(bytes) if bytes.len() > MAX_INLINE_BYTES => {
                        return Err(FormatError::InlineContentTooLarge(bytes.len()));
                    }
                    FileContent::Segment(slice) => slice.validate()?,
                    FileContent::Recipe { logical_len: 0, .. } => {
                        return Err(FormatError::InvalidRecipeParts(0));
                    }
                    _ => {}
                }
                if *size != actual {
                    return Err(FormatError::FileSizeMismatch {
                        declared: *size,
                        actual,
                    });
                }
                Ok(())
            }
            EntryData::Directory { .. } => Ok(()),
            EntryData::Symlink { target } if target.is_empty() || target.contains(&0) => {
                Err(FormatError::InvalidSymlinkTarget)
            }
            EntryData::Symlink { .. } => Ok(()),
        }
    }
}

fn validate_name(name: &[u8]) -> Result<(), FormatError> {
    if name.is_empty() || name == b"." || name == b".." || name.contains(&0) || name.contains(&b'/')
    {
        Err(FormatError::InvalidName)
    } else {
        Ok(())
    }
}

fn validate_tree_level(level: u8) -> Result<(), FormatError> {
    if level == 0 || level > MAX_TREE_DEPTH {
        Err(FormatError::InvalidTreeLevel(level))
    } else {
        Ok(())
    }
}

fn validate_strict_names<'a>(names: impl Iterator<Item = &'a [u8]>) -> Result<(), FormatError> {
    let mut previous: Option<&[u8]> = None;
    for name in names {
        validate_name(name)?;
        if previous.is_some_and(|p| p >= name) {
            return Err(FormatError::UnsortedDirectoryEntries);
        }
        previous = Some(name);
    }
    Ok(())
}

fn validate_xattrs(xattrs: &[Xattr]) -> Result<(), FormatError> {
    let mut previous: Option<&[u8]> = None;
    for xattr in xattrs {
        if xattr.name.is_empty()
            || xattr.name.contains(&0)
            || previous.is_some_and(|p| p >= xattr.name.as_slice())
        {
            return Err(FormatError::InvalidXattrs);
        }
        previous = Some(&xattr.name);
    }
    Ok(())
}

fn checked_sum(declared: u64, current: u64, next: u64) -> Result<u64, FormatError> {
    current
        .checked_add(next)
        .ok_or(FormatError::RecipeSizeMismatch {
            declared,
            actual: u64::MAX,
        })
}

fn require_version(version: u16) -> Result<(), FormatError> {
    if version == FORMAT_VERSION {
        Ok(())
    } else {
        Err(FormatError::UnsupportedVersion(version))
    }
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, FormatError> {
    let bytes =
        postcard::to_allocvec(value).map_err(|error| FormatError::Encode(error.to_string()))?;
    if bytes.len() > MAX_CANONICAL_OBJECT_BYTES {
        return Err(FormatError::CanonicalObjectTooLarge {
            actual: bytes.len(),
        });
    }
    Ok(bytes)
}

fn validate_canonical_size<T: Serialize + ?Sized>(value: &T) -> Result<(), FormatError> {
    let actual = postcard::experimental::serialized_size(value)
        .map_err(|error| FormatError::Encode(error.to_string()))?;
    if actual > MAX_CANONICAL_OBJECT_BYTES {
        Err(FormatError::CanonicalObjectTooLarge { actual })
    } else {
        Ok(())
    }
}

fn hash_domain(domain: &[u8], payload: &[u8]) -> ObjectId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    hasher.update(payload);
    ObjectId::from_array(*hasher.finalize().as_bytes())
}

#[derive(Clone, Debug)]
pub enum NativePushEvent {
    Scanned {
        files: usize,
        directories: usize,
        logical_bytes: u64,
        stored_bytes: u64,
        segments: usize,
    },
    Negotiated {
        missing_segments: usize,
        total_segments: usize,
        transport: String,
    },
    Uploaded {
        segments: usize,
        stored_bytes: u64,
    },
    Verifying {
        snapshot_id: String,
    },
    Published {
        snapshot_id: String,
    },
}

pub type NativePushProgress = Arc<dyn Fn(NativePushEvent) + Send + Sync>;

#[derive(Clone, Default)]
pub struct NativePushOptions {
    pub message: String,
    pub expected_snapshot_id: Option<String>,
    /// Advance and promote this durable native workspace instead of publishing head directly.
    pub workspace_id: Option<String>,
    pub operation_id: Option<String>,
    pub progress: Option<NativePushProgress>,
}

/// One local entry to insert or replace in an incremental native snapshot. `path` is the
/// slash-separated filesystem path; `source` is read exactly once only when it is a file.
#[derive(Clone, Debug)]
pub struct NativeLocalUpsert {
    pub path: String,
    pub source: PathBuf,
}

/// Move an existing immutable subtree by reference. No descendant bytes or metadata are read.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeRename {
    pub from: String,
    pub to: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NativePreparedChangeSet {
    upserts: NativePreparedUpserts,
    deletes: Vec<String>,
    renames: Vec<NativeRename>,
    preparation_ms: u64,
    total_segments: usize,
    uploaded_segments: usize,
    uploaded_bytes: u64,
}

/// Fully verified immutable snapshot prepared for one journal watermark. No local path, content
/// upload, Merkle rebuild, or declaration verification remains; publishing this candidate is a
/// single workspace/head operation guarded by `base_snapshot_id`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NativePreparedSnapshotCandidate {
    root_id: String,
    pub base_snapshot_id: Option<String>,
    changes: NativeSnapshotChanges,
    pub preparation_operation_id: String,
    pub preparation_ms: u64,
    pub files: usize,
    pub directories: usize,
    pub logical_bytes: u64,
    pub stored_bytes: u64,
    pub total_segments: usize,
    pub uploaded_segments: usize,
    pub uploaded_bytes: u64,
    /// Identities observed by the unavoidable cold walk. Kept client-side so a durable direct
    /// push can seed its reconciliation index without performing a second metadata walk.
    #[serde(default)]
    source_observations: Vec<NativeSourceObservation>,
    /// Mounted deltas retain immutable entry/recipe declarations so a CAS loser can recompose
    /// the exact change set onto the serialized winner without rereading local file bytes.
    #[serde(default)]
    prepared_changes: Option<Box<NativePreparedChangeSet>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NativeSourceObservation {
    pub path: String,
    pub device: u64,
    pub inode: u64,
    pub size: u64,
    pub mtime_secs: i64,
    pub mtime_nanos: i64,
    pub ctime_secs: i64,
    pub ctime_nanos: i64,
    pub mode: u32,
    pub observed_at_secs: i64,
    pub observed_at_nanos: i64,
}

impl NativePreparedSnapshotCandidate {
    pub fn root_id(&self) -> &str {
        &self.root_id
    }

    pub fn files(&self) -> usize {
        self.files
    }

    pub fn is_rebaseable(&self) -> bool {
        self.prepared_changes.is_some()
    }

    pub fn directories(&self) -> usize {
        self.directories
    }

    pub fn logical_bytes(&self) -> u64 {
        self.logical_bytes
    }

    pub fn stored_bytes(&self) -> u64 {
        self.stored_bytes
    }

    pub fn total_segments(&self) -> usize {
        self.total_segments
    }

    pub fn uploaded_segments(&self) -> usize {
        self.uploaded_segments
    }

    pub fn uploaded_bytes(&self) -> u64 {
        self.uploaded_bytes
    }

    pub fn source_observations(&self) -> &[NativeSourceObservation] {
        &self.source_observations
    }
}

impl NativePreparedChangeSet {
    pub fn paths(&self) -> impl Iterator<Item = &str> {
        self.upserts
            .paths()
            .chain(self.deletes.iter().map(String::as_str))
            .chain(
                self.renames
                    .iter()
                    .flat_map(|rename| [rename.from.as_str(), rename.to.as_str()]),
            )
    }

    pub fn files(&self) -> usize {
        self.upserts.files()
    }

    pub fn logical_bytes(&self) -> u64 {
        self.upserts.logical_bytes()
    }
}

/// A mounted-save delta relative to `NativePushOptions::expected_snapshot_id`.
#[derive(Clone, Debug, Default)]
pub struct NativeChangeSet {
    pub upserts: Vec<NativeLocalUpsert>,
    pub deletes: Vec<String>,
    pub renames: Vec<NativeRename>,
}

/// Content-prepared mounted upserts. File bytes have already been read once, compressed into
/// aggregate segments, and uploaded; this durable value contains only immutable references and
/// metadata, so a later seal can publish it without reopening the source paths.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NativePreparedUpserts {
    entries: BTreeMap<String, DirectoryEntry>,
    local_directories: BTreeSet<String>,
    recipes: Vec<ChunkRecipe>,
    files: usize,
    directories: usize,
    logical_bytes: u64,
    stored_bytes: u64,
}

impl NativePreparedUpserts {
    pub fn paths(&self) -> impl Iterator<Item = &str> {
        self.entries.keys().map(String::as_str)
    }

    pub fn files(&self) -> usize {
        self.files
    }

    pub fn logical_bytes(&self) -> u64 {
        self.logical_bytes
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativePushReport {
    pub operation_id: String,
    pub snapshot_id: String,
    pub previous_snapshot_id: Option<String>,
    pub files: usize,
    pub directories: usize,
    pub logical_bytes: u64,
    pub stored_bytes: u64,
    pub total_segments: usize,
    pub uploaded_segments: usize,
    pub uploaded_bytes: u64,
    pub transport: String,
    pub client_timings: NativePushTimings,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NativeCandidatePublishOutcome {
    Published(NativePushReport),
    Conflict {
        operation_id: String,
        snapshot_id: String,
        actual_snapshot_id: Option<String>,
    },
}

/// Client-observed timings for one save. Milestones are offsets from operation start because scan,
/// compression, metadata staging, and direct uploads overlap and therefore do not add up linearly.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct NativePushTimings {
    pub total_ms: u64,
    pub walk_ms: Option<u64>,
    pub metadata_scan_ms: Option<u64>,
    pub duplicate_hash_ms: Option<u64>,
    pub content_prepare_ms: Option<u64>,
    pub metadata_build_ms: Option<u64>,
    pub first_segment_ready_ms: Option<u64>,
    pub scan_complete_ms: Option<u64>,
    pub upload_complete_ms: Option<u64>,
    pub metadata_complete_ms: Option<u64>,
    pub verification_complete_ms: Option<u64>,
    pub publish_complete_ms: Option<u64>,
    pub producer_blocked_ms: u64,
}

#[derive(Clone, Debug, Default)]
struct NativePrepareTimings {
    walk_ms: Option<u64>,
    metadata_scan_ms: Option<u64>,
    duplicate_hash_ms: Option<u64>,
    content_prepare_ms: Option<u64>,
    metadata_build_ms: Option<u64>,
}

struct NativePipelineMeasurements {
    operation_started: std::time::Instant,
    first_segment_ready_ms: std::sync::atomic::AtomicU64,
    producer_blocked_ns: std::sync::atomic::AtomicU64,
}

impl NativePipelineMeasurements {
    fn new(operation_started: std::time::Instant) -> Self {
        Self {
            operation_started,
            first_segment_ready_ms: std::sync::atomic::AtomicU64::new(u64::MAX),
            producer_blocked_ns: std::sync::atomic::AtomicU64::new(0),
        }
    }

    fn note_segment_ready(&self) {
        use std::sync::atomic::Ordering;
        let elapsed = self.operation_started.elapsed().as_millis() as u64;
        let _ = self.first_segment_ready_ms.compare_exchange(
            u64::MAX,
            elapsed,
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
    }

    fn note_blocked(&self, elapsed: Duration) {
        use std::sync::atomic::Ordering;
        self.producer_blocked_ns.fetch_add(
            elapsed.as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
    }

    fn first_segment_ready_ms(&self) -> Option<u64> {
        use std::sync::atomic::Ordering;
        match self.first_segment_ready_ms.load(Ordering::Relaxed) {
            u64::MAX => None,
            value => Some(value),
        }
    }

    fn producer_blocked_ms(&self) -> u64 {
        use std::sync::atomic::Ordering;
        self.producer_blocked_ns.load(Ordering::Relaxed) / 1_000_000
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum NativeClientOperationPhase {
    Preparing,
    Uploading,
    Verifying,
    Publishing,
    Completed,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum NativeClientOperationState {
    Running,
    Succeeded,
    Failed,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum NativeClientOperationMode {
    ColdDirectory,
    MountedDelta,
}

#[derive(Clone, Debug, Default, Serialize)]
struct NativeClientOperationMetrics {
    total_ms: u64,
    walk_ms: Option<u64>,
    metadata_scan_ms: Option<u64>,
    duplicate_hash_ms: Option<u64>,
    content_prepare_ms: Option<u64>,
    metadata_build_ms: Option<u64>,
    first_segment_ready_ms: Option<u64>,
    scan_complete_ms: Option<u64>,
    upload_complete_ms: Option<u64>,
    metadata_complete_ms: Option<u64>,
    verification_complete_ms: Option<u64>,
    publish_complete_ms: Option<u64>,
    producer_blocked_ms: u64,
    files: u64,
    directories: u64,
    logical_bytes: u64,
    stored_bytes: u64,
    uploaded_bytes: u64,
    total_segments: u64,
    uploaded_segments: u64,
}

impl NativeClientOperationMetrics {
    fn timings(&self) -> NativePushTimings {
        NativePushTimings {
            total_ms: self.total_ms,
            walk_ms: self.walk_ms,
            metadata_scan_ms: self.metadata_scan_ms,
            duplicate_hash_ms: self.duplicate_hash_ms,
            content_prepare_ms: self.content_prepare_ms,
            metadata_build_ms: self.metadata_build_ms,
            first_segment_ready_ms: self.first_segment_ready_ms,
            scan_complete_ms: self.scan_complete_ms,
            upload_complete_ms: self.upload_complete_ms,
            metadata_complete_ms: self.metadata_complete_ms,
            verification_complete_ms: self.verification_complete_ms,
            publish_complete_ms: self.publish_complete_ms,
            producer_blocked_ms: self.producer_blocked_ms,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct NativeClientOperationRequest {
    format_ver: u16,
    phase: NativeClientOperationPhase,
    state: NativeClientOperationState,
    mode: NativeClientOperationMode,
    client_version: &'static str,
    client_os: &'static str,
    client_arch: &'static str,
    upload_session: Option<String>,
    transport: Option<String>,
    failure_code: Option<String>,
    metrics: NativeClientOperationMetrics,
}

#[derive(Clone)]
struct NativeClientOperationReporter {
    client: ArtifactStorageClient,
    project_id: String,
    repo: String,
    username: String,
    token: String,
    operation_id: String,
    mode: NativeClientOperationMode,
    started: std::time::Instant,
    current: Arc<std::sync::Mutex<NativeClientOperationCurrent>>,
}

#[derive(Clone)]
struct NativeClientOperationCurrent {
    phase: NativeClientOperationPhase,
    upload_session: Option<String>,
    transport: Option<String>,
    metrics: NativeClientOperationMetrics,
}

impl NativeClientOperationReporter {
    #[allow(clippy::too_many_arguments)]
    fn new(
        client: &ArtifactStorageClient,
        project_id: &str,
        repo: &str,
        username: &str,
        token: &str,
        operation_id: String,
        mode: NativeClientOperationMode,
    ) -> Self {
        Self {
            client: client.clone(),
            project_id: project_id.to_string(),
            repo: repo.to_string(),
            username: username.to_string(),
            token: token.to_string(),
            operation_id,
            mode,
            started: std::time::Instant::now(),
            current: Arc::new(std::sync::Mutex::new(NativeClientOperationCurrent {
                phase: NativeClientOperationPhase::Preparing,
                upload_session: None,
                transport: None,
                metrics: NativeClientOperationMetrics::default(),
            })),
        }
    }

    fn elapsed_ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }

    fn update(&self, f: impl FnOnce(&mut NativeClientOperationCurrent)) {
        let mut current = self.current.lock().expect("native telemetry lock");
        f(&mut current);
        current.metrics.total_ms = self.elapsed_ms();
    }

    fn set_session(&self, session: &NativeUploadSession) {
        self.update(|current| {
            current.upload_session = Some(session.session_id.clone());
            current.transport = Some(session.transport.clone());
        });
    }

    fn note_prepared(
        &self,
        prepared: &PreparedNativeSnapshot,
        measurements: &NativePipelineMeasurements,
    ) {
        self.update(|current| {
            current.metrics.walk_ms = prepared.prepare_timings.walk_ms;
            current.metrics.metadata_scan_ms = prepared.prepare_timings.metadata_scan_ms;
            current.metrics.duplicate_hash_ms = prepared.prepare_timings.duplicate_hash_ms;
            current.metrics.content_prepare_ms = prepared.prepare_timings.content_prepare_ms;
            current.metrics.metadata_build_ms = prepared.prepare_timings.metadata_build_ms;
            current.metrics.first_segment_ready_ms = measurements.first_segment_ready_ms();
            current.metrics.scan_complete_ms = Some(self.elapsed_ms());
            current.metrics.producer_blocked_ms = measurements.producer_blocked_ms();
            current.metrics.files = prepared.files as u64;
            current.metrics.directories = prepared.directories as u64;
            current.metrics.logical_bytes = prepared.logical_bytes;
            current.metrics.stored_bytes = prepared.stored_bytes;
            current.metrics.total_segments = prepared.segments.len() as u64;
        });
    }

    fn note_prepared_changes(&self, prepared: &PreparedLocalUpserts) {
        self.update(|current| {
            let elapsed = self.elapsed_ms();
            current.metrics.content_prepare_ms = Some(elapsed);
            current.metrics.scan_complete_ms = Some(elapsed);
            current.metrics.files = prepared.files as u64;
            current.metrics.directories = prepared.directories as u64;
            current.metrics.logical_bytes = prepared.logical_bytes;
            current.metrics.stored_bytes = prepared.stored_bytes;
            current.metrics.total_segments = prepared.segments.len() as u64;
        });
    }

    fn note_upload_complete(&self, uploaded_segments: usize, uploaded_bytes: u64) {
        self.update(|current| {
            current.metrics.upload_complete_ms = Some(self.elapsed_ms());
            current.metrics.uploaded_segments = uploaded_segments as u64;
            current.metrics.uploaded_bytes = uploaded_bytes;
        });
    }

    fn note_metadata_complete(&self) {
        self.update(|current| {
            current.metrics.metadata_complete_ms = Some(self.elapsed_ms());
        });
    }

    fn set_phase(&self, phase: NativeClientOperationPhase) {
        self.update(|current| current.phase = phase);
        self.spawn_report(NativeClientOperationState::Running, None);
    }

    fn note_verification_complete(&self) {
        self.update(|current| {
            current.metrics.verification_complete_ms = Some(self.elapsed_ms());
        });
    }

    fn note_publish_complete(&self) {
        self.update(|current| {
            current.phase = NativeClientOperationPhase::Completed;
            current.metrics.publish_complete_ms = Some(self.elapsed_ms());
        });
    }

    fn note_preparation_complete(&self) {
        self.update(|current| {
            current.phase = NativeClientOperationPhase::Completed;
        });
    }

    fn request(
        &self,
        state: NativeClientOperationState,
        failure_code: Option<String>,
    ) -> NativeClientOperationRequest {
        let current = self.current.lock().expect("native telemetry lock").clone();
        NativeClientOperationRequest {
            format_ver: 1,
            phase: current.phase,
            state,
            mode: self.mode,
            client_version: env!("CARGO_PKG_VERSION"),
            client_os: std::env::consts::OS,
            client_arch: std::env::consts::ARCH,
            upload_session: current.upload_session,
            transport: current.transport,
            failure_code,
            metrics: current.metrics,
        }
    }

    fn spawn_report(&self, state: NativeClientOperationState, failure_code: Option<String>) {
        let reporter = self.clone();
        let request = self.request(state, failure_code);
        tokio::spawn(async move {
            let _ = tokio::time::timeout(Duration::from_millis(750), reporter.send(request)).await;
        });
    }

    async fn send(&self, request: NativeClientOperationRequest) -> Result<(), SdkError> {
        let suffix = format!("fs/client-operations/{}", self.operation_id);
        let (builder, _) = self.client.git_request(
            Method::PUT,
            &self.project_id,
            &self.repo,
            Some(&suffix),
            &self.username,
            &self.token,
        )?;
        let response = builder.json(&request).send().await?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(client_error(format!(
                "native client telemetry returned HTTP {}",
                response.status()
            )))
        }
    }

    async fn send_terminal(&self, state: NativeClientOperationState, failure_code: Option<String>) {
        self.update(|_| {});
        let request = self.request(state, failure_code);
        let _ = tokio::time::timeout(Duration::from_millis(750), self.send(request)).await;
    }

    fn timings(&self) -> NativePushTimings {
        self.current
            .lock()
            .expect("native telemetry lock")
            .metrics
            .timings()
    }
}

fn native_client_failure_code(error: &SdkError) -> String {
    match error {
        SdkError::Authentication(_) => "authentication",
        SdkError::Authorization(_) => "authorization",
        SdkError::Http(_) | SdkError::Middleware(_) => "network",
        SdkError::Io(_) => "local_io",
        SdkError::ServerError { status, .. } if status.is_server_error() => "server_5xx",
        SdkError::ServerError { status, .. } if *status == StatusCode::CONFLICT => "conflict",
        SdkError::ServerError { .. } => "server_4xx",
        SdkError::ClientError(message) if message.contains("filesystem head changed") => "conflict",
        SdkError::ClientError(message)
            if message.contains("changed") && message.contains("retry") =>
        {
            "source_changed"
        }
        SdkError::ClientError(_) => "client",
        _ => "other",
    }
    .to_string()
}

struct BuiltSegment {
    id: ObjectId,
    len: u64,
    records: Vec<SegmentSlice>,
    // Pipelined cold snapshots move the in-memory body to the uploader immediately and retain only
    // this segment's immutable identity. Non-pipelined delta preparation keeps its existing
    // tempfile fallback so a large change set does not accumulate all segment bodies in memory.
    temp: Option<tempfile::NamedTempFile>,
}

#[derive(Clone)]
struct PreparedSegmentUpload {
    id: ObjectId,
    len: u64,
    records: Vec<SegmentSlice>,
    body: PreparedSegmentBody,
}

#[derive(Clone)]
enum PreparedSegmentBody {
    Memory(bytes::Bytes),
    File(PathBuf),
}

struct PipelinedSegmentReport {
    uploaded_ids: HashSet<ObjectId>,
    uploaded_bytes: u64,
}

#[derive(Clone)]
struct PendingSlice {
    segment_index: usize,
    offset: u64,
    stored_len: u32,
    logical_len: u64,
    content_id: ObjectId,
}

#[derive(Clone)]
struct PendingRecipeNode {
    level: u8,
    part_range: Range<usize>,
    children: Vec<usize>,
    logical_len: u64,
    content_id: ObjectId,
}

#[derive(Clone)]
struct PendingRecipe {
    parts: Vec<PendingSlice>,
    nodes: Vec<PendingRecipeNode>,
    root: usize,
}

struct RecipeHashNode {
    level: u8,
    part_range: Range<usize>,
    children: Vec<usize>,
    hasher: ObjectIdHasher,
    logical_len: u64,
}

struct RecipeHashPlan {
    nodes: Vec<RecipeHashNode>,
    paths: Vec<Vec<usize>>,
    root: usize,
}

#[derive(Clone)]
enum PendingContent {
    Inline(Vec<u8>),
    Segments {
        logical_len: u64,
        content_id: ObjectId,
        recipe: PendingRecipe,
    },
}

enum ScannedData {
    File {
        size: u64,
        content: PendingContent,
        hardlink_group: Option<[u8; 16]>,
    },
    Directory,
    Symlink {
        target: Vec<u8>,
    },
}

struct ScannedEntry {
    rel: PathBuf,
    metadata: EntryMetadata,
    data: ScannedData,
}

enum PreScannedData {
    File { hardlink_group: Option<[u8; 16]> },
    Directory,
    Symlink { target: Vec<u8> },
}

struct PreScannedEntry {
    rel: PathBuf,
    source: PathBuf,
    before: std::fs::Metadata,
    metadata: EntryMetadata,
    data: PreScannedData,
}

struct ParallelFileTask {
    entry_index: usize,
    source: PathBuf,
    before: std::fs::Metadata,
    expected_content: Option<ObjectId>,
}

struct ParallelFileGroup {
    files: Vec<(usize, PendingContent, u64)>,
    segments: Vec<BuiltSegment>,
}

struct PreparedNativeSnapshot {
    root: ObjectId,
    pages: Vec<DirectoryPage>,
    recipes: Vec<ChunkRecipe>,
    segments: Vec<BuiltSegment>,
    files: usize,
    directories: usize,
    logical_bytes: u64,
    stored_bytes: u64,
    prepare_timings: NativePrepareTimings,
    changes: NativeSnapshotChanges,
    source_observations: Vec<NativeSourceObservation>,
}

struct PreparedLocalUpserts {
    entries: BTreeMap<String, DirectoryEntry>,
    local_directories: BTreeSet<String>,
    recipes: Vec<ChunkRecipe>,
    segments: Vec<BuiltSegment>,
    files: usize,
    directories: usize,
    logical_bytes: u64,
    stored_bytes: u64,
}

struct SegmentBuilder {
    target_bytes: u64,
    max_bytes: u64,
    current: Option<OpenSegment>,
    complete: Vec<BuiltSegment>,
    completed_segments: Option<tokio::sync::mpsc::Sender<Vec<PreparedSegmentUpload>>>,
    measurements: Option<Arc<NativePipelineMeasurements>>,
    cancellation: Option<Arc<AtomicBool>>,
    staging_directory: Option<PathBuf>,
}

struct OpenSegment {
    bytes: Vec<u8>,
    hasher: SegmentIdHasher,
    len: u64,
    records: Vec<SegmentSlice>,
}

impl SegmentBuilder {
    fn new(target_bytes: u64, max_bytes: u64) -> Result<Self, SdkError> {
        if target_bytes == 0 || max_bytes == 0 || target_bytes > max_bytes {
            return Err(client_error(
                "server returned invalid native segment limits",
            ));
        }
        Ok(Self {
            target_bytes: target_bytes.min(NATIVE_MEMORY_SEGMENT_TARGET_BYTES),
            max_bytes: max_bytes.min(MAX_SEGMENT_BYTES),
            current: None,
            complete: Vec::new(),
            completed_segments: None,
            measurements: None,
            cancellation: None,
            staging_directory: None,
        })
    }

    fn with_completed_segments(
        mut self,
        completed_segments: Option<tokio::sync::mpsc::Sender<Vec<PreparedSegmentUpload>>>,
    ) -> Self {
        self.completed_segments = completed_segments;
        self
    }

    fn with_measurements(mut self, measurements: Option<Arc<NativePipelineMeasurements>>) -> Self {
        self.measurements = measurements;
        self
    }

    fn with_cancellation(mut self, cancellation: Option<Arc<AtomicBool>>) -> Self {
        self.cancellation = cancellation;
        self
    }

    fn with_staging_directory(mut self, staging_directory: Option<PathBuf>) -> Self {
        self.staging_directory = staging_directory;
        self
    }

    fn check_cancelled(&self) -> Result<(), SdkError> {
        if self
            .cancellation
            .as_ref()
            .is_some_and(|cancelled| cancelled.load(Ordering::Relaxed))
        {
            Err(client_error("native snapshot upload was cancelled"))
        } else {
            Ok(())
        }
    }

    fn append_record(
        &mut self,
        logical: &[u8],
        content_id: ObjectId,
    ) -> Result<PendingSlice, SdkError> {
        self.check_cancelled()?;
        let stored = zstd::stream::encode_all(logical, NATIVE_SEGMENT_ZSTD_LEVEL)?;
        let stored_len = u32::try_from(stored.len())
            .map_err(|_| client_error("compressed native record exceeds u32"))?;
        if stored.is_empty() || stored.len() as u64 > self.max_bytes {
            return Err(client_error(format!(
                "compressed native record is {} bytes; server maximum is {}",
                stored.len(),
                self.max_bytes
            )));
        }
        if self.current.as_ref().is_some_and(|current| {
            current.records.len() >= NATIVE_RECORDS_PER_SEGMENT
                || (current.len > 0 && current.len + stored.len() as u64 > self.target_bytes)
        }) {
            self.finish_current()?;
        }
        let current = self.current.get_or_insert(OpenSegment {
            bytes: Vec::with_capacity(self.target_bytes as usize),
            hasher: ObjectId::segment_hasher(),
            len: 0,
            records: Vec::new(),
        });
        if current.len + stored.len() as u64 > self.max_bytes {
            return Err(client_error(format!(
                "native aggregate segment would exceed {} bytes",
                self.max_bytes
            )));
        }
        let offset = current.len;
        current.bytes.extend_from_slice(&stored);
        current.hasher.update(&stored);
        current.len += stored.len() as u64;
        current.records.push(SegmentSlice {
            segment: ObjectId([0; 32]),
            offset,
            stored_len,
            logical_len: logical.len() as u64,
            compression: Compression::Zstd,
            content_id,
        });
        Ok(PendingSlice {
            segment_index: self.complete.len(),
            offset,
            stored_len,
            logical_len: logical.len() as u64,
            content_id,
        })
    }

    fn finish_current(&mut self) -> Result<(), SdkError> {
        let Some(current) = self.current.take() else {
            return Ok(());
        };
        let id = current.hasher.finalize();
        let len = current.len;
        let mut records = current.records;
        for record in &mut records {
            record.segment = id;
        }
        let bytes = bytes::Bytes::from(current.bytes);
        if let Some(measurements) = &self.measurements {
            measurements.note_segment_ready();
        }
        if let Some(sender) = &self.completed_segments {
            // Backpressure bounds the combined scanner/uploader memory even when blob storage is
            // slower than local compression. A dropped receiver means another upload already
            // failed; scanning still returns its own precise result to the async join point.
            let blocked = std::time::Instant::now();
            sender
                .blocking_send(vec![PreparedSegmentUpload {
                    id,
                    len,
                    records: records.clone(),
                    body: PreparedSegmentBody::Memory(bytes),
                }])
                .map_err(|_| client_error("native segment uploader stopped during preparation"))?;
            if let Some(measurements) = &self.measurements {
                measurements.note_blocked(blocked.elapsed());
            }
            self.complete.push(BuiltSegment {
                id,
                len,
                records,
                temp: None,
            });
        } else {
            let mut temp = match &self.staging_directory {
                Some(directory) => {
                    std::fs::create_dir_all(directory)?;
                    tempfile::NamedTempFile::new_in(directory)?
                }
                None => tempfile::NamedTempFile::new()?,
            };
            temp.write_all(&bytes)?;
            temp.flush()?;
            self.complete.push(BuiltSegment {
                id,
                len,
                records,
                temp: Some(temp),
            });
        }
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<BuiltSegment>, SdkError> {
        self.finish_current()?;
        Ok(self.complete)
    }
}

#[cfg(test)]
fn prepare_native_snapshot(
    root: &Path,
    target_segment_bytes: u64,
    max_segment_bytes: u64,
) -> Result<PreparedNativeSnapshot, SdkError> {
    prepare_native_snapshot_with_sender(
        root,
        target_segment_bytes,
        max_segment_bytes,
        None,
        None,
        None,
    )
}

fn prepare_native_snapshot_with_sender(
    root: &Path,
    target_segment_bytes: u64,
    max_segment_bytes: u64,
    completed_segments: Option<tokio::sync::mpsc::Sender<Vec<PreparedSegmentUpload>>>,
    measurements: Option<Arc<NativePipelineMeasurements>>,
    cancellation: Option<Arc<AtomicBool>>,
) -> Result<PreparedNativeSnapshot, SdkError> {
    let walk_started = std::time::Instant::now();
    let observed_at = native_observation_stamp();
    let root = root.canonicalize()?;
    if !root.is_dir() {
        return Err(client_error(format!(
            "native snapshot source {} is not a directory",
            root.display()
        )));
    }
    let mut walker = WalkBuilder::new(&root);
    walker
        .hidden(false)
        .ignore(false)
        .parents(false)
        .require_git(false)
        .git_ignore(true)
        // Native filesystems have no implicit Git exclusions. Only an explicit .gitignore
        // may remove a path from a snapshot.
        .git_exclude(false)
        .git_global(false)
        .follow_links(false)
        .sort_by_file_path(|a, b| a.cmp(b));
    let mut paths = Vec::new();
    for result in walker.build() {
        if cancellation
            .as_ref()
            .is_some_and(|cancelled| cancelled.load(Ordering::Relaxed))
        {
            return Err(client_error("native snapshot upload was cancelled"));
        }
        let entry = result.map_err(|error| client_error(error.to_string()))?;
        let path = entry.path();
        if path == root {
            continue;
        }
        let rel = path
            .strip_prefix(&root)
            .map_err(|_| client_error("snapshot walker escaped its root"))?
            .to_path_buf();
        paths.push((path.to_path_buf(), rel));
    }
    let walk_ms = walk_started.elapsed().as_millis() as u64;

    // Stat/xattr work is syscall-heavy on build trees. Preserve the walker's deterministic order
    // while issuing those independent operations in parallel.
    let metadata_scan_started = std::time::Instant::now();
    let pre_scanned: Vec<PreScannedEntry> = paths
        .into_par_iter()
        .map(|(source, rel)| {
            if cancellation
                .as_ref()
                .is_some_and(|cancelled| cancelled.load(Ordering::Relaxed))
            {
                return Err(client_error("native snapshot upload was cancelled"));
            }
            let before = std::fs::symlink_metadata(&source)?;
            let metadata = native_entry_metadata(&source, &before)?;
            let file_type = before.file_type();
            let data = if file_type.is_dir() {
                PreScannedData::Directory
            } else if file_type.is_symlink() {
                PreScannedData::Symlink {
                    target: raw_path_bytes(&std::fs::read_link(&source)?),
                }
            } else if file_type.is_file() {
                PreScannedData::File {
                    hardlink_group: hardlink_group(&before),
                }
            } else {
                return Err(client_error(format!(
                    "{} is not a regular file, directory, or symlink",
                    source.display()
                )));
            };
            if !file_type.is_file() {
                let after = std::fs::symlink_metadata(&source)?;
                if !same_snapshot_stat(&before, &after) {
                    return Err(client_error(format!(
                        "{} changed while it was being snapshotted; retry",
                        source.display()
                    )));
                }
            }
            Ok(PreScannedEntry {
                rel,
                source,
                before,
                metadata,
                data,
            })
        })
        .collect::<Result<_, SdkError>>()?;
    let metadata_scan_ms = metadata_scan_started.elapsed().as_millis() as u64;

    // Assign each hardlink inode one deterministic primary. Primaries are partitioned into stable
    // byte-balanced groups; every group owns a SegmentBuilder, avoiding locks in compression and
    // temp-file writes while keeping aggregate-object counts small.
    let mut hardlink_primaries = HashMap::<[u8; 16], usize>::new();
    let mut hardlink_aliases = vec![None; pre_scanned.len()];
    let mut file_tasks = Vec::new();
    for (entry_index, entry) in pre_scanned.iter().enumerate() {
        let PreScannedData::File { hardlink_group } = &entry.data else {
            continue;
        };
        if let Some(group) = hardlink_group {
            if let Some(primary) = hardlink_primaries.get(group).copied() {
                if !same_snapshot_stat(&pre_scanned[primary].before, &entry.before) {
                    return Err(client_error(format!(
                        "{} changed through another hardlink while it was being snapshotted; retry",
                        entry.source.display()
                    )));
                }
                hardlink_aliases[entry_index] = Some(primary);
                continue;
            }
            hardlink_primaries.insert(*group, entry_index);
        }
        file_tasks.push(ParallelFileTask {
            entry_index,
            source: entry.source.clone(),
            before: entry.before.clone(),
            expected_content: None,
        });
    }

    // Exact copies are common in compiler caches and language build trees. Hash large files in a
    // cheap parallel pass, select the first path deterministically, and let later paths reference
    // that one immutable recipe. This avoids compressing and uploading identical bytes repeatedly;
    // inline files are already embedded in metadata and do not consume segment bandwidth.
    let duplicate_hash_started = std::time::Instant::now();
    let mut size_frequency = HashMap::<u64, usize>::new();
    for task in &file_tasks {
        *size_frequency.entry(task.before.len()).or_default() += 1;
    }
    let hashed_tasks: Vec<ParallelFileTask> = file_tasks
        .into_par_iter()
        .map(|mut task| {
            if cancellation
                .as_ref()
                .is_some_and(|cancelled| cancelled.load(Ordering::Relaxed))
            {
                return Err(client_error("native snapshot upload was cancelled"));
            }
            if task.before.len() > MAX_INLINE_BYTES as u64
                && size_frequency
                    .get(&task.before.len())
                    .is_some_and(|count| *count > 1)
            {
                task.expected_content = Some(hash_file_once(&task.source, &task.before)?);
            }
            Ok(task)
        })
        .collect::<Result<_, SdkError>>()?;
    let mut content_primaries = HashMap::<(ObjectId, u64), usize>::new();
    let mut content_aliases = vec![None; pre_scanned.len()];
    let mut file_tasks = Vec::with_capacity(hashed_tasks.len());
    for task in hashed_tasks {
        if let Some(content_id) = task.expected_content {
            let key = (content_id, task.before.len());
            if let Some(primary) = content_primaries.get(&key).copied() {
                content_aliases[task.entry_index] = Some(primary);
                continue;
            }
            content_primaries.insert(key, task.entry_index);
        }
        file_tasks.push(task);
    }
    let duplicate_hash_ms = duplicate_hash_started.elapsed().as_millis() as u64;

    let content_prepare_started = std::time::Instant::now();
    // Group boundaries are derived only from stable path order and a fixed byte target. Host CPU
    // count controls scheduling, never aggregate segment identity or cross-machine deduplication.
    let group_target = NATIVE_CONTENT_GROUP_TARGET_BYTES;
    let mut groups: Vec<Vec<ParallelFileTask>> = Vec::new();
    let mut group = Vec::new();
    let mut group_weight = 0u64;
    for task in file_tasks {
        let weight = task.before.len().max(64 * 1024);
        if !group.is_empty() && group_weight.saturating_add(weight) > group_target {
            groups.push(std::mem::take(&mut group));
            group_weight = 0;
        }
        group_weight = group_weight.saturating_add(weight);
        group.push(task);
    }
    if !group.is_empty() {
        groups.push(group);
    }
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(NATIVE_CONTENT_PREPARE_CONCURRENCY)
        .thread_name(|index| format!("native-content-{index}"))
        .build()
        .map_err(|error| client_error(format!("native content worker pool failed: {error}")))?;
    let group_results: Vec<ParallelFileGroup> = pool.install(|| {
        groups
            .into_par_iter()
            .map(|tasks| {
                let mut builder = SegmentBuilder::new(target_segment_bytes, max_segment_bytes)?
                    .with_completed_segments(completed_segments.clone())
                    .with_measurements(measurements.clone())
                    .with_cancellation(cancellation.clone());
                let mut files = Vec::with_capacity(tasks.len());
                for task in tasks {
                    builder.check_cancelled()?;
                    let (content, size) = read_file_once(&task.source, &task.before, &mut builder)?;
                    if let Some(expected) = task.expected_content {
                        let actual = match &content {
                            PendingContent::Segments { content_id, .. } => *content_id,
                            PendingContent::Inline(bytes) => ObjectId::file_content(bytes),
                        };
                        if actual != expected {
                            return Err(client_error(format!(
                                "{} changed after duplicate detection; retry",
                                task.source.display()
                            )));
                        }
                    }
                    let after = std::fs::symlink_metadata(&task.source)?;
                    if !same_snapshot_stat(&task.before, &after) {
                        return Err(client_error(format!(
                            "{} changed while it was being snapshotted; retry",
                            task.source.display()
                        )));
                    }
                    files.push((task.entry_index, content, size));
                }
                let segments = builder.finish()?;
                Ok(ParallelFileGroup { files, segments })
            })
            .collect::<Result<_, SdkError>>()
    })?;
    let content_prepare_ms = content_prepare_started.elapsed().as_millis() as u64;

    let metadata_build_started = std::time::Instant::now();
    let mut file_content: Vec<Option<(PendingContent, u64)>> =
        (0..pre_scanned.len()).map(|_| None).collect();
    let mut segments = Vec::new();
    for result in group_results {
        let segment_base = segments.len();
        for (entry_index, mut content, size) in result.files {
            shift_pending_segment_indexes(&mut content, segment_base);
            file_content[entry_index] = Some((content, size));
        }
        segments.extend(result.segments);
    }
    for (alias, primary) in content_aliases.iter().enumerate() {
        let Some(primary) = primary else { continue };
        let after = std::fs::symlink_metadata(&pre_scanned[alias].source)?;
        if !same_snapshot_stat(&pre_scanned[alias].before, &after) {
            return Err(client_error(format!(
                "{} changed after duplicate detection; retry",
                pre_scanned[alias].source.display()
            )));
        }
        file_content[alias] = file_content[*primary].clone();
    }
    for (alias, primary) in hardlink_aliases.iter().enumerate() {
        let Some(primary) = primary else { continue };
        let after = std::fs::symlink_metadata(&pre_scanned[alias].source)?;
        if !same_snapshot_stat(&pre_scanned[alias].before, &after) {
            return Err(client_error(format!(
                "{} changed while it was being snapshotted; retry",
                pre_scanned[alias].source.display()
            )));
        }
        file_content[alias] = file_content[*primary].clone();
    }

    let mut files = 0usize;
    let mut directories = 1usize;
    let mut logical_bytes = 0u64;
    let source_observations = pre_scanned
        .iter()
        .map(|entry| native_source_observation(&entry.rel, &entry.before, observed_at))
        .collect::<Result<Vec<_>, _>>()?;
    let mut scanned = Vec::with_capacity(pre_scanned.len());
    for (entry_index, entry) in pre_scanned.into_iter().enumerate() {
        let data = match entry.data {
            PreScannedData::Directory => {
                directories += 1;
                ScannedData::Directory
            }
            PreScannedData::Symlink { target } => {
                files += 1;
                logical_bytes = logical_bytes.saturating_add(target.len() as u64);
                ScannedData::Symlink { target }
            }
            PreScannedData::File { hardlink_group } => {
                files += 1;
                let (content, size) = file_content[entry_index]
                    .take()
                    .ok_or_else(|| client_error("parallel native scanner lost file content"))?;
                logical_bytes = logical_bytes.saturating_add(size);
                ScannedData::File {
                    size,
                    content,
                    hardlink_group,
                }
            }
        };
        scanned.push(ScannedEntry {
            rel: entry.rel,
            metadata: entry.metadata,
            data,
        });
    }
    let stored_bytes = segments.iter().map(|segment| segment.len).sum();
    let (root, pages, recipes) = build_metadata(scanned, &segments)?;
    let metadata_build_ms = metadata_build_started.elapsed().as_millis() as u64;
    Ok(PreparedNativeSnapshot {
        root,
        pages,
        recipes,
        segments,
        files,
        directories,
        logical_bytes,
        stored_bytes,
        prepare_timings: NativePrepareTimings {
            walk_ms: Some(walk_ms),
            metadata_scan_ms: Some(metadata_scan_ms),
            duplicate_hash_ms: Some(duplicate_hash_ms),
            content_prepare_ms: Some(content_prepare_ms),
            metadata_build_ms: Some(metadata_build_ms),
        },
        changes: NativeSnapshotChanges::default(),
        source_observations,
    })
}

fn shift_pending_segment_indexes(content: &mut PendingContent, base: usize) {
    if let PendingContent::Segments { recipe, .. } = content {
        for part in &mut recipe.parts {
            part.segment_index += base;
        }
    }
}

fn prepare_local_upserts(
    upserts: Vec<NativeLocalUpsert>,
    target_segment_bytes: u64,
    max_segment_bytes: u64,
    staging_directory: Option<PathBuf>,
) -> Result<PreparedLocalUpserts, SdkError> {
    let mut segment_builder = SegmentBuilder::new(target_segment_bytes, max_segment_bytes)?
        .with_staging_directory(staging_directory);
    let mut scanned = Vec::with_capacity(upserts.len());
    let mut files = 0usize;
    let mut directories = 0usize;
    let mut logical_bytes = 0u64;
    let mut hardlinks: HashMap<[u8; 16], (PendingContent, u64, std::fs::Metadata)> = HashMap::new();
    for upsert in upserts {
        let name = native_path_name(&upsert.path)?.to_vec();
        let before = std::fs::symlink_metadata(&upsert.source)?;
        let metadata = native_entry_metadata(&upsert.source, &before)?;
        let file_type = before.file_type();
        let data = if file_type.is_dir() {
            directories += 1;
            ScannedData::Directory
        } else if file_type.is_symlink() {
            files += 1;
            let target = raw_path_bytes(&std::fs::read_link(&upsert.source)?);
            logical_bytes = logical_bytes.saturating_add(target.len() as u64);
            ScannedData::Symlink { target }
        } else if file_type.is_file() {
            files += 1;
            let hardlink_group = hardlink_group(&before);
            let (content, size) = match hardlink_group.and_then(|group| hardlinks.get(&group)) {
                Some((content, size, cached)) if same_snapshot_stat(cached, &before) => {
                    (content.clone(), *size)
                }
                Some(_) => {
                    return Err(client_error(format!(
                        "{} changed through another hardlink while it was being snapshotted; retry",
                        upsert.source.display()
                    )));
                }
                None => {
                    let content = read_file_once(&upsert.source, &before, &mut segment_builder)?;
                    if let Some(group) = hardlink_group {
                        hardlinks.insert(group, (content.0.clone(), content.1, before.clone()));
                    }
                    content
                }
            };
            logical_bytes = logical_bytes.saturating_add(size);
            ScannedData::File {
                size,
                content,
                hardlink_group,
            }
        } else {
            return Err(client_error(format!(
                "{} is not a regular file, directory, or symlink",
                upsert.source.display()
            )));
        };
        let after = std::fs::symlink_metadata(&upsert.source)?;
        if !same_snapshot_stat(&before, &after) {
            return Err(client_error(format!(
                "{} changed while it was being snapshotted; retry",
                upsert.source.display()
            )));
        }
        scanned.push((upsert.path, name, metadata, data));
    }
    let segments = segment_builder.finish()?;
    let stored_bytes = segments.iter().map(|segment| segment.len).sum();
    let mut recipes = Vec::new();
    let mut entries = BTreeMap::new();
    let mut local_directories = BTreeSet::new();
    for (path, name, metadata, data) in scanned {
        let data = match data {
            ScannedData::Directory => {
                local_directories.insert(path.clone());
                // The composer replaces this with either the reused old root or a canonical
                // empty/newly-built root before the entry can be serialized.
                EntryData::Directory {
                    root: ObjectId::default(),
                }
            }
            ScannedData::Symlink { target } => EntryData::Symlink { target },
            ScannedData::File {
                size,
                content,
                hardlink_group,
            } => EntryData::File {
                size,
                content: resolve_content(content, &segments, &mut recipes)?,
                hardlink_group,
            },
        };
        if entries
            .insert(
                path.clone(),
                DirectoryEntry {
                    name,
                    metadata,
                    data,
                },
            )
            .is_some()
        {
            return Err(client_error(format!(
                "duplicate native upsert path {path:?}"
            )));
        }
    }
    Ok(PreparedLocalUpserts {
        entries,
        local_directories,
        recipes,
        segments,
        files,
        directories,
        logical_bytes,
        stored_bytes,
    })
}

fn read_file_once(
    path: &Path,
    expected: &std::fs::Metadata,
    segments: &mut SegmentBuilder,
) -> Result<(PendingContent, u64), SdkError> {
    let mut file = std::fs::File::open(path)?;
    let opened = file.metadata()?;
    if !same_snapshot_stat(expected, &opened) {
        return Err(client_error(format!(
            "{} changed before it could be read; retry",
            path.display()
        )));
    }

    let mut prefix = Vec::with_capacity(MAX_INLINE_BYTES + 1);
    Read::by_ref(&mut file)
        .take((MAX_INLINE_BYTES + 1) as u64)
        .read_to_end(&mut prefix)?;
    if prefix.len() <= MAX_INLINE_BYTES {
        return Ok((PendingContent::Inline(prefix.clone()), prefix.len() as u64));
    }

    let expected_slices = expected
        .len()
        .max(prefix.len() as u64)
        .div_ceil(NATIVE_RECORD_LOGICAL_BYTES) as usize;
    // A one-slice recipe's leaf, root, record, and whole-file identities are identical. Avoid
    // hashing nearly every ordinary file three times. Multi-slice files still hash each record
    // once plus the recipe nodes needed for range-addressable reads; the recipe root is already
    // the whole-file identity, so a separate whole-file hasher is always redundant.
    let mut recipe_plan = if expected_slices == 1 {
        None
    } else {
        Some(RecipeHashPlan::new(expected_slices)?)
    };
    let mut parts = Vec::with_capacity(expected_slices);
    let mut total_len = 0u64;
    let mut logical = prefix;
    loop {
        let remaining = NATIVE_RECORD_LOGICAL_BYTES as usize - logical.len();
        Read::by_ref(&mut file)
            .take(remaining as u64)
            .read_to_end(&mut logical)?;
        if logical.is_empty() {
            break;
        }
        let part_index = parts.len();
        if let Some(plan) = &mut recipe_plan {
            plan.note(part_index, &logical).map_err(|_| {
                client_error(format!(
                    "{} grew while it was being snapshotted; retry",
                    path.display()
                ))
            })?;
        }
        let record_id = ObjectId::file_content(&logical);
        total_len += logical.len() as u64;
        parts.push(segments.append_record(&logical, record_id)?);
        if logical.len() < NATIVE_RECORD_LOGICAL_BYTES as usize {
            break;
        }
        logical = Vec::with_capacity(NATIVE_RECORD_LOGICAL_BYTES as usize);
    }
    if parts.len() != expected_slices {
        return Err(client_error(format!(
            "{} changed size while it was being snapshotted; retry",
            path.display()
        )));
    }
    let recipe = match recipe_plan {
        Some(plan) => plan.finish(parts),
        None => {
            let content_id = parts[0].content_id;
            PendingRecipe {
                parts,
                nodes: vec![PendingRecipeNode {
                    level: 0,
                    part_range: 0..1,
                    children: Vec::new(),
                    logical_len: total_len,
                    content_id,
                }],
                root: 0,
            }
        }
    };
    let content_id = recipe.nodes[recipe.root].content_id;
    debug_assert_eq!(recipe.nodes[recipe.root].logical_len, total_len);
    Ok((
        PendingContent::Segments {
            logical_len: total_len,
            content_id,
            recipe,
        },
        total_len,
    ))
}

fn hash_file_once(path: &Path, expected: &std::fs::Metadata) -> Result<ObjectId, SdkError> {
    let mut file = std::fs::File::open(path)?;
    let opened = file.metadata()?;
    if !same_snapshot_stat(expected, &opened) {
        return Err(client_error(format!(
            "{} changed before duplicate detection; retry",
            path.display()
        )));
    }
    let mut hasher = ObjectId::file_content_hasher();
    let mut total = 0u64;
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        total = total.saturating_add(read as u64);
    }
    let after = std::fs::symlink_metadata(path)?;
    if total != expected.len() || !same_snapshot_stat(expected, &after) {
        return Err(client_error(format!(
            "{} changed during duplicate detection; retry",
            path.display()
        )));
    }
    Ok(hasher.finalize())
}

impl RecipeHashPlan {
    fn new(parts: usize) -> Result<Self, SdkError> {
        if parts == 0 {
            return Err(client_error(
                "non-inline file has no native segment records",
            ));
        }
        let mut nodes = Vec::new();
        let mut current = Vec::new();
        for range in balanced_ranges(parts, MAX_RECIPE_PAGE_ENTRIES) {
            let index = nodes.len();
            nodes.push(RecipeHashNode {
                level: 0,
                part_range: range,
                children: Vec::new(),
                hasher: ObjectId::file_content_hasher(),
                logical_len: 0,
            });
            current.push(index);
        }
        let mut level = 1u8;
        while current.len() > 1 {
            if level > MAX_TREE_DEPTH {
                return Err(client_error(format!(
                    "file needs a recipe tree deeper than {MAX_TREE_DEPTH} levels"
                )));
            }
            let mut next = Vec::new();
            for group in balanced_ranges(current.len(), MAX_RECIPE_PAGE_ENTRIES) {
                let children = current[group].to_vec();
                let part_range = nodes[*children.first().expect("balanced group")]
                    .part_range
                    .start
                    ..nodes[*children.last().expect("balanced group")]
                        .part_range
                        .end;
                let index = nodes.len();
                nodes.push(RecipeHashNode {
                    level,
                    part_range,
                    children,
                    hasher: ObjectId::file_content_hasher(),
                    logical_len: 0,
                });
                next.push(index);
            }
            current = next;
            level += 1;
        }
        let root = current[0];
        let mut paths = vec![Vec::new(); parts];
        for (node_index, node) in nodes.iter().enumerate() {
            for path in &mut paths[node.part_range.clone()] {
                path.push(node_index);
            }
        }
        Ok(Self { nodes, paths, root })
    }

    fn note(&mut self, part: usize, bytes: &[u8]) -> Result<(), ()> {
        let path = self.paths.get(part).ok_or(())?;
        for node_index in path {
            let node = &mut self.nodes[*node_index];
            node.hasher.update(bytes);
            node.logical_len = node.logical_len.saturating_add(bytes.len() as u64);
        }
        Ok(())
    }

    fn finish(self, parts: Vec<PendingSlice>) -> PendingRecipe {
        let nodes = self
            .nodes
            .into_iter()
            .map(|node| PendingRecipeNode {
                level: node.level,
                part_range: node.part_range,
                children: node.children,
                logical_len: node.logical_len,
                content_id: node.hasher.finalize(),
            })
            .collect();
        PendingRecipe {
            parts,
            nodes,
            root: self.root,
        }
    }
}

fn balanced_ranges(len: usize, maximum: usize) -> Vec<Range<usize>> {
    let groups = len.div_ceil(maximum);
    let base = len / groups;
    let larger = len % groups;
    let mut start = 0usize;
    (0..groups)
        .map(|index| {
            let size = base + usize::from(index < larger);
            let range = start..start + size;
            start = range.end;
            range
        })
        .collect()
}

fn build_metadata(
    scanned: Vec<ScannedEntry>,
    segments: &[BuiltSegment],
) -> Result<(ObjectId, Vec<DirectoryPage>, Vec<ChunkRecipe>), SdkError> {
    let mut directories: BTreeMap<PathBuf, Vec<ScannedEntry>> = BTreeMap::new();
    directories.insert(PathBuf::new(), Vec::new());
    for entry in scanned {
        let parent = entry.rel.parent().unwrap_or_else(|| Path::new(""));
        if !directories.contains_key(parent) {
            return Err(client_error(format!(
                "snapshot walker did not enumerate parent {}",
                parent.display()
            )));
        }
        if matches!(entry.data, ScannedData::Directory) {
            directories.entry(entry.rel.clone()).or_default();
        }
        directories
            .get_mut(parent)
            .expect("parent checked")
            .push(entry);
    }

    let mut levels: BTreeMap<usize, Vec<PathBuf>> = BTreeMap::new();
    for directory in directories.keys() {
        levels
            .entry(directory.components().count())
            .or_default()
            .push(directory.clone());
    }
    let mut roots = BTreeMap::new();
    let mut pages = Vec::new();
    let mut recipes = Vec::new();
    for (_, directories_at_depth) in levels.into_iter().rev() {
        // A directory only references roots from deeper levels, so all directories at one depth
        // are independent. Collecting an indexed parallel iterator retains lexical input order and
        // therefore keeps the staged-object vectors deterministic as well as their content ids.
        let work: Vec<_> = directories_at_depth
            .into_iter()
            .map(|directory| {
                let children = directories.remove(&directory).unwrap_or_default();
                (directory, children)
            })
            .collect();
        let built: Vec<_> = work
            .into_par_iter()
            .map(|(directory, children)| build_one_directory(directory, children, &roots, segments))
            .collect::<Result<_, SdkError>>()?;
        for built_directory in built {
            pages.extend(built_directory.pages);
            recipes.extend(built_directory.recipes);
            roots.insert(built_directory.path, built_directory.root);
        }
    }
    let root = roots
        .remove(&PathBuf::new())
        .ok_or_else(|| client_error("snapshot root was not built"))?;
    Ok((root, pages, recipes))
}

struct BuiltDirectory {
    path: PathBuf,
    root: ObjectId,
    pages: Vec<DirectoryPage>,
    recipes: Vec<ChunkRecipe>,
}

fn build_one_directory(
    directory: PathBuf,
    children: Vec<ScannedEntry>,
    roots: &BTreeMap<PathBuf, ObjectId>,
    segments: &[BuiltSegment],
) -> Result<BuiltDirectory, SdkError> {
    let mut entries = Vec::with_capacity(children.len());
    let mut recipes = Vec::new();
    for child in children {
        let name = child
            .rel
            .file_name()
            .map(raw_os_bytes)
            .ok_or_else(|| client_error("filesystem entry has no filename"))?;
        let data = match child.data {
            ScannedData::Directory => EntryData::Directory {
                root: *roots.get(&child.rel).ok_or_else(|| {
                    client_error(format!(
                        "directory {} was not built bottom-up",
                        child.rel.display()
                    ))
                })?,
            },
            ScannedData::Symlink { target } => EntryData::Symlink { target },
            ScannedData::File {
                size,
                content,
                hardlink_group,
            } => EntryData::File {
                size,
                content: resolve_content(content, segments, &mut recipes)?,
                hardlink_group,
            },
        };
        entries.push(DirectoryEntry {
            name,
            metadata: child.metadata,
            data,
        });
    }
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    let mut pages = Vec::new();
    let root = build_directory_pages(entries, &mut pages)?;
    Ok(BuiltDirectory {
        path: directory,
        root,
        pages,
        recipes,
    })
}

fn resolve_content(
    content: PendingContent,
    segments: &[BuiltSegment],
    recipes: &mut Vec<ChunkRecipe>,
) -> Result<FileContent, SdkError> {
    match content {
        PendingContent::Inline(bytes) => Ok(FileContent::Inline(bytes)),
        PendingContent::Segments {
            logical_len,
            content_id,
            recipe,
        } => {
            let parts: Vec<SegmentSlice> = recipe
                .parts
                .iter()
                .map(|part| {
                    let segment = segments.get(part.segment_index).ok_or_else(|| {
                        client_error("native record references an unknown aggregate segment")
                    })?;
                    Ok(SegmentSlice {
                        segment: segment.id,
                        offset: part.offset,
                        stored_len: part.stored_len,
                        logical_len: part.logical_len,
                        compression: Compression::Zstd,
                        content_id: part.content_id,
                    })
                })
                .collect::<Result<_, SdkError>>()?;
            if parts.len() == 1 {
                return Ok(FileContent::Segment(parts[0].clone()));
            }

            let mut ids = Vec::with_capacity(recipe.nodes.len());
            for node in &recipe.nodes {
                let page = if node.level == 0 {
                    ChunkRecipe::Leaf {
                        version: FORMAT_VERSION,
                        logical_len: node.logical_len,
                        content_id: node.content_id,
                        parts: parts[node.part_range.clone()].to_vec(),
                    }
                } else {
                    ChunkRecipe::Index {
                        version: FORMAT_VERSION,
                        level: node.level,
                        logical_len: node.logical_len,
                        content_id: node.content_id,
                        children: node
                            .children
                            .iter()
                            .map(|child| RecipeChild {
                                recipe: ids[*child],
                                logical_len: recipe.nodes[*child].logical_len,
                            })
                            .collect(),
                    }
                };
                let id = page
                    .id()
                    .map_err(|error| client_error(format!("invalid native recipe: {error}")))?;
                recipes.push(page);
                ids.push(id);
            }
            let root = ids[recipe.root];
            Ok(FileContent::Recipe {
                recipe: root,
                logical_len,
                content_id,
            })
        }
    }
}

fn build_directory_pages(
    entries: Vec<DirectoryEntry>,
    pages: &mut Vec<DirectoryPage>,
) -> Result<ObjectId, SdkError> {
    let mut leaves = Vec::new();
    let mut current = Vec::new();
    for entry in entries {
        current.push(entry);
        let candidate = DirectoryPage::Leaf {
            version: FORMAT_VERSION,
            entries: current.clone(),
        };
        if current.len() > MAX_DIRECTORY_PAGE_ENTRIES
            || matches!(
                candidate.validate(),
                Err(FormatError::CanonicalObjectTooLarge { .. })
            )
        {
            let last = current.pop().expect("entry just pushed");
            if current.is_empty() {
                return Err(client_error(
                    "one filesystem entry exceeds the metadata page limit",
                ));
            }
            leaves.push(stage_directory_page(
                DirectoryPage::Leaf {
                    version: FORMAT_VERSION,
                    entries: std::mem::take(&mut current),
                },
                pages,
            )?);
            current.push(last);
        } else if let Err(error) = candidate.validate() {
            return Err(client_error(format!("invalid filesystem entry: {error}")));
        }
    }
    if !current.is_empty() || leaves.is_empty() {
        leaves.push(stage_directory_page(
            DirectoryPage::Leaf {
                version: FORMAT_VERSION,
                entries: current,
            },
            pages,
        )?);
    }
    if leaves.len() == 1 {
        return Ok(leaves[0].1);
    }

    let mut children: Vec<DirectoryChild> = leaves
        .into_iter()
        .map(|(first_name, page)| DirectoryChild {
            first_name: first_name.expect("non-empty leaf in multi-page directory"),
            page,
        })
        .collect();
    let mut level = 1u8;
    loop {
        let groups = group_directory_children(children)?;
        let mut next = Vec::new();
        for group in groups {
            let first_name = group[0].first_name.clone();
            let page = DirectoryPage::Index {
                version: FORMAT_VERSION,
                level,
                children: group,
            };
            let (_, id) = stage_directory_page(page, pages)?;
            next.push(DirectoryChild {
                first_name,
                page: id,
            });
        }
        if next.len() == 1 {
            return Ok(next[0].page);
        }
        level += 1;
        if level > MAX_TREE_DEPTH {
            return Err(client_error(format!(
                "directory needs an index deeper than {MAX_TREE_DEPTH} levels"
            )));
        }
        children = next;
    }
}

fn group_directory_children(
    children: Vec<DirectoryChild>,
) -> Result<Vec<Vec<DirectoryChild>>, SdkError> {
    let mut groups: Vec<Vec<DirectoryChild>> = Vec::new();
    let mut current = Vec::new();
    for child in children {
        current.push(child);
        let candidate = DirectoryPage::Index {
            version: FORMAT_VERSION,
            level: 1,
            children: current.clone(),
        };
        if current.len() > MAX_DIRECTORY_PAGE_ENTRIES
            || matches!(
                candidate.validate(),
                Err(FormatError::CanonicalObjectTooLarge { .. })
            )
        {
            let last = current.pop().expect("child just pushed");
            if current.len() < 2 {
                return Err(client_error(
                    "directory routing entry exceeds the metadata page limit",
                ));
            }
            groups.push(std::mem::take(&mut current));
            current.push(last);
        }
    }
    if !current.is_empty() {
        groups.push(current);
    }
    if groups.last().is_some_and(|group| group.len() == 1) {
        let lone = groups.pop().expect("last group").pop().expect("lone child");
        let previous = groups
            .last_mut()
            .ok_or_else(|| client_error("directory index cannot contain one child"))?;
        let moved = previous
            .pop()
            .ok_or_else(|| client_error("directory index cannot be rebalanced"))?;
        if previous.len() < 2 {
            return Err(client_error("directory index cannot be rebalanced"));
        }
        groups.push(vec![moved, lone]);
    }
    Ok(groups)
}

fn stage_directory_page(
    page: DirectoryPage,
    pages: &mut Vec<DirectoryPage>,
) -> Result<(Option<Vec<u8>>, ObjectId), SdkError> {
    let first_name = match &page {
        DirectoryPage::Leaf { entries, .. } => entries.first().map(|entry| entry.name.clone()),
        DirectoryPage::Index { children, .. } => {
            children.first().map(|child| child.first_name.clone())
        }
    };
    let id = page
        .id()
        .map_err(|error| client_error(format!("invalid native directory page: {error}")))?;
    pages.push(page);
    Ok((first_name, id))
}

fn native_entry_metadata(
    path: &Path,
    metadata: &std::fs::Metadata,
) -> Result<EntryMetadata, SdkError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let mut xattrs = Vec::new();
        for name in xattr::list(path)? {
            let Some(value) = xattr::get(path, &name)? else {
                continue;
            };
            xattrs.push(Xattr {
                name: raw_os_bytes(&name),
                value,
            });
        }
        xattrs.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(EntryMetadata {
            mode: metadata.mode() & ENTRY_MODE_MASK,
            mtime_ns: metadata
                .mtime()
                .saturating_mul(1_000_000_000)
                .saturating_add(metadata.mtime_nsec()),
            uid: Some(metadata.uid()),
            gid: Some(metadata.gid()),
            xattrs,
        })
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        let mtime_ns = metadata
            .modified()
            .ok()
            .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
            .and_then(|duration| i64::try_from(duration.as_nanos()).ok())
            .unwrap_or_default();
        Ok(EntryMetadata {
            mode: if metadata.permissions().readonly() {
                0o444
            } else {
                0o644
            },
            mtime_ns,
            uid: None,
            gid: None,
            xattrs: Vec::new(),
        })
    }
}

fn native_observation_stamp() -> (i64, i64) {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| (duration.as_secs() as i64, duration.subsec_nanos() as i64))
        .unwrap_or((0, 0))
}

#[cfg(unix)]
fn native_source_observation(
    path: &Path,
    metadata: &std::fs::Metadata,
    observed_at: (i64, i64),
) -> Result<NativeSourceObservation, SdkError> {
    use std::os::unix::fs::MetadataExt;

    Ok(NativeSourceObservation {
        path: path
            .to_str()
            .ok_or_else(|| {
                client_error(format!(
                    "native filesystem paths must be valid UTF-8: {}",
                    path.display()
                ))
            })?
            .to_string(),
        device: metadata.dev(),
        inode: metadata.ino(),
        size: metadata.size(),
        mtime_secs: metadata.mtime(),
        mtime_nanos: metadata.mtime_nsec(),
        ctime_secs: metadata.ctime(),
        ctime_nanos: metadata.ctime_nsec(),
        mode: metadata.mode(),
        observed_at_secs: observed_at.0,
        observed_at_nanos: observed_at.1,
    })
}

#[cfg(not(unix))]
fn native_source_observation(
    path: &Path,
    metadata: &std::fs::Metadata,
    observed_at: (i64, i64),
) -> Result<NativeSourceObservation, SdkError> {
    let modified = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| (duration.as_secs() as i64, duration.subsec_nanos() as i64))
        .unwrap_or((0, 0));
    Ok(NativeSourceObservation {
        path: path
            .to_str()
            .ok_or_else(|| {
                client_error(format!(
                    "native filesystem paths must be valid UTF-8: {}",
                    path.display()
                ))
            })?
            .to_string(),
        device: 0,
        inode: 0,
        size: metadata.len(),
        mtime_secs: modified.0,
        mtime_nanos: modified.1,
        ctime_secs: modified.0,
        ctime_nanos: modified.1,
        mode: if metadata.is_dir() {
            0o040755
        } else {
            0o100644
        },
        observed_at_secs: observed_at.0,
        observed_at_nanos: observed_at.1,
    })
}

#[cfg(unix)]
fn raw_os_bytes(value: &std::ffi::OsStr) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;
    value.as_bytes().to_vec()
}

#[cfg(not(unix))]
fn raw_os_bytes(value: &std::ffi::OsStr) -> Vec<u8> {
    value.to_string_lossy().into_owned().into_bytes()
}

fn raw_path_bytes(value: &Path) -> Vec<u8> {
    raw_os_bytes(value.as_os_str())
}

#[cfg(unix)]
fn hardlink_group(metadata: &std::fs::Metadata) -> Option<[u8; 16]> {
    use std::os::unix::fs::MetadataExt;
    if metadata.nlink() < 2 {
        return None;
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"tensorlake.fs.local-hardlink.v1\0");
    hasher.update(&metadata.dev().to_le_bytes());
    hasher.update(&metadata.ino().to_le_bytes());
    Some(
        hasher.finalize().as_bytes()[..16]
            .try_into()
            .expect("sized"),
    )
}

#[cfg(not(unix))]
fn hardlink_group(_metadata: &std::fs::Metadata) -> Option<[u8; 16]> {
    None
}

#[cfg(unix)]
fn same_snapshot_stat(a: &std::fs::Metadata, b: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    a.dev() == b.dev()
        && a.ino() == b.ino()
        && a.mode() == b.mode()
        && a.len() == b.len()
        && a.mtime() == b.mtime()
        && a.mtime_nsec() == b.mtime_nsec()
        && a.ctime() == b.ctime()
        && a.ctime_nsec() == b.ctime_nsec()
}

#[cfg(not(unix))]
fn same_snapshot_stat(a: &std::fs::Metadata, b: &std::fs::Metadata) -> bool {
    a.len() == b.len()
        && a.permissions().readonly() == b.permissions().readonly()
        && a.modified().ok() == b.modified().ok()
        && a.file_type() == b.file_type()
}

fn client_error(message: impl Into<String>) -> SdkError {
    SdkError::ClientError(message.into())
}

#[derive(Clone, Debug, Deserialize)]
pub struct NativeUploadSession {
    pub session_id: String,
    pub expires_at_ms: u64,
    pub transport: String,
    pub target_segment_bytes: u64,
    pub max_segment_bytes: u64,
    pub max_segments_per_query: usize,
}

#[derive(Clone, Debug, Deserialize)]
pub struct NativeSegmentTarget {
    pub staging_id: String,
    pub url: Option<String>,
    pub checksum_sha256: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct MissingSegmentsRequest<'a> {
    segment_ids: &'a [String],
}

#[derive(Clone, Debug, Deserialize)]
struct MissingSegmentsResponse {
    missing_segment_ids: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
struct SegmentTargetRequest<'a> {
    segment_id: &'a str,
    stored_len: u64,
}

#[derive(Clone, Debug, Serialize)]
struct RegisterSegmentRequest<'a> {
    segment_id: &'a str,
    stored_len: u64,
    records: Vec<NativeSegmentRecordRequest>,
}

#[derive(Clone, Debug, Serialize)]
struct NativeSegmentRecordRequest {
    offset: u64,
    stored_len: u32,
    logical_len: u64,
    compression: &'static str,
    content_id: String,
}

fn native_segment_record_requests(records: &[SegmentSlice]) -> Vec<NativeSegmentRecordRequest> {
    records
        .iter()
        .map(|record| NativeSegmentRecordRequest {
            offset: record.offset,
            stored_len: record.stored_len,
            logical_len: record.logical_len,
            compression: match record.compression {
                Compression::Raw => "raw",
                Compression::Zstd => "zstd",
            },
            content_id: record.content_id.to_hex(),
        })
        .collect()
}

#[derive(Clone, Debug, Serialize)]
struct MetadataRequest<'a> {
    pages: &'a [DirectoryPage],
    recipes: &'a [ChunkRecipe],
}

#[derive(Clone, Debug, Serialize)]
struct PreparedRecipeVerificationRequest {
    recipes: Vec<PreparedRecipeDeclaration>,
}

#[derive(Clone, Debug, Serialize)]
struct PreparedRecipeDeclaration {
    recipe: String,
    logical_len: u64,
    content_id: String,
}

fn prepared_recipe_declarations(
    entries: &BTreeMap<String, DirectoryEntry>,
) -> Vec<PreparedRecipeDeclaration> {
    let mut roots = BTreeMap::new();
    for entry in entries.values() {
        if let EntryData::File {
            content:
                FileContent::Recipe {
                    recipe,
                    logical_len,
                    content_id,
                },
            ..
        } = &entry.data
        {
            roots.entry(*recipe).or_insert((*logical_len, *content_id));
        }
    }
    roots
        .into_iter()
        .map(
            |(recipe, (logical_len, content_id))| PreparedRecipeDeclaration {
                recipe: recipe.to_hex(),
                logical_len,
                content_id: content_id.to_hex(),
            },
        )
        .collect()
}

fn next_metadata_batch(
    pages: &[DirectoryPage],
    recipes: &[ChunkRecipe],
    page_offset: usize,
    recipe_offset: usize,
) -> Result<(usize, usize), SdkError> {
    let mut encoded_bytes = serde_json::to_vec(&MetadataRequest {
        pages: &[],
        recipes: &[],
    })
    .map_err(|error| client_error(format!("cannot encode native metadata request: {error}")))?
    .len();
    let mut object_count = 0usize;
    let mut page_end = page_offset;
    let mut recipe_end = recipe_offset;

    while page_end < pages.len() && object_count < NATIVE_METADATA_REQUEST_MAX_OBJECTS {
        let object_bytes = serde_json::to_vec(&pages[page_end])
            .map_err(|error| client_error(format!("cannot encode native directory page: {error}")))?
            .len();
        let separator = usize::from(page_end > page_offset);
        if object_count > 0
            && encoded_bytes
                .saturating_add(separator)
                .saturating_add(object_bytes)
                > NATIVE_METADATA_REQUEST_TARGET_BYTES
        {
            break;
        }
        encoded_bytes = encoded_bytes
            .saturating_add(separator)
            .saturating_add(object_bytes);
        page_end += 1;
        object_count += 1;
    }

    while recipe_end < recipes.len() && object_count < NATIVE_METADATA_REQUEST_MAX_OBJECTS {
        let object_bytes = serde_json::to_vec(&recipes[recipe_end])
            .map_err(|error| client_error(format!("cannot encode native chunk recipe: {error}")))?
            .len();
        let separator = usize::from(recipe_end > recipe_offset);
        if object_count > 0
            && encoded_bytes
                .saturating_add(separator)
                .saturating_add(object_bytes)
                > NATIVE_METADATA_REQUEST_TARGET_BYTES
        {
            break;
        }
        encoded_bytes = encoded_bytes
            .saturating_add(separator)
            .saturating_add(object_bytes);
        recipe_end += 1;
        object_count += 1;
    }

    Ok((page_end, recipe_end))
}

#[derive(Clone, Debug, Deserialize)]
pub struct NativeMetadataResponse {
    pub pages: Vec<String>,
    pub recipes: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct SubmitNativeSnapshotRequest {
    pub root: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parents: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
    pub operation_id: String,
    #[serde(default, skip_serializing_if = "NativeSnapshotChanges::is_empty")]
    pub changes: NativeSnapshotChanges,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct NativeSnapshotChanges {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub upserts: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deletes: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub renames: Vec<NativeSnapshotRename>,
}

impl NativeSnapshotChanges {
    fn is_empty(&self) -> bool {
        self.upserts.is_empty() && self.deletes.is_empty() && self.renames.is_empty()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NativeSnapshotRename {
    pub from: String,
    pub to: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct SubmitNativeSnapshotResponse {
    pub snapshot_id: String,
    pub verification: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct NativeSnapshotStatus {
    pub state: String,
    #[serde(default)]
    pub at_ms: Option<u64>,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct NativeHead {
    pub snapshot_id: Option<String>,
    pub generation: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NativeHeadEvent {
    pub actor: String,
    pub at_ms: u64,
    pub previous_snapshot_id: Option<String>,
    pub snapshot_id: String,
    pub source_workspace_id: Option<String>,
}

#[derive(Deserialize)]
struct NativeHeadEvents {
    events: Vec<NativeHeadEvent>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct NativeSnapshotHistoryPage {
    pub snapshot_ids: Vec<String>,
    pub next_after: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NativeSnapshotInfo {
    pub snapshot_id: String,
    pub filesystem_id: String,
    pub root: String,
    pub parents: Vec<String>,
    pub created_at_ms: u64,
    pub principal: String,
    pub message: String,
    pub operation_id: String,
    #[serde(default)]
    pub pinned: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NativeSnapshotPinState {
    pub snapshot_id: String,
    pub pinned: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NativeWorkspaceInfo {
    pub workspace_id: String,
    pub principal: String,
    pub base_snapshot_id: Option<String>,
    pub latest_snapshot_id: Option<String>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub expires_at_ms: Option<u64>,
    pub read_only: bool,
}

#[derive(Deserialize)]
struct NativeWorkspaceList {
    workspaces: Vec<NativeWorkspaceInfo>,
    /// Servers predating workspace pagination returned only `workspaces`; treating omission as
    /// end-of-list keeps an upgraded CLI compatible during a rolling server deployment.
    #[serde(default)]
    next_after: Option<String>,
}

#[derive(Deserialize)]
struct NativeTreePage {
    entries: Vec<DirectoryEntry>,
    next_after: Option<String>,
}

#[derive(Serialize)]
struct CreateNativeWorkspaceRequest<'a> {
    snapshot_id: Option<&'a str>,
    read_only: bool,
    ttl_seconds: Option<u64>,
}

#[derive(Serialize)]
struct NativeWorkspaceHeartbeatRequest {
    ttl_seconds: u64,
}

/// Native workspaces are durable enough to survive a disconnected mount, but not immortal. The
/// CLI refreshes this one-day lease every twenty minutes while mounted.
pub const NATIVE_WORKSPACE_TTL_SECONDS: u64 = 24 * 60 * 60;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NativeHeadAdvance {
    Published {
        previous_snapshot_id: Option<String>,
        snapshot_id: String,
    },
    Conflict {
        actual_snapshot_id: Option<String>,
        snapshot_id: String,
    },
}

#[derive(Serialize)]
struct AdvanceNativeHeadRequest<'a> {
    snapshot_id: &'a str,
    expected_snapshot_id: Option<&'a str>,
}

#[derive(Deserialize)]
struct AdvanceNativeHeadResponse {
    published: bool,
    #[serde(default)]
    previous_snapshot_id: Option<String>,
    #[serde(default)]
    actual_snapshot_id: Option<String>,
    snapshot_id: String,
}

impl ArtifactStorageClient {
    /// Scan `root` once, upload aggregate native segments directly to blob storage when the
    /// service offers presigned PUTs, verify the immutable snapshot, and publish it with head CAS.
    pub async fn push_native_directory(
        &self,
        project_id: &str,
        repo: &str,
        root: &Path,
        options: NativePushOptions,
    ) -> Result<NativePushReport, SdkError> {
        let credential = self.git_credential_for_repo(project_id, repo).await?;
        self.push_native_directory_with_credential(
            project_id,
            repo,
            root,
            &credential.git_username,
            &credential.token,
            options,
        )
        .await
    }

    pub async fn push_native_directory_with_credential(
        &self,
        project_id: &str,
        repo: &str,
        root: &Path,
        username: &str,
        token: &str,
        mut options: NativePushOptions,
    ) -> Result<NativePushReport, SdkError> {
        let operation_id = options
            .operation_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        options.operation_id = Some(operation_id.clone());
        let reporter = NativeClientOperationReporter::new(
            self,
            project_id,
            repo,
            username,
            token,
            operation_id,
            NativeClientOperationMode::ColdDirectory,
        );
        let measurements = Arc::new(NativePipelineMeasurements::new(reporter.started));
        reporter.spawn_report(NativeClientOperationState::Running, None);
        let result = self
            .push_native_directory_with_credential_inner(
                project_id,
                repo,
                root,
                username,
                token,
                options,
                &reporter,
                measurements,
            )
            .await;
        match result {
            Ok(mut report) => {
                reporter
                    .send_terminal(NativeClientOperationState::Succeeded, None)
                    .await;
                report.client_timings = reporter.timings();
                Ok(report)
            }
            Err(error) => {
                reporter
                    .send_terminal(
                        NativeClientOperationState::Failed,
                        Some(native_client_failure_code(&error)),
                    )
                    .await;
                Err(error)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn push_native_directory_with_credential_inner(
        &self,
        project_id: &str,
        repo: &str,
        root: &Path,
        username: &str,
        token: &str,
        options: NativePushOptions,
        reporter: &NativeClientOperationReporter,
        measurements: Arc<NativePipelineMeasurements>,
    ) -> Result<NativePushReport, SdkError> {
        let head: NativeHead = {
            let (request, _) = self.git_request(
                Method::GET,
                project_id,
                repo,
                Some("fs/head"),
                username,
                token,
            )?;
            expect_json(request.send().await?).await?
        };
        let expected = options
            .expected_snapshot_id
            .clone()
            .or(head.snapshot_id.clone());
        let expected_workspace = match options.workspace_id.as_deref() {
            Some(workspace_id) => {
                self.native_workspace_with_credential(
                    project_id,
                    repo,
                    workspace_id,
                    username,
                    token,
                )
                .await?
                .latest_snapshot_id
            }
            None => None,
        };
        let session: NativeUploadSession = {
            let (request, _) = self.git_request(
                Method::POST,
                project_id,
                repo,
                Some("fs/uploads"),
                username,
                token,
            )?;
            expect_json(request.send().await?).await?
        };
        reporter.set_session(&session);

        let source = root.to_path_buf();
        let target_segment_bytes = session.target_segment_bytes;
        let max_segment_bytes = session.max_segment_bytes;
        let (segment_sender, segment_receiver) =
            tokio::sync::mpsc::channel(NATIVE_SEGMENT_UPLOAD_QUEUE);
        let cancellation = Arc::new(AtomicBool::new(false));
        let scanner_measurements = measurements.clone();
        let scanner_cancellation = cancellation.clone();
        let mut scanner = tokio::task::spawn_blocking(move || {
            prepare_native_snapshot_with_sender(
                &source,
                target_segment_bytes,
                max_segment_bytes,
                Some(segment_sender),
                Some(scanner_measurements),
                Some(scanner_cancellation),
            )
        });
        let upload_client = self.clone();
        let upload_project = project_id.to_string();
        let upload_repo = repo.to_string();
        let upload_session = session.clone();
        let upload_username = username.to_string();
        let upload_token = token.to_string();
        let upload_cancellation = cancellation.clone();
        let mut uploader = tokio::spawn(async move {
            let result = upload_pipelined_segments(
                &upload_client,
                &upload_project,
                &upload_repo,
                &upload_session,
                &upload_username,
                &upload_token,
                segment_receiver,
            )
            .await;
            upload_cancellation.store(true, Ordering::Relaxed);
            result
        });
        // Surface an upload/auth failure as soon as it happens. The blocking scanner observes the
        // cancellation flag or dropped receiver cooperatively; awaiting it here prevents detached
        // CPU work while preserving the uploader's precise error for the caller.
        let (prepared, early_upload) = tokio::select! {
            scanned = &mut scanner => {
                let prepared = match scanned {
                    Ok(Ok(prepared)) => prepared,
                    Ok(Err(error)) => {
                        uploader.abort();
                        return Err(error);
                    }
                    Err(error) => {
                        uploader.abort();
                        return Err(client_error(format!(
                            "native snapshot scanner failed: {error}"
                        )));
                    }
                };
                (prepared, None)
            }
            uploaded = &mut uploader => {
                match uploaded {
                    Ok(Ok(report)) => {
                        let prepared = scanner.await.map_err(|error| {
                            client_error(format!("native snapshot scanner failed: {error}"))
                        })??;
                        (prepared, Some(report))
                    }
                    Ok(Err(error)) => {
                        cancellation.store(true, Ordering::Relaxed);
                        let _ = scanner.await;
                        return Err(error);
                    }
                    Err(error) => {
                        cancellation.store(true, Ordering::Relaxed);
                        let _ = scanner.await;
                        return Err(client_error(format!(
                            "native upload coordinator failed: {error}"
                        )));
                    }
                }
            }
        };
        reporter.note_prepared(&prepared, &measurements);
        reporter.set_phase(NativeClientOperationPhase::Uploading);
        note_progress(
            &options.progress,
            NativePushEvent::Scanned {
                files: prepared.files,
                directories: prepared.directories,
                logical_bytes: prepared.logical_bytes,
                stored_bytes: prepared.stored_bytes,
                segments: prepared.segments.len(),
            },
        );
        // Metadata is independent of aggregate-object transfer once scanning has produced its
        // canonical ids. Stage it while the final segment PUTs are still draining.
        let metadata = stage_native_metadata_batches(
            self,
            project_id,
            repo,
            &session.session_id,
            username,
            token,
            &prepared.pages,
            &prepared.recipes,
        )
        .await;
        if metadata.is_ok() {
            reporter.note_metadata_complete();
        }
        let pipelined = match early_upload {
            Some(report) => report,
            None => uploader.await.map_err(|error| {
                client_error(format!("native upload coordinator failed: {error}"))
            })??,
        };
        reporter.note_upload_complete(pipelined.uploaded_ids.len(), pipelined.uploaded_bytes);
        metadata?;
        finish_native_push(
            self,
            project_id,
            repo,
            username,
            token,
            expected,
            expected_workspace,
            session,
            prepared,
            Some(pipelined),
            true,
            options,
            Some(reporter),
        )
        .await
    }

    /// Perform the unavoidable first walk of an arbitrary directory, pipeline its aggregate
    /// segments to blob storage, and persist all metadata needed for a later metadata-only
    /// publication. The returned candidate contains no local paths or credentials and can be
    /// durably stored by the CLI's local generation engine.
    pub async fn prepare_native_directory_snapshot_candidate_with_credential(
        &self,
        project_id: &str,
        repo: &str,
        root: &Path,
        username: &str,
        token: &str,
        options: NativePushOptions,
    ) -> Result<NativePreparedSnapshotCandidate, SdkError> {
        let operation_id = uuid::Uuid::new_v4().to_string();
        self.prepare_native_directory_snapshot_candidate_with_operation_id_and_credential(
            project_id,
            repo,
            root,
            username,
            token,
            options,
            operation_id,
        )
        .await
    }

    /// Operation-id explicit form used by the durable local generation engine. The caller must
    /// commit `operation_id` before invoking this method so a crash before the response remints
    /// transport credentials but never creates an ambiguous second preparation identity.
    #[allow(clippy::too_many_arguments)]
    pub async fn prepare_native_directory_snapshot_candidate_with_operation_id_and_credential(
        &self,
        project_id: &str,
        repo: &str,
        root: &Path,
        username: &str,
        token: &str,
        options: NativePushOptions,
        operation_id: String,
    ) -> Result<NativePreparedSnapshotCandidate, SdkError> {
        let base_snapshot_id = options.expected_snapshot_id.clone();
        let reporter = NativeClientOperationReporter::new(
            self,
            project_id,
            repo,
            username,
            token,
            operation_id.clone(),
            NativeClientOperationMode::ColdDirectory,
        );
        let measurements = Arc::new(NativePipelineMeasurements::new(reporter.started));
        reporter.spawn_report(NativeClientOperationState::Running, None);
        let result = self
            .prepare_native_directory_snapshot_candidate_inner(
                project_id,
                repo,
                root,
                username,
                token,
                &options,
                &reporter,
                measurements,
                base_snapshot_id,
                operation_id,
            )
            .await;
        match result {
            Ok(candidate) => {
                reporter.note_preparation_complete();
                reporter
                    .send_terminal(NativeClientOperationState::Succeeded, None)
                    .await;
                Ok(candidate)
            }
            Err(error) => {
                reporter
                    .send_terminal(
                        NativeClientOperationState::Failed,
                        Some(native_client_failure_code(&error)),
                    )
                    .await;
                Err(error)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn prepare_native_directory_snapshot_candidate_inner(
        &self,
        project_id: &str,
        repo: &str,
        root: &Path,
        username: &str,
        token: &str,
        options: &NativePushOptions,
        reporter: &NativeClientOperationReporter,
        measurements: Arc<NativePipelineMeasurements>,
        base_snapshot_id: Option<String>,
        preparation_operation_id: String,
    ) -> Result<NativePreparedSnapshotCandidate, SdkError> {
        let session: NativeUploadSession = {
            let (request, _) = self.git_request(
                Method::POST,
                project_id,
                repo,
                Some("fs/uploads"),
                username,
                token,
            )?;
            expect_json(request.send().await?).await?
        };
        reporter.set_session(&session);

        let source = root.to_path_buf();
        let target_segment_bytes = session.target_segment_bytes;
        let max_segment_bytes = session.max_segment_bytes;
        let (segment_sender, segment_receiver) =
            tokio::sync::mpsc::channel(NATIVE_SEGMENT_UPLOAD_QUEUE);
        let cancellation = Arc::new(AtomicBool::new(false));
        let scanner_measurements = measurements.clone();
        let scanner_cancellation = cancellation.clone();
        let mut scanner = tokio::task::spawn_blocking(move || {
            prepare_native_snapshot_with_sender(
                &source,
                target_segment_bytes,
                max_segment_bytes,
                Some(segment_sender),
                Some(scanner_measurements),
                Some(scanner_cancellation),
            )
        });
        let upload_client = self.clone();
        let upload_project = project_id.to_string();
        let upload_repo = repo.to_string();
        let upload_session = session.clone();
        let upload_username = username.to_string();
        let upload_token = token.to_string();
        let upload_cancellation = cancellation.clone();
        let mut uploader = tokio::spawn(async move {
            let result = upload_pipelined_segments(
                &upload_client,
                &upload_project,
                &upload_repo,
                &upload_session,
                &upload_username,
                &upload_token,
                segment_receiver,
            )
            .await;
            upload_cancellation.store(true, Ordering::Relaxed);
            result
        });
        let (prepared, early_upload) = tokio::select! {
            scanned = &mut scanner => {
                let prepared = match scanned {
                    Ok(Ok(prepared)) => prepared,
                    Ok(Err(error)) => {
                        uploader.abort();
                        return Err(error);
                    }
                    Err(error) => {
                        uploader.abort();
                        return Err(client_error(format!(
                            "native snapshot scanner failed: {error}"
                        )));
                    }
                };
                (prepared, None)
            }
            uploaded = &mut uploader => {
                match uploaded {
                    Ok(Ok(report)) => {
                        let prepared = scanner.await.map_err(|error| {
                            client_error(format!("native snapshot scanner failed: {error}"))
                        })??;
                        (prepared, Some(report))
                    }
                    Ok(Err(error)) => {
                        cancellation.store(true, Ordering::Relaxed);
                        let _ = scanner.await;
                        return Err(error);
                    }
                    Err(error) => {
                        cancellation.store(true, Ordering::Relaxed);
                        let _ = scanner.await;
                        return Err(client_error(format!(
                            "native upload coordinator failed: {error}"
                        )));
                    }
                }
            }
        };
        reporter.note_prepared(&prepared, &measurements);
        reporter.set_phase(NativeClientOperationPhase::Uploading);
        note_progress(
            &options.progress,
            NativePushEvent::Scanned {
                files: prepared.files,
                directories: prepared.directories,
                logical_bytes: prepared.logical_bytes,
                stored_bytes: prepared.stored_bytes,
                segments: prepared.segments.len(),
            },
        );
        let metadata = stage_native_metadata_batches(
            self,
            project_id,
            repo,
            &session.session_id,
            username,
            token,
            &prepared.pages,
            &prepared.recipes,
        )
        .await;
        if metadata.is_ok() {
            reporter.note_metadata_complete();
        }
        let pipelined = match early_upload {
            Some(report) => report,
            None => uploader.await.map_err(|error| {
                client_error(format!("native upload coordinator failed: {error}"))
            })??,
        };
        reporter.note_upload_complete(pipelined.uploaded_ids.len(), pipelined.uploaded_bytes);
        metadata?;
        note_progress(
            &options.progress,
            NativePushEvent::Negotiated {
                missing_segments: pipelined.uploaded_ids.len(),
                total_segments: prepared.segments.len(),
                transport: session.transport.clone(),
            },
        );
        note_progress(
            &options.progress,
            NativePushEvent::Uploaded {
                segments: pipelined.uploaded_ids.len(),
                stored_bytes: pipelined.uploaded_bytes,
            },
        );

        reporter.set_phase(NativeClientOperationPhase::Verifying);
        let root_id = prepared.root.to_hex();
        let suffix = format!("fs/uploads/{}/metadata/verify-root", session.session_id);
        let (request, _) = self.git_request(
            Method::POST,
            project_id,
            repo,
            Some(&suffix),
            username,
            token,
        )?;
        expect_ok(
            request
                .json(&serde_json::json!({ "root": &root_id }))
                .send()
                .await?,
        )
        .await?;
        reporter.note_verification_complete();

        Ok(NativePreparedSnapshotCandidate {
            root_id,
            base_snapshot_id,
            changes: prepared.changes,
            preparation_operation_id,
            preparation_ms: reporter.elapsed_ms(),
            files: prepared.files,
            directories: prepared.directories,
            logical_bytes: prepared.logical_bytes,
            stored_bytes: prepared.stored_bytes,
            total_segments: prepared.segments.len(),
            uploaded_segments: pipelined.uploaded_ids.len(),
            uploaded_bytes: pipelined.uploaded_bytes,
            source_observations: prepared.source_observations,
            prepared_changes: None,
        })
    }

    /// Publish a mounted filesystem delta without rereading unchanged files. Existing file
    /// recipes and untouched directory roots remain content-addressed references; only dirty
    /// local file bytes and directory pages on affected ancestor paths are produced.
    pub async fn push_native_changes_with_credential(
        &self,
        project_id: &str,
        repo: &str,
        changes: NativeChangeSet,
        username: &str,
        token: &str,
        mut options: NativePushOptions,
    ) -> Result<NativePushReport, SdkError> {
        if changes.upserts.is_empty() && changes.deletes.is_empty() && changes.renames.is_empty() {
            return Err(client_error("native change set is empty"));
        }
        let operation_id = options
            .operation_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        options.operation_id = Some(operation_id.clone());
        let reporter = NativeClientOperationReporter::new(
            self,
            project_id,
            repo,
            username,
            token,
            operation_id,
            NativeClientOperationMode::MountedDelta,
        );
        let measurements = Arc::new(NativePipelineMeasurements::new(reporter.started));
        reporter.spawn_report(NativeClientOperationState::Running, None);
        let result = self
            .push_native_changes_with_credential_inner(
                project_id,
                repo,
                changes,
                username,
                token,
                options,
                &reporter,
                &measurements,
            )
            .await;
        match result {
            Ok(mut report) => {
                reporter
                    .send_terminal(NativeClientOperationState::Succeeded, None)
                    .await;
                report.client_timings = reporter.timings();
                Ok(report)
            }
            Err(error) => {
                reporter
                    .send_terminal(
                        NativeClientOperationState::Failed,
                        Some(native_client_failure_code(&error)),
                    )
                    .await;
                Err(error)
            }
        }
    }

    /// Prepare and upload a mounted delta without publishing a snapshot. Mount daemons call this
    /// while files are quiet; the returned value is safe to persist because it contains immutable
    /// content references rather than source paths or credentials.
    pub async fn prepare_native_changes_with_credential(
        &self,
        project_id: &str,
        repo: &str,
        changes: NativeChangeSet,
        username: &str,
        token: &str,
    ) -> Result<NativePreparedChangeSet, SdkError> {
        self.prepare_native_changes_with_reporter(
            project_id, repo, changes, username, token, None, None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn prepare_native_changes_with_reporter(
        &self,
        project_id: &str,
        repo: &str,
        changes: NativeChangeSet,
        username: &str,
        token: &str,
        reporter: Option<&NativeClientOperationReporter>,
        staging_directory: Option<PathBuf>,
    ) -> Result<NativePreparedChangeSet, SdkError> {
        let started = std::time::Instant::now();
        let session: NativeUploadSession = {
            let (request, _) = self.git_request(
                Method::POST,
                project_id,
                repo,
                Some("fs/uploads"),
                username,
                token,
            )?;
            expect_json(request.send().await?).await?
        };
        if let Some(reporter) = reporter {
            reporter.set_session(&session);
        }
        let local = tokio::task::spawn_blocking({
            let target_segment_bytes = session.target_segment_bytes;
            let max_segment_bytes = session.max_segment_bytes;
            move || {
                prepare_local_upserts(
                    changes.upserts,
                    target_segment_bytes,
                    max_segment_bytes,
                    staging_directory,
                )
            }
        })
        .await
        .map_err(|error| client_error(format!("native background prepare failed: {error}")))??;
        if let Some(reporter) = reporter {
            reporter.note_prepared_changes(&local);
            reporter.set_phase(NativeClientOperationPhase::Uploading);
        }
        let total_segments = local.segments.len();
        let (uploaded_segments, uploaded_bytes) = upload_built_segments(
            self,
            project_id,
            repo,
            &session,
            username,
            token,
            &local.segments,
        )
        .await?;
        if let Some(reporter) = reporter {
            reporter.note_upload_complete(uploaded_segments, uploaded_bytes);
            reporter.set_phase(NativeClientOperationPhase::Verifying);
        }
        let recipe_declarations = prepared_recipe_declarations(&local.entries);
        if !recipe_declarations.is_empty() {
            stage_native_metadata_batches(
                self,
                project_id,
                repo,
                &session.session_id,
                username,
                token,
                &[],
                &local.recipes,
            )
            .await?;
            let suffix = format!("fs/uploads/{}/metadata/verify-recipes", session.session_id);
            let (request, _) = self.git_request(
                Method::POST,
                project_id,
                repo,
                Some(&suffix),
                username,
                token,
            )?;
            expect_ok(
                request
                    .json(&PreparedRecipeVerificationRequest {
                        recipes: recipe_declarations,
                    })
                    .send()
                    .await?,
            )
            .await?;
        }
        Ok(NativePreparedChangeSet {
            upserts: NativePreparedUpserts {
                entries: local.entries,
                local_directories: local.local_directories,
                recipes: local.recipes,
                files: local.files,
                directories: local.directories,
                logical_bytes: local.logical_bytes,
                stored_bytes: local.stored_bytes,
            },
            deletes: changes.deletes,
            renames: changes.renames,
            preparation_ms: started.elapsed().as_millis() as u64,
            total_segments,
            uploaded_segments,
            uploaded_bytes,
        })
    }

    /// Prepare, upload, compose, stage, and verify a mounted journal generation without publishing
    /// it. This is the daemon's continuous write path: the returned candidate is immutable and
    /// durable, so the later snapshot operation only freezes the matching watermark and advances a
    /// pointer.
    pub async fn prepare_native_snapshot_candidate_with_credential(
        &self,
        project_id: &str,
        repo: &str,
        base_snapshot_id: &str,
        changes: NativeChangeSet,
        username: &str,
        token: &str,
    ) -> Result<NativePreparedSnapshotCandidate, SdkError> {
        let operation_id = uuid::Uuid::new_v4().to_string();
        self.prepare_native_snapshot_candidate_with_operation_id_and_credential(
            project_id,
            repo,
            base_snapshot_id,
            changes,
            username,
            token,
            operation_id,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn prepare_native_snapshot_candidate_with_operation_id_and_credential(
        &self,
        project_id: &str,
        repo: &str,
        base_snapshot_id: &str,
        changes: NativeChangeSet,
        username: &str,
        token: &str,
        operation_id: String,
    ) -> Result<NativePreparedSnapshotCandidate, SdkError> {
        self.prepare_native_snapshot_candidate_with_operation_and_staging_directory(
            project_id,
            repo,
            base_snapshot_id,
            changes,
            username,
            token,
            operation_id,
            None,
        )
        .await
    }

    /// Durable CLI form: non-pipelined segment bodies live inside the generation-owned staging
    /// directory, so an abnormal exit cannot leak anonymous system tempfiles.
    #[allow(clippy::too_many_arguments)]
    pub async fn prepare_native_snapshot_candidate_with_operation_and_staging_directory(
        &self,
        project_id: &str,
        repo: &str,
        base_snapshot_id: &str,
        changes: NativeChangeSet,
        username: &str,
        token: &str,
        operation_id: String,
        staging_directory: Option<PathBuf>,
    ) -> Result<NativePreparedSnapshotCandidate, SdkError> {
        let reporter = NativeClientOperationReporter::new(
            self,
            project_id,
            repo,
            username,
            token,
            operation_id.clone(),
            NativeClientOperationMode::MountedDelta,
        );
        reporter.spawn_report(NativeClientOperationState::Running, None);
        let result = async {
            let prepared_changes = self
                .prepare_native_changes_with_reporter(
                    project_id,
                    repo,
                    changes,
                    username,
                    token,
                    Some(&reporter),
                    staging_directory,
                )
                .await?;
            self.finish_preparing_native_snapshot_candidate(
                project_id,
                repo,
                base_snapshot_id,
                prepared_changes,
                username,
                token,
                operation_id,
                Some(&reporter),
            )
            .await
        }
        .await;
        match result {
            Ok(candidate) => {
                reporter.note_preparation_complete();
                reporter
                    .send_terminal(NativeClientOperationState::Succeeded, None)
                    .await;
                Ok(candidate)
            }
            Err(error) => {
                reporter
                    .send_terminal(
                        NativeClientOperationState::Failed,
                        Some(native_client_failure_code(&error)),
                    )
                    .await;
                Err(error)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn finish_preparing_native_snapshot_candidate(
        &self,
        project_id: &str,
        repo: &str,
        base_snapshot_id: &str,
        prepared_changes: NativePreparedChangeSet,
        username: &str,
        token: &str,
        preparation_operation_id: String,
        reporter: Option<&NativeClientOperationReporter>,
    ) -> Result<NativePreparedSnapshotCandidate, SdkError> {
        let session: NativeUploadSession = {
            let (request, _) = self.git_request(
                Method::POST,
                project_id,
                repo,
                Some("fs/uploads"),
                username,
                token,
            )?;
            expect_json(request.send().await?).await?
        };
        let reusable_changes = prepared_changes.clone();
        let NativePreparedChangeSet {
            upserts,
            deletes,
            renames,
            preparation_ms,
            total_segments,
            uploaded_segments,
            uploaded_bytes,
        } = prepared_changes;
        let snapshot_changes = NativeSnapshotChanges {
            upserts: upserts.entries.keys().cloned().collect(),
            deletes: deletes.clone(),
            renames: renames
                .iter()
                .map(|rename| NativeSnapshotRename {
                    from: rename.from.clone(),
                    to: rename.to.clone(),
                })
                .collect(),
        };
        let mut prepared = compose_native_changes(
            self,
            project_id,
            repo,
            username,
            token,
            base_snapshot_id,
            PreparedLocalUpserts {
                entries: upserts.entries,
                local_directories: upserts.local_directories,
                recipes: upserts.recipes,
                segments: Vec::new(),
                files: upserts.files,
                directories: upserts.directories,
                logical_bytes: upserts.logical_bytes,
                stored_bytes: upserts.stored_bytes,
            },
            deletes,
            renames,
        )
        .await?;
        prepared.changes = snapshot_changes;
        stage_native_metadata_batches(
            self,
            project_id,
            repo,
            &session.session_id,
            username,
            token,
            &prepared.pages,
            &prepared.recipes,
        )
        .await?;
        if let Some(reporter) = reporter {
            reporter.note_metadata_complete();
        }
        let root_id = prepared.root.to_hex();
        let suffix = format!("fs/uploads/{}/metadata/verify-root", session.session_id);
        let (request, _) = self.git_request(
            Method::POST,
            project_id,
            repo,
            Some(&suffix),
            username,
            token,
        )?;
        expect_ok(
            request
                .json(&serde_json::json!({ "root": &root_id }))
                .send()
                .await?,
        )
        .await?;
        if let Some(reporter) = reporter {
            reporter.note_verification_complete();
        }
        let preparation_ms = reporter
            .map(NativeClientOperationReporter::elapsed_ms)
            .unwrap_or(preparation_ms);
        Ok(NativePreparedSnapshotCandidate {
            root_id,
            base_snapshot_id: Some(base_snapshot_id.to_string()),
            changes: prepared.changes,
            preparation_operation_id,
            preparation_ms,
            files: prepared.files,
            directories: prepared.directories,
            logical_bytes: prepared.logical_bytes,
            stored_bytes: prepared.stored_bytes,
            total_segments,
            uploaded_segments,
            uploaded_bytes,
            source_observations: Vec::new(),
            prepared_changes: Some(Box::new(reusable_changes)),
        })
    }

    /// Recompose an already-uploaded mounted delta onto a newer serialized filesystem head.
    /// This performs metadata reads/staging only; content bytes and recipes are reused.
    pub async fn rebase_native_snapshot_candidate_with_credential(
        &self,
        project_id: &str,
        repo: &str,
        candidate: &NativePreparedSnapshotCandidate,
        new_base_snapshot_id: &str,
        username: &str,
        token: &str,
    ) -> Result<NativePreparedSnapshotCandidate, SdkError> {
        let prepared_changes = candidate
            .prepared_changes
            .as_deref()
            .cloned()
            .ok_or_else(|| {
                client_error(
                    "this prepared snapshot does not retain a rebaseable mounted change set",
                )
            })?;
        use sha2::{Digest, Sha256};
        let mut digest = Sha256::new();
        digest.update(candidate.preparation_operation_id.as_bytes());
        digest.update([0]);
        digest.update(new_base_snapshot_id.as_bytes());
        let operation_id = format!(
            "{}-{}",
            candidate.preparation_operation_id,
            hex::encode(digest.finalize())
        );
        self.finish_preparing_native_snapshot_candidate(
            project_id,
            repo,
            new_base_snapshot_id,
            prepared_changes,
            username,
            token,
            operation_id,
            None,
        )
        .await
    }

    /// Publish a previously verified journal root. This records the user's final snapshot message
    /// and attribution, performs a metadata-only closure check, and advances the workspace/head;
    /// it never reads a local file, uploads content, or rebuilds a directory.
    pub async fn publish_native_snapshot_candidate_outcome_with_credential(
        &self,
        project_id: &str,
        repo: &str,
        candidate: NativePreparedSnapshotCandidate,
        username: &str,
        token: &str,
        options: NativePushOptions,
    ) -> Result<NativeCandidatePublishOutcome, SdkError> {
        let started = std::time::Instant::now();
        let operation_id = options
            .operation_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let session: NativeUploadSession = {
            let (request, _) = self.git_request(
                Method::POST,
                project_id,
                repo,
                Some("fs/uploads"),
                username,
                token,
            )?;
            expect_json(request.send().await?).await?
        };
        let request_body = SubmitNativeSnapshotRequest {
            root: candidate.root_id.clone(),
            parents: candidate.base_snapshot_id.iter().cloned().collect(),
            created_at_ms: None,
            message: options.message.clone(),
            operation_id: operation_id.clone(),
            changes: candidate.changes.clone(),
        };
        let submitted: SubmitNativeSnapshotResponse = {
            let suffix = format!("fs/uploads/{}/snapshots", session.session_id);
            let (request, _) = self.git_request(
                Method::POST,
                project_id,
                repo,
                Some(&suffix),
                username,
                token,
            )?;
            expect_json(request.json(&request_body).send().await?).await?
        };
        note_progress(
            &options.progress,
            NativePushEvent::Verifying {
                snapshot_id: submitted.snapshot_id.clone(),
            },
        );
        wait_for_native_verification(
            self,
            project_id,
            repo,
            username,
            token,
            &session,
            &submitted.snapshot_id,
            &operation_id,
        )
        .await?;
        let advance = if let Some(workspace_id) = options.workspace_id.as_deref() {
            publish_native_workspace_snapshot(
                self,
                project_id,
                repo,
                username,
                token,
                workspace_id,
                &session.session_id,
                &submitted.snapshot_id,
                candidate.base_snapshot_id.as_deref(),
                candidate.base_snapshot_id.as_deref(),
            )
            .await?
        } else {
            advance_native_head_with_credential(
                self,
                project_id,
                repo,
                username,
                token,
                &submitted.snapshot_id,
                candidate.base_snapshot_id.as_deref(),
            )
            .await?
        };
        let (previous_snapshot_id, snapshot_id) = match advance {
            NativeHeadAdvance::Published {
                previous_snapshot_id,
                snapshot_id,
            } => (previous_snapshot_id, snapshot_id),
            NativeHeadAdvance::Conflict {
                actual_snapshot_id,
                snapshot_id,
            } => {
                return Ok(NativeCandidatePublishOutcome::Conflict {
                    operation_id,
                    snapshot_id,
                    actual_snapshot_id,
                });
            }
        };
        note_progress(
            &options.progress,
            NativePushEvent::Published {
                snapshot_id: snapshot_id.clone(),
            },
        );
        Ok(NativeCandidatePublishOutcome::Published(NativePushReport {
            operation_id,
            snapshot_id,
            previous_snapshot_id,
            files: candidate.files,
            directories: candidate.directories,
            logical_bytes: candidate.logical_bytes,
            stored_bytes: candidate.stored_bytes,
            total_segments: candidate.total_segments,
            uploaded_segments: candidate.uploaded_segments,
            uploaded_bytes: candidate.uploaded_bytes,
            transport: "prepared_journal".to_string(),
            client_timings: NativePushTimings {
                total_ms: started.elapsed().as_millis() as u64,
                publish_complete_ms: Some(started.elapsed().as_millis() as u64),
                ..NativePushTimings::default()
            },
        }))
    }

    pub async fn publish_native_snapshot_candidate_with_credential(
        &self,
        project_id: &str,
        repo: &str,
        candidate: NativePreparedSnapshotCandidate,
        username: &str,
        token: &str,
        options: NativePushOptions,
    ) -> Result<NativePushReport, SdkError> {
        match self
            .publish_native_snapshot_candidate_outcome_with_credential(
                project_id, repo, candidate, username, token, options,
            )
            .await?
        {
            NativeCandidatePublishOutcome::Published(report) => Ok(report),
            NativeCandidatePublishOutcome::Conflict {
                snapshot_id,
                actual_snapshot_id,
                ..
            } => Err(client_error(format!(
                "filesystem head changed to {} while prepared snapshot {snapshot_id} was waiting to publish",
                actual_snapshot_id.as_deref().unwrap_or("empty")
            ))),
        }
    }

    /// Publish a previously prepared mounted delta. This performs only base-tree composition,
    /// metadata staging, verification gating, and the workspace/head transaction; it never opens
    /// a local file or uploads content bytes.
    #[allow(clippy::too_many_arguments)]
    pub async fn push_prepared_native_changes_with_credential(
        &self,
        project_id: &str,
        repo: &str,
        prepared_changes: NativePreparedChangeSet,
        username: &str,
        token: &str,
        mut options: NativePushOptions,
    ) -> Result<NativePushReport, SdkError> {
        let expected = match options.expected_snapshot_id.clone() {
            Some(expected) => expected,
            None => self
                .native_head(project_id, repo)
                .await?
                .snapshot_id
                .ok_or_else(|| client_error("prepared native push needs an existing base"))?,
        };
        let expected_workspace = match options.workspace_id.as_deref() {
            Some(workspace_id) => {
                self.native_workspace_with_credential(
                    project_id,
                    repo,
                    workspace_id,
                    username,
                    token,
                )
                .await?
                .latest_snapshot_id
            }
            None => None,
        };
        let session: NativeUploadSession = {
            let (request, _) = self.git_request(
                Method::POST,
                project_id,
                repo,
                Some("fs/uploads"),
                username,
                token,
            )?;
            expect_json(request.send().await?).await?
        };
        let NativePreparedChangeSet {
            upserts,
            deletes,
            renames,
            ..
        } = prepared_changes;
        let snapshot_changes = NativeSnapshotChanges {
            upserts: upserts.entries.keys().cloned().collect(),
            deletes: deletes.clone(),
            renames: renames
                .iter()
                .map(|rename| NativeSnapshotRename {
                    from: rename.from.clone(),
                    to: rename.to.clone(),
                })
                .collect(),
        };
        let mut prepared = compose_native_changes(
            self,
            project_id,
            repo,
            username,
            token,
            &expected,
            PreparedLocalUpserts {
                entries: upserts.entries,
                local_directories: upserts.local_directories,
                recipes: upserts.recipes,
                segments: Vec::new(),
                files: upserts.files,
                directories: upserts.directories,
                logical_bytes: upserts.logical_bytes,
                stored_bytes: upserts.stored_bytes,
            },
            deletes,
            renames,
        )
        .await?;
        prepared.changes = snapshot_changes;
        options
            .operation_id
            .get_or_insert_with(|| uuid::Uuid::new_v4().to_string());
        finish_native_push(
            self,
            project_id,
            repo,
            username,
            token,
            Some(expected),
            expected_workspace,
            session,
            prepared,
            None,
            false,
            options,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn push_native_changes_with_credential_inner(
        &self,
        project_id: &str,
        repo: &str,
        changes: NativeChangeSet,
        username: &str,
        token: &str,
        options: NativePushOptions,
        reporter: &NativeClientOperationReporter,
        measurements: &NativePipelineMeasurements,
    ) -> Result<NativePushReport, SdkError> {
        let expected = match options.expected_snapshot_id.clone() {
            Some(expected) => expected,
            None => {
                let (request, _) = self.git_request(
                    Method::GET,
                    project_id,
                    repo,
                    Some("fs/head"),
                    username,
                    token,
                )?;
                let head: NativeHead = expect_json(request.send().await?).await?;
                head.snapshot_id.ok_or_else(|| {
                    client_error("incremental native push needs an existing base snapshot")
                })?
            }
        };
        let expected_workspace = match options.workspace_id.as_deref() {
            Some(workspace_id) => {
                self.native_workspace_with_credential(
                    project_id,
                    repo,
                    workspace_id,
                    username,
                    token,
                )
                .await?
                .latest_snapshot_id
            }
            None => None,
        };
        let session: NativeUploadSession = {
            let (request, _) = self.git_request(
                Method::POST,
                project_id,
                repo,
                Some("fs/uploads"),
                username,
                token,
            )?;
            expect_json(request.send().await?).await?
        };
        reporter.set_session(&session);
        let snapshot_changes = NativeSnapshotChanges {
            upserts: changes
                .upserts
                .iter()
                .map(|upsert| upsert.path.clone())
                .collect(),
            deletes: changes.deletes.clone(),
            renames: changes
                .renames
                .iter()
                .map(|rename| NativeSnapshotRename {
                    from: rename.from.clone(),
                    to: rename.to.clone(),
                })
                .collect(),
        };
        let upserts = changes.upserts;
        let target_segment_bytes = session.target_segment_bytes;
        let max_segment_bytes = session.max_segment_bytes;
        let content_prepare_started = std::time::Instant::now();
        let local = tokio::task::spawn_blocking(move || {
            prepare_local_upserts(upserts, target_segment_bytes, max_segment_bytes, None)
        })
        .await
        .map_err(|error| client_error(format!("native delta scanner failed: {error}")))??;
        let content_prepare_ms = content_prepare_started.elapsed().as_millis() as u64;
        let metadata_build_started = std::time::Instant::now();
        let mut prepared = compose_native_changes(
            self,
            project_id,
            repo,
            username,
            token,
            &expected,
            local,
            changes.deletes,
            changes.renames,
        )
        .await?;
        prepared.changes = snapshot_changes;
        prepared.prepare_timings.content_prepare_ms = Some(content_prepare_ms);
        prepared.prepare_timings.metadata_build_ms =
            Some(metadata_build_started.elapsed().as_millis() as u64);
        reporter.note_prepared(&prepared, measurements);
        reporter.set_phase(NativeClientOperationPhase::Uploading);
        note_progress(
            &options.progress,
            NativePushEvent::Scanned {
                files: prepared.files,
                directories: prepared.directories,
                logical_bytes: prepared.logical_bytes,
                stored_bytes: prepared.stored_bytes,
                segments: prepared.segments.len(),
            },
        );
        finish_native_push(
            self,
            project_id,
            repo,
            username,
            token,
            Some(expected),
            expected_workspace,
            session,
            prepared,
            None,
            false,
            options,
            Some(reporter),
        )
        .await
    }

    pub async fn create_native_upload(
        &self,
        project_id: &str,
        repo: &str,
    ) -> Result<NativeUploadSession, SdkError> {
        let credential = self.git_credential_for_repo(project_id, repo).await?;
        let (request, _) = self.git_request(
            Method::POST,
            project_id,
            repo,
            Some("fs/uploads"),
            &credential.git_username,
            &credential.token,
        )?;
        expect_json(request.send().await?).await
    }

    pub async fn native_missing_segments(
        &self,
        project_id: &str,
        repo: &str,
        session: &str,
        segment_ids: &[String],
    ) -> Result<Vec<String>, SdkError> {
        let credential = self.git_credential_for_repo(project_id, repo).await?;
        let suffix = format!("fs/uploads/{session}/segments/missing");
        let (request, _) = self.git_request(
            Method::POST,
            project_id,
            repo,
            Some(&suffix),
            &credential.git_username,
            &credential.token,
        )?;
        let response: MissingSegmentsResponse = expect_json(
            request
                .json(&MissingSegmentsRequest { segment_ids })
                .send()
                .await?,
        )
        .await?;
        Ok(response.missing_segment_ids)
    }

    pub async fn create_native_segment_target(
        &self,
        project_id: &str,
        repo: &str,
        session: &str,
        segment_id: &str,
        stored_len: u64,
    ) -> Result<NativeSegmentTarget, SdkError> {
        let credential = self.git_credential_for_repo(project_id, repo).await?;
        let suffix = format!("fs/uploads/{session}/segments");
        let (request, _) = self.git_request(
            Method::POST,
            project_id,
            repo,
            Some(&suffix),
            &credential.git_username,
            &credential.token,
        )?;
        expect_json(
            request
                .json(&SegmentTargetRequest {
                    segment_id,
                    stored_len,
                })
                .send()
                .await?,
        )
        .await
    }

    /// Upload one segment through the authenticated service fallback.
    pub async fn upload_native_segment(
        &self,
        project_id: &str,
        repo: &str,
        session: &str,
        staging_id: &str,
        segment_id: &str,
        stored_len: u64,
        body: Body,
    ) -> Result<(), SdkError> {
        let credential = self.git_credential_for_repo(project_id, repo).await?;
        let suffix = format!(
            "fs/uploads/{session}/segments/{staging_id}?segment_id={}",
            urlencoding::encode(segment_id)
        );
        let (request, _) = self.git_request(
            Method::PUT,
            project_id,
            repo,
            Some(&suffix),
            &credential.git_username,
            &credential.token,
        )?;
        expect_ok(
            request
                .header(reqwest::header::CONTENT_LENGTH, stored_len)
                .body(body)
                .send()
                .await?,
        )
        .await
    }

    /// Upload one segment directly to object storage, without attaching service credentials.
    pub async fn put_native_segment(
        &self,
        url: &str,
        stored_len: u64,
        checksum_sha256: &str,
        body: Body,
    ) -> Result<(), SdkError> {
        expect_ok(
            self.git_client
                .put(url)
                .header(reqwest::header::CONTENT_LENGTH, stored_len)
                .header("x-amz-checksum-sha256", checksum_sha256)
                .body(body)
                .send()
                .await?,
        )
        .await
    }

    pub async fn register_native_segment(
        &self,
        project_id: &str,
        repo: &str,
        session: &str,
        staging_id: &str,
        segment_id: &str,
        stored_len: u64,
        records: &[SegmentSlice],
    ) -> Result<(), SdkError> {
        let credential = self.git_credential_for_repo(project_id, repo).await?;
        let suffix = format!("fs/uploads/{session}/segments/{staging_id}/register");
        let (request, _) = self.git_request(
            Method::POST,
            project_id,
            repo,
            Some(&suffix),
            &credential.git_username,
            &credential.token,
        )?;
        expect_ok(
            request
                .json(&RegisterSegmentRequest {
                    segment_id,
                    stored_len,
                    records: native_segment_record_requests(records),
                })
                .send()
                .await?,
        )
        .await
    }

    pub async fn stage_native_metadata(
        &self,
        project_id: &str,
        repo: &str,
        session: &str,
        pages: &[DirectoryPage],
        recipes: &[ChunkRecipe],
    ) -> Result<NativeMetadataResponse, SdkError> {
        let credential = self.git_credential_for_repo(project_id, repo).await?;
        stage_native_metadata_batches(
            self,
            project_id,
            repo,
            session,
            &credential.git_username,
            &credential.token,
            pages,
            recipes,
        )
        .await
    }

    pub async fn submit_native_snapshot(
        &self,
        project_id: &str,
        repo: &str,
        session: &str,
        snapshot: &SubmitNativeSnapshotRequest,
    ) -> Result<SubmitNativeSnapshotResponse, SdkError> {
        let credential = self.git_credential_for_repo(project_id, repo).await?;
        let suffix = format!("fs/uploads/{session}/snapshots");
        let (request, _) = self.git_request(
            Method::POST,
            project_id,
            repo,
            Some(&suffix),
            &credential.git_username,
            &credential.token,
        )?;
        expect_json(request.json(snapshot).send().await?).await
    }

    pub async fn native_snapshot_status(
        &self,
        project_id: &str,
        repo: &str,
        snapshot_id: &str,
        session: Option<&str>,
    ) -> Result<NativeSnapshotStatus, SdkError> {
        let credential = self.git_credential_for_repo(project_id, repo).await?;
        let mut suffix = format!("fs/snapshots/{snapshot_id}/status");
        if let Some(session) = session {
            suffix.push_str("?session=");
            suffix.push_str(&urlencoding::encode(session));
        }
        let (request, _) = self.git_request(
            Method::GET,
            project_id,
            repo,
            Some(&suffix),
            &credential.git_username,
            &credential.token,
        )?;
        expect_json(request.send().await?).await
    }

    pub async fn native_head(&self, project_id: &str, repo: &str) -> Result<NativeHead, SdkError> {
        let credential = self.git_credential_for_repo(project_id, repo).await?;
        self.native_head_with_credential(
            project_id,
            repo,
            &credential.git_username,
            &credential.token,
        )
        .await
    }

    pub async fn native_head_with_credential(
        &self,
        project_id: &str,
        repo: &str,
        username: &str,
        token: &str,
    ) -> Result<NativeHead, SdkError> {
        let (request, _) = self.git_request(
            Method::GET,
            project_id,
            repo,
            Some("fs/head"),
            username,
            token,
        )?;
        expect_json(request.send().await?).await
    }

    pub async fn native_snapshot_history_with_credential(
        &self,
        project_id: &str,
        repo: &str,
        after: Option<&str>,
        limit: usize,
        username: &str,
        token: &str,
    ) -> Result<NativeSnapshotHistoryPage, SdkError> {
        let mut suffix = format!("fs/snapshots?limit={}", limit.clamp(1, 1000));
        if let Some(after) = after {
            suffix.push_str("&after=");
            suffix.push_str(&urlencoding::encode(after));
        }
        let (request, _) = self.git_request(
            Method::GET,
            project_id,
            repo,
            Some(&suffix),
            username,
            token,
        )?;
        expect_json(request.send().await?).await
    }

    pub async fn native_head_events_with_credential(
        &self,
        project_id: &str,
        repo: &str,
        limit: usize,
        username: &str,
        token: &str,
    ) -> Result<Vec<NativeHeadEvent>, SdkError> {
        let suffix = format!("fs/head/events?limit={}", limit.clamp(1, 1000));
        let (request, _) = self.git_request(
            Method::GET,
            project_id,
            repo,
            Some(&suffix),
            username,
            token,
        )?;
        let response: NativeHeadEvents = expect_json(request.send().await?).await?;
        Ok(response.events)
    }

    pub async fn native_snapshot_with_credential(
        &self,
        project_id: &str,
        repo: &str,
        snapshot_id: &str,
        username: &str,
        token: &str,
    ) -> Result<NativeSnapshotInfo, SdkError> {
        let suffix = format!("fs/snapshots/{snapshot_id}");
        let (request, _) = self.git_request(
            Method::GET,
            project_id,
            repo,
            Some(&suffix),
            username,
            token,
        )?;
        expect_json(request.send().await?).await
    }

    pub async fn pin_native_snapshot_with_credential(
        &self,
        project_id: &str,
        repo: &str,
        snapshot_id: &str,
        username: &str,
        token: &str,
    ) -> Result<NativeSnapshotPinState, SdkError> {
        let suffix = format!("fs/snapshots/{snapshot_id}/pin");
        let (request, _) = self.git_request(
            Method::PUT,
            project_id,
            repo,
            Some(&suffix),
            username,
            token,
        )?;
        expect_json(request.send().await?).await
    }

    pub async fn unpin_native_snapshot_with_credential(
        &self,
        project_id: &str,
        repo: &str,
        snapshot_id: &str,
        username: &str,
        token: &str,
    ) -> Result<NativeSnapshotPinState, SdkError> {
        let suffix = format!("fs/snapshots/{snapshot_id}/pin");
        let (request, _) = self.git_request(
            Method::DELETE,
            project_id,
            repo,
            Some(&suffix),
            username,
            token,
        )?;
        expect_json(request.send().await?).await
    }

    /// Resolve either a full native snapshot id or an unambiguous hexadecimal prefix. This is
    /// intentionally client-side until the point lookup API grows prefix semantics: history is
    /// paginated and bounded, so resolution never relies on a truncated first page.
    pub async fn resolve_native_snapshot_id_with_credential(
        &self,
        project_id: &str,
        repo: &str,
        prefix: &str,
        username: &str,
        token: &str,
    ) -> Result<String, SdkError> {
        if prefix.is_empty()
            || prefix.len() > 64
            || !prefix.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(client_error(format!(
                "invalid native snapshot id prefix {prefix:?}"
            )));
        }
        let prefix = prefix.to_ascii_lowercase();
        let mut after = None;
        let mut found = None;
        loop {
            let page = self
                .native_snapshot_history_with_credential(
                    project_id,
                    repo,
                    after.as_deref(),
                    1000,
                    username,
                    token,
                )
                .await?;
            for snapshot_id in page.snapshot_ids {
                if snapshot_id.starts_with(&prefix) {
                    if found.is_some() {
                        return Err(client_error(format!(
                            "native snapshot prefix {prefix:?} is ambiguous"
                        )));
                    }
                    found = Some(snapshot_id);
                }
            }
            let Some(next) = page.next_after else { break };
            after = Some(next);
        }
        found.ok_or_else(|| client_error(format!("native snapshot {prefix:?} was not found")))
    }

    pub async fn create_native_workspace_with_credential(
        &self,
        project_id: &str,
        repo: &str,
        snapshot_id: Option<&str>,
        read_only: bool,
        username: &str,
        token: &str,
    ) -> Result<NativeWorkspaceInfo, SdkError> {
        let (request, _) = self.git_request(
            Method::POST,
            project_id,
            repo,
            Some("fs/workspaces"),
            username,
            token,
        )?;
        expect_json(
            request
                .json(&CreateNativeWorkspaceRequest {
                    snapshot_id,
                    read_only,
                    ttl_seconds: Some(NATIVE_WORKSPACE_TTL_SECONDS),
                })
                .send()
                .await?,
        )
        .await
    }

    pub async fn list_native_workspaces_with_credential(
        &self,
        project_id: &str,
        repo: &str,
        username: &str,
        token: &str,
    ) -> Result<Vec<NativeWorkspaceInfo>, SdkError> {
        let mut workspaces = Vec::new();
        let mut after: Option<String> = None;
        loop {
            let mut suffix = "fs/workspaces?limit=1000".to_string();
            if let Some(cursor) = after.as_deref() {
                suffix.push_str("&after=");
                suffix.push_str(&urlencoding::encode(cursor));
            }
            let (request, _) = self.git_request(
                Method::GET,
                project_id,
                repo,
                Some(&suffix),
                username,
                token,
            )?;
            let response: NativeWorkspaceList = expect_json(request.send().await?).await?;
            workspaces.extend(response.workspaces);
            let Some(next) = response.next_after else {
                break;
            };
            if after.as_deref() == Some(next.as_str()) {
                return Err(client_error(
                    "native workspace pagination cursor did not advance",
                ));
            }
            after = Some(next);
        }
        Ok(workspaces)
    }

    pub async fn native_workspace_heartbeat_with_credential(
        &self,
        project_id: &str,
        repo: &str,
        workspace_id: &str,
        username: &str,
        token: &str,
    ) -> Result<NativeWorkspaceInfo, SdkError> {
        let suffix = format!("fs/workspaces/{workspace_id}/heartbeat");
        let (request, _) = self.git_request(
            Method::POST,
            project_id,
            repo,
            Some(&suffix),
            username,
            token,
        )?;
        expect_json(
            request
                .json(&NativeWorkspaceHeartbeatRequest {
                    ttl_seconds: NATIVE_WORKSPACE_TTL_SECONDS,
                })
                .send()
                .await?,
        )
        .await
    }

    pub async fn native_workspace_with_credential(
        &self,
        project_id: &str,
        repo: &str,
        workspace_id: &str,
        username: &str,
        token: &str,
    ) -> Result<NativeWorkspaceInfo, SdkError> {
        let suffix = format!("fs/workspaces/{workspace_id}");
        let (request, _) = self.git_request(
            Method::GET,
            project_id,
            repo,
            Some(&suffix),
            username,
            token,
        )?;
        expect_json(request.send().await?).await
    }

    pub async fn delete_native_workspace_with_credential(
        &self,
        project_id: &str,
        repo: &str,
        workspace_id: &str,
        username: &str,
        token: &str,
    ) -> Result<(), SdkError> {
        let suffix = format!("fs/workspaces/{workspace_id}");
        let (request, _) = self.git_request(
            Method::DELETE,
            project_id,
            repo,
            Some(&suffix),
            username,
            token,
        )?;
        expect_ok(request.send().await?).await
    }

    /// Restore by reference: mint a new snapshot pointing at a verified historical root, then
    /// advance/promote the workspace. No file or segment bytes move.
    pub async fn restore_native_snapshot_with_credential(
        &self,
        project_id: &str,
        repo: &str,
        workspace_id: &str,
        target_snapshot_id: &str,
        current_snapshot_id: &str,
        operation_id: &str,
        created_at_ms: u64,
        username: &str,
        token: &str,
    ) -> Result<String, SdkError> {
        let target = self
            .native_snapshot_with_credential(project_id, repo, target_snapshot_id, username, token)
            .await?;
        let session: NativeUploadSession = {
            let (request, _) = self.git_request(
                Method::POST,
                project_id,
                repo,
                Some("fs/uploads"),
                username,
                token,
            )?;
            expect_json(request.send().await?).await?
        };
        let request = SubmitNativeSnapshotRequest {
            root: target.root,
            parents: vec![current_snapshot_id.to_string()],
            created_at_ms: Some(created_at_ms),
            message: format!(
                "Restore {}",
                &target_snapshot_id[..target_snapshot_id.len().min(12)]
            ),
            operation_id: operation_id.to_string(),
            changes: NativeSnapshotChanges::default(),
        };
        let submitted: SubmitNativeSnapshotResponse = {
            let suffix = format!("fs/uploads/{}/snapshots", session.session_id);
            let (builder, _) = self.git_request(
                Method::POST,
                project_id,
                repo,
                Some(&suffix),
                username,
                token,
            )?;
            expect_json(builder.json(&request).send().await?).await?
        };
        wait_for_native_verification(
            self,
            project_id,
            repo,
            username,
            token,
            &session,
            &submitted.snapshot_id,
            operation_id,
        )
        .await?;
        match publish_native_workspace_snapshot(
            self,
            project_id,
            repo,
            username,
            token,
            workspace_id,
            &session.session_id,
            &submitted.snapshot_id,
            Some(current_snapshot_id),
            Some(current_snapshot_id),
        )
        .await?
        {
            NativeHeadAdvance::Published { snapshot_id, .. } => Ok(snapshot_id),
            NativeHeadAdvance::Conflict {
                actual_snapshot_id,
                snapshot_id,
            } => Err(client_error(format!(
                "filesystem changed to {} while restore snapshot {snapshot_id} was being verified; retry",
                actual_snapshot_id.as_deref().unwrap_or("empty")
            ))),
        }
    }

    pub async fn advance_native_head(
        &self,
        project_id: &str,
        repo: &str,
        snapshot_id: &str,
        expected_snapshot_id: Option<&str>,
    ) -> Result<NativeHeadAdvance, SdkError> {
        let credential = self.git_credential_for_repo(project_id, repo).await?;
        let (request, _) = self.git_request(
            Method::PUT,
            project_id,
            repo,
            Some("fs/head"),
            &credential.git_username,
            &credential.token,
        )?;
        let response = request
            .json(&AdvanceNativeHeadRequest {
                snapshot_id,
                expected_snapshot_id,
            })
            .send()
            .await?;
        if response.status() != StatusCode::CONFLICT && !response.status().is_success() {
            return Err(response_error(response).await);
        }
        let response: AdvanceNativeHeadResponse = response.json().await.map_err(|error| {
            SdkError::ClientError(format!("bad native head response body: {error}"))
        })?;
        if response.published {
            Ok(NativeHeadAdvance::Published {
                previous_snapshot_id: response.previous_snapshot_id,
                snapshot_id: response.snapshot_id,
            })
        } else {
            Ok(NativeHeadAdvance::Conflict {
                actual_snapshot_id: response.actual_snapshot_id,
                snapshot_id: response.snapshot_id,
            })
        }
    }
}

fn native_path_parts(path: &str) -> Result<Vec<&str>, SdkError> {
    if path.is_empty() || path.starts_with('/') || path.ends_with('/') {
        return Err(client_error(format!("invalid native path {path:?}")));
    }
    let parts: Vec<_> = path.split('/').collect();
    if parts
        .iter()
        .any(|part| part.is_empty() || *part == "." || *part == ".." || part.contains('\0'))
    {
        return Err(client_error(format!("invalid native path {path:?}")));
    }
    Ok(parts)
}

fn native_path_name(path: &str) -> Result<&[u8], SdkError> {
    Ok(native_path_parts(path)?
        .last()
        .expect("non-empty path")
        .as_bytes())
}

fn native_parent_and_name(path: &str) -> Result<(String, Vec<u8>), SdkError> {
    let parts = native_path_parts(path)?;
    let name = parts.last().expect("non-empty path").as_bytes().to_vec();
    Ok((parts[..parts.len() - 1].join("/"), name))
}

fn add_native_ancestor_dirs(
    path: &str,
    directories: &mut BTreeSet<String>,
) -> Result<(), SdkError> {
    let (mut parent, _) = native_parent_and_name(path)?;
    loop {
        directories.insert(parent.clone());
        let Some((next, _)) = parent.rsplit_once('/') else {
            if !parent.is_empty() {
                parent.clear();
                continue;
            }
            break;
        };
        parent = next.to_string();
    }
    Ok(())
}

fn native_depth(path: &str) -> usize {
    if path.is_empty() {
        0
    } else {
        path.bytes().filter(|byte| *byte == b'/').count() + 1
    }
}

fn remote_directory_alias(path: &str, renames: &[NativeRename]) -> String {
    let mut current = path.to_string();
    // Resolve nested rename destinations from most-specific to least-specific. The bound also
    // makes malformed cyclic rename sets harmless and deterministic.
    for _ in 0..=renames.len() {
        let Some(rename) = renames
            .iter()
            .filter(|rename| {
                current == rename.to
                    || current
                        .strip_prefix(&rename.to)
                        .is_some_and(|rest| rest.starts_with('/'))
            })
            .max_by_key(|rename| rename.to.len())
        else {
            break;
        };
        let suffix = &current[rename.to.len()..];
        let next = format!("{}{suffix}", rename.from);
        if next == current {
            break;
        }
        current = next;
    }
    current
}

fn take_native_entry(entries: &mut Vec<DirectoryEntry>, name: &[u8]) -> Option<DirectoryEntry> {
    entries
        .iter()
        .position(|entry| entry.name == name)
        .map(|index| entries.remove(index))
}

fn put_native_entry(entries: &mut Vec<DirectoryEntry>, entry: DirectoryEntry) {
    if let Some(index) = entries
        .iter()
        .position(|candidate| candidate.name == entry.name)
    {
        entries[index] = entry;
    } else {
        entries.push(entry);
    }
}

#[allow(clippy::too_many_arguments)]
async fn fetch_native_directory(
    client: &ArtifactStorageClient,
    project_id: &str,
    repo: &str,
    username: &str,
    token: &str,
    snapshot: &str,
    path: &str,
) -> Result<Option<Vec<DirectoryEntry>>, SdkError> {
    let mut after = None;
    let mut entries = Vec::new();
    loop {
        let mut suffix = format!(
            "fs/tree?snapshot={}&path={}&limit=4096",
            urlencoding::encode(snapshot),
            urlencoding::encode(path),
        );
        if let Some(cursor) = after.as_deref() {
            suffix.push_str("&after=");
            suffix.push_str(&urlencoding::encode(cursor));
        }
        let page: Option<NativeTreePage> = super::ingest::with_transient_retries(|| async {
            let (request, _) = client.git_request(
                Method::GET,
                project_id,
                repo,
                Some(&suffix),
                username,
                token,
            )?;
            let response = request.send().await?;
            if response.status() == StatusCode::NOT_FOUND {
                return Ok(None);
            }
            expect_json(response).await.map(Some)
        })
        .await?;
        let Some(page) = page else {
            return Ok(None);
        };
        entries.extend(page.entries);
        let Some(next) = page.next_after else { break };
        after = Some(next);
    }
    Ok(Some(entries))
}

#[allow(clippy::too_many_arguments)]
async fn compose_native_changes(
    client: &ArtifactStorageClient,
    project_id: &str,
    repo: &str,
    username: &str,
    token: &str,
    base_snapshot: &str,
    mut local: PreparedLocalUpserts,
    deletes: Vec<String>,
    renames: Vec<NativeRename>,
) -> Result<PreparedNativeSnapshot, SdkError> {
    let mut dirty_directories = BTreeSet::new();
    for path in local.entries.keys().chain(deletes.iter()) {
        add_native_ancestor_dirs(path, &mut dirty_directories)?;
    }
    for rename in &renames {
        add_native_ancestor_dirs(&rename.from, &mut dirty_directories)?;
        add_native_ancestor_dirs(&rename.to, &mut dirty_directories)?;
    }

    // Every changed directory is fetched once. A destination below a lower-directory rename is
    // read through its immutable source coordinate; a genuinely new local directory starts empty.
    let fetched: Vec<Result<(String, Option<Vec<DirectoryEntry>>), SdkError>> =
        futures::stream::iter(dirty_directories.iter().cloned())
            .map(|directory| {
                let remote = remote_directory_alias(&directory, &renames);
                async move {
                    let entries = fetch_native_directory(
                        client,
                        project_id,
                        repo,
                        username,
                        token,
                        base_snapshot,
                        &remote,
                    )
                    .await?;
                    Ok((directory, entries))
                }
            })
            .buffer_unordered(16)
            .collect()
            .await;
    let mut directories: HashMap<String, Vec<DirectoryEntry>> = HashMap::new();
    for result in fetched {
        let (path, entries) = result?;
        match entries {
            Some(entries) => {
                directories.insert(path, entries);
            }
            None if local.local_directories.contains(&path) => {
                directories.insert(path, Vec::new());
            }
            None => {
                return Err(client_error(format!(
                    "base snapshot {base_snapshot} has no directory {path:?}"
                )));
            }
        }
    }

    // Root-reference renames happen before ordinary deletes/upserts. The source entry carries
    // the entire immutable subtree; only the two parents become dirty.
    for rename in &renames {
        let (from_parent, from_name) = native_parent_and_name(&rename.from)?;
        let (to_parent, to_name) = native_parent_and_name(&rename.to)?;
        let source = take_native_entry(
            directories
                .get_mut(&from_parent)
                .ok_or_else(|| client_error(format!("missing rename parent {from_parent:?}")))?,
            &from_name,
        )
        .ok_or_else(|| client_error(format!("rename source {:?} does not exist", rename.from)))?;
        let mut destination = source;
        destination.name = to_name;
        put_native_entry(
            directories
                .get_mut(&to_parent)
                .ok_or_else(|| client_error(format!("missing rename parent {to_parent:?}")))?,
            destination,
        );
    }

    for path in deletes {
        let (parent, name) = native_parent_and_name(&path)?;
        if let Some(entries) = directories.get_mut(&parent) {
            take_native_entry(entries, &name);
        }
    }

    let mut pages = Vec::new();
    // Parent directories must exist in the working maps before their children are built.
    let mut upsert_paths: Vec<String> = local.entries.keys().cloned().collect();
    upsert_paths.sort_by_key(|path| native_depth(path));
    for path in upsert_paths {
        let (parent, _) = native_parent_and_name(&path)?;
        let parent_entries = directories
            .get_mut(&parent)
            .ok_or_else(|| client_error(format!("missing upsert parent {parent:?}")))?;
        let mut entry = local
            .entries
            .remove(&path)
            .expect("path collected from map");
        if local.local_directories.contains(&path) {
            let reused = parent_entries.iter().find_map(|old| {
                (old.name == entry.name)
                    .then_some(&old.data)
                    .and_then(|data| match data {
                        EntryData::Directory { root } => Some(*root),
                        _ => None,
                    })
            });
            let root = match reused {
                Some(root) => root,
                None if dirty_directories.contains(&path) => ObjectId::default(),
                None => build_directory_pages(Vec::new(), &mut pages)?,
            };
            entry.data = EntryData::Directory { root };
        }
        put_native_entry(parent_entries, entry);
    }

    let mut build_order: Vec<String> = dirty_directories.into_iter().collect();
    build_order.sort_by_key(|path| std::cmp::Reverse(native_depth(path)));
    let mut root = None;
    for directory in build_order {
        // A parent deletion/replacement wins over any stale descendant event in the same delta.
        if !directory.is_empty() {
            let (parent, name) = native_parent_and_name(&directory)?;
            let still_directory = directories.get(&parent).is_some_and(|entries| {
                entries.iter().any(|entry| {
                    entry.name == name && matches!(entry.data, EntryData::Directory { .. })
                })
            });
            if !still_directory {
                continue;
            }
        }
        let mut entries = directories.remove(&directory).unwrap_or_default();
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        let directory_root = build_directory_pages(entries, &mut pages)?;
        if directory.is_empty() {
            root = Some(directory_root);
        } else {
            let (parent, name) = native_parent_and_name(&directory)?;
            let parent_entries = directories
                .get_mut(&parent)
                .ok_or_else(|| client_error(format!("missing ancestor directory {parent:?}")))?;
            let child = parent_entries
                .iter_mut()
                .find(|entry| entry.name == name)
                .ok_or_else(|| client_error(format!("missing directory entry {directory:?}")))?;
            child.data = EntryData::Directory {
                root: directory_root,
            };
        }
    }
    let root = root.ok_or_else(|| client_error("native delta did not rebuild the root"))?;
    Ok(PreparedNativeSnapshot {
        root,
        pages,
        recipes: local.recipes,
        segments: local.segments,
        files: local.files,
        directories: local.directories,
        logical_bytes: local.logical_bytes,
        stored_bytes: local.stored_bytes,
        prepare_timings: NativePrepareTimings::default(),
        changes: NativeSnapshotChanges::default(),
        source_observations: Vec::new(),
    })
}

#[allow(clippy::too_many_arguments)]
async fn upload_built_segments(
    client: &ArtifactStorageClient,
    project_id: &str,
    repo: &str,
    session: &NativeUploadSession,
    username: &str,
    token: &str,
    segments: &[BuiltSegment],
) -> Result<(usize, u64), SdkError> {
    let mut missing = HashSet::new();
    let query_size = session.max_segments_per_query.clamp(1, 4096);
    let segment_ids: Vec<String> = segments.iter().map(|segment| segment.id.to_hex()).collect();
    for ids in segment_ids.chunks(query_size) {
        let suffix = format!("fs/uploads/{}/segments/missing", session.session_id);
        let (request, _) = client.git_request(
            Method::POST,
            project_id,
            repo,
            Some(&suffix),
            username,
            token,
        )?;
        let response: MissingSegmentsResponse = expect_json(
            request
                .json(&MissingSegmentsRequest { segment_ids: ids })
                .send()
                .await?,
        )
        .await?;
        missing.extend(response.missing_segment_ids);
    }
    let missing_segments: Vec<_> = segments
        .iter()
        .filter(|segment| missing.contains(&segment.id.to_hex()))
        .collect();
    let mut uploaded_bytes = 0u64;
    for batch in missing_segments.chunks(NATIVE_SEGMENT_UPLOAD_CONCURRENCY) {
        let uploads = batch.iter().map(|segment| {
            upload_prepared_segment(
                client,
                project_id,
                repo,
                &session.session_id,
                username,
                token,
                segment,
            )
        });
        for result in futures::future::join_all(uploads).await {
            uploaded_bytes = uploaded_bytes.saturating_add(result?);
        }
    }
    Ok((missing_segments.len(), uploaded_bytes))
}

#[allow(clippy::too_many_arguments)]
async fn finish_native_push(
    client: &ArtifactStorageClient,
    project_id: &str,
    repo: &str,
    username: &str,
    token: &str,
    expected: Option<String>,
    expected_workspace: Option<String>,
    session: NativeUploadSession,
    prepared: PreparedNativeSnapshot,
    pipelined: Option<PipelinedSegmentReport>,
    metadata_staged: bool,
    options: NativePushOptions,
    reporter: Option<&NativeClientOperationReporter>,
) -> Result<NativePushReport, SdkError> {
    let upload_was_pipelined = pipelined.is_some();
    let (uploaded_segments, uploaded_bytes) = if let Some(report) = pipelined {
        (report.uploaded_ids.len(), report.uploaded_bytes)
    } else {
        let mut missing = HashSet::new();
        let segment_ids: Vec<String> = prepared
            .segments
            .iter()
            .map(|segment| segment.id.to_hex())
            .collect();
        let query_size = session.max_segments_per_query.clamp(1, 4096);
        for ids in segment_ids.chunks(query_size) {
            let suffix = format!("fs/uploads/{}/segments/missing", session.session_id);
            let (request, _) = client.git_request(
                Method::POST,
                project_id,
                repo,
                Some(&suffix),
                username,
                token,
            )?;
            let response: MissingSegmentsResponse = expect_json(
                request
                    .json(&MissingSegmentsRequest { segment_ids: ids })
                    .send()
                    .await?,
            )
            .await?;
            missing.extend(response.missing_segment_ids);
        }

        let mut uploaded_bytes = 0u64;
        let missing_indexes: Vec<usize> = prepared
            .segments
            .iter()
            .enumerate()
            .filter_map(|(index, segment)| missing.contains(&segment.id.to_hex()).then_some(index))
            .collect();
        // Aggregate segments are independent and direct PUTs do not consume service-body
        // bandwidth. Thirty-two streams fills a high-bandwidth object-store link while bounding open
        // temp files and HTTP bodies.
        for indexes in missing_indexes.chunks(32) {
            let uploads: Vec<_> = indexes
                .iter()
                .map(|index| {
                    upload_prepared_segment(
                        client,
                        project_id,
                        repo,
                        &session.session_id,
                        username,
                        token,
                        &prepared.segments[*index],
                    )
                })
                .collect();
            for result in futures::future::join_all(uploads).await {
                uploaded_bytes = uploaded_bytes.saturating_add(result?);
            }
        }
        (missing.len(), uploaded_bytes)
    };
    if !upload_was_pipelined && let Some(reporter) = reporter {
        reporter.note_upload_complete(uploaded_segments, uploaded_bytes);
    }
    note_progress(
        &options.progress,
        NativePushEvent::Negotiated {
            missing_segments: uploaded_segments,
            total_segments: prepared.segments.len(),
            transport: session.transport.clone(),
        },
    );
    note_progress(
        &options.progress,
        NativePushEvent::Uploaded {
            segments: uploaded_segments,
            stored_bytes: uploaded_bytes,
        },
    );

    if !metadata_staged {
        stage_native_metadata_batches(
            client,
            project_id,
            repo,
            &session.session_id,
            username,
            token,
            &prepared.pages,
            &prepared.recipes,
        )
        .await?;
        if let Some(reporter) = reporter {
            reporter.note_metadata_complete();
        }
    }

    let operation_id = options
        .operation_id
        .clone()
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    if let Some(reporter) = reporter {
        reporter.set_phase(NativeClientOperationPhase::Verifying);
    }
    let snapshot_request = SubmitNativeSnapshotRequest {
        root: prepared.root.to_hex(),
        parents: expected.iter().cloned().collect(),
        created_at_ms: None,
        message: options.message,
        operation_id: operation_id.clone(),
        changes: prepared.changes,
    };
    let submitted: SubmitNativeSnapshotResponse = {
        let suffix = format!("fs/uploads/{}/snapshots", session.session_id);
        let (request, _) = client.git_request(
            Method::POST,
            project_id,
            repo,
            Some(&suffix),
            username,
            token,
        )?;
        expect_json(request.json(&snapshot_request).send().await?).await?
    };
    note_progress(
        &options.progress,
        NativePushEvent::Verifying {
            snapshot_id: submitted.snapshot_id.clone(),
        },
    );
    wait_for_native_verification(
        client,
        project_id,
        repo,
        username,
        token,
        &session,
        &submitted.snapshot_id,
        &operation_id,
    )
    .await?;
    if let Some(reporter) = reporter {
        reporter.note_verification_complete();
        reporter.set_phase(NativeClientOperationPhase::Publishing);
    }

    let advance = if let Some(workspace_id) = options.workspace_id.as_deref() {
        publish_native_workspace_snapshot(
            client,
            project_id,
            repo,
            username,
            token,
            workspace_id,
            &session.session_id,
            &submitted.snapshot_id,
            expected_workspace.as_deref(),
            expected.as_deref(),
        )
        .await?
    } else {
        advance_native_head_with_credential(
            client,
            project_id,
            repo,
            username,
            token,
            &submitted.snapshot_id,
            expected.as_deref(),
        )
        .await?
    };
    let (previous_snapshot_id, published_snapshot_id) = match advance {
        NativeHeadAdvance::Published {
            previous_snapshot_id,
            snapshot_id,
        } => (previous_snapshot_id, snapshot_id),
        NativeHeadAdvance::Conflict {
            actual_snapshot_id,
            snapshot_id,
        } => {
            return Err(client_error(format!(
                "filesystem head changed to {} while snapshot {snapshot_id} was uploading; the verified snapshot was preserved and can be retried",
                actual_snapshot_id.as_deref().unwrap_or("empty")
            )));
        }
    };
    note_progress(
        &options.progress,
        NativePushEvent::Published {
            snapshot_id: published_snapshot_id.clone(),
        },
    );
    if let Some(reporter) = reporter {
        reporter.note_publish_complete();
    }
    Ok(NativePushReport {
        operation_id,
        snapshot_id: published_snapshot_id,
        previous_snapshot_id,
        files: prepared.files,
        directories: prepared.directories,
        logical_bytes: prepared.logical_bytes,
        stored_bytes: prepared.stored_bytes,
        total_segments: prepared.segments.len(),
        uploaded_segments,
        uploaded_bytes,
        transport: session.transport,
        client_timings: NativePushTimings::default(),
    })
}

#[allow(clippy::too_many_arguments)]
async fn upload_pipelined_segments(
    client: &ArtifactStorageClient,
    project_id: &str,
    repo: &str,
    session: &NativeUploadSession,
    username: &str,
    token: &str,
    mut completed: tokio::sync::mpsc::Receiver<Vec<PreparedSegmentUpload>>,
) -> Result<PipelinedSegmentReport, SdkError> {
    let query_limit = session.max_segments_per_query.clamp(1, 4096);
    let mut seen = HashSet::new();
    let mut uploaded_ids = HashSet::new();
    let mut uploaded_bytes = 0u64;
    let mut uploads = tokio::task::JoinSet::new();

    loop {
        let next_batch = if uploads.is_empty() {
            completed.recv().await
        } else {
            tokio::select! {
                biased;
                result = uploads.join_next() => {
                    let (id, bytes) = result
                        .expect("non-empty native upload set")
                        .map_err(|error| client_error(format!("native upload task failed: {error}")))??;
                    uploaded_ids.insert(id);
                    uploaded_bytes = uploaded_bytes.saturating_add(bytes);
                    continue;
                }
                batch = completed.recv() => batch,
            }
        };
        let Some(mut batch) = next_batch else {
            break;
        };
        while batch.len() < query_limit {
            let Ok(mut ready) = completed.try_recv() else {
                break;
            };
            batch.append(&mut ready);
        }
        batch.retain(|segment| seen.insert(segment.id));
        if batch.is_empty() {
            continue;
        }
        let segment_ids: Vec<String> = batch.iter().map(|segment| segment.id.to_hex()).collect();
        let suffix = format!("fs/uploads/{}/segments/missing", session.session_id);
        let (request, _) = client.git_request(
            Method::POST,
            project_id,
            repo,
            Some(&suffix),
            username,
            token,
        )?;
        let response: MissingSegmentsResponse = expect_json(
            request
                .json(&MissingSegmentsRequest {
                    segment_ids: &segment_ids,
                })
                .send()
                .await?,
        )
        .await?;
        let missing: HashSet<ObjectId> = response
            .missing_segment_ids
            .into_iter()
            .map(|id| {
                ObjectId::from_hex(&id)
                    .map_err(|_| client_error("server returned an invalid native segment id"))
            })
            .collect::<Result<_, SdkError>>()?;
        for segment in batch {
            if !missing.contains(&segment.id) {
                continue;
            }
            while uploads.len() >= NATIVE_SEGMENT_UPLOAD_CONCURRENCY {
                let (id, bytes) = uploads
                    .join_next()
                    .await
                    .expect("non-empty native upload set")
                    .map_err(|error| {
                        client_error(format!("native upload task failed: {error}"))
                    })??;
                uploaded_ids.insert(id);
                uploaded_bytes = uploaded_bytes.saturating_add(bytes);
            }
            let upload_client = client.clone();
            let project_id = project_id.to_string();
            let repo = repo.to_string();
            let session_id = session.session_id.clone();
            let username = username.to_string();
            let token = token.to_string();
            uploads.spawn(async move {
                let id = segment.id;
                let bytes = upload_prepared_segment_file(
                    &upload_client,
                    &project_id,
                    &repo,
                    &session_id,
                    &username,
                    &token,
                    segment,
                )
                .await?;
                Ok::<_, SdkError>((id, bytes))
            });
        }
    }
    while let Some(result) = uploads.join_next().await {
        let (id, bytes) = result
            .map_err(|error| client_error(format!("native upload task failed: {error}")))??;
        uploaded_ids.insert(id);
        uploaded_bytes = uploaded_bytes.saturating_add(bytes);
    }
    Ok(PipelinedSegmentReport {
        uploaded_ids,
        uploaded_bytes,
    })
}

async fn upload_prepared_segment(
    client: &ArtifactStorageClient,
    project_id: &str,
    repo: &str,
    session: &str,
    username: &str,
    token: &str,
    segment: &BuiltSegment,
) -> Result<u64, SdkError> {
    let temp = segment.temp.as_ref().ok_or_else(|| {
        client_error("pipelined native segment body was not handed to its uploader")
    })?;
    upload_prepared_segment_file(
        client,
        project_id,
        repo,
        session,
        username,
        token,
        PreparedSegmentUpload {
            id: segment.id,
            len: segment.len,
            records: segment.records.clone(),
            body: PreparedSegmentBody::File(temp.path().to_path_buf()),
        },
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn upload_prepared_segment_file(
    client: &ArtifactStorageClient,
    project_id: &str,
    repo: &str,
    session: &str,
    username: &str,
    token: &str,
    segment: PreparedSegmentUpload,
) -> Result<u64, SdkError> {
    for attempt in 0..2 {
        let segment_id = segment.id.to_hex();
        let suffix = format!("fs/uploads/{session}/segments");
        let (request, _) = client.git_request(
            Method::POST,
            project_id,
            repo,
            Some(&suffix),
            username,
            token,
        )?;
        let target: NativeSegmentTarget = expect_json(
            request
                .json(&SegmentTargetRequest {
                    segment_id: &segment_id,
                    stored_len: segment.len,
                })
                .send()
                .await?,
        )
        .await?;
        let body = prepared_segment_body(&segment.body).await?;
        match target.url {
            Some(url) => {
                let checksum = target.checksum_sha256.as_deref().ok_or_else(|| {
                    client_error("presigned native segment target omitted its SHA-256 header")
                })?;
                let response = client
                    .git_client
                    .put(url)
                    .header(reqwest::header::CONTENT_LENGTH, segment.len)
                    .header("x-amz-checksum-sha256", checksum)
                    .body(body)
                    .send()
                    .await?;
                if attempt == 0
                    && matches!(
                        response.status(),
                        StatusCode::FORBIDDEN | StatusCode::UNAUTHORIZED
                    )
                {
                    // A short-lived object-store signature can expire while a segment is
                    // streaming. Its immutable in-memory body or fallback tempfile can be replayed
                    // with a freshly minted random target without rereading the user's source.
                    continue;
                }
                expect_ok(response).await?;
                let suffix = format!(
                    "fs/uploads/{session}/segments/{}/register",
                    target.staging_id
                );
                let (request, _) = client.git_request(
                    Method::POST,
                    project_id,
                    repo,
                    Some(&suffix),
                    username,
                    token,
                )?;
                expect_ok(
                    request
                        .json(&RegisterSegmentRequest {
                            segment_id: &segment_id,
                            stored_len: segment.len,
                            records: native_segment_record_requests(&segment.records),
                        })
                        .send()
                        .await?,
                )
                .await?;
            }
            None => {
                let suffix = format!(
                    "fs/uploads/{session}/segments/{}?segment_id={}",
                    target.staging_id,
                    segment.id.to_hex()
                );
                let (request, _) = client.git_request(
                    Method::PUT,
                    project_id,
                    repo,
                    Some(&suffix),
                    username,
                    token,
                )?;
                expect_ok(
                    request
                        .header(reqwest::header::CONTENT_LENGTH, segment.len)
                        .body(body)
                        .send()
                        .await?,
                )
                .await?;
                let suffix = format!(
                    "fs/uploads/{session}/segments/{}/register",
                    target.staging_id
                );
                let (request, _) = client.git_request(
                    Method::POST,
                    project_id,
                    repo,
                    Some(&suffix),
                    username,
                    token,
                )?;
                expect_ok(
                    request
                        .json(&RegisterSegmentRequest {
                            segment_id: &segment_id,
                            stored_len: segment.len,
                            records: native_segment_record_requests(&segment.records),
                        })
                        .send()
                        .await?,
                )
                .await?;
            }
        }
        return Ok(segment.len);
    }
    unreachable!("the final presigned upload attempt returns")
}

async fn prepared_segment_body(body: &PreparedSegmentBody) -> Result<Body, SdkError> {
    match body {
        PreparedSegmentBody::Memory(bytes) => Ok(Body::from(bytes.clone())),
        PreparedSegmentBody::File(path) => {
            let file = tokio::fs::File::open(path).await?;
            // Multi-MiB frames keep syscall and Hyper framing overhead below the object-store
            // transfer cost for the non-pipelined tempfile fallback.
            Ok(Body::wrap_stream(
                tokio_util::io::ReaderStream::with_capacity(file, 4 * 1024 * 1024),
            ))
        }
    }
}

async fn wait_for_native_verification(
    client: &ArtifactStorageClient,
    project_id: &str,
    repo: &str,
    username: &str,
    token: &str,
    session: &NativeUploadSession,
    snapshot_id: &str,
    operation_id: &str,
) -> Result<(), SdkError> {
    loop {
        let suffix = format!(
            "fs/snapshots/{snapshot_id}/status?session={}&operation={}",
            urlencoding::encode(&session.session_id),
            urlencoding::encode(operation_id),
        );
        let (request, _) = client.git_request(
            Method::GET,
            project_id,
            repo,
            Some(&suffix),
            username,
            token,
        )?;
        let status: NativeSnapshotStatus = expect_json(request.send().await?).await?;
        match status.state.as_str() {
            "verified" => return Ok(()),
            "rejected" => {
                return Err(client_error(format!(
                    "native snapshot verification rejected: {}",
                    status
                        .reason
                        .as_deref()
                        .unwrap_or("unknown integrity error")
                )));
            }
            "pending" => {}
            state => {
                return Err(client_error(format!(
                    "server returned unknown native verification state {state:?}"
                )));
            }
        }
        if now_ms() >= session.expires_at_ms {
            return Err(client_error(
                "native snapshot verification did not finish before its upload session expired",
            ));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn advance_native_head_with_credential(
    client: &ArtifactStorageClient,
    project_id: &str,
    repo: &str,
    username: &str,
    token: &str,
    snapshot_id: &str,
    expected_snapshot_id: Option<&str>,
) -> Result<NativeHeadAdvance, SdkError> {
    let (request, _) = client.git_request(
        Method::PUT,
        project_id,
        repo,
        Some("fs/head"),
        username,
        token,
    )?;
    let response = request
        .json(&AdvanceNativeHeadRequest {
            snapshot_id,
            expected_snapshot_id,
        })
        .send()
        .await?;
    if response.status() != StatusCode::CONFLICT && !response.status().is_success() {
        return Err(response_error(response).await);
    }
    let response: AdvanceNativeHeadResponse = response
        .json()
        .await
        .map_err(|error| client_error(format!("bad native head response body: {error}")))?;
    if response.published {
        Ok(NativeHeadAdvance::Published {
            previous_snapshot_id: response.previous_snapshot_id,
            snapshot_id: response.snapshot_id,
        })
    } else {
        Ok(NativeHeadAdvance::Conflict {
            actual_snapshot_id: response.actual_snapshot_id,
            snapshot_id: response.snapshot_id,
        })
    }
}

#[derive(Serialize)]
struct AdvanceNativeWorkspaceRequest<'a> {
    snapshot_id: &'a str,
    expected_snapshot_id: Option<&'a str>,
    upload_session: Option<&'a str>,
}

#[derive(Serialize)]
struct PromoteNativeWorkspaceRequest<'a> {
    expected_head_snapshot_id: Option<&'a str>,
}

#[derive(Deserialize)]
struct NativeWorkspaceAdvanceResponse {
    #[serde(default)]
    previous_snapshot_id: Option<String>,
    #[serde(default)]
    actual_snapshot_id: Option<String>,
    #[serde(default)]
    preserved_snapshot_id: Option<String>,
    snapshot_id: Option<String>,
}

async fn publish_native_workspace_snapshot(
    client: &ArtifactStorageClient,
    project_id: &str,
    repo: &str,
    username: &str,
    token: &str,
    workspace_id: &str,
    upload_session: &str,
    snapshot_id: &str,
    expected_workspace_snapshot_id: Option<&str>,
    expected_head_snapshot_id: Option<&str>,
) -> Result<NativeHeadAdvance, SdkError> {
    let suffix = format!("fs/workspaces/{workspace_id}/snapshot");
    let (request, _) = client.git_request(
        Method::PUT,
        project_id,
        repo,
        Some(&suffix),
        username,
        token,
    )?;
    let response = request
        .json(&AdvanceNativeWorkspaceRequest {
            snapshot_id,
            expected_snapshot_id: expected_workspace_snapshot_id,
            upload_session: Some(upload_session),
        })
        .send()
        .await?;
    let conflict = response.status() == StatusCode::CONFLICT;
    if !conflict && !response.status().is_success() {
        return Err(response_error(response).await);
    }
    let workspace: NativeWorkspaceAdvanceResponse = response
        .json()
        .await
        .map_err(|error| client_error(format!("bad native workspace response: {error}")))?;
    if conflict {
        return Err(client_error(format!(
            "native workspace moved to {} while snapshot {} was uploading; snapshot {} was preserved",
            workspace.actual_snapshot_id.as_deref().unwrap_or("empty"),
            snapshot_id,
            workspace
                .preserved_snapshot_id
                .as_deref()
                .unwrap_or(snapshot_id)
        )));
    }

    let suffix = format!("fs/workspaces/{workspace_id}/promote");
    let (request, _) = client.git_request(
        Method::POST,
        project_id,
        repo,
        Some(&suffix),
        username,
        token,
    )?;
    let response = request
        .json(&PromoteNativeWorkspaceRequest {
            expected_head_snapshot_id,
        })
        .send()
        .await?;
    let conflict = response.status() == StatusCode::CONFLICT;
    if !conflict && !response.status().is_success() {
        return Err(response_error(response).await);
    }
    let promoted: NativeWorkspaceAdvanceResponse = response
        .json()
        .await
        .map_err(|error| client_error(format!("bad native workspace promote response: {error}")))?;
    if conflict {
        Ok(NativeHeadAdvance::Conflict {
            actual_snapshot_id: promoted.actual_snapshot_id,
            snapshot_id: promoted
                .preserved_snapshot_id
                .unwrap_or_else(|| snapshot_id.to_string()),
        })
    } else {
        Ok(NativeHeadAdvance::Published {
            previous_snapshot_id: promoted.previous_snapshot_id,
            snapshot_id: promoted
                .snapshot_id
                .unwrap_or_else(|| snapshot_id.to_string()),
        })
    }
}

fn note_progress(progress: &Option<NativePushProgress>, event: NativePushEvent) {
    if let Some(progress) = progress {
        progress(event);
    }
}

#[allow(clippy::too_many_arguments)]
async fn stage_native_metadata_batches(
    client: &ArtifactStorageClient,
    project_id: &str,
    repo: &str,
    session: &str,
    username: &str,
    token: &str,
    pages: &[DirectoryPage],
    recipes: &[ChunkRecipe],
) -> Result<NativeMetadataResponse, SdkError> {
    let suffix = format!("fs/uploads/{session}/metadata");
    let mut batches = Vec::new();
    let mut page_offset = 0usize;
    let mut recipe_offset = 0usize;
    if pages.is_empty() && recipes.is_empty() {
        batches.push((0, 0, 0, 0));
    }
    while page_offset < pages.len() || recipe_offset < recipes.len() {
        let (page_end, recipe_end) =
            next_metadata_batch(pages, recipes, page_offset, recipe_offset)?;
        batches.push((page_offset, page_end, recipe_offset, recipe_end));
        page_offset = page_end;
        recipe_offset = recipe_end;
    }

    // Metadata objects are content-addressed and each batch is independently idempotent. Sending
    // the byte-bounded batches concurrently removes a full network round trip per ~8 MiB page of
    // inline-heavy directory metadata while retaining input order in the combined response.
    let mut responses = futures::stream::iter(batches)
        .map(|(page_offset, page_end, recipe_offset, recipe_end)| {
            let suffix = &suffix;
            async move {
                super::ingest::with_transient_retries(|| async {
                    let (request, _) = client.git_request(
                        Method::POST,
                        project_id,
                        repo,
                        Some(suffix),
                        username,
                        token,
                    )?;
                    expect_json(
                        request
                            .json(&MetadataRequest {
                                pages: &pages[page_offset..page_end],
                                recipes: &recipes[recipe_offset..recipe_end],
                            })
                            .send()
                            .await?,
                    )
                    .await
                })
                .await
            }
        })
        .buffered(NATIVE_METADATA_UPLOAD_CONCURRENCY);
    let mut staged = NativeMetadataResponse {
        pages: Vec::with_capacity(pages.len()),
        recipes: Vec::with_capacity(recipes.len()),
    };
    while let Some(response) = responses.next().await {
        let response: NativeMetadataResponse = response?;
        staged.pages.extend(response.pages);
        staged.recipes.extend(response.recipes);
    }
    Ok(staged)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

async fn expect_json<T: serde::de::DeserializeOwned>(
    response: reqwest::Response,
) -> Result<T, SdkError> {
    if !response.status().is_success() {
        return Err(response_error(response).await);
    }
    response
        .json()
        .await
        .map_err(|error| SdkError::ClientError(format!("bad native response body: {error}")))
}

async fn expect_ok(response: reqwest::Response) -> Result<(), SdkError> {
    if response.status().is_success() {
        Ok(())
    } else {
        Err(response_error(response).await)
    }
}

async fn response_error(response: reqwest::Response) -> SdkError {
    let status = response.status();
    let message = response.text().await.unwrap_or_default();
    SdkError::ServerError { status, message }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn metadata() -> EntryMetadata {
        EntryMetadata {
            mode: 0o644,
            mtime_ns: 1_234_567_890,
            uid: None,
            gid: None,
            xattrs: Vec::new(),
        }
    }

    #[test]
    fn native_client_telemetry_wire_contains_only_bounded_aggregate_fields() {
        let request = NativeClientOperationRequest {
            format_ver: 1,
            phase: NativeClientOperationPhase::Completed,
            state: NativeClientOperationState::Succeeded,
            mode: NativeClientOperationMode::ColdDirectory,
            client_version: "0.5.75",
            client_os: "linux",
            client_arch: "x86_64",
            upload_session: Some("session-1".into()),
            transport: Some("presigned_put".into()),
            failure_code: None,
            metrics: NativeClientOperationMetrics {
                total_ms: 123,
                logical_bytes: 4_000,
                uploaded_bytes: 1_000,
                ..Default::default()
            },
        };
        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"mode\":\"cold_directory\""));
        assert!(json.contains("\"logical_bytes\":4000"));
        for forbidden in ["path", "filename", "content_id", "segment_id", "token"] {
            assert!(!json.contains(forbidden), "wire leaked field {forbidden:?}");
        }
    }

    fn page() -> DirectoryPage {
        DirectoryPage::Leaf {
            version: FORMAT_VERSION,
            entries: vec![
                DirectoryEntry {
                    name: b"a.txt".to_vec(),
                    metadata: metadata(),
                    data: EntryData::File {
                        size: 3,
                        content: FileContent::Inline(b"one".to_vec()),
                        hardlink_group: None,
                    },
                },
                DirectoryEntry {
                    name: b"subdir".to_vec(),
                    metadata: metadata(),
                    data: EntryData::Directory {
                        root: DirectoryPage::empty().id().unwrap(),
                    },
                },
            ],
        }
    }

    #[test]
    fn canonical_encoding_matches_artifact_storage_v1() {
        let page = page();
        assert_eq!(
            hex::encode(page.canonical_bytes().unwrap()),
            "00010205612e747874a403a48bb09909000000000300036f6e650006737562646972a403a48bb09909000000011fc6ecb40faf96e3afd97eb1f8675db9d9cec4abaf7c9234ba97cdd737c15351"
        );
        assert_eq!(
            page.id().unwrap().to_hex(),
            "4cf798651aba32646b64ba8cd7c84f2a7d5596b28a06c8fe3e3d6e8de5ee7263"
        );
        assert_eq!(
            ObjectId::segment(b"one").to_hex(),
            "7692c3ad3540bb803c020b3aee66cd8887123234ea0c6e7143c0add73ff431ed"
        );
        assert_eq!(
            ObjectId::file_content(b"one").to_hex(),
            "de96402d5a99b298272d66594805683e5bdffa42b3956aad73e24f8d16b94586"
        );
    }

    #[tokio::test]
    async fn native_delta_retries_rate_limits_and_reuses_renamed_root() {
        let reused_root = DirectoryPage::empty().id().unwrap();
        let root_entries = vec![
            DirectoryEntry {
                name: b"keep.txt".to_vec(),
                metadata: metadata(),
                data: EntryData::File {
                    size: 3,
                    content: FileContent::Inline(b"old".to_vec()),
                    hardlink_group: None,
                },
            },
            DirectoryEntry {
                name: b"old-dir".to_vec(),
                metadata: metadata(),
                data: EntryData::Directory { root: reused_root },
            },
        ];
        let response = serde_json::to_vec(&serde_json::json!({
            "entries": root_entries,
            "next_after": null,
        }))
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for request_index in 0..2 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                loop {
                    let mut bytes = [0u8; 4096];
                    let count = stream.read(&mut bytes).await.unwrap();
                    request.extend_from_slice(&bytes[..count]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                assert!(String::from_utf8_lossy(&request).contains("/project/p/repos/r/fs/tree?"));
                if request_index == 0 {
                    stream
                        .write_all(
                            b"HTTP/1.1 429 Too Many Requests\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
                        )
                        .await
                        .unwrap();
                    continue;
                }
                stream
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                            response.len()
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
                stream.write_all(&response).await.unwrap();
            }
        });
        let base = format!("http://{addr}");
        let api = crate::ClientBuilder::new(&base)
            .bearer_token("unused")
            .build()
            .unwrap();
        let client = ArtifactStorageClient::new(api, &base).unwrap();
        let mut entries = BTreeMap::new();
        entries.insert(
            "changed.txt".to_string(),
            DirectoryEntry {
                name: b"changed.txt".to_vec(),
                metadata: metadata(),
                data: EntryData::File {
                    size: 3,
                    content: FileContent::Inline(b"new".to_vec()),
                    hardlink_group: None,
                },
            },
        );
        let prepared = compose_native_changes(
            &client,
            "p",
            "r",
            "u",
            "t",
            &"a".repeat(64),
            PreparedLocalUpserts {
                entries,
                local_directories: BTreeSet::new(),
                recipes: Vec::new(),
                segments: Vec::new(),
                files: 1,
                directories: 0,
                logical_bytes: 3,
                stored_bytes: 0,
            },
            vec!["keep.txt".to_string()],
            vec![NativeRename {
                from: "old-dir".to_string(),
                to: "new-dir".to_string(),
            }],
        )
        .await
        .unwrap();
        server.await.unwrap();
        assert_eq!(prepared.pages.len(), 1);
        let DirectoryPage::Leaf { entries, .. } = &prepared.pages[0] else {
            panic!("small changed root should remain one leaf")
        };
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.name.as_slice())
                .collect::<Vec<_>>(),
            vec![b"changed.txt".as_slice(), b"new-dir".as_slice()]
        );
        let renamed = entries
            .iter()
            .find(|entry| entry.name == b"new-dir")
            .unwrap();
        assert_eq!(renamed.data, EntryData::Directory { root: reused_root });
    }

    #[test]
    fn native_scan_uses_only_gitignore_and_keeps_git_and_target() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(temp.path().join(".gitignore"), "ignored\n").unwrap();
        std::fs::create_dir(temp.path().join("ignored")).unwrap();
        std::fs::write(temp.path().join("ignored/nope"), b"excluded").unwrap();
        std::fs::create_dir(temp.path().join("target")).unwrap();
        std::fs::write(temp.path().join("target/cache"), b"rust cache").unwrap();
        std::fs::create_dir(temp.path().join(".git")).unwrap();
        std::fs::write(temp.path().join(".git/config"), b"not implicitly special").unwrap();
        std::fs::create_dir(temp.path().join("empty")).unwrap();
        let payload = vec![b'x'; MAX_INLINE_BYTES + 123];
        std::fs::write(temp.path().join("large.bin"), &payload).unwrap();

        let prepared =
            prepare_native_snapshot(temp.path(), 96 * 1024 * 1024, MAX_SEGMENT_BYTES).unwrap();
        let root = prepared
            .pages
            .iter()
            .find(|page| page.id().unwrap() == prepared.root)
            .unwrap();
        let DirectoryPage::Leaf { entries, .. } = root else {
            panic!("small root should be one leaf")
        };
        let names: Vec<&[u8]> = entries.iter().map(|entry| entry.name.as_slice()).collect();
        assert!(names.contains(&b".git".as_slice()));
        assert!(names.contains(&b".gitignore".as_slice()));
        assert!(names.contains(&b"target".as_slice()));
        assert!(names.contains(&b"empty".as_slice()));
        assert!(!names.contains(&b"ignored".as_slice()));
        let observed: BTreeSet<_> = prepared
            .source_observations
            .iter()
            .map(|entry| entry.path.as_str())
            .collect();
        assert!(observed.contains("target/cache"));
        assert!(observed.contains(".git/config"));
        assert!(observed.contains("empty"));
        assert!(!observed.contains("ignored/nope"));
        let large_observation = prepared
            .source_observations
            .iter()
            .find(|entry| entry.path == "large.bin")
            .unwrap();
        assert_eq!(large_observation.size, payload.len() as u64);
        assert!(
            (large_observation.mtime_secs, large_observation.mtime_nanos)
                <= (
                    large_observation.observed_at_secs,
                    large_observation.observed_at_nanos
                ),
            "cold manifest carries the racy-window observation boundary"
        );

        let large = entries
            .iter()
            .find(|entry| entry.name == b"large.bin")
            .unwrap();
        let EntryData::File {
            content: FileContent::Segment(slice),
            ..
        } = &large.data
        else {
            panic!("single-record file should directly reference its aggregate segment")
        };
        let segment = prepared
            .segments
            .iter()
            .find(|segment| segment.id == slice.segment)
            .unwrap();
        let bytes = std::fs::read(segment.temp.as_ref().unwrap().path()).unwrap();
        let stored =
            &bytes[slice.offset as usize..slice.offset as usize + slice.stored_len as usize];
        assert_eq!(zstd::stream::decode_all(stored).unwrap(), payload);
    }

    #[test]
    fn native_cold_scan_hands_off_memory_without_a_tempfile() {
        let temp = tempfile::tempdir().unwrap();
        let payload = vec![b'm'; MAX_INLINE_BYTES + 123];
        std::fs::write(temp.path().join("payload.bin"), &payload).unwrap();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(2);
        let measurements = Arc::new(NativePipelineMeasurements::new(std::time::Instant::now()));

        let prepared = prepare_native_snapshot_with_sender(
            temp.path(),
            96 * 1024 * 1024,
            MAX_SEGMENT_BYTES,
            Some(sender),
            Some(measurements.clone()),
            None,
        )
        .unwrap();

        assert_eq!(prepared.segments.len(), 1);
        assert!(prepared.segments[0].temp.is_none());
        let uploads = receiver.blocking_recv().unwrap();
        assert_eq!(uploads.len(), 1);
        let PreparedSegmentBody::Memory(bytes) = &uploads[0].body else {
            panic!("cold scan must hand an in-memory segment directly to the uploader")
        };
        assert_eq!(bytes.len() as u64, uploads[0].len);
        assert_eq!(ObjectId::segment(bytes), uploads[0].id);
        assert_eq!(zstd::stream::decode_all(bytes.as_ref()).unwrap(), payload);
        assert!(measurements.first_segment_ready_ms().is_some());
    }

    #[test]
    fn native_cold_scan_stops_when_uploader_disappears() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(
            temp.path().join("payload.bin"),
            vec![b'x'; MAX_INLINE_BYTES + 123],
        )
        .unwrap();
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        drop(receiver);
        let result = prepare_native_snapshot_with_sender(
            temp.path(),
            96 * 1024 * 1024,
            MAX_SEGMENT_BYTES,
            Some(sender),
            None,
            Some(Arc::new(AtomicBool::new(false))),
        );
        let Err(error) = result else {
            panic!("snapshot preparation unexpectedly survived a dropped uploader")
        };
        assert!(error.to_string().contains("uploader stopped"));
    }

    #[cfg(unix)]
    #[test]
    fn native_scan_reads_hardlinked_content_once() {
        let temp = tempfile::tempdir().unwrap();
        let payload = vec![b'h'; MAX_INLINE_BYTES + 123];
        std::fs::write(temp.path().join("first.bin"), &payload).unwrap();
        std::fs::hard_link(
            temp.path().join("first.bin"),
            temp.path().join("second.bin"),
        )
        .unwrap();

        let prepared =
            prepare_native_snapshot(temp.path(), 96 * 1024 * 1024, MAX_SEGMENT_BYTES).unwrap();
        assert_eq!(
            prepared.segments.len(),
            1,
            "the second directory entry must reuse the first inode's pending content"
        );
        let root = prepared
            .pages
            .iter()
            .find(|page| page.id().unwrap() == prepared.root)
            .unwrap();
        let DirectoryPage::Leaf { entries, .. } = root else {
            panic!("two hardlinks should fit in one directory page")
        };
        let hardlinks: Vec<_> = entries
            .iter()
            .filter_map(|entry| match &entry.data {
                EntryData::File {
                    content,
                    hardlink_group: Some(group),
                    ..
                } => Some((*group, content)),
                _ => None,
            })
            .collect();
        assert_eq!(hardlinks.len(), 2);
        assert_eq!(hardlinks[0], hardlinks[1]);
    }

    #[test]
    fn native_scan_reuses_exact_copy_content() {
        let temp = tempfile::tempdir().unwrap();
        let payload: Vec<u8> = (0..MAX_INLINE_BYTES + 4096)
            .map(|index| (index % 251) as u8)
            .collect();
        std::fs::write(temp.path().join("first.bin"), &payload).unwrap();
        std::fs::write(temp.path().join("second.bin"), &payload).unwrap();

        let prepared =
            prepare_native_snapshot(temp.path(), 96 * 1024 * 1024, MAX_SEGMENT_BYTES).unwrap();
        let root = prepared
            .pages
            .iter()
            .find(|page| page.id().unwrap() == prepared.root)
            .unwrap();
        let DirectoryPage::Leaf { entries, .. } = root else {
            panic!("two copied files should fit in one directory page")
        };
        let copies: Vec<_> = entries
            .iter()
            .filter_map(|entry| match &entry.data {
                EntryData::File {
                    content,
                    hardlink_group: None,
                    ..
                } => Some(content),
                _ => None,
            })
            .collect();
        assert_eq!(copies.len(), 2);
        assert_eq!(
            copies[0], copies[1],
            "separate inodes with identical bytes must share one immutable content reference"
        );
    }

    #[test]
    fn native_parallel_scan_is_deterministic() {
        let temp = tempfile::tempdir().unwrap();
        for directory in 0..64 {
            let nested = temp.path().join(format!("dir-{directory:02}/nested"));
            std::fs::create_dir_all(&nested).unwrap();
            let payload: Vec<u8> = (0..MAX_INLINE_BYTES + directory + 1)
                .map(|index| ((index + directory) % 251) as u8)
                .collect();
            std::fs::write(nested.join("payload.bin"), payload).unwrap();
        }

        let scan = |threads| {
            rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap()
                .install(|| {
                    prepare_native_snapshot(temp.path(), 96 * 1024 * 1024, MAX_SEGMENT_BYTES)
                        .unwrap()
                })
        };
        let first = scan(1);
        let second = scan(12);

        assert_eq!(first.root, second.root);
        assert_eq!(
            first
                .pages
                .iter()
                .map(|page| page.id().unwrap())
                .collect::<Vec<_>>(),
            second
                .pages
                .iter()
                .map(|page| page.id().unwrap())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            first
                .segments
                .iter()
                .map(|segment| (segment.id, segment.len))
                .collect::<Vec<_>>(),
            second
                .segments
                .iter()
                .map(|segment| (segment.id, segment.len))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn recipe_hash_plan_builds_non_degenerate_indexes() {
        let mut plan = RecipeHashPlan::new(MAX_RECIPE_PAGE_ENTRIES + 1).unwrap();
        let mut whole = Vec::new();
        for part in 0..=MAX_RECIPE_PAGE_ENTRIES {
            let bytes = [(part % 251) as u8];
            whole.extend_from_slice(&bytes);
            plan.note(part, &bytes).unwrap();
        }
        let pending = (0..=MAX_RECIPE_PAGE_ENTRIES)
            .map(|_| PendingSlice {
                segment_index: 0,
                offset: 0,
                stored_len: 1,
                logical_len: 1,
                content_id: ObjectId::file_content(b"x"),
            })
            .collect();
        let recipe = plan.finish(pending);
        assert_eq!(recipe.nodes[recipe.root].level, 1);
        assert_eq!(recipe.nodes[recipe.root].children.len(), 2);
        assert_eq!(
            recipe.nodes[recipe.root].content_id,
            ObjectId::file_content(&whole)
        );
        assert!(recipe.nodes.iter().all(|node| {
            node.level == 0 || (2..=MAX_RECIPE_PAGE_ENTRIES).contains(&node.children.len())
        }));
    }

    #[test]
    fn directory_pages_split_on_encoded_size() {
        let entries = (0..30)
            .map(|index| DirectoryEntry {
                name: format!("{index:02}").into_bytes(),
                metadata: metadata(),
                data: EntryData::File {
                    size: MAX_INLINE_BYTES as u64,
                    content: FileContent::Inline(vec![index as u8; MAX_INLINE_BYTES]),
                    hardlink_group: None,
                },
            })
            .collect();
        let mut pages = Vec::new();
        let root = build_directory_pages(entries, &mut pages).unwrap();
        let root = pages
            .iter()
            .find(|page| page.id().unwrap() == root)
            .unwrap();
        let DirectoryPage::Index { children, .. } = root else {
            panic!("encoded-size split should create an index")
        };
        assert!(children.len() >= 2);
        assert!(pages.iter().all(|page| page.canonical_bytes().is_ok()));
    }

    #[test]
    fn native_metadata_batches_bound_serialized_body() {
        let page = DirectoryPage::Leaf {
            version: FORMAT_VERSION,
            entries: vec![
                DirectoryEntry {
                    name: b"a".to_vec(),
                    metadata: metadata(),
                    data: EntryData::File {
                        size: MAX_INLINE_BYTES as u64,
                        content: FileContent::Inline(vec![b'a'; MAX_INLINE_BYTES]),
                        hardlink_group: None,
                    },
                },
                DirectoryEntry {
                    name: b"b".to_vec(),
                    metadata: metadata(),
                    data: EntryData::File {
                        size: MAX_INLINE_BYTES as u64,
                        content: FileContent::Inline(vec![b'b'; MAX_INLINE_BYTES]),
                        hardlink_group: None,
                    },
                },
            ],
        };
        let pages = vec![page; 1_100];
        let mut offset = 0usize;
        let mut batches = 0usize;
        while offset < pages.len() {
            let (end, recipe_end) = next_metadata_batch(&pages, &[], offset, 0).unwrap();
            assert!(end > offset);
            assert_eq!(recipe_end, 0);
            let body = serde_json::to_vec(&MetadataRequest {
                pages: &pages[offset..end],
                recipes: &[],
            })
            .unwrap();
            assert!(body.len() <= NATIVE_METADATA_REQUEST_TARGET_BYTES);
            assert!(end - offset <= NATIVE_METADATA_REQUEST_MAX_OBJECTS);
            offset = end;
            batches += 1;
        }
        assert!(batches >= 2, "large metadata must span multiple requests");
    }

    #[test]
    fn native_metadata_batches_bound_object_count() {
        let pages = vec![DirectoryPage::empty(); NATIVE_METADATA_REQUEST_MAX_OBJECTS + 1];
        let (page_end, recipe_end) = next_metadata_batch(&pages, &[], 0, 0).unwrap();
        assert_eq!(page_end, NATIVE_METADATA_REQUEST_MAX_OBJECTS);
        assert_eq!(recipe_end, 0);
    }

    #[tokio::test]
    async fn native_workspace_client_paginates_and_renews_its_lease() {
        fn workspace(id: &str) -> serde_json::Value {
            serde_json::json!({
                "workspace_id": id,
                "principal": "user:test",
                "base_snapshot_id": null,
                "latest_snapshot_id": null,
                "created_at_ms": 1,
                "updated_at_ms": 1,
                "expires_at_ms": 86_400_001u64,
                "read_only": false,
            })
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for request_index in 0..3 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                loop {
                    let mut bytes = [0u8; 4096];
                    let count = stream.read(&mut bytes).await.unwrap();
                    request.extend_from_slice(&bytes[..count]);
                    let Some(header_end) = request
                        .windows(4)
                        .position(|window| window == b"\r\n\r\n")
                        .map(|offset| offset + 4)
                    else {
                        continue;
                    };
                    let headers = String::from_utf8_lossy(&request[..header_end]);
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .and_then(|value| value.trim().parse::<usize>().ok())
                        })
                        .unwrap_or(0);
                    if request.len() >= header_end + content_length {
                        break;
                    }
                }
                let text = String::from_utf8_lossy(&request);
                let body = match request_index {
                    0 => {
                        assert!(text.contains("GET /project/p/repos/r/fs/workspaces?limit=1000 "));
                        serde_json::json!({
                            "workspaces": [workspace("ws-1")],
                            "next_after": "ws-1",
                        })
                    }
                    1 => {
                        assert!(text.contains(
                            "GET /project/p/repos/r/fs/workspaces?limit=1000&after=ws-1 "
                        ));
                        serde_json::json!({
                            "workspaces": [workspace("ws-2")],
                        })
                    }
                    _ => {
                        assert!(
                            text.contains("POST /project/p/repos/r/fs/workspaces/ws-1/heartbeat ")
                        );
                        assert!(
                            text.contains(&format!(
                                "\"ttl_seconds\":{NATIVE_WORKSPACE_TTL_SECONDS}"
                            ))
                        );
                        workspace("ws-1")
                    }
                };
                let body = serde_json::to_vec(&body).unwrap();
                stream
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
                stream.write_all(&body).await.unwrap();
            }
        });
        let base = format!("http://{addr}");
        let api = crate::ClientBuilder::new(&base)
            .bearer_token("unused")
            .build()
            .unwrap();
        let client = ArtifactStorageClient::new(api, &base).unwrap();
        let workspaces = client
            .list_native_workspaces_with_credential("p", "r", "user", "token")
            .await
            .unwrap();
        assert_eq!(
            workspaces
                .iter()
                .map(|workspace| workspace.workspace_id.as_str())
                .collect::<Vec<_>>(),
            vec!["ws-1", "ws-2"]
        );
        client
            .native_workspace_heartbeat_with_credential("p", "r", "ws-1", "user", "token")
            .await
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn native_workspace_publish_returns_the_reconciled_server_head() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for request_index in 0..2 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                loop {
                    let mut bytes = [0u8; 4096];
                    let count = stream.read(&mut bytes).await.unwrap();
                    request.extend_from_slice(&bytes[..count]);
                    let Some(header_end) = request
                        .windows(4)
                        .position(|window| window == b"\r\n\r\n")
                        .map(|offset| offset + 4)
                    else {
                        continue;
                    };
                    let headers = String::from_utf8_lossy(&request[..header_end]);
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .and_then(|value| value.trim().parse::<usize>().ok())
                        })
                        .unwrap_or(0);
                    if request.len() >= header_end + content_length {
                        break;
                    }
                }
                let text = String::from_utf8_lossy(&request);
                let body = if request_index == 0 {
                    assert!(text.contains("PUT /project/p/repos/r/fs/workspaces/ws/snapshot "));
                    serde_json::json!({
                        "previous_snapshot_id": "workspace-base",
                        "snapshot_id": "source-snapshot",
                    })
                } else {
                    assert!(text.contains("POST /project/p/repos/r/fs/workspaces/ws/promote "));
                    serde_json::json!({
                        "previous_snapshot_id": "concurrent-head",
                        "snapshot_id": "reconciled-head",
                    })
                };
                let body = serde_json::to_vec(&body).unwrap();
                stream
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
                stream.write_all(&body).await.unwrap();
            }
        });
        let base = format!("http://{addr}");
        let api = crate::ClientBuilder::new(&base)
            .bearer_token("unused")
            .build()
            .unwrap();
        let client = ArtifactStorageClient::new(api, &base).unwrap();
        let result = publish_native_workspace_snapshot(
            &client,
            "p",
            "r",
            "user",
            "token",
            "ws",
            "upload",
            "source-snapshot",
            Some("workspace-base"),
            Some("shared-base"),
        )
        .await
        .unwrap();
        assert_eq!(
            result,
            NativeHeadAdvance::Published {
                previous_snapshot_id: Some("concurrent-head".into()),
                snapshot_id: "reconciled-head".into(),
            }
        );
        server.await.unwrap();
    }
}
