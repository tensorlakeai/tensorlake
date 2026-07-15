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
use std::time::Duration;

use futures::StreamExt;
use ignore::WalkBuilder;
use reqwest::{Body, Method, StatusCode};
use serde::{Deserialize, Serialize};

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

const SEGMENT_DOMAIN: &[u8] = b"tensorlake.fs.segment.v1\0";
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

    pub fn segment_hasher() -> ObjectIdHasher {
        ObjectIdHasher::new(SEGMENT_DOMAIN)
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
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeRename {
    pub from: String,
    pub to: String,
}

/// A mounted-save delta relative to `NativePushOptions::expected_snapshot_id`.
#[derive(Clone, Debug, Default)]
pub struct NativeChangeSet {
    pub upserts: Vec<NativeLocalUpsert>,
    pub deletes: Vec<String>,
    pub renames: Vec<NativeRename>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativePushReport {
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
}

struct BuiltSegment {
    id: ObjectId,
    len: u64,
    temp: tempfile::NamedTempFile,
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

struct PreparedNativeSnapshot {
    root: ObjectId,
    pages: Vec<DirectoryPage>,
    recipes: Vec<ChunkRecipe>,
    segments: Vec<BuiltSegment>,
    files: usize,
    directories: usize,
    logical_bytes: u64,
    stored_bytes: u64,
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
}

struct OpenSegment {
    temp: tempfile::NamedTempFile,
    hasher: ObjectIdHasher,
    len: u64,
}

impl SegmentBuilder {
    fn new(target_bytes: u64, max_bytes: u64) -> Result<Self, SdkError> {
        if target_bytes == 0 || max_bytes == 0 || target_bytes > max_bytes {
            return Err(client_error(
                "server returned invalid native segment limits",
            ));
        }
        Ok(Self {
            target_bytes,
            max_bytes: max_bytes.min(MAX_SEGMENT_BYTES),
            current: None,
            complete: Vec::new(),
        })
    }

    fn append_record(
        &mut self,
        logical: &[u8],
        content_id: ObjectId,
    ) -> Result<PendingSlice, SdkError> {
        let stored = zstd::stream::encode_all(logical, 1)?;
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
            current.len > 0 && current.len + stored.len() as u64 > self.target_bytes
        }) {
            self.finish_current()?;
        }
        let current = self.current.get_or_insert(OpenSegment {
            temp: tempfile::NamedTempFile::new()?,
            hasher: ObjectId::segment_hasher(),
            len: 0,
        });
        if current.len + stored.len() as u64 > self.max_bytes {
            return Err(client_error(format!(
                "native aggregate segment would exceed {} bytes",
                self.max_bytes
            )));
        }
        let offset = current.len;
        current.temp.write_all(&stored)?;
        current.hasher.update(&stored);
        current.len += stored.len() as u64;
        Ok(PendingSlice {
            segment_index: self.complete.len(),
            offset,
            stored_len,
            logical_len: logical.len() as u64,
            content_id,
        })
    }

    fn finish_current(&mut self) -> Result<(), SdkError> {
        let Some(mut current) = self.current.take() else {
            return Ok(());
        };
        current.temp.flush()?;
        self.complete.push(BuiltSegment {
            id: current.hasher.finalize(),
            len: current.len,
            temp: current.temp,
        });
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<BuiltSegment>, SdkError> {
        self.finish_current()?;
        Ok(self.complete)
    }
}

fn prepare_native_snapshot(
    root: &Path,
    target_segment_bytes: u64,
    max_segment_bytes: u64,
) -> Result<PreparedNativeSnapshot, SdkError> {
    let root = root.canonicalize()?;
    if !root.is_dir() {
        return Err(client_error(format!(
            "native snapshot source {} is not a directory",
            root.display()
        )));
    }
    let mut segment_builder = SegmentBuilder::new(target_segment_bytes, max_segment_bytes)?;
    let mut scanned = Vec::new();
    let mut files = 0usize;
    let mut directories = 1usize;
    let mut logical_bytes = 0u64;
    let mut hardlinks: HashMap<[u8; 16], (PendingContent, u64, std::fs::Metadata)> = HashMap::new();

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

    for result in walker.build() {
        let entry = result.map_err(|error| client_error(error.to_string()))?;
        let path = entry.path();
        if path == root {
            continue;
        }
        let rel = path
            .strip_prefix(&root)
            .map_err(|_| client_error("snapshot walker escaped its root"))?
            .to_path_buf();
        let before = std::fs::symlink_metadata(path)?;
        let metadata = native_entry_metadata(path, &before)?;
        let file_type = before.file_type();
        let data = if file_type.is_dir() {
            directories += 1;
            ScannedData::Directory
        } else if file_type.is_symlink() {
            files += 1;
            let target = raw_path_bytes(&std::fs::read_link(path)?);
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
                        path.display()
                    )));
                }
                None => {
                    let content = read_file_once(path, &before, &mut segment_builder)?;
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
                path.display()
            )));
        };
        let after = std::fs::symlink_metadata(path)?;
        if !same_snapshot_stat(&before, &after) {
            return Err(client_error(format!(
                "{} changed while it was being snapshotted; retry",
                path.display()
            )));
        }
        scanned.push(ScannedEntry {
            rel,
            metadata,
            data,
        });
    }

    let segments = segment_builder.finish()?;
    let stored_bytes = segments.iter().map(|segment| segment.len).sum();
    let (root, pages, recipes) = build_metadata(scanned, &segments)?;
    Ok(PreparedNativeSnapshot {
        root,
        pages,
        recipes,
        segments,
        files,
        directories,
        logical_bytes,
        stored_bytes,
    })
}

fn prepare_local_upserts(
    upserts: Vec<NativeLocalUpsert>,
    target_segment_bytes: u64,
    max_segment_bytes: u64,
) -> Result<PreparedLocalUpserts, SdkError> {
    let mut segment_builder = SegmentBuilder::new(target_segment_bytes, max_segment_bytes)?;
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
        .div_ceil(MAX_SEGMENT_SLICE_LOGICAL_BYTES) as usize;
    let mut recipe_plan = RecipeHashPlan::new(expected_slices)?;
    let mut whole_hasher = ObjectId::file_content_hasher();
    let mut parts = Vec::with_capacity(expected_slices);
    let mut total_len = 0u64;
    let mut logical = prefix;
    loop {
        let remaining = MAX_SEGMENT_SLICE_LOGICAL_BYTES as usize - logical.len();
        Read::by_ref(&mut file)
            .take(remaining as u64)
            .read_to_end(&mut logical)?;
        if logical.is_empty() {
            break;
        }
        let part_index = parts.len();
        recipe_plan.note(part_index, &logical).map_err(|_| {
            client_error(format!(
                "{} grew while it was being snapshotted; retry",
                path.display()
            ))
        })?;
        let record_id = ObjectId::file_content(&logical);
        whole_hasher.update(&logical);
        total_len += logical.len() as u64;
        parts.push(segments.append_record(&logical, record_id)?);
        if logical.len() < MAX_SEGMENT_SLICE_LOGICAL_BYTES as usize {
            break;
        }
        logical = Vec::with_capacity(MAX_SEGMENT_SLICE_LOGICAL_BYTES as usize);
    }
    if parts.len() != expected_slices {
        return Err(client_error(format!(
            "{} changed size while it was being snapshotted; retry",
            path.display()
        )));
    }
    let content_id = whole_hasher.finalize();
    let recipe = recipe_plan.finish(parts);
    debug_assert_eq!(recipe.nodes[recipe.root].logical_len, total_len);
    debug_assert_eq!(recipe.nodes[recipe.root].content_id, content_id);
    Ok((
        PendingContent::Segments {
            logical_len: total_len,
            content_id,
            recipe,
        },
        total_len,
    ))
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

    let mut order: Vec<PathBuf> = directories.keys().cloned().collect();
    order.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    let mut roots = BTreeMap::new();
    let mut pages = Vec::new();
    let mut recipes = Vec::new();
    for directory in order {
        let children = directories.remove(&directory).unwrap_or_default();
        let mut entries = Vec::with_capacity(children.len());
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
        let root = build_directory_pages(entries, &mut pages)?;
        roots.insert(directory, root);
    }
    let root = roots
        .remove(&PathBuf::new())
        .ok_or_else(|| client_error("snapshot root was not built"))?;
    Ok((root, pages, recipes))
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
struct RegisterSegmentRequest<'a> {
    segment_id: &'a str,
    stored_len: u64,
}

#[derive(Clone, Debug, Serialize)]
struct MetadataRequest<'a> {
    pages: &'a [DirectoryPage],
    recipes: &'a [ChunkRecipe],
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
        options: NativePushOptions,
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

        let source = root.to_path_buf();
        let target_segment_bytes = session.target_segment_bytes;
        let max_segment_bytes = session.max_segment_bytes;
        let prepared = tokio::task::spawn_blocking(move || {
            prepare_native_snapshot(&source, target_segment_bytes, max_segment_bytes)
        })
        .await
        .map_err(|error| client_error(format!("native snapshot scanner failed: {error}")))??;
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
            options,
        )
        .await
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
        options: NativePushOptions,
    ) -> Result<NativePushReport, SdkError> {
        if changes.upserts.is_empty() && changes.deletes.is_empty() && changes.renames.is_empty() {
            return Err(client_error("native change set is empty"));
        }
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
        let upserts = changes.upserts;
        let target_segment_bytes = session.target_segment_bytes;
        let max_segment_bytes = session.max_segment_bytes;
        let local = tokio::task::spawn_blocking(move || {
            prepare_local_upserts(upserts, target_segment_bytes, max_segment_bytes)
        })
        .await
        .map_err(|error| client_error(format!("native delta scanner failed: {error}")))??;
        let prepared = compose_native_changes(
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
            options,
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
        expect_json(request.send().await?).await
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
        body: Body,
    ) -> Result<(), SdkError> {
        expect_ok(
            self.git_client
                .put(url)
                .header(reqwest::header::CONTENT_LENGTH, stored_len)
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
        let suffix = format!("fs/uploads/{session}/metadata");
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
                .json(&MetadataRequest { pages, recipes })
                .send()
                .await?,
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
        let (request, _) = self.git_request(
            Method::GET,
            project_id,
            repo,
            Some("fs/head"),
            &credential.git_username,
            &credential.token,
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
                    ttl_seconds: None,
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
        let (request, _) = self.git_request(
            Method::GET,
            project_id,
            repo,
            Some("fs/workspaces"),
            username,
            token,
        )?;
        let response: NativeWorkspaceList = expect_json(request.send().await?).await?;
        Ok(response.workspaces)
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
        username: &str,
        token: &str,
    ) -> Result<String, SdkError> {
        let expected_workspace = self
            .native_workspace_with_credential(project_id, repo, workspace_id, username, token)
            .await?
            .latest_snapshot_id;
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
            created_at_ms: None,
            message: format!(
                "Restore {}",
                &target_snapshot_id[..target_snapshot_id.len().min(12)]
            ),
            operation_id: uuid::Uuid::new_v4().to_string(),
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
            expected_workspace.as_deref(),
            Some(current_snapshot_id),
        )
        .await?
        {
            NativeHeadAdvance::Published { .. } => Ok(submitted.snapshot_id),
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
        let page: NativeTreePage = expect_json(response).await?;
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
    })
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
    options: NativePushOptions,
) -> Result<NativePushReport, SdkError> {
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
    note_progress(
        &options.progress,
        NativePushEvent::Negotiated {
            missing_segments: missing.len(),
            total_segments: prepared.segments.len(),
            transport: session.transport.clone(),
        },
    );

    let mut uploaded_bytes = 0u64;
    let missing_indexes: Vec<usize> = prepared
        .segments
        .iter()
        .enumerate()
        .filter_map(|(index, segment)| missing.contains(&segment.id.to_hex()).then_some(index))
        .collect();
    for indexes in missing_indexes.chunks(4) {
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
    note_progress(
        &options.progress,
        NativePushEvent::Uploaded {
            segments: missing.len(),
            stored_bytes: uploaded_bytes,
        },
    );

    let mut page_offset = 0usize;
    let mut recipe_offset = 0usize;
    while page_offset < prepared.pages.len() || recipe_offset < prepared.recipes.len() {
        let page_end = (page_offset + 4096).min(prepared.pages.len());
        let remaining = 4096 - (page_end - page_offset);
        let recipe_end = (recipe_offset + remaining).min(prepared.recipes.len());
        let suffix = format!("fs/uploads/{}/metadata", session.session_id);
        let (request, _) = client.git_request(
            Method::POST,
            project_id,
            repo,
            Some(&suffix),
            username,
            token,
        )?;
        let _: NativeMetadataResponse = expect_json(
            request
                .json(&MetadataRequest {
                    pages: &prepared.pages[page_offset..page_end],
                    recipes: &prepared.recipes[recipe_offset..recipe_end],
                })
                .send()
                .await?,
        )
        .await?;
        page_offset = page_end;
        recipe_offset = recipe_end;
    }

    let snapshot_request = SubmitNativeSnapshotRequest {
        root: prepared.root.to_hex(),
        parents: expected.iter().cloned().collect(),
        created_at_ms: None,
        message: options.message,
        operation_id: options
            .operation_id
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
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
    )
    .await?;

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
    let previous_snapshot_id = match advance {
        NativeHeadAdvance::Published {
            previous_snapshot_id,
            ..
        } => previous_snapshot_id,
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
            snapshot_id: submitted.snapshot_id.clone(),
        },
    );
    Ok(NativePushReport {
        snapshot_id: submitted.snapshot_id,
        previous_snapshot_id,
        files: prepared.files,
        directories: prepared.directories,
        logical_bytes: prepared.logical_bytes,
        stored_bytes: prepared.stored_bytes,
        total_segments: prepared.segments.len(),
        uploaded_segments: missing.len(),
        uploaded_bytes,
        transport: session.transport,
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
    for attempt in 0..2 {
        let suffix = format!("fs/uploads/{session}/segments");
        let (request, _) = client.git_request(
            Method::POST,
            project_id,
            repo,
            Some(&suffix),
            username,
            token,
        )?;
        let target: NativeSegmentTarget = expect_json(request.send().await?).await?;
        let file = tokio::fs::File::open(segment.temp.path()).await?;
        let body = Body::wrap_stream(tokio_util::io::ReaderStream::new(file));
        match target.url {
            Some(url) => {
                let response = client
                    .git_client
                    .put(url)
                    .header(reqwest::header::CONTENT_LENGTH, segment.len)
                    .body(body)
                    .send()
                    .await?;
                if attempt == 0
                    && matches!(
                        response.status(),
                        StatusCode::FORBIDDEN | StatusCode::UNAUTHORIZED
                    )
                {
                    // A short-lived object-store signature can expire while a large segment is
                    // streaming. The segment is already in a local temp file, so mint a fresh
                    // random target and replay it without rereading the user's source file.
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
                            segment_id: &segment.id.to_hex(),
                            stored_len: segment.len,
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
            }
        }
        return Ok(segment.len);
    }
    unreachable!("the final presigned upload attempt returns")
}

async fn wait_for_native_verification(
    client: &ArtifactStorageClient,
    project_id: &str,
    repo: &str,
    username: &str,
    token: &str,
    session: &NativeUploadSession,
    snapshot_id: &str,
) -> Result<(), SdkError> {
    loop {
        let suffix = format!(
            "fs/snapshots/{snapshot_id}/status?session={}",
            urlencoding::encode(&session.session_id)
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
            "ff637f16c476ddfab990720821fdd4269930d14d43ee6cd574613da58c8e9098"
        );
        assert_eq!(
            ObjectId::file_content(b"one").to_hex(),
            "de96402d5a99b298272d66594805683e5bdffa42b3956aad73e24f8d16b94586"
        );
    }

    #[tokio::test]
    async fn native_delta_reuses_renamed_root_and_rebuilds_only_parent() {
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
        let bytes = std::fs::read(segment.temp.path()).unwrap();
        let stored =
            &bytes[slice.offset as usize..slice.offset as usize + slice.stored_len as usize];
        assert_eq!(zstd::stream::decode_all(stored).unwrap(), payload);
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
}
