//! Resumable ingest client: push files to artifact storage as content-defined chunks.
//!
//! The unit of transfer is the CDC chunk, so resume state is *content*, not cursors: every step
//! is idempotent, and retrying a failed push re-negotiates and uploads only what the server still
//! lacks — across process restarts, pods, and even prior pushes of overlapping content. The
//! client never runs `git pack-objects` and holds at most one upload batch in memory, so pushes
//! of arbitrarily large trees work from small sandboxes.
//!
//! Protocol (server: artifact-storage `/project/{p}/repos/{r}/ingest/...`):
//! 1. open a session (returns the CDC parameters to chunk with and capability markers),
//! 2. negotiate which chunk hashes the server lacks,
//! 3. upload: mostly-missing files stream whole under file tokens, several files in flight —
//!    every request is independent server-side, so parallel requests are the throughput model;
//!    the server verifies + hashes in transit (and on `staged_small_files` servers deflates
//!    small files straight into pack entries at upload). Remaining missing chunks upload
//!    through the dedup path (presigned packs or service-mediated frames),
//! 4. commit by reference: tokened files publish their verified oid with zero read-back,
//!    known-oid files move no bytes at all, and bare chunk references are verified by
//!    server-side read-back.
//!
//! Idempotent requests auto-retry transient failures (transport errors, 5xx/429) with
//! backoff; large tokened offset uploads are the one deliberate exception (a lost success
//! would make the replayed offset conflict).

use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use ignore::WalkBuilder;
use reqwest::Method;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::Traced;
use crate::error::SdkError;

use super::ArtifactStorageClient;

/// One file in a push.
#[derive(Clone, Debug)]
pub struct PushFile {
    /// Path inside the repository (forward-slash separated).
    pub repo_path: String,
    pub source: PushSource,
    /// Octal git mode (`0o100644`, `0o100755`, `0o120000`); server defaults to `100644`.
    pub mode: Option<u32>,
    /// Delete this path instead of writing it (`source` is ignored).
    pub delete: bool,
}

// Variants get added (StablePrefix arrived after KnownOid, and more will follow); external
// matches must carry a wildcard arm so additions stay semver-compatible on this published crate.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum PushSource {
    /// Read (twice: hash pass + upload pass) from the local filesystem.
    Path(PathBuf),
    /// In-memory content.
    Bytes(Vec<u8>),
    /// Reference a blob **already present in the repository's network** by its git blob oid
    /// (40-hex) — no bytes are read, hashed, or uploaded. This is the repeat-content path:
    /// callers that know content is unchanged (e.g. from a mount manifest or a prior
    /// `PushReport::file_blob_oids`) commit it by reference. The server presence-checks the
    /// oid at commit and rejects unknown or non-blob oids.
    ///
    /// Requires a server advertising the `oid_files` ingest capability; `push_files` fails
    /// fast otherwise (an older server would ignore the field and publish an empty file).
    KnownOid(String),
    /// The append fast path: a file whose leading bytes are byte-identical to a previous push
    /// of the same content, described by that push's CDC chunk list. Only bytes past the
    /// prefix are read and chunked, so a growing log seals in O(appended bytes) instead of
    /// O(file size). The caller owns the stability claim (e.g. a mount overlay's write
    /// extents prove no write touched the prefix) and must end the prefix on a boundary a
    /// previous CDC pass produced (`PushReport::file_chunks`) — FastCDC restarts its state at
    /// every boundary, so chunking resumed there matches a full re-chunk exactly.
    ///
    /// These files always take the dedup upload path (never tokened), and their
    /// `PushReport::file_blob_oids` entry is empty: the blob oid is a whole-content hash this
    /// path deliberately never computes — the server derives identity from commit-time
    /// read-back of the chunk list. Falls back to a full read (as `Path`) when the file
    /// shrank below the prefix, the prefix is empty, or the file is under the server's
    /// small-file threshold.
    StablePrefix {
        path: PathBuf,
        /// `(sha256, size)` covering exactly the file's leading bytes, in file order.
        stable_chunks: Vec<([u8; 32], u32)>,
    },
}

/// Progress events, emitted in order. Rendering (progress bars, logs) is the caller's concern.
#[derive(Clone, Debug)]
pub enum PushEvent {
    /// Local chunk+hash progress (throttled; `files_done` of `files_total` finished so far).
    Chunking {
        files_done: usize,
        files_total: usize,
        bytes_hashed: u64,
    },
    /// All files chunked and hashed locally.
    Hashed {
        files: usize,
        chunks: usize,
        bytes: u64,
    },
    /// Negotiation complete: this many chunks (of the total) need uploading.
    Negotiated { missing: usize, total: usize },
    /// One upload batch accepted (`chunks`/`bytes` are cumulative for the push).
    UploadedBatch { chunks: usize, bytes: u64 },
    /// All bytes are on the server; the commit request is being published (tree build +
    /// read-back happen server-side, so large pushes dwell here).
    Committing { files: usize },
    /// The commit outlived the server's inline grace window and detached as a durable job.
    /// From here the push survives any disconnect: the job can be observed (or picked back
    /// up) out-of-band via `GET .../commits/jobs/{job_id}`.
    CommitDetached { job_id: String },
    /// Server-side progress of a detached commit job (async path), straight from the job's
    /// state machine. `done`/`total` are read-back chunk counts when the phase reports them.
    CommitProgress {
        phase: String,
        done: u64,
        total: u64,
    },
    /// The commit published.
    Committed { commit: String, ref_name: String },
}

pub type PushProgress = Arc<dyn Fn(PushEvent) + Send + Sync>;

#[derive(Clone)]
pub struct PushOptions {
    pub branch: String,
    pub message: String,
    /// Optional explicit parent commit (hex oid).
    pub base: Option<String>,
    /// Force-with-lease: require the branch to currently equal this hex oid.
    pub expect_oid: Option<String>,
    /// Upper bound on one upload request's payload.
    pub upload_batch_bytes: usize,
    pub progress: Option<PushProgress>,
    /// When set, the commit publishes as a snapshot on this workspace's ref
    /// (`workspaces/{id}/snapshots`) instead of advancing `branch`; `branch`/`base` are ignored.
    pub workspace_snapshot: Option<String>,
    /// Return every CDC-path file's chunk list in `PushReport::file_chunks`, so the caller can
    /// hand them back as `PushSource::StablePrefix` prefixes on the next push of the same
    /// content. Off by default: the lists cost memory proportional to the push.
    pub collect_file_chunks: bool,
}

impl Default for PushOptions {
    fn default() -> Self {
        PushOptions {
            branch: "main".to_string(),
            message: String::new(),
            base: None,
            expect_oid: None,
            upload_batch_bytes: 48 * 1024 * 1024,
            progress: None,
            workspace_snapshot: None,
            collect_file_chunks: false,
        }
    }
}

/// Outcome of a push.
// Fields get added (file_chunks most recently); non_exhaustive keeps external full-literal
// construction/destructuring from breaking on this published crate's lockstep releases.
#[non_exhaustive]
#[derive(Clone, Debug, Serialize)]
pub struct PushReport {
    pub commit: String,
    pub tree: String,
    pub ref_name: String,
    pub created: bool,
    pub files: usize,
    pub bytes_total: u64,
    pub chunks_total: usize,
    /// Chunks the server lacked at first negotiation (uploaded by this push).
    pub chunks_uploaded: usize,
    pub bytes_uploaded: u64,
    /// Client-computed git blob oid per file (`(repo_path, hex oid)`), from the chunk pass.
    /// Deletes carry an empty oid — as do `StablePrefix` fast-path files, which never hash
    /// their full content.
    pub file_blob_oids: Vec<(String, String)>,
    /// CDC chunk lists per content file (`(repo_path, [(sha256, size)])`), populated only
    /// when `PushOptions::collect_file_chunks` is set. The raw material for the next push's
    /// `PushSource::StablePrefix`. Never serialized: caller-local plumbing, not wire data.
    #[serde(skip_serializing)]
    pub file_chunks: Vec<(String, Vec<([u8; 32], u32)>)>,
}

#[derive(Deserialize)]
struct IngestSessionWire {
    session_id: String,
    cdc_min_bytes: usize,
    cdc_avg_bytes: usize,
    cdc_max_bytes: usize,
    max_hashes_per_query: usize,
    #[allow(dead_code)]
    max_chunk_bytes: usize,
    /// Server capability markers (absent on older servers): `staged_small_files` (small
    /// tokened files are deflated + staged at upload; their chunks are NOT registered for
    /// dedup), `oid_files` (the commit endpoint accepts `{path, mode, oid}` references),
    /// `batch_files` (the multi-file batch upload endpoint exists).
    #[serde(default)]
    features: Vec<String>,
    /// Files strictly under this take the batch endpoint; 0 on servers without it.
    #[serde(default)]
    small_file_max_bytes: u64,
    /// Cap on file records per batch request; 0 on servers without the endpoint.
    #[serde(default)]
    max_batch_files: usize,
}

#[derive(Serialize)]
struct MissingWire<'a> {
    hashes: &'a [String],
}

#[derive(Deserialize)]
struct MissingRespWire {
    missing: Vec<String>,
}

#[derive(Deserialize)]
struct IngestStagingWire {
    pack_id: String,
    /// Presigned PUT URL; absent when the backend cannot sign (fall back to chunk frames).
    url: Option<String>,
}

#[derive(Deserialize)]
struct BatchUploadRespWire {
    files: Vec<BatchFileRespWire>,
}

#[derive(Deserialize)]
struct BatchFileRespWire {
    token: String,
    /// Server-computed git blob oid — must match the client's own hash (integrity check).
    oid: String,
    #[allow(dead_code)]
    #[serde(default)]
    deduplicated: bool,
}

#[derive(Serialize)]
struct StagedEntryWire {
    /// 64-hex chunk hash (uncompressed content address).
    hash: String,
    /// Byte offset of the zstd frame within the staged pack.
    offset: u64,
    /// Compressed frame length.
    length: u32,
    /// Uncompressed chunk size.
    size: u32,
}

#[derive(Serialize)]
struct StagedRegisterWire {
    pack_id: String,
    entries: Vec<StagedEntryWire>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CommitJobReadBack {
    pub done: u64,
    pub total: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct CommitJobError {
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub retryable: bool,
}

/// A commit job's state machine as rendered by the server (submission response and polls).
/// This is the only commit protocol: both the branch-commit and workspace-snapshot endpoints
/// answer it, and success embeds the commit fields at the top level.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CommitJobStatus {
    pub job_id: String,
    pub state: String,
    #[serde(default)]
    pub phase: Option<String>,
    #[serde(default)]
    pub read_back: Option<CommitJobReadBack>,
    #[serde(default)]
    pub commit: Option<String>,
    #[serde(default)]
    pub tree: Option<String>,
    #[serde(default)]
    pub ref_name: Option<String>,
    #[serde(default)]
    pub parent: Option<String>,
    #[serde(default)]
    pub created: Option<bool>,
    #[serde(default)]
    pub error: Option<CommitJobError>,
}

/// zstd level for staged chunk frames — the server stores frames verbatim, so this matches its
/// own chunk compression default (speed/ratio balance; any valid zstd frame decodes).
const STAGED_ZSTD_LEVEL: i32 = 3;

#[derive(Serialize)]
struct CommitFileWire {
    path: String,
    /// Omitted when empty: a zero-byte file has no chunks and must publish as inline
    /// `content` instead (the server rejects an explicit empty chunk list), and deletes
    /// and oid references carry neither.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    chunks: Vec<CommitChunkWire>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    file_token: Option<String>,
    /// Reference an already-present blob by oid (no bytes move). Only sent to servers
    /// advertising the `oid_files` capability.
    #[serde(skip_serializing_if = "Option::is_none")]
    oid: Option<String>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    delete: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    mode: Option<String>,
}

#[derive(Serialize)]
struct CommitChunkWire {
    hash: String,
    size: u32,
}

#[derive(Serialize)]
struct CommitWire {
    #[serde(skip_serializing_if = "Option::is_none")]
    branch: Option<String>,
    message: String,
    session_id: String,
    files: Vec<CommitFileWire>,
    #[serde(skip_serializing_if = "Option::is_none")]
    base: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expect_oid: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct CommitByReferenceResponse {
    pub commit: String,
    pub tree: String,
    pub ref_name: String,
    pub parent: Option<String>,
    pub created: bool,
}

struct ChunkedFile {
    repo_path: String,
    source: PushSource,
    /// `(sha256, size)` in file order; empty for deletes and known-oid references.
    chunks: Vec<([u8; 32], u32)>,
    mode: Option<u32>,
    delete: bool,
    /// The file is a `PushSource::KnownOid` reference: commit by oid, move no bytes.
    known_oid: bool,
    blob_oid: String,
    /// The chunk list came from the CDC chunker (not the whole-file small path), so it is
    /// boundary-stable and reusable as a future `StablePrefix`.
    cdc: bool,
}

/// One streaming pass over the source produces both the CDC chunk list and the git blob oid
/// (`sha1("blob <len>\0" + bytes)`), so callers never read a file once for identity and again
/// for chunking.
fn chunk_source(
    source: &PushSource,
    min: usize,
    avg: usize,
    max: usize,
) -> Result<(Vec<([u8; 32], u32)>, String), SdkError> {
    let len: u64 = match source {
        PushSource::Path(p) => std::fs::metadata(p).map_err(io_err)?.len(),
        PushSource::Bytes(b) => b.len() as u64,
        PushSource::KnownOid(_) | PushSource::StablePrefix { .. } => {
            return Err(SdkError::ClientError(
                "source kind carries no full byte stream to chunk".to_string(),
            ));
        }
    };
    let reader: Box<dyn Read + Send + '_> = match source {
        PushSource::Path(p) => Box::new(std::fs::File::open(p).map_err(io_err)?),
        PushSource::Bytes(b) => Box::new(std::io::Cursor::new(b.as_slice())),
        PushSource::KnownOid(_) | PushSource::StablePrefix { .. } => {
            unreachable!("guarded above")
        }
    };
    let mut blob_hasher = BlobOidHasher::new(len);
    let mut out = Vec::new();
    for chunk in fastcdc::v2020::StreamCDC::new(reader, min as u32, avg as u32, max as u32) {
        let chunk = chunk.map_err(|e| SdkError::ClientError(format!("chunking failed: {e}")))?;
        let hash: [u8; 32] = Sha256::digest(&chunk.data).into();
        blob_hasher.update(&chunk.data);
        out.push((hash, chunk.data.len() as u32));
    }
    Ok((out, blob_hasher.finalize_hex()))
}

/// The append fast path behind [`PushSource::StablePrefix`]: keep the caller's prefix chunk
/// list verbatim and read only bytes past it. FastCDC restarts its rolling state at every
/// emitted boundary, so resuming at a previous pass's boundary yields exactly the chunks a
/// full re-chunk would from that offset. No blob oid comes out of this — the prefix is never
/// read (see the variant's docs).
fn chunk_stable_prefix(
    path: &Path,
    stable_chunks: &[([u8; 32], u32)],
    min: usize,
    avg: usize,
    max: usize,
) -> Result<Vec<([u8; 32], u32)>, SdkError> {
    use std::io::Seek as _;
    let stable_len: u64 = stable_chunks.iter().map(|(_, s)| *s as u64).sum();
    let mut file = std::fs::File::open(path).map_err(io_err)?;
    file.seek(std::io::SeekFrom::Start(stable_len))
        .map_err(io_err)?;
    let mut out = stable_chunks.to_vec();
    for chunk in fastcdc::v2020::StreamCDC::new(file, min as u32, avg as u32, max as u32) {
        let chunk = chunk.map_err(|e| SdkError::ClientError(format!("chunking failed: {e}")))?;
        let hash: [u8; 32] = Sha256::digest(&chunk.data).into();
        out.push((hash, chunk.data.len() as u32));
    }
    Ok(out)
}

fn io_err(e: std::io::Error) -> SdkError {
    SdkError::Io(e)
}

/// Git blob-oid hasher: `sha1("blob <len>\0" || bytes)`. This is the single piece of git's
/// object model the SDK needs, computed locally so the public crate carries no dependency on
/// the private `gsvc-codec` packfile codec (which is no longer vendored into this repo).
struct BlobOidHasher(sha1::Sha1);

impl BlobOidHasher {
    fn new(len: u64) -> BlobOidHasher {
        use sha1::Digest as _;
        let mut h = sha1::Sha1::new();
        h.update(format!("blob {len}\0").as_bytes());
        BlobOidHasher(h)
    }

    fn update(&mut self, data: &[u8]) {
        use sha1::Digest as _;
        self.0.update(data);
    }

    fn finalize_hex(self) -> String {
        use sha1::Digest as _;
        hex::encode(self.0.finalize())
    }
}

/// Retries (beyond the first attempt) for idempotent ingest requests.
const TRANSIENT_RETRIES: usize = 3;

/// Only transient failures retry: transport errors and 5xx/429 responses. 4xx rejections are
/// deterministic and never retried.
fn is_transient(e: &SdkError) -> bool {
    match e {
        SdkError::ServerError { status, .. } => status.is_server_error() || status.as_u16() == 429,
        SdkError::Http(_) | SdkError::Middleware(_) => true,
        _ => false,
    }
}

/// Run an **idempotent** request with backoff on transient failures. Safe wherever it is used
/// here because every wrapped operation replays cleanly: batch and small-token completion is
/// idempotent server-side (identical content → `AlreadyComplete`), untokened chunks and
/// presigned packs are content-addressed, staging targets and sessions mint fresh ids whose
/// orphans the server GC's, and commit submission reattaches through its idempotency key.
/// Large tokened offset requests are deliberately NOT wrapped — a lost success would make the
/// replayed offset conflict.
async fn with_transient_retries<T, F, Fut>(mut op: F) -> Result<T, SdkError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, SdkError>>,
{
    let mut delay = std::time::Duration::from_millis(250);
    let mut attempt = 0usize;
    loop {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) if attempt < TRANSIENT_RETRIES && is_transient(&e) => {
                attempt += 1;
                let jitter = std::time::Duration::from_millis(u64::from(rand::random::<u8>()));
                tokio::time::sleep(delay + jitter).await;
                delay = (delay * 4).min(std::time::Duration::from_secs(4));
            }
            Err(e) => return Err(e),
        }
    }
}

/// Whole-file identity pass for small staged files: one read yields the git blob oid and the
/// file's single upload frame. CDC buys nothing under the staged threshold — those chunks are
/// never registered for dedup server-side — so small files skip the chunker (and, downstream,
/// the `missing` negotiation) entirely.
fn chunk_source_whole(source: &PushSource) -> Result<(Vec<([u8; 32], u32)>, String), SdkError> {
    let owned: Vec<u8>;
    let data: &[u8] = match source {
        PushSource::Path(p) => {
            owned = std::fs::read(p).map_err(io_err)?;
            &owned
        }
        PushSource::Bytes(b) => b,
        PushSource::KnownOid(_) | PushSource::StablePrefix { .. } => {
            return Err(SdkError::ClientError(
                "source kind carries no full byte stream to hash".to_string(),
            ));
        }
    };
    let mut blob = BlobOidHasher::new(data.len() as u64);
    blob.update(&data);
    // An empty file has an empty chunk list (like the CDC path) so it publishes as inline
    // `content` — a zero-length chunk frame is rejected by the server.
    let chunks = if data.is_empty() {
        Vec::new()
    } else {
        let hash: [u8; 32] = Sha256::digest(&data).into();
        vec![(hash, data.len() as u32)]
    };
    Ok((chunks, blob.finalize_hex()))
}

/// A chunk-bearing source re-opened for the upload pass. Chunks are visited in file order;
/// [`UploadReader::skip`] seeks past chunks the upload does not need, so a mostly-deduped file
/// (an appended log especially) never re-reads the bytes the server already has.
enum UploadReader<'a> {
    File(std::fs::File),
    // Borrowed, not cloned: pushes are zero-copy over in-memory sources.
    Bytes(std::io::Cursor<&'a [u8]>),
}

impl<'a> UploadReader<'a> {
    fn open(source: &'a PushSource) -> Result<UploadReader<'a>, SdkError> {
        match source {
            PushSource::Path(p) => Ok(UploadReader::File(std::fs::File::open(p).map_err(io_err)?)),
            PushSource::StablePrefix { path, .. } => Ok(UploadReader::File(
                std::fs::File::open(path).map_err(io_err)?,
            )),
            PushSource::Bytes(b) => Ok(UploadReader::Bytes(std::io::Cursor::new(b.as_slice()))),
            PushSource::KnownOid(_) => Err(SdkError::ClientError(
                "known-oid sources carry no bytes to upload".to_string(),
            )),
        }
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), SdkError> {
        match self {
            UploadReader::File(f) => f.read_exact(buf).map_err(io_err),
            UploadReader::Bytes(c) => c.read_exact(buf).map_err(io_err),
        }
    }

    fn skip(&mut self, n: u64) -> Result<(), SdkError> {
        use std::io::Seek as _;
        match self {
            UploadReader::File(f) => f.seek(std::io::SeekFrom::Current(n as i64)).map(|_| ()),
            UploadReader::Bytes(c) => c.seek(std::io::SeekFrom::Current(n as i64)).map(|_| ()),
        }
        .map_err(io_err)
    }
}

/// Which files take the tokened (verified-at-upload) path: content-bearing files whose bytes the
/// server mostly lacks. Mostly-present files stay on the dedup path (upload only missing chunks,
/// accept commit-time read-back proportional to reused bytes); deletes and known-oid references
/// upload nothing.
fn elect_tokened(
    chunked: &[ChunkedFile],
    missing: &std::collections::HashSet<[u8; 32]>,
    small_max: u64,
) -> Vec<bool> {
    chunked
        .iter()
        .map(|file| {
            if file.delete || file.known_oid || file.chunks.is_empty() {
                return false;
            }
            // Stable-prefix files never computed a blob oid (tokening's identity check needs
            // one) and exist to avoid re-reading their prefix (tokening streams every byte).
            if matches!(file.source, PushSource::StablePrefix { .. }) {
                return false;
            }
            let total: u64 = file.chunks.iter().map(|(_, s)| *s as u64).sum();
            // Small staged files are always tokened: they skip negotiation (their chunks are
            // never registered server-side), so there is no missing-ratio to consult.
            if small_max > 0 && total < small_max {
                return true;
            }
            let missing_bytes: u64 = file
                .chunks
                .iter()
                .filter(|(h, _)| missing.contains(h))
                .map(|(_, s)| *s as u64)
                .sum();
            missing_bytes * 2 >= total
        })
        .collect()
}

/// The chunks that must upload through the dedup path: missing chunks referenced by at least
/// one NON-tokened content file. Tokened uploads never count as coverage for other files —
/// on servers with upload-time staging, a small tokened file's chunks are not registered, and
/// the client does not know the server's small/large threshold.
fn dedup_needed_chunks(
    chunked: &[ChunkedFile],
    tokened: &[bool],
    missing: &std::collections::HashSet<[u8; 32]>,
) -> std::collections::HashSet<[u8; 32]> {
    chunked
        .iter()
        .zip(tokened)
        .filter(|(file, tokened)| !**tokened && !file.delete && !file.known_oid)
        .flat_map(|(file, _)| file.chunks.iter().map(|(h, _)| *h))
        .filter(|h| missing.contains(h))
        .collect()
}

fn collect_worktree_files(root: &Path, out: &mut Vec<PushFile>) -> Result<(), SdkError> {
    let mut walker = WalkBuilder::new(root);
    walker
        // Git tracks dotfiles by default; only ignore patterns should exclude them.
        .hidden(false)
        // `git add` does not read ripgrep-style `.ignore` files.
        .ignore(false)
        // Keep snapshot roots self-contained instead of reading parent `.gitignore` files.
        .parents(false)
        // Honor Git's ignore sources, including `.gitignore` in roots that are not full repos.
        .require_git(false)
        .git_ignore(true)
        .git_exclude(true)
        .git_global(true)
        .follow_links(false)
        .sort_by_file_path(|a, b| a.cmp(b))
        .filter_entry(|entry| entry.file_name() != ".git");

    for entry in walker.build() {
        let entry = entry.map_err(|err| SdkError::ClientError(err.to_string()))?;
        let path = entry.path();
        let meta = std::fs::symlink_metadata(path)?;
        if meta.is_dir() {
            continue;
        }

        let repo_path = repo_path_for_worktree(root, path);
        if repo_path.is_empty() {
            continue;
        }

        let file_type = meta.file_type();
        if file_type.is_symlink() {
            let target = std::fs::read_link(path)?;
            out.push(PushFile {
                repo_path,
                source: PushSource::Bytes(symlink_target_bytes(&target)),
                mode: Some(0o120000),
                delete: false,
            });
        } else if file_type.is_file() {
            out.push(PushFile {
                repo_path,
                source: PushSource::Path(path.to_path_buf()),
                mode: Some(regular_file_mode(&meta)),
                delete: false,
            });
        }
    }

    Ok(())
}

fn regular_file_mode(meta: &std::fs::Metadata) -> u32 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if meta.permissions().mode() & 0o111 != 0 {
            0o100755
        } else {
            0o100644
        }
    }
    #[cfg(not(unix))]
    {
        let _ = meta;
        0o100644
    }
}

fn symlink_target_bytes(target: &Path) -> Vec<u8> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        target.as_os_str().as_bytes().to_vec()
    }
    #[cfg(not(unix))]
    {
        target.to_string_lossy().into_owned().into_bytes()
    }
}

/// Repo path for a file under a worktree root, using forward slashes and only normal path
/// components. This keeps `.` and `..` out of server commit payloads.
fn repo_path_for_worktree(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .components()
        .filter_map(|c| match c {
            Component::Normal(part) => Some(part.to_string_lossy()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}

impl ArtifactStorageClient {
    /// Push every Git-visible file under `root` as one commit. `.git` is skipped, `.gitignore`
    /// rules are honored, repository paths are rooted at `root`, and regular-file executable bits
    /// plus symlinks are preserved.
    #[allow(clippy::too_many_arguments)]
    pub async fn push_worktree(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
        root: impl AsRef<Path>,
        opts: PushOptions,
    ) -> Result<Traced<PushReport>, SdkError> {
        let root = root.as_ref();
        let mut files = Vec::new();
        collect_worktree_files(root, &mut files)?;
        if files.is_empty() {
            return Err(SdkError::ClientError(
                "no files found under the Git worktree".to_string(),
            ));
        }
        self.push_files(project_id, repo, git_username, git_token, files, opts)
            .await
    }

    /// Push a set of files as one commit, transferring only chunks the server lacks. Safe to
    /// retry wholesale on any failure: completed work is discovered, not redone.
    #[allow(clippy::too_many_arguments)]
    pub async fn push_files(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
        files: Vec<PushFile>,
        opts: PushOptions,
    ) -> Result<Traced<PushReport>, SdkError> {
        let emit = |ev: PushEvent| {
            if let Some(p) = &opts.progress {
                p(ev)
            }
        };

        // 1. Session: the server dictates chunking parameters and limits.
        let session: IngestSessionWire = with_transient_retries(|| async {
            let (req, _trace) = self.git_request(
                Method::POST,
                project_id,
                repo,
                Some("ingest/sessions"),
                git_username,
                git_token,
            )?;
            expect_json(req.send().await?).await
        })
        .await?;

        // 2. Chunk + hash every file locally: one streaming pass per file yields the CDC chunk
        //    list and the git blob oid together, files fanned across blocking threads.
        let (min, avg, max) = (
            session.cdc_min_bytes,
            session.cdc_avg_bytes,
            session.cdc_max_bytes,
        );
        // Files under the server's staged threshold hash whole (single frame, one read):
        // their chunks never register server-side, so CDC granularity and negotiation are
        // pure overhead for them. 0 (older server) keeps everything on the CDC path.
        let small_max = session.small_file_max_bytes;
        let hash_parallelism = std::thread::available_parallelism()
            .map(|n| n.get().min(8))
            .unwrap_or(4);
        let chunked: Vec<ChunkedFile> = {
            use futures::StreamExt as _;
            let files_total = files.len();
            let mut stream = futures::stream::iter(files.into_iter().map(|f| async move {
                if f.delete {
                    return Ok::<ChunkedFile, SdkError>(ChunkedFile {
                        repo_path: f.repo_path,
                        source: f.source,
                        chunks: Vec::new(),
                        mode: f.mode,
                        delete: true,
                        known_oid: false,
                        blob_oid: String::new(),
                        cdc: false,
                    });
                }
                if let PushSource::KnownOid(oid) = &f.source {
                    let oid = oid.to_ascii_lowercase();
                    if oid.len() != 40 || !oid.bytes().all(|b| b.is_ascii_hexdigit()) {
                        return Err(SdkError::ClientError(format!(
                            "known oid for {:?} is not a 40-hex git oid: {oid:?}",
                            f.repo_path
                        )));
                    }
                    return Ok(ChunkedFile {
                        repo_path: f.repo_path,
                        blob_oid: oid,
                        source: f.source,
                        chunks: Vec::new(),
                        mode: f.mode,
                        delete: false,
                        known_oid: true,
                        cdc: false,
                    });
                }
                let mut f = f;
                let source_len: u64 = match &f.source {
                    PushSource::Path(p) => std::fs::metadata(p).map_err(io_err)?.len(),
                    PushSource::Bytes(b) => b.len() as u64,
                    PushSource::StablePrefix { path, .. } => {
                        std::fs::metadata(path).map_err(io_err)?.len()
                    }
                    PushSource::KnownOid(_) => unreachable!("handled above"),
                };
                let whole = small_max > 0 && source_len < small_max;
                // A stable prefix is only usable on the CDC path and only while the file still
                // covers it; otherwise demote to a plain path so every downstream stage (small
                // staging, tokened election, oid computation) treats the file normally.
                if let PushSource::StablePrefix {
                    path,
                    stable_chunks,
                } = &f.source
                {
                    let stable_len: u64 = stable_chunks.iter().map(|(_, s)| *s as u64).sum();
                    if whole || stable_chunks.is_empty() || stable_len > source_len {
                        f.source = PushSource::Path(path.clone());
                    }
                }
                tokio::task::spawn_blocking(move || {
                    let (chunks, blob_oid, cdc) = match &f.source {
                        PushSource::StablePrefix {
                            path,
                            stable_chunks,
                        } => (
                            chunk_stable_prefix(path, stable_chunks, min, avg, max)?,
                            String::new(),
                            true,
                        ),
                        source if whole => {
                            let (chunks, oid) = chunk_source_whole(source)?;
                            (chunks, oid, false)
                        }
                        source => {
                            let (chunks, oid) = chunk_source(source, min, avg, max)?;
                            (chunks, oid, true)
                        }
                    };
                    Ok(ChunkedFile {
                        repo_path: f.repo_path,
                        source: f.source,
                        chunks,
                        mode: f.mode,
                        delete: false,
                        known_oid: false,
                        blob_oid,
                        cdc,
                    })
                })
                .await
                .map_err(|e| SdkError::ClientError(format!("chunking task failed: {e}")))?
            }))
            .buffered(hash_parallelism);
            // Stream results so progress can flow while hashing runs; a kernel-scale tree
            // spends tens of seconds here and a silent spinner reads as a hang.
            let mut out: Vec<ChunkedFile> = Vec::with_capacity(files_total);
            let mut bytes_hashed = 0u64;
            let mut last_emit = std::time::Instant::now();
            while let Some(result) = stream.next().await {
                let file: ChunkedFile = result?;
                bytes_hashed += file.chunks.iter().map(|(_, s)| *s as u64).sum::<u64>();
                out.push(file);
                if last_emit.elapsed() >= std::time::Duration::from_millis(100) {
                    last_emit = std::time::Instant::now();
                    emit(PushEvent::Chunking {
                        files_done: out.len(),
                        files_total,
                        bytes_hashed,
                    });
                }
            }
            out
        };
        // Known-oid files require explicit server support: an older server ignores the
        // unknown `oid` field and would silently publish an EMPTY file at the path.
        if chunked.iter().any(|f| f.known_oid) && !session.features.iter().any(|f| f == "oid_files")
        {
            return Err(SdkError::ClientError(
                "this artifact-storage server does not support known-oid file references \
                 (`oid_files` capability missing); re-push the content instead"
                    .to_string(),
            ));
        }
        let total_chunks: usize = chunked.iter().map(|f| f.chunks.len()).sum();
        let total_bytes: u64 = chunked
            .iter()
            .flat_map(|f| f.chunks.iter().map(|(_, s)| *s as u64))
            .sum();
        emit(PushEvent::Hashed {
            files: chunked.len(),
            chunks: total_chunks,
            bytes: total_bytes,
        });

        // 3. Negotiate: which distinct hashes does the server lack? Small staged files are
        // excluded — their chunks are never registered, so the answer is always "missing" and
        // the round trips are wasted; they take the tokened path unconditionally.
        let is_small = |f: &ChunkedFile| -> bool {
            small_max > 0
                && f.chunks.iter().map(|(_, s)| *s as u64).sum::<u64>() < small_max
                && !f.delete
                && !f.known_oid
                && !f.chunks.is_empty()
        };
        let mut distinct: Vec<[u8; 32]> = chunked
            .iter()
            .filter(|f| !is_small(f))
            .flat_map(|f| f.chunks.iter().map(|(h, _)| *h))
            .collect();
        distinct.sort_unstable();
        distinct.dedup();
        let missing = self
            .negotiate_missing(
                project_id,
                repo,
                git_username,
                git_token,
                &session,
                &distinct,
            )
            .await?;
        emit(PushEvent::Negotiated {
            missing: missing.len(),
            total: distinct.len(),
        });

        // 4. Upload. Per file, pick the cheapest identity path (artifact_storage#26 + #57):
        //    known-oid files move no bytes at all; a file the server mostly lacks streams IN
        //    FULL under a file token — the server hashes it during upload (and, on servers with
        //    upload-time staging, deflates small files straight into pack entries), so the
        //    commit needs zero read-back; a file the server mostly has uploads only its missing
        //    chunks and accepts server-side read-back proportional to the reused bytes.
        //
        //    Every tokened request is independent server-side (any pod, no ordering), so
        //    tokened files upload several at a time — parallel requests are the throughput
        //    model. Identical content shares one token: the server accepts several files
        //    presenting the same completed token, so each distinct blob uploads once.
        let (mut uploaded_chunks, mut uploaded_bytes) = (0usize, 0u64);
        let mut file_tokens: Vec<Option<String>> = vec![None; chunked.len()];
        let tokened = elect_tokened(&chunked, &missing, small_max);
        {
            let mut token_by_oid: std::collections::HashMap<&str, String> =
                std::collections::HashMap::new();
            let mut owners: Vec<usize> = Vec::new();
            for (i, file) in chunked.iter().enumerate() {
                if !tokened[i] {
                    continue;
                }
                let token = token_by_oid
                    .entry(file.blob_oid.as_str())
                    .or_insert_with(|| {
                        owners.push(i);
                        format!("f{i}-{}", hex::encode(&file.chunks[0].0[..8]))
                    });
                file_tokens[i] = Some(token.clone());
            }
            use futures::StreamExt as _;
            const TOKENED_UPLOADS_IN_FLIGHT: usize = 4;
            // Small files go through the BATCH endpoint when the server has one: many whole
            // files per request, staged into one segment server-side — per-request overhead
            // amortizes across the group and the contiguous segment is what commit-time
            // compose copies instead of re-uploading. Group bodies stay modest (16 MiB) so
            // in-flight memory is bounded and the server keeps its single-put segment shape.
            let batch_capable = session.features.iter().any(|f| f == "batch_files")
                && session.small_file_max_bytes > 0
                && session.max_batch_files > 0;
            let mut batch_owners: Vec<usize> = Vec::new();
            let mut single_owners: Vec<usize> = Vec::new();
            for i in owners {
                let total: u64 = chunked[i].chunks.iter().map(|(_, s)| *s as u64).sum();
                if batch_capable && total < session.small_file_max_bytes {
                    batch_owners.push(i);
                } else {
                    single_owners.push(i);
                }
            }
            const BATCH_GROUP_BYTES: usize = 16 * 1024 * 1024;
            let mut groups: Vec<Vec<usize>> = Vec::new();
            {
                let mut cur: Vec<usize> = Vec::new();
                let mut cur_bytes = 0usize;
                for &i in &batch_owners {
                    let file = &chunked[i];
                    let record_bytes: usize = 14
                        + file_tokens[i].as_ref().map(String::len).unwrap_or(0)
                        + file
                            .chunks
                            .iter()
                            .map(|(_, s)| 36 + *s as usize)
                            .sum::<usize>();
                    if !cur.is_empty()
                        && (cur_bytes + record_bytes > BATCH_GROUP_BYTES
                            || cur.len() >= session.max_batch_files)
                    {
                        groups.push(std::mem::take(&mut cur));
                        cur_bytes = 0;
                    }
                    cur.push(i);
                    cur_bytes += record_bytes;
                }
                if !cur.is_empty() {
                    groups.push(cur);
                }
            }
            let mut batch_results = futures::stream::iter(groups.into_iter().map(|group| {
                let session_id = session.session_id.as_str();
                let file_tokens = &file_tokens;
                let chunked = &chunked;
                async move {
                    let mut body = Vec::new();
                    let mut group_chunks = 0usize;
                    let mut group_bytes = 0u64;
                    let mut expected: Vec<(String, String)> = Vec::new();
                    for &i in &group {
                        let file = &chunked[i];
                        let token = file_tokens[i].clone().expect("owner has a token");
                        let total: u64 = file.chunks.iter().map(|(_, s)| *s as u64).sum();
                        body.extend_from_slice(&(token.len() as u16).to_be_bytes());
                        body.extend_from_slice(&total.to_be_bytes());
                        body.extend_from_slice(&(file.chunks.len() as u32).to_be_bytes());
                        body.extend_from_slice(token.as_bytes());
                        let mut reader: Box<dyn Read + Send + '_> = match &file.source {
                            PushSource::Path(p) => {
                                Box::new(std::fs::File::open(p).map_err(io_err)?)
                            }
                            PushSource::Bytes(b) => Box::new(std::io::Cursor::new(b.as_slice())),
                            PushSource::KnownOid(_) | PushSource::StablePrefix { .. } => {
                                unreachable!("never tokened")
                            }
                        };
                        for (hash, size) in &file.chunks {
                            let mut data = vec![0u8; *size as usize];
                            reader.read_exact(&mut data).map_err(io_err)?;
                            body.extend_from_slice(hash);
                            body.extend_from_slice(&(data.len() as u32).to_be_bytes());
                            body.extend_from_slice(&data);
                            group_chunks += 1;
                            group_bytes += data.len() as u64;
                        }
                        expected.push((token, file.blob_oid.clone()));
                    }
                    let resp = self
                        .put_batch_files(
                            project_id,
                            repo,
                            git_username,
                            git_token,
                            session_id,
                            body,
                        )
                        .await?;
                    let by_token: std::collections::HashMap<&str, &str> = resp
                        .files
                        .iter()
                        .map(|f| (f.token.as_str(), f.oid.as_str()))
                        .collect();
                    for (token, oid) in &expected {
                        match by_token.get(token.as_str()) {
                            Some(server) if server == oid => {}
                            Some(server) => {
                                return Err(SdkError::ClientError(format!(
                                    "server hashed {token:?} to {server} but the client                                      computed {oid}; was the file modified mid-push?"
                                )));
                            }
                            None => {
                                return Err(SdkError::ClientError(format!(
                                    "batch upload response is missing token {token:?}"
                                )));
                            }
                        }
                    }
                    Ok::<(usize, u64), SdkError>((group_chunks, group_bytes))
                }
            }))
            .buffer_unordered(TOKENED_UPLOADS_IN_FLIGHT);
            while let Some(done) = batch_results.next().await {
                let (chunks, bytes) = done?;
                uploaded_chunks += chunks;
                uploaded_bytes += bytes;
                emit(PushEvent::UploadedBatch {
                    chunks: uploaded_chunks,
                    bytes: uploaded_bytes,
                });
            }
            drop(batch_results);
            let mut results = futures::stream::iter(single_owners.into_iter().map(|i| {
                let file = &chunked[i];
                let token = file_tokens[i].clone().expect("owner has a token");
                let session_id = session.session_id.as_str();
                let batch_bytes = opts.upload_batch_bytes;
                async move {
                    let total: u64 = file.chunks.iter().map(|(_, s)| *s as u64).sum();
                    let mut reader: Box<dyn Read + Send + '_> = match &file.source {
                        PushSource::Path(p) => Box::new(std::fs::File::open(p).map_err(io_err)?),
                        PushSource::Bytes(b) => Box::new(std::io::Cursor::new(b.as_slice())),
                        PushSource::KnownOid(_) | PushSource::StablePrefix { .. } => {
                            unreachable!("never tokened")
                        }
                    };
                    // The whole file goes in ONE request when it fits a batch (required for
                    // small files on staging servers — they cannot resume — and one round trip
                    // for everything else); larger files continue at explicit offsets.
                    let (mut sent_chunks, mut sent_bytes) = (0usize, 0u64);
                    let mut offset = 0u64;
                    let mut frame = Vec::with_capacity(batch_bytes + 64);
                    for (hash, size) in &file.chunks {
                        let mut data = vec![0u8; *size as usize];
                        reader.read_exact(&mut data).map_err(io_err)?;
                        frame.extend_from_slice(hash);
                        frame.extend_from_slice(&(data.len() as u32).to_be_bytes());
                        frame.extend_from_slice(&data);
                        sent_chunks += 1;
                        sent_bytes += data.len() as u64;
                        if frame.len() >= batch_bytes {
                            let sent: u64 = frame_payload_bytes(&frame);
                            self.put_chunk_frame(
                                project_id,
                                repo,
                                git_username,
                                git_token,
                                session_id,
                                Some((&token, total, offset)),
                                std::mem::take(&mut frame),
                            )
                            .await?;
                            offset += sent;
                        }
                    }
                    if !frame.is_empty() {
                        self.put_chunk_frame(
                            project_id,
                            repo,
                            git_username,
                            git_token,
                            session_id,
                            Some((&token, total, offset)),
                            frame,
                        )
                        .await?;
                    }
                    Ok::<(usize, u64), SdkError>((sent_chunks, sent_bytes))
                }
            }))
            .buffer_unordered(TOKENED_UPLOADS_IN_FLIGHT);
            while let Some(done) = results.next().await {
                let (chunks, bytes) = done?;
                uploaded_chunks += chunks;
                uploaded_bytes += bytes;
                emit(PushEvent::UploadedBatch {
                    chunks: uploaded_chunks,
                    bytes: uploaded_bytes,
                });
            }
        }
        // Chunks still needed by dedup-path files upload below even when a tokened file also
        // carried them: on staging servers a small tokened file's chunks are never registered,
        // and the client deliberately doesn't know the server's small/large threshold — so a
        // tokened upload is never relied on to cover another file's chunk references.
        let mut to_upload = dedup_needed_chunks(&chunked, &tokened, &missing);

        // Dedup path for everything else: only chunks the server lacks. When the backend
        // presigns, bytes are zstd-framed into client-assembled chunk packs and PUT straight
        // to the object store — the service only records session-scoped layout claims, and
        // identity is minted by commit-time read-back, so service bandwidth drops out of the
        // push entirely. Backends that cannot presign fall back to service-mediated frames.
        // Either way a few uploads stay in flight while the next batch is read and assembled.
        const DEDUP_UPLOADS_IN_FLIGHT: usize = 3;
        let staging_target = if to_upload.is_empty() {
            None
        } else {
            match self
                .ingest_staging_target(
                    project_id,
                    repo,
                    git_username,
                    git_token,
                    &session.session_id,
                )
                .await
            {
                Ok(probe) => probe.url.is_some().then_some(probe),
                // A server without the staging endpoint (or with it disabled) is not an
                // error — the frame path below works against every server version.
                Err(SdkError::ServerError { status, .. })
                    if status.as_u16() == 404 || status.as_u16() == 405 =>
                {
                    None
                }
                Err(e) => return Err(e),
            }
        };
        if let Some(first_target) = staging_target {
            use futures::StreamExt as _;
            let mut inflight = futures::stream::FuturesUnordered::new();
            let mut next_target = Some(first_target);
            let mut pack: Vec<u8> = Vec::with_capacity(opts.upload_batch_bytes + 256 * 1024);
            let mut entries: Vec<StagedEntryWire> = Vec::new();
            for file in &chunked {
                if to_upload.is_empty() {
                    break;
                }
                if file.chunks.is_empty() {
                    continue; // deletes and known-oid references carry no bytes
                }
                let mut reader = UploadReader::open(&file.source)?;
                for (hash, size) in &file.chunks {
                    if !to_upload.remove(hash) {
                        reader.skip(*size as u64)?;
                        continue;
                    }
                    let mut data = vec![0u8; *size as usize];
                    reader.read_exact(&mut data)?;
                    let zframe = zstd::encode_all(&data[..], STAGED_ZSTD_LEVEL)
                        .map_err(|e| SdkError::ClientError(format!("zstd encode: {e}")))?;
                    entries.push(StagedEntryWire {
                        hash: hex_lower(hash),
                        offset: pack.len() as u64,
                        length: zframe.len() as u32,
                        size: *size,
                    });
                    pack.extend_from_slice(&zframe);
                    uploaded_chunks += 1;
                    uploaded_bytes += data.len() as u64;
                    if pack.len() >= opts.upload_batch_bytes {
                        let target = match next_target.take() {
                            Some(t) => t,
                            None => {
                                self.ingest_staging_target(
                                    project_id,
                                    repo,
                                    git_username,
                                    git_token,
                                    &session.session_id,
                                )
                                .await?
                            }
                        };
                        inflight.push(self.upload_staged_pack(
                            project_id,
                            repo,
                            git_username,
                            git_token,
                            &session.session_id,
                            target,
                            std::mem::take(&mut pack),
                            std::mem::take(&mut entries),
                        ));
                        emit(PushEvent::UploadedBatch {
                            chunks: uploaded_chunks,
                            bytes: uploaded_bytes,
                        });
                        if inflight.len() >= DEDUP_UPLOADS_IN_FLIGHT {
                            inflight.next().await.expect("inflight upload present")?;
                        }
                    }
                }
            }
            if !pack.is_empty() {
                let target = match next_target.take() {
                    Some(t) => t,
                    None => {
                        self.ingest_staging_target(
                            project_id,
                            repo,
                            git_username,
                            git_token,
                            &session.session_id,
                        )
                        .await?
                    }
                };
                inflight.push(self.upload_staged_pack(
                    project_id,
                    repo,
                    git_username,
                    git_token,
                    &session.session_id,
                    target,
                    pack,
                    entries,
                ));
                emit(PushEvent::UploadedBatch {
                    chunks: uploaded_chunks,
                    bytes: uploaded_bytes,
                });
            }
            while let Some(done) = inflight.next().await {
                done?;
            }
        } else {
            use futures::StreamExt as _;
            let mut inflight = futures::stream::FuturesUnordered::new();
            let mut frame = Vec::with_capacity(opts.upload_batch_bytes + 64);
            for file in &chunked {
                if to_upload.is_empty() {
                    break;
                }
                if file.chunks.is_empty() {
                    continue; // deletes and known-oid references carry no bytes
                }
                let mut reader = UploadReader::open(&file.source)?;
                for (hash, size) in &file.chunks {
                    if !to_upload.remove(hash) {
                        reader.skip(*size as u64)?;
                        continue;
                    }
                    let mut data = vec![0u8; *size as usize];
                    reader.read_exact(&mut data)?;
                    frame.extend_from_slice(hash);
                    frame.extend_from_slice(&(data.len() as u32).to_be_bytes());
                    frame.extend_from_slice(&data);
                    uploaded_chunks += 1;
                    uploaded_bytes += data.len() as u64;
                    if frame.len() >= opts.upload_batch_bytes {
                        inflight.push(self.put_chunk_frame(
                            project_id,
                            repo,
                            git_username,
                            git_token,
                            &session.session_id,
                            None,
                            std::mem::take(&mut frame),
                        ));
                        emit(PushEvent::UploadedBatch {
                            chunks: uploaded_chunks,
                            bytes: uploaded_bytes,
                        });
                        if inflight.len() >= DEDUP_UPLOADS_IN_FLIGHT {
                            inflight.next().await.expect("inflight upload present")?;
                        }
                    }
                }
            }
            if !frame.is_empty() {
                inflight.push(self.put_chunk_frame(
                    project_id,
                    repo,
                    git_username,
                    git_token,
                    &session.session_id,
                    None,
                    frame,
                ));
                emit(PushEvent::UploadedBatch {
                    chunks: uploaded_chunks,
                    bytes: uploaded_bytes,
                });
            }
            while let Some(done) = inflight.next().await {
                done?;
            }
        }

        // 5. Commit by reference. Small staged files commit TOKEN-ONLY ({path, file_token,
        // oid}, no chunk list): upload already verified their content, so re-declaring chunks
        // only bloats the request. Large tokened files keep their chunk lists (recipes are
        // built from them); untokened files keep them for read-back verification.
        let token_commits = session.features.iter().any(|f| f == "token_commits");
        let commit_files: Vec<CommitFileWire> = chunked
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let token_only = token_commits && file_tokens[i].is_some() && is_small(f);
                CommitFileWire {
                    path: f.repo_path.clone(),
                    chunks: if token_only {
                        Vec::new()
                    } else {
                        f.chunks
                            .iter()
                            .map(|(h, s)| CommitChunkWire {
                                hash: hex_lower(h),
                                size: *s,
                            })
                            .collect()
                    },
                    content: (!f.delete && !f.known_oid && !token_only && f.chunks.is_empty())
                        .then(String::new),
                    file_token: file_tokens[i].clone(),
                    oid: (f.known_oid || token_only).then(|| f.blob_oid.clone()),
                    delete: f.delete,
                    mode: f.mode.map(|m| format!("{m:o}")),
                }
            })
            .collect();
        let (commit_suffix, branch) = match &opts.workspace_snapshot {
            Some(ws_id) => (format!("workspaces/{ws_id}/snapshots"), None),
            None => ("commits".to_string(), Some(opts.branch.clone())),
        };
        let body = CommitWire {
            branch,
            message: opts.message.clone(),
            session_id: session.session_id.clone(),
            files: commit_files,
            base: opts.base.clone(),
            expect_oid: opts.expect_oid.clone(),
        };
        emit(PushEvent::Committing {
            files: body.files.len(),
        });
        // Every commit is a durable job server-side: 201 with the result when it finishes
        // within the grace window, 202 with a job id when it detaches. Either way no
        // connection stays silent long enough for an LB idle timeout to reap it (which used
        // to CANCEL the in-flight commit), and losing a response is recoverable: the
        // idempotency key reattaches to the same job.
        let idem_key = {
            let bytes: [u8; 16] = rand::random();
            bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
        };
        // The idempotency key makes the submit replayable: a retry after a lost response
        // reattaches to the same durable job instead of double-committing.
        let (accepted, mut job, trace_id) = with_transient_retries(|| async {
            let (req, trace_id) = self.git_request(
                Method::POST,
                project_id,
                repo,
                Some(&commit_suffix),
                git_username,
                git_token,
            )?;
            let submit = req
                // Deployed servers from the header-opt-in era only answer the job shape when
                // asked; the job-only server ignores this. Removable once every environment is
                // job-only.
                .header("x-commit-async", "1")
                .header("idempotency-key", &idem_key)
                .timeout(std::time::Duration::from_secs(60))
                .json(&body)
                .send()
                .await?;
            let accepted = submit.status().as_u16() == 202;
            let job: CommitJobStatus = expect_json(submit).await?;
            Ok((accepted, job, trace_id))
        })
        .await?;
        if accepted {
            let job_id = job.job_id.clone();
            emit(PushEvent::CommitDetached {
                job_id: job_id.clone(),
            });
            // Poll the job's state machine to terminal. Each poll is a fresh, short request.
            let poll_suffix = format!("{commit_suffix}/jobs/{job_id}");
            let mut delay = std::time::Duration::from_millis(500);
            loop {
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(std::time::Duration::from_secs(2));
                job = with_transient_retries(|| async {
                    let (req, _t) = self.git_request(
                        Method::GET,
                        project_id,
                        repo,
                        Some(&poll_suffix),
                        git_username,
                        git_token,
                    )?;
                    expect_json(
                        req.timeout(std::time::Duration::from_secs(30))
                            .send()
                            .await?,
                    )
                    .await
                })
                .await?;
                match job.state.as_str() {
                    "committed" | "failed" => break,
                    _ => {
                        if let Some(rb) = &job.read_back {
                            emit(PushEvent::CommitProgress {
                                phase: job.phase.clone().unwrap_or_else(|| job.state.clone()),
                                done: rb.done,
                                total: rb.total,
                            });
                        } else {
                            emit(PushEvent::CommitProgress {
                                phase: job.phase.clone().unwrap_or_else(|| job.state.clone()),
                                done: 0,
                                total: 0,
                            });
                        }
                    }
                }
            }
        }
        if job.state == "failed" {
            let err = job.error.unwrap_or_default();
            return Err(SdkError::ClientError(format!(
                "commit job failed ({}): {}{}",
                err.kind,
                err.message,
                if err.retryable {
                    " (safe to retry: uploaded chunks are deduplicated)"
                } else {
                    ""
                }
            )));
        }
        let resp = CommitByReferenceResponse {
            commit: job.commit.ok_or_else(|| {
                SdkError::ClientError("committed job missing commit oid".to_string())
            })?,
            tree: job
                .tree
                .ok_or_else(|| SdkError::ClientError("committed job missing tree".to_string()))?,
            ref_name: job.ref_name.ok_or_else(|| {
                SdkError::ClientError("committed job missing ref name".to_string())
            })?,
            parent: job.parent,
            created: job.created.unwrap_or(false),
        };
        emit(PushEvent::Committed {
            commit: resp.commit.clone(),
            ref_name: resp.ref_name.clone(),
        });
        Ok(Traced::new(
            trace_id,
            PushReport {
                commit: resp.commit,
                tree: resp.tree,
                ref_name: resp.ref_name,
                created: resp.created,
                files: chunked.len(),
                bytes_total: total_bytes,
                chunks_total: distinct.len(),
                chunks_uploaded: uploaded_chunks,
                bytes_uploaded: uploaded_bytes,
                file_chunks: if opts.collect_file_chunks {
                    chunked
                        .iter()
                        .filter(|f| !f.delete && !f.known_oid && f.cdc)
                        .map(|f| (f.repo_path.clone(), f.chunks.clone()))
                        .collect()
                } else {
                    Vec::new()
                },
                file_blob_oids: chunked
                    .into_iter()
                    .map(|f| (f.repo_path, f.blob_oid))
                    .collect(),
            },
        ))
    }

    async fn negotiate_missing(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
        session: &IngestSessionWire,
        distinct: &[[u8; 32]],
    ) -> Result<std::collections::HashSet<[u8; 32]>, SdkError> {
        let mut missing = std::collections::HashSet::new();
        for batch in distinct.chunks(session.max_hashes_per_query.max(1)) {
            let hashes: Vec<String> = batch.iter().map(hex_lower).collect();
            let resp: MissingRespWire = with_transient_retries(|| async {
                let (req, _t) = self.git_request(
                    Method::POST,
                    project_id,
                    repo,
                    Some(&format!("ingest/sessions/{}/missing", session.session_id)),
                    git_username,
                    git_token,
                )?;
                expect_json(req.json(&MissingWire { hashes: &hashes }).send().await?).await
            })
            .await?;
            for h in resp.missing {
                let mut arr = [0u8; 32];
                hex::decode_to_slice(&h, &mut arr)
                    .map_err(|e| SdkError::ClientError(format!("bad hash from server: {e}")))?;
                missing.insert(arr);
            }
        }
        Ok(missing)
    }

    /// Poll a commit job's state machine (`GET .../commits/jobs/{id}`) — the out-of-band view
    /// of an async commit, usable from any process that knows the job id.
    pub async fn commit_job_status(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
        job_id: &str,
    ) -> Result<Traced<CommitJobStatus>, SdkError> {
        let (req, trace_id) = self.git_request(
            Method::GET,
            project_id,
            repo,
            Some(&format!("commits/jobs/{job_id}")),
            git_username,
            git_token,
        )?;
        let value = expect_json(
            req.timeout(std::time::Duration::from_secs(30))
                .send()
                .await?,
        )
        .await?;
        Ok(Traced::new(trace_id, value))
    }

    /// Mint a presigned chunk-pack staging target. `url` is `None` when the backend cannot
    /// presign (e.g. filesystem stores) — callers fall back to service-mediated frames.
    async fn ingest_staging_target(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
        session_id: &str,
    ) -> Result<IngestStagingWire, SdkError> {
        with_transient_retries(|| async {
            let (req, _t) = self.git_request(
                Method::POST,
                project_id,
                repo,
                Some(&format!("ingest/sessions/{session_id}/staging")),
                git_username,
                git_token,
            )?;
            expect_json(req.send().await?).await
        })
        .await
    }

    /// PUT a client-assembled chunk pack directly to its presigned target, then register its
    /// layout with the session. Entry hashes are claims — the server mints identity only at
    /// commit read-back, so a corrupt upload fails the commit, never the store.
    #[allow(clippy::too_many_arguments)]
    async fn upload_staged_pack(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
        session_id: &str,
        target: IngestStagingWire,
        pack: Vec<u8>,
        entries: Vec<StagedEntryWire>,
    ) -> Result<(), SdkError> {
        let url = target.url.as_deref().ok_or_else(|| {
            SdkError::ClientError("staged upload requires a presigned url".to_string())
        })?;
        // The signature lives in the query string — no auth headers (an Authorization header
        // would conflict with the presigned signature). Content-addressed and same-key: safe
        // to retry.
        let pack = bytes::Bytes::from(pack);
        with_transient_retries(|| {
            let pack = pack.clone();
            async move {
                let resp = self.git_client.put(url).body(pack).send().await?;
                if !resp.status().is_success() {
                    return Err(SdkError::ServerError {
                        status: resp.status(),
                        message: format!(
                            "staged pack PUT: {}",
                            resp.text().await.unwrap_or_default()
                        ),
                    });
                }
                Ok(())
            }
        })
        .await?;
        // Bounded batches: the server caps entries per registration call.
        const REGISTER_BATCH: usize = 4096;
        let mut remaining = entries;
        while !remaining.is_empty() {
            let take = remaining.len().min(REGISTER_BATCH);
            let batch: Vec<StagedEntryWire> = remaining.drain(..take).collect();
            let body = StagedRegisterWire {
                pack_id: target.pack_id.clone(),
                entries: batch,
            };
            with_transient_retries(|| async {
                let (req, _t) = self.git_request(
                    Method::POST,
                    project_id,
                    repo,
                    Some(&format!("ingest/sessions/{session_id}/staged")),
                    git_username,
                    git_token,
                )?;
                expect_ok(req.json(&body).send().await?).await
            })
            .await?;
        }
        Ok(())
    }

    /// PUT one batch-upload body (`.../ingest/sessions/{id}/files`): repeated file records,
    /// each a complete small file under its own token.
    #[allow(clippy::too_many_arguments)]
    async fn put_batch_files(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
        session_id: &str,
        body: Vec<u8>,
    ) -> Result<BatchUploadRespWire, SdkError> {
        let body = bytes::Bytes::from(body);
        with_transient_retries(|| {
            let body = body.clone();
            async move {
                let (req, _t) = self.git_request(
                    Method::PUT,
                    project_id,
                    repo,
                    Some(&format!("ingest/sessions/{session_id}/files")),
                    git_username,
                    git_token,
                )?;
                expect_json(req.body(body).send().await?).await
            }
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn put_chunk_frame(
        &self,
        project_id: &str,
        repo: &str,
        git_username: &str,
        git_token: &str,
        session_id: &str,
        file: Option<(&str, u64, u64)>,
        frame: Vec<u8>,
    ) -> Result<(), SdkError> {
        let suffix = match file {
            None => format!("ingest/sessions/{session_id}/chunks"),
            Some((token, total, offset)) => format!(
                "ingest/sessions/{session_id}/chunks?file={token}&file_total={total}&file_offset={offset}"
            ),
        };
        // Untokened frames are content-addressed and fully idempotent → retried. Tokened
        // offset requests are not blindly replayable (a lost success makes the replayed
        // offset conflict), so they keep single-shot semantics.
        let retriable = file.is_none();
        let frame = bytes::Bytes::from(frame);
        let send = || {
            let frame = frame.clone();
            let suffix = suffix.clone();
            async move {
                let (req, _t) = self.git_request(
                    Method::PUT,
                    project_id,
                    repo,
                    Some(&suffix),
                    git_username,
                    git_token,
                )?;
                expect_ok(req.body(frame).send().await?).await
            }
        };
        if retriable {
            with_transient_retries(send).await
        } else {
            send().await
        }
    }
}

/// Payload bytes (sum of chunk data lengths) inside a well-formed frame buffer.
fn frame_payload_bytes(frame: &[u8]) -> u64 {
    let mut cursor = frame;
    let mut total = 0u64;
    while cursor.len() >= 36 {
        let len = u32::from_be_bytes(cursor[32..36].try_into().expect("sized")) as u64;
        total += len;
        cursor = &cursor[36 + len as usize..];
    }
    total
}

fn hex_lower(h: &[u8; 32]) -> String {
    hex::encode(h)
}

pub(super) async fn expect_json<T: serde::de::DeserializeOwned>(
    resp: reqwest::Response,
) -> Result<T, SdkError> {
    let status = resp.status();
    if !status.is_success() {
        let message = resp.text().await.unwrap_or_default();
        return Err(SdkError::ServerError { status, message });
    }
    resp.json::<T>()
        .await
        .map_err(|e| SdkError::ClientError(format!("bad response body: {e}")))
}

async fn expect_ok(resp: reqwest::Response) -> Result<(), SdkError> {
    let status = resp.status();
    if !status.is_success() {
        let message = resp.text().await.unwrap_or_default();
        return Err(SdkError::ServerError { status, message });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cf(chunks: Vec<([u8; 32], u32)>, delete: bool, known_oid: bool) -> ChunkedFile {
        let cdc = !delete && !known_oid && !chunks.is_empty();
        ChunkedFile {
            repo_path: "p".to_string(),
            source: PushSource::Bytes(Vec::new()),
            chunks,
            mode: None,
            delete,
            known_oid,
            blob_oid: String::new(),
            cdc,
        }
    }

    fn h(b: u8) -> [u8; 32] {
        [b; 32]
    }

    /// Transient failures (5xx/429/transport) retry with backoff and then succeed; 4xx
    /// rejections are deterministic and never retried.
    #[tokio::test(start_paused = true)]
    async fn transient_retries_back_off_and_stop_on_client_errors() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let calls = AtomicUsize::new(0);
        let out: Result<u32, SdkError> = with_transient_retries(|| {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            async move {
                if n < 2 {
                    Err(SdkError::ServerError {
                        status: reqwest::StatusCode::SERVICE_UNAVAILABLE,
                        message: "wobble".into(),
                    })
                } else {
                    Ok(7)
                }
            }
        })
        .await;
        assert_eq!(out.unwrap(), 7);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            3,
            "two transient failures retried"
        );

        let calls = AtomicUsize::new(0);
        let out: Result<u32, SdkError> = with_transient_retries(|| {
            calls.fetch_add(1, Ordering::SeqCst);
            async {
                Err(SdkError::ServerError {
                    status: reqwest::StatusCode::BAD_REQUEST,
                    message: "no".into(),
                })
            }
        })
        .await;
        assert!(out.is_err());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "4xx must not retry: the rejection is deterministic"
        );

        // Budget exhausted: a persistent 503 surfaces after the final retry.
        let calls = AtomicUsize::new(0);
        let out: Result<u32, SdkError> = with_transient_retries(|| {
            calls.fetch_add(1, Ordering::SeqCst);
            async {
                Err(SdkError::ServerError {
                    status: reqwest::StatusCode::SERVICE_UNAVAILABLE,
                    message: "down".into(),
                })
            }
        })
        .await;
        assert!(out.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1 + TRANSIENT_RETRIES);
    }

    /// Small files (under the staged threshold) are always tokened; above it the
    /// missing-ratio heuristic decides; deletes, known-oid references, and empty files never
    /// token. With no advertised threshold (older server), ratio-only applies everywhere.
    #[test]
    fn tokened_election_smalls_always_larges_by_ratio() {
        let missing: std::collections::HashSet<[u8; 32]> = [h(1), h(2)].into();
        let files = vec![
            cf(vec![(h(9), 100)], false, false), // small, fully PRESENT → tokened anyway
            cf(vec![(h(1), 300), (h(9), 300)], false, false), // large, half missing → tokened
            cf(vec![(h(9), 300), (h(8), 300)], false, false), // large, mostly present → dedup
            cf(vec![(h(2), 100)], true, false),  // delete → never
            cf(Vec::new(), false, true),         // known oid → never
            cf(Vec::new(), false, false),        // empty file → inline content
        ];
        assert_eq!(
            elect_tokened(&files, &missing, 200),
            vec![true, true, false, false, false, false]
        );
        // Threshold 0 (older server): the tiny fully-present file falls back to the dedup path.
        assert_eq!(
            elect_tokened(&files, &missing, 0),
            vec![false, true, false, false, false, false]
        );
    }

    /// A chunk carried by a tokened file must STILL upload through the dedup path when a
    /// non-tokened file references it: staging servers do not register small tokened files'
    /// chunks, so tokened uploads never count as coverage for other files.
    #[test]
    fn dedup_coverage_ignores_tokened_uploads() {
        let missing: std::collections::HashSet<[u8; 32]> = [h(1), h(2), h(3)].into();
        let files = vec![
            // Tokened: carries h(1) (shared) and h(2) (private to this file).
            cf(vec![(h(1), 100), (h(2), 100)], false, false),
            // Dedup path (mostly present): references shared h(1) and missing h(3).
            cf(vec![(h(1), 100), (h(3), 100), (h(9), 800)], false, false),
        ];
        let tokened = vec![true, false];
        let need = dedup_needed_chunks(&files, &tokened, &missing);
        assert!(need.contains(&h(1)), "shared chunk must upload via dedup");
        assert!(need.contains(&h(3)));
        assert!(
            !need.contains(&h(2)),
            "chunks only a tokened file carries ride the tokened stream"
        );
    }

    #[test]
    fn worktree_repo_paths_from_dot_have_no_dot_components() {
        let root = std::path::Path::new(".");
        assert_eq!(
            repo_path_for_worktree(root, std::path::Path::new("./.clang-format")),
            ".clang-format"
        );
        assert_eq!(
            repo_path_for_worktree(root, std::path::Path::new("./arch/boot.c")),
            "arch/boot.c"
        );
    }

    #[test]
    fn worktree_repo_paths_from_absolute_root_are_root_relative() {
        let root = std::path::Path::new("/tmp/somedir");
        assert_eq!(
            repo_path_for_worktree(root, std::path::Path::new("/tmp/somedir/a/b.txt")),
            "a/b.txt"
        );
    }

    #[test]
    fn collect_worktree_files_skips_git_metadata() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.txt"), "a").unwrap();
        std::fs::create_dir(dir.path().join(".git")).unwrap();
        std::fs::write(dir.path().join(".git").join("config"), "ignored").unwrap();

        let mut files = Vec::new();
        collect_worktree_files(dir.path(), &mut files).unwrap();

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].repo_path, "a.txt");
    }

    #[test]
    fn collect_worktree_files_honors_gitignore_without_git_dir() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(".gitignore"), "ignored.bin\n").unwrap();
        std::fs::write(dir.path().join("ignored.bin"), "ignored").unwrap();
        std::fs::write(dir.path().join("kept.tensorlake-test"), "kept").unwrap();

        let mut files = Vec::new();
        collect_worktree_files(dir.path(), &mut files).unwrap();

        let paths: Vec<_> = files.iter().map(|file| file.repo_path.as_str()).collect();
        assert_eq!(paths, vec![".gitignore", "kept.tensorlake-test"]);
    }

    #[cfg(unix)]
    #[test]
    fn collect_worktree_files_preserves_executable_mode() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let script = dir.path().join("script.sh");
        std::fs::write(&script, "#!/bin/sh\n").unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();

        let mut files = Vec::new();
        collect_worktree_files(dir.path(), &mut files).unwrap();

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].repo_path, "script.sh");
        assert_eq!(files[0].mode, Some(0o100755));
    }

    #[cfg(unix)]
    #[test]
    fn collect_worktree_files_preserves_symlink_mode_and_target() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("target.txt"), "target").unwrap();
        std::os::unix::fs::symlink("target.txt", dir.path().join("link.txt")).unwrap();

        let mut files = Vec::new();
        collect_worktree_files(dir.path(), &mut files).unwrap();

        let link = files
            .iter()
            .find(|file| file.repo_path == "link.txt")
            .expect("symlink should be collected");
        assert_eq!(link.mode, Some(0o120000));
        match &link.source {
            PushSource::Bytes(bytes) => assert_eq!(bytes, b"target.txt"),
            other => panic!("expected symlink bytes, got {other:?}"),
        }
    }

    /// The commit wire shape for a known-oid file: `oid` set, no chunks, no content.
    #[test]
    fn known_oid_commit_wire_shape() {
        let wire = CommitFileWire {
            path: "copies/b.txt".to_string(),
            chunks: Vec::new(),
            content: None,
            file_token: None,
            oid: Some("00112233445566778899aabbccddeeff00112233".to_string()),
            delete: false,
            mode: None,
        };
        let json = serde_json::to_value(&wire).unwrap();
        assert_eq!(json["oid"], "00112233445566778899aabbccddeeff00112233");
        assert!(json.get("chunks").is_none());
        assert!(json.get("content").is_none());
    }

    /// Live integration: the reworked upload paths against a running artifact-storage server
    /// (issue #57): small files take the tokened staged path, identical content shares one
    /// token, and a second commit references the first's blob by oid with no bytes.
    ///
    /// `cargo run -p gsvc-server` in an artifact_storage checkout, then
    /// `cargo test -p tensorlake -- push_paths --ignored --nocapture`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires a local artifact-storage server on 127.0.0.1:8080"]
    async fn push_paths_roundtrip_against_local_server() {
        const BASE: &str = "http://127.0.0.1:8080";
        if std::net::TcpStream::connect_timeout(
            &"127.0.0.1:8080".parse().unwrap(),
            std::time::Duration::from_millis(500),
        )
        .is_err()
        {
            eprintln!("skipping: no local artifact-storage server");
            return;
        }
        let client = crate::ClientBuilder::new(BASE)
            .bearer_token("dummy")
            .build()
            .unwrap();
        let sdk = ArtifactStorageClient::new(client, BASE).unwrap();
        let repo = format!(
            "ingest-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        sdk.create_repo_with_credential("ingesttest", &repo, None, "t", "devtoken")
            .await
            .unwrap();

        // Push 1: small fresh files (tokened + staged server-side); identical content at two
        // paths shares one token and one upload.
        let salt = repo.as_bytes().to_vec();
        let content_a: Vec<u8> = [b"alpha ".as_slice(), &salt].concat();
        let big: Vec<u8> = (0..3usize * 1024 * 1024)
            .map(|i| {
                (i as u8)
                    .wrapping_mul(31)
                    .wrapping_add(salt[i % salt.len()])
            })
            .collect();
        let report = sdk
            .push_files(
                "ingesttest",
                &repo,
                "t",
                "devtoken",
                vec![
                    PushFile {
                        repo_path: "a.txt".to_string(),
                        source: PushSource::Bytes(content_a.clone()),
                        mode: None,
                        delete: false,
                    },
                    PushFile {
                        repo_path: "dup/a-again.txt".to_string(),
                        source: PushSource::Bytes(content_a.clone()),
                        mode: None,
                        delete: false,
                    },
                    PushFile {
                        repo_path: "big.bin".to_string(),
                        source: PushSource::Bytes(big),
                        mode: None,
                        delete: false,
                    },
                    // Zero-byte file: must publish as inline content, never as a
                    // zero-length chunk (the server rejects those).
                    PushFile {
                        repo_path: "empty.txt".to_string(),
                        source: PushSource::Bytes(Vec::new()),
                        mode: None,
                        delete: false,
                    },
                ],
                PushOptions {
                    message: "seed".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .into_inner();
        assert_eq!(report.files, 4);
        assert_eq!(
            report
                .file_blob_oids
                .iter()
                .find(|(p, _)| p == "empty.txt")
                .unwrap()
                .1,
            "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391",
            "empty file must carry git's empty-blob oid"
        );
        let a_oid = report
            .file_blob_oids
            .iter()
            .find(|(p, _)| p == "a.txt")
            .unwrap()
            .1
            .clone();
        let dup_oid = &report
            .file_blob_oids
            .iter()
            .find(|(p, _)| p == "dup/a-again.txt")
            .unwrap()
            .1;
        assert_eq!(&a_oid, dup_oid, "identical content has one blob oid");

        // Push 2: reference the seeded blob by oid — no bytes anywhere in the request.
        let report2 = sdk
            .push_files(
                "ingesttest",
                &repo,
                "t",
                "devtoken",
                vec![PushFile {
                    repo_path: "copies/by-ref.txt".to_string(),
                    source: PushSource::KnownOid(a_oid.clone()),
                    mode: None,
                    delete: false,
                }],
                PushOptions {
                    message: "by reference".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .into_inner();
        assert_eq!(report2.bytes_uploaded, 0, "oid references upload nothing");
        assert_eq!(
            report2.file_blob_oids,
            vec![("copies/by-ref.txt".to_string(), a_oid)]
        );

        // An unknown oid is rejected by the server's presence check.
        let bogus = sdk
            .push_files(
                "ingesttest",
                &repo,
                "t",
                "devtoken",
                vec![PushFile {
                    repo_path: "copies/bogus.txt".to_string(),
                    source: PushSource::KnownOid(
                        "00112233445566778899aabbccddeeff00112233".to_string(),
                    ),
                    mode: None,
                    delete: false,
                }],
                PushOptions {
                    message: "bogus".into(),
                    ..Default::default()
                },
            )
            .await;
        assert!(bogus.is_err(), "unknown oid must fail the commit");
    }

    /// CDC over the same bytes must be deterministic (the upload pass re-reads and re-chunks),
    /// and the frame layout must match the server: `32-byte hash | u32-be len | bytes`.
    #[test]
    fn chunking_is_deterministic_and_frames_are_well_formed() {
        let data: Vec<u8> = (0..3_000_000usize).map(|i| (i % 251) as u8).collect();
        let src = PushSource::Bytes(data.clone());
        let (a, oid_a) = chunk_source(&src, 256 * 1024, 1024 * 1024, 4 * 1024 * 1024).unwrap();
        let (b, oid_b) = chunk_source(&src, 256 * 1024, 1024 * 1024, 4 * 1024 * 1024).unwrap();
        assert_eq!(a, b, "CDC must be deterministic across passes");
        assert_eq!(oid_a, oid_b, "blob oid must be deterministic");
        // The single-pass blob oid must equal a straight git blob hash of the same bytes.
        let mut reference = BlobOidHasher::new(data.len() as u64);
        reference.update(&data);
        assert_eq!(oid_a, reference.finalize_hex());
        assert_eq!(
            a.iter().map(|(_, s)| *s as usize).sum::<usize>(),
            data.len(),
            "chunks must cover the file exactly"
        );
        // Frame one chunk and validate the layout.
        let (hash, size) = a[0];
        let mut frame = Vec::new();
        frame.extend_from_slice(&hash);
        frame.extend_from_slice(&size.to_be_bytes());
        frame.extend_from_slice(&data[..size as usize]);
        assert_eq!(&frame[..32], &hash);
        assert_eq!(u32::from_be_bytes(frame[32..36].try_into().unwrap()), size);
        assert_eq!(
            <[u8; 32]>::from(Sha256::digest(&frame[36..])),
            hash,
            "framed bytes must hash to the declared id"
        );
    }

    /// The whole-file (small staged) pass must match the CDC pass's contract for empty files:
    /// no chunks — a zero-length chunk frame is rejected by the server ("chunk length 0
    /// outside (0, 8388608]"), and an empty chunk list is what routes the file to inline
    /// `content` at commit.
    #[test]
    fn whole_file_pass_yields_no_chunks_for_empty_files() {
        let (chunks, oid) = chunk_source_whole(&PushSource::Bytes(Vec::new())).unwrap();
        assert!(chunks.is_empty(), "an empty file must have no chunks");
        assert_eq!(
            oid, "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391",
            "must still hash to git's empty-blob oid"
        );
        let (cdc_chunks, cdc_oid) =
            chunk_source(&PushSource::Bytes(Vec::new()), 256, 1024, 4096).unwrap();
        assert_eq!(chunks, cdc_chunks, "both passes must agree on empty input");
        assert_eq!(oid, cdc_oid);

        // Non-empty small files still produce exactly one whole-file chunk.
        let data = b"hello".to_vec();
        let (chunks, _) = chunk_source_whole(&PushSource::Bytes(data.clone())).unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].1 as usize, data.len());
        assert_eq!(chunks[0].0, <[u8; 32]>::from(Sha256::digest(&data)));
    }

    /// The append fast path's whole premise: FastCDC restarts its state at every emitted
    /// boundary, so chunking resumed at a previous pass's boundary yields exactly what a full
    /// re-chunk of the grown file yields from that offset — the reused prefix plus the resumed
    /// tail must equal the full pass chunk-for-chunk.
    #[test]
    fn stable_prefix_chunking_matches_a_full_rechunk() {
        let (min, avg, max) = (256 * 1024, 1024 * 1024, 4 * 1024 * 1024);
        let old: Vec<u8> = (0..5_000_000usize).map(|i| (i % 251) as u8).collect();
        let appended: Vec<u8> = (0..2_000_000usize).map(|i| (i % 13) as u8).collect();
        let mut grown = old.clone();
        grown.extend_from_slice(&appended);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("grown.bin");
        std::fs::write(&path, &grown).unwrap();

        let (old_chunks, _) = chunk_source(&PushSource::Bytes(old), min, avg, max).unwrap();
        // The sealer's contract: the final chunk of the previous pass was cut at the old EOF
        // (not a content boundary) and is dropped before reuse.
        let stable = &old_chunks[..old_chunks.len() - 1];
        let resumed = chunk_stable_prefix(&path, stable, min, avg, max).unwrap();

        let (full, _) = chunk_source(&PushSource::Path(path.clone()), min, avg, max).unwrap();
        assert_eq!(
            resumed, full,
            "prefix + resumed tail must equal a full re-chunk exactly"
        );
        assert_eq!(
            resumed.iter().map(|(_, s)| *s as u64).sum::<u64>(),
            grown.len() as u64,
            "chunks must cover the grown file exactly"
        );
    }
}
