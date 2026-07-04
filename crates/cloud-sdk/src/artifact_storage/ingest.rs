//! Resumable ingest client: push files to artifact storage as content-defined chunks.
//!
//! The unit of transfer is the CDC chunk, so resume state is *content*, not cursors: every step
//! is idempotent, and retrying a failed push re-negotiates and uploads only what the server still
//! lacks — across process restarts, pods, and even prior pushes of overlapping content. The
//! client never runs `git pack-objects` and holds at most one upload batch in memory, so pushes
//! of arbitrarily large trees work from small sandboxes.
//!
//! Protocol (server: artifact-storage `/project/{p}/repos/{r}/ingest/...`):
//! 1. open a session (returns the CDC parameters to chunk with),
//! 2. negotiate which chunk hashes the server lacks,
//! 3. upload missing chunks in length-framed batches,
//! 4. commit by chunk reference (server verifies identity by read-back).

use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;

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

#[derive(Clone, Debug)]
pub enum PushSource {
    /// Read (twice: hash pass + upload pass) from the local filesystem.
    Path(PathBuf),
    /// In-memory content.
    Bytes(Vec<u8>),
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
        }
    }
}

/// Outcome of a push.
#[derive(Clone, Debug)]
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
    /// Deletes carry an empty oid.
    pub file_blob_oids: Vec<(String, String)>,
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

/// zstd level for staged chunk frames — the server stores frames verbatim, so this matches its
/// own chunk compression default (speed/ratio balance; any valid zstd frame decodes).
const STAGED_ZSTD_LEVEL: i32 = 3;

#[derive(Serialize)]
struct CommitFileWire {
    path: String,
    /// Omitted when empty: a zero-byte file has no chunks and must publish as inline
    /// `content` instead (the server rejects an explicit empty chunk list), and deletes
    /// carry neither.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    chunks: Vec<CommitChunkWire>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    file_token: Option<String>,
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
    /// `(sha256, size)` in file order.
    chunks: Vec<([u8; 32], u32)>,
    mode: Option<u32>,
    delete: bool,
    blob_oid: String,
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
    };
    let reader: Box<dyn Read> = match source {
        PushSource::Path(p) => Box::new(std::fs::File::open(p).map_err(io_err)?),
        PushSource::Bytes(b) => Box::new(std::io::Cursor::new(b.clone())),
    };
    let mut blob_hasher = gsvc_codec::BlobOidHasher::new(len);
    let mut out = Vec::new();
    for chunk in fastcdc::v2020::StreamCDC::new(reader, min as u32, avg as u32, max as u32) {
        let chunk = chunk.map_err(|e| SdkError::ClientError(format!("chunking failed: {e}")))?;
        let hash: [u8; 32] = Sha256::digest(&chunk.data).into();
        blob_hasher.update(&chunk.data);
        out.push((hash, chunk.data.len() as u32));
    }
    Ok((out, blob_hasher.finalize().to_hex()))
}

fn io_err(e: std::io::Error) -> SdkError {
    SdkError::Io(e)
}

impl ArtifactStorageClient {
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
        let (req, _trace) = self.git_request(
            Method::POST,
            project_id,
            repo,
            Some("ingest/sessions"),
            git_username,
            git_token,
        )?;
        let session: IngestSessionWire = expect_json(req.send().await?).await?;

        // 2. Chunk + hash every file locally: one streaming pass per file yields the CDC chunk
        //    list and the git blob oid together, files fanned across blocking threads.
        let (min, avg, max) = (
            session.cdc_min_bytes,
            session.cdc_avg_bytes,
            session.cdc_max_bytes,
        );
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
                        blob_oid: String::new(),
                    });
                }
                tokio::task::spawn_blocking(move || {
                    let (chunks, blob_oid) = chunk_source(&f.source, min, avg, max)?;
                    Ok(ChunkedFile {
                        repo_path: f.repo_path,
                        source: f.source,
                        chunks,
                        mode: f.mode,
                        delete: false,
                        blob_oid,
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

        // 3. Negotiate: which distinct hashes does the server lack?
        let mut distinct: Vec<[u8; 32]> = chunked
            .iter()
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

        // 4. Upload. Per file, pick the cheaper identity path (Phase 2, artifact_storage#26):
        //    a file the server mostly lacks streams IN FULL under a file token — the server
        //    hashes it during upload and the commit needs zero read-back; a file the server
        //    mostly has uploads only its missing chunks and accepts server-side read-back
        //    proportional to the reused bytes.
        let mut to_upload = missing.clone();
        let (mut uploaded_chunks, mut uploaded_bytes) = (0usize, 0u64);
        let mut file_tokens: Vec<Option<String>> = vec![None; chunked.len()];
        for (i, file) in chunked.iter().enumerate() {
            if file.delete {
                continue;
            }
            let total: u64 = file.chunks.iter().map(|(_, s)| *s as u64).sum();
            let missing_bytes: u64 = file
                .chunks
                .iter()
                .filter(|(h, _)| missing.contains(h))
                .map(|(_, s)| *s as u64)
                .sum();
            // Tokened only pays off for recipe-tier files that are mostly fresh.
            if total < 8 * 1024 * 1024 || missing_bytes * 2 < total {
                continue;
            }
            let token = format!("f{i}-{}", hex::encode(&file.chunks[0].0[..8]));
            let mut reader: Box<dyn Read> = match &file.source {
                PushSource::Path(p) => Box::new(std::fs::File::open(p).map_err(io_err)?),
                PushSource::Bytes(b) => Box::new(std::io::Cursor::new(b.clone())),
            };
            let mut offset = 0u64;
            let mut frame = Vec::with_capacity(opts.upload_batch_bytes + 64);
            for (hash, size) in &file.chunks {
                let mut data = vec![0u8; *size as usize];
                reader.read_exact(&mut data).map_err(io_err)?;
                to_upload.remove(hash);
                frame.extend_from_slice(hash);
                frame.extend_from_slice(&(data.len() as u32).to_be_bytes());
                frame.extend_from_slice(&data);
                uploaded_chunks += 1;
                uploaded_bytes += data.len() as u64;
                if frame.len() >= opts.upload_batch_bytes {
                    let sent: u64 = frame_payload_bytes(&frame);
                    self.put_chunk_frame(
                        project_id,
                        repo,
                        git_username,
                        git_token,
                        &session.session_id,
                        Some((&token, total, offset)),
                        std::mem::take(&mut frame),
                    )
                    .await?;
                    offset += sent;
                    emit(PushEvent::UploadedBatch {
                        chunks: uploaded_chunks,
                        bytes: uploaded_bytes,
                    });
                }
            }
            if !frame.is_empty() {
                self.put_chunk_frame(
                    project_id,
                    repo,
                    git_username,
                    git_token,
                    &session.session_id,
                    Some((&token, total, offset)),
                    frame,
                )
                .await?;
                emit(PushEvent::UploadedBatch {
                    chunks: uploaded_chunks,
                    bytes: uploaded_bytes,
                });
            }
            file_tokens[i] = Some(token);
        }

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
                let mut reader: Box<dyn Read> = match &file.source {
                    PushSource::Path(p) => Box::new(std::fs::File::open(p).map_err(io_err)?),
                    PushSource::Bytes(b) => Box::new(std::io::Cursor::new(b.clone())),
                };
                for (hash, size) in &file.chunks {
                    let mut data = vec![0u8; *size as usize];
                    reader.read_exact(&mut data).map_err(io_err)?;
                    if !to_upload.remove(hash) {
                        continue;
                    }
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
                let mut reader: Box<dyn Read> = match &file.source {
                    PushSource::Path(p) => Box::new(std::fs::File::open(p).map_err(io_err)?),
                    PushSource::Bytes(b) => Box::new(std::io::Cursor::new(b.clone())),
                };
                for (hash, size) in &file.chunks {
                    let mut data = vec![0u8; *size as usize];
                    reader.read_exact(&mut data).map_err(io_err)?;
                    if !to_upload.remove(hash) {
                        continue;
                    }
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

        // 5. Commit by reference. The server re-verifies identity by reading chunks back.
        let commit_files: Vec<CommitFileWire> = chunked
            .iter()
            .enumerate()
            .map(|(i, f)| CommitFileWire {
                path: f.repo_path.clone(),
                chunks: f
                    .chunks
                    .iter()
                    .map(|(h, s)| CommitChunkWire {
                        hash: hex_lower(h),
                        size: *s,
                    })
                    .collect(),
                content: (!f.delete && f.chunks.is_empty()).then(String::new),
                file_token: file_tokens[i].clone(),
                delete: f.delete,
                mode: f.mode.map(|m| format!("{m:o}")),
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
        let (req, trace_id) = self.git_request(
            Method::POST,
            project_id,
            repo,
            Some(&commit_suffix),
            git_username,
            git_token,
        )?;
        let resp: CommitByReferenceResponse = expect_json(req.json(&body).send().await?).await?;
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
            let (req, _t) = self.git_request(
                Method::POST,
                project_id,
                repo,
                Some(&format!("ingest/sessions/{}/missing", session.session_id)),
                git_username,
                git_token,
            )?;
            let resp: MissingRespWire =
                expect_json(req.json(&MissingWire { hashes: &hashes }).send().await?).await?;
            for h in resp.missing {
                let mut arr = [0u8; 32];
                hex::decode_to_slice(&h, &mut arr)
                    .map_err(|e| SdkError::ClientError(format!("bad hash from server: {e}")))?;
                missing.insert(arr);
            }
        }
        Ok(missing)
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
        let (req, _t) = self.git_request(
            Method::POST,
            project_id,
            repo,
            Some(&format!("ingest/sessions/{session_id}/staging")),
            git_username,
            git_token,
        )?;
        expect_json(req.send().await?).await
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
        // would conflict with the presigned signature).
        let resp = self.git_client.put(url).body(pack).send().await?;
        if !resp.status().is_success() {
            return Err(SdkError::ServerError {
                status: resp.status(),
                message: format!("staged pack PUT: {}", resp.text().await.unwrap_or_default()),
            });
        }
        // Bounded batches: the server caps entries per registration call.
        const REGISTER_BATCH: usize = 4096;
        let mut remaining = entries;
        while !remaining.is_empty() {
            let take = remaining.len().min(REGISTER_BATCH);
            let batch: Vec<StagedEntryWire> = remaining.drain(..take).collect();
            let (req, _t) = self.git_request(
                Method::POST,
                project_id,
                repo,
                Some(&format!("ingest/sessions/{session_id}/staged")),
                git_username,
                git_token,
            )?;
            let body = StagedRegisterWire {
                pack_id: target.pack_id.clone(),
                entries: batch,
            };
            expect_ok(req.json(&body).send().await?).await?;
        }
        Ok(())
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
        let (req, _t) = self.git_request(
            Method::PUT,
            project_id,
            repo,
            Some(&suffix),
            git_username,
            git_token,
        )?;
        let resp = req.body(frame).send().await?;
        expect_ok(resp).await
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
        let mut reference = gsvc_codec::BlobOidHasher::new(data.len() as u64);
        reference.update(&data);
        assert_eq!(oid_a, reference.finalize().to_hex());
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
}
