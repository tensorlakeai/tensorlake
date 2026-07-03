//! Fast clone client: install trusted pack artifacts directly, then leave a normal Git repo.
//!
//! This is not a new Git protocol. The gsvc server exposes a read-only manifest and immutable
//! pack/idx artifacts; this client downloads them into a content-addressed local cache,
//! hardlinks/copies self-contained packs into `.git/objects/pack`, fixes any advertised thin packs
//! after their bases are installed, writes refs, and checks out the requested HEAD. After that, the
//! checkout uses ordinary `git fetch` / `git push` against the original remote.
//!
//! Vendored from `artifact_storage` (`crates/gsvc-server/src/fastclone.rs` and the
//! `FastCloneManifest` wire types in `crates/gsvc-server/src/service.rs`). If the manifest format or
//! client behavior changes upstream, open a companion PR here to keep this copy in sync — see the
//! note in that repo's `AGENTS.md`.

use std::collections::HashSet;
use std::fs;
use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use flate2::Compression;
use gsvc_codec::{BlobOidHasher, IdxV2, Oid};
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use tokio::io::AsyncWriteExt;

/// Read-only manifest served by gsvc at `<repo>/fast/clone-manifest`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FastCloneManifest {
    pub version: u32,
    #[serde(default)]
    pub source: FastCloneManifestSource,
    pub repo: String,
    pub head: Option<String>,
    pub refs: Vec<FastCloneRef>,
    pub packs: Vec<FastClonePack>,
    pub large_blobs: Vec<FastCloneBlob>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FastCloneManifestSource {
    Empty,
    #[default]
    Verbatim,
    Optimized,
    Mixed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FastCloneRef {
    pub name: String,
    pub oid: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FastClonePack {
    pub pack_id: String,
    pub pack_hash: String,
    #[serde(default)]
    pub source: FastClonePackSource,
    #[serde(default)]
    pub requires_fix_thin: bool,
    pub pack_bytes: u64,
    pub idx_bytes: u64,
    pub object_count: u32,
    pub pack_path: String,
    pub idx_path: String,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FastClonePackSource {
    #[default]
    Verbatim,
    Optimized,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FastCloneBlob {
    pub oid: String,
    pub bytes: u64,
    pub path: String,
}

#[derive(Clone, Debug)]
pub struct FastCloneOptions {
    /// Clean https(s) repo URL, no embedded credentials (e.g. from `repo_url()`).
    pub repo_url: String,
    pub dest: PathBuf,
    pub cache_dir: Option<PathBuf>,
    pub cache_max_bytes: Option<u64>,
    /// Basic-auth credential for the gsvc origin, e.g. from a minted git token.
    pub credential: Option<BasicAuth>,
    pub checkout: bool,
    /// Spinner already shown by the caller (e.g. while minting a credential); reused and
    /// switched to a byte progress bar once the manifest's artifact sizes are known.
    pub progress: Option<ProgressBar>,
}

/// A spinner for indeterminate-length work (auth, manifest fetch), or `None` when stderr isn't a
/// TTY. `fast_clone` converts it into a byte progress bar once download sizes are known.
pub fn new_spinner(message: &str) -> Option<ProgressBar> {
    if !std::io::IsTerminal::is_terminal(&std::io::stderr()) {
        return None;
    }
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"])
            .template("{spinner} {msg}")
            .unwrap(),
    );
    pb.set_message(message.to_string());
    pb.enable_steady_tick(std::time::Duration::from_millis(80));
    Some(pb)
}

#[derive(Clone, Debug, Default)]
pub struct FastCloneStats {
    pub packs: usize,
    pub pack_bytes: u64,
    pub idx_bytes: u64,
    pub downloaded_bytes: u64,
    pub reused_bytes: u64,
    pub installed_bytes: u64,
    pub large_blobs: usize,
    pub large_blob_bytes: u64,
    pub cache_pruned_bytes: u64,
    pub cache_pruned_files: usize,
    pub git_commands: Vec<Vec<String>>,
}

#[derive(Clone, Debug)]
pub struct BasicAuth {
    pub username: String,
    pub password: Option<String>,
}

struct HttpCtx {
    client: reqwest::Client,
    origin: Url,
    auth: Option<BasicAuth>,
    progress: Option<ProgressBar>,
}

fn byte_progress_style() -> ProgressStyle {
    ProgressStyle::with_template(
        "{spinner} {bytes}/{total_bytes} ({bytes_per_sec}, eta {eta}) {msg}",
    )
    .unwrap()
    .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"])
}

/// Run a fast clone into `opts.dest`. `opts.progress`, if set, is shown while resolving the
/// manifest and then converted into a byte progress bar once download sizes are known.
pub async fn fast_clone(opts: FastCloneOptions) -> Result<FastCloneStats> {
    ensure_clone_target_available(&opts.dest)?;
    let mut clean_base = Url::parse(&opts.repo_url)
        .with_context(|| format!("invalid repo URL: {}", opts.repo_url))?;
    if !matches!(clean_base.scheme(), "http" | "https") {
        bail!("fast-clone requires an http(s) gsvc repo URL");
    }
    if !clean_base.path().ends_with('/') {
        let path = format!("{}/", clean_base.path());
        clean_base.set_path(&path);
    }
    let mut ctx = HttpCtx {
        client: reqwest::Client::builder().build()?,
        origin: clean_base.clone(),
        auth: opts.credential.clone(),
        progress: opts.progress,
    };
    let manifest_url = clean_base.join("fast/clone-manifest")?;
    let manifest: FastCloneManifest = get_json(&ctx, manifest_url).await?;
    if manifest.version != 1 {
        bail!(
            "unsupported fast-clone manifest version {}",
            manifest.version
        );
    }

    let cache_dir = opts.cache_dir.clone().unwrap_or_else(default_cache_dir);
    std::fs::create_dir_all(&cache_dir)
        .with_context(|| format!("create cache dir {}", cache_dir.display()))?;

    let total_bytes: u64 = manifest
        .packs
        .iter()
        .map(|p| p.pack_bytes + p.idx_bytes)
        .sum::<u64>()
        + manifest.large_blobs.iter().map(|b| b.bytes).sum::<u64>();
    if let Some(pb) = &ctx.progress {
        pb.set_style(byte_progress_style());
        pb.set_length(total_bytes);
        pb.set_position(0);
        pb.set_message("fetching pack artifacts");
    } else if std::io::IsTerminal::is_terminal(&std::io::stderr()) {
        let pb = ProgressBar::new(total_bytes);
        pb.set_style(byte_progress_style());
        pb.set_message("fetching pack artifacts");
        pb.enable_steady_tick(std::time::Duration::from_millis(80));
        ctx.progress = Some(pb);
    }

    let mut stats = FastCloneStats {
        packs: manifest.packs.len(),
        large_blobs: manifest.large_blobs.len(),
        ..Default::default()
    };
    for pack in &manifest.packs {
        let _storage_id = Oid::from_hex(&pack.pack_id)
            .with_context(|| format!("bad fast-clone pack id {}", pack.pack_id))?;
        let pack_hash = Oid::from_hex(&pack.pack_hash)
            .with_context(|| format!("bad fast-clone pack hash {}", pack.pack_hash))?;
        stats.pack_bytes += pack.pack_bytes;
        stats.idx_bytes += pack.idx_bytes;

        let pack_cache = cache_dir.join(format!("pack-{}.pack", pack.pack_id));
        let idx_cache = cache_dir.join(format!("pack-{}.idx", pack.pack_id));
        let pack_url = resolve_artifact_url(&clean_base, &pack.pack_path)?;
        let idx_url = resolve_artifact_url(&clean_base, &pack.idx_path)?;

        if ensure_cached_artifact(&ctx, pack_url, &pack_cache, pack.pack_bytes, |path| {
            validate_pack_cache(path, pack.pack_bytes, pack_hash)
        })
        .await?
        {
            stats.downloaded_bytes += pack.pack_bytes;
        } else {
            stats.reused_bytes += pack.pack_bytes;
        }
        if ensure_cached_artifact(&ctx, idx_url, &idx_cache, pack.idx_bytes, |path| {
            validate_idx_cache(path, pack.idx_bytes, pack_hash)
        })
        .await?
        {
            stats.downloaded_bytes += pack.idx_bytes;
        } else {
            stats.reused_bytes += pack.idx_bytes;
        }
    }
    for blob in &manifest.large_blobs {
        let oid = Oid::from_hex(&blob.oid).with_context(|| format!("bad blob oid {}", blob.oid))?;
        stats.large_blob_bytes += blob.bytes;
        let blob_cache = cache_dir.join(format!("blob-{}.loose", blob.oid));
        let blob_url = resolve_artifact_url(&clean_base, &blob.path)?;
        if ensure_loose_blob_cached(&ctx, blob_url, &blob_cache, oid, blob.bytes).await? {
            stats.downloaded_bytes += blob.bytes;
        } else {
            stats.reused_bytes += blob.bytes;
        }
    }

    if let Some(pb) = ctx.progress.take() {
        pb.finish_and_clear();
    }
    eprintln!("installing objects into {}", opts.dest.display());

    run_git_outside(&mut stats, &["init", "-q", opts.dest.to_str().unwrap()])?;
    let git_dir = opts.dest.join(".git");
    let pack_dir = git_dir.join("objects/pack");
    std::fs::create_dir_all(&pack_dir)
        .with_context(|| format!("create pack dir {}", pack_dir.display()))?;

    for pack in manifest.packs.iter().filter(|pack| !pack.requires_fix_thin) {
        let pack_cache = cache_dir.join(format!("pack-{}.pack", pack.pack_id));
        let idx_cache = cache_dir.join(format!("pack-{}.idx", pack.pack_id));
        let pack_dst = pack_dir.join(format!("pack-{}.pack", pack.pack_id));
        let idx_dst = pack_dir.join(format!("pack-{}.idx", pack.pack_id));
        link_or_copy(&pack_cache, &pack_dst)?;
        link_or_copy(&idx_cache, &idx_dst)?;
        stats.installed_bytes += pack.pack_bytes + pack.idx_bytes;
    }
    for blob in &manifest.large_blobs {
        let blob_cache = cache_dir.join(format!("blob-{}.loose", blob.oid));
        install_loose_object(&blob_cache, &git_dir, &blob.oid)?;
        stats.installed_bytes += blob.bytes;
    }
    for pack in manifest.packs.iter().filter(|pack| pack.requires_fix_thin) {
        let pack_cache = cache_dir.join(format!("pack-{}.pack", pack.pack_id));
        let installed = fix_thin_pack(&mut stats, &opts.dest, &git_dir, &pack_cache)?;
        stats.installed_bytes += installed;
    }

    write_refs(&git_dir, &manifest)?;
    configure_remote(&mut stats, &opts.dest, &opts.repo_url, &manifest)?;

    if opts.checkout && !manifest.refs.is_empty() {
        run_git(&mut stats, &opts.dest, &["reset", "--hard", "-q", "HEAD"])?;
    }
    if let Some(max_bytes) = opts.cache_max_bytes {
        let protected = protected_cache_entries(&manifest)?;
        let (files, bytes) = prune_cache_dir(&cache_dir, max_bytes, &protected)?;
        stats.cache_pruned_files = files;
        stats.cache_pruned_bytes = bytes;
    }
    Ok(stats)
}

pub fn format_fast_clone_stats(prefix: &str, stats: &FastCloneStats) -> String {
    let mut out = format!(
        "{prefix} installed {} pack(s), {} loose blob(s), downloaded {:.2} MiB, reused {:.2} MiB",
        stats.packs,
        stats.large_blobs,
        stats.downloaded_bytes as f64 / (1024.0 * 1024.0),
        stats.reused_bytes as f64 / (1024.0 * 1024.0)
    );
    if stats.cache_pruned_files > 0 {
        out.push_str(&format!(
            ", pruned {:.2} MiB/{} file(s)",
            stats.cache_pruned_bytes as f64 / (1024.0 * 1024.0),
            stats.cache_pruned_files
        ));
    }
    out
}

pub fn parse_cache_max_bytes(value: &str) -> Result<u64> {
    let raw = value.trim();
    if raw.is_empty() {
        bail!("cache size cannot be empty");
    }
    let lower = raw.to_ascii_lowercase();
    let suffixes = [
        ("tib", 1024_u64.pow(4)),
        ("tb", 1024_u64.pow(4)),
        ("t", 1024_u64.pow(4)),
        ("gib", 1024_u64.pow(3)),
        ("gb", 1024_u64.pow(3)),
        ("g", 1024_u64.pow(3)),
        ("mib", 1024_u64.pow(2)),
        ("mb", 1024_u64.pow(2)),
        ("m", 1024_u64.pow(2)),
        ("kib", 1024),
        ("kb", 1024),
        ("k", 1024),
        ("b", 1),
    ];
    let (digits, multiplier) = suffixes
        .iter()
        .find_map(|(suffix, multiplier)| {
            lower
                .strip_suffix(suffix)
                .map(|digits| (digits.trim(), *multiplier))
        })
        .unwrap_or((raw, 1));
    let bytes = digits
        .parse::<u64>()
        .with_context(|| format!("invalid cache size {value:?}"))?;
    bytes
        .checked_mul(multiplier)
        .ok_or_else(|| anyhow::anyhow!("cache size {value:?} is too large"))
}

/// Derive the default destination directory from a repository URL, matching `git clone` convention.
pub fn default_dest_from_url(raw: &str) -> PathBuf {
    let parsed = Url::parse(raw).ok();
    let name = parsed
        .as_ref()
        .and_then(|u| u.path_segments().and_then(|mut s| s.next_back()))
        .filter(|s| !s.is_empty())
        .unwrap_or("repo");
    let name = name.strip_suffix(".git").unwrap_or(name);
    PathBuf::from(name)
}

async fn get_json<T: serde::de::DeserializeOwned>(ctx: &HttpCtx, url: Url) -> Result<T> {
    let resp = authed_get(ctx, url.clone()).send().await?;
    let status = resp.status();
    let body = resp
        .text()
        .await
        .with_context(|| format!("GET {url} ({status}): failed reading response body"))?;
    if !status.is_success() {
        bail!("GET {url} failed with {status}: {body}");
    }
    serde_json::from_str(&body).with_context(|| {
        let preview: String = body.chars().take(500).collect();
        format!("GET {url} ({status}): failed to decode JSON response body: {preview:?}")
    })
}

async fn ensure_cached_artifact<F>(
    ctx: &HttpCtx,
    url: Url,
    path: &Path,
    expected_bytes: u64,
    validate: F,
) -> Result<bool>
where
    F: Fn(&Path) -> Result<()>,
{
    if path.exists() {
        match file_size(path)? {
            Some(size) if size == expected_bytes => match validate(path) {
                Ok(()) => {
                    if let Some(pb) = &ctx.progress {
                        pb.inc(expected_bytes);
                    }
                    return Ok(false);
                }
                Err(_) => {
                    let _ = fs::remove_file(path);
                }
            },
            Some(_) => {
                let _ = fs::remove_file(path);
            }
            None => {}
        }
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create cache parent {}", parent.display()))?;
    }
    let tmp = path.with_extension(format!(
        "tmp-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let mut resp = authed_get(ctx, url.clone()).send().await?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        bail!("GET {url} failed with {status}: {body}");
    }
    let mut out = tokio::fs::File::create(&tmp)
        .await
        .with_context(|| format!("create temp artifact {}", tmp.display()))?;
    let mut written = 0u64;
    while let Some(chunk) = resp.chunk().await? {
        written += chunk.len() as u64;
        if let Some(pb) = &ctx.progress {
            pb.inc(chunk.len() as u64);
        }
        out.write_all(&chunk).await?;
    }
    out.flush().await?;
    drop(out);
    if written != expected_bytes {
        let _ = fs::remove_file(&tmp);
        bail!(
            "downloaded {} bytes from {}, expected {}",
            written,
            url,
            expected_bytes
        );
    }
    if let Err(err) = validate(&tmp) {
        let _ = fs::remove_file(&tmp);
        return Err(err).with_context(|| format!("validate artifact from {url}"));
    }
    let _ = fs::remove_file(path);
    fs::rename(&tmp, path).with_context(|| format!("commit cache artifact {}", path.display()))?;
    Ok(true)
}

async fn ensure_loose_blob_cached(
    ctx: &HttpCtx,
    url: Url,
    path: &Path,
    oid: Oid,
    expected_bytes: u64,
) -> Result<bool> {
    if path.exists() {
        match validate_loose_blob_cache(path, oid, expected_bytes) {
            Ok(()) => {
                if let Some(pb) = &ctx.progress {
                    pb.inc(expected_bytes);
                }
                return Ok(false);
            }
            Err(_) => {
                let _ = fs::remove_file(path);
            }
        }
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create cache parent {}", parent.display()))?;
    }
    let tmp = path.with_extension(format!(
        "tmp-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let mut resp = authed_get(ctx, url.clone()).send().await?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        bail!("GET {url} failed with {status}: {body}");
    }
    let mut hasher = BlobOidHasher::new(expected_bytes);
    let file = fs::File::create(&tmp)
        .with_context(|| format!("create temp loose object {}", tmp.display()))?;
    let mut enc = flate2::write::ZlibEncoder::new(file, Compression::default());
    enc.write_all(format!("blob {expected_bytes}\0").as_bytes())?;
    let mut written = 0u64;
    while let Some(chunk) = resp.chunk().await? {
        written += chunk.len() as u64;
        if let Some(pb) = &ctx.progress {
            pb.inc(chunk.len() as u64);
        }
        hasher.update(&chunk);
        enc.write_all(&chunk)?;
    }
    let file = enc.finish()?;
    file.sync_all()?;
    if written != expected_bytes {
        let _ = fs::remove_file(&tmp);
        bail!(
            "downloaded {} bytes from {}, expected {}",
            written,
            url,
            expected_bytes
        );
    }
    let actual = hasher.finalize();
    if actual != oid {
        let _ = fs::remove_file(&tmp);
        bail!(
            "blob {} failed oid verification: downloaded {}",
            oid.to_hex(),
            actual.to_hex()
        );
    }
    validate_loose_blob_cache(&tmp, oid, expected_bytes)?;
    let _ = fs::remove_file(path);
    fs::rename(&tmp, path)
        .with_context(|| format!("commit loose-object cache {}", path.display()))?;
    Ok(true)
}

fn validate_pack_cache(path: &Path, expected_bytes: u64, pack_hash: Oid) -> Result<()> {
    let mut file = fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let len = file
        .metadata()
        .with_context(|| format!("stat {}", path.display()))?
        .len();
    if len != expected_bytes {
        bail!(
            "pack cache {} has {} bytes, expected {}",
            path.display(),
            len,
            expected_bytes
        );
    }
    if len < 32 {
        bail!("pack cache {} is too short", path.display());
    }
    let mut magic = [0u8; 4];
    file.read_exact(&mut magic)
        .with_context(|| format!("read pack header {}", path.display()))?;
    if &magic != b"PACK" {
        bail!("pack cache {} has bad PACK header", path.display());
    }
    let mut trailer = [0u8; 20];
    file.seek(SeekFrom::End(-20))
        .with_context(|| format!("seek pack trailer {}", path.display()))?;
    file.read_exact(&mut trailer)
        .with_context(|| format!("read pack trailer {}", path.display()))?;
    let actual = Oid::from_bytes(&trailer)?;
    if actual != pack_hash {
        bail!(
            "pack cache {} trailer is {}, expected {}",
            path.display(),
            actual.to_hex(),
            pack_hash.to_hex()
        );
    }
    Ok(())
}

fn validate_idx_cache(path: &Path, expected_bytes: u64, pack_hash: Oid) -> Result<()> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    if bytes.len() as u64 != expected_bytes {
        bail!(
            "idx cache {} has {} bytes, expected {}",
            path.display(),
            bytes.len(),
            expected_bytes
        );
    }
    if bytes.len() < 40 {
        bail!("idx cache {} is too short", path.display());
    }
    IdxV2::parse(&bytes).with_context(|| format!("parse idx {}", path.display()))?;
    let pack_hash_off = bytes.len() - 40;
    let idx_hash_off = bytes.len() - 20;
    let actual_pack = Oid::from_bytes(&bytes[pack_hash_off..idx_hash_off])?;
    if actual_pack != pack_hash {
        bail!(
            "idx cache {} points at pack {}, expected {}",
            path.display(),
            actual_pack.to_hex(),
            pack_hash.to_hex()
        );
    }
    let mut hasher = Sha1::new();
    hasher.update(&bytes[..idx_hash_off]);
    let digest: [u8; 20] = hasher.finalize().into();
    if digest.as_slice() != &bytes[idx_hash_off..] {
        bail!("idx cache {} has bad checksum", path.display());
    }
    Ok(())
}

fn validate_loose_blob_cache(path: &Path, oid: Oid, expected_bytes: u64) -> Result<()> {
    let file = fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut decoder = flate2::read::ZlibDecoder::new(file);
    let mut hasher = BlobOidHasher::new(expected_bytes);
    let mut header = Vec::with_capacity(32);
    let mut saw_header = false;
    let mut content_bytes = 0u64;
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = decoder
            .read(&mut buf)
            .with_context(|| format!("inflate loose blob {}", path.display()))?;
        if n == 0 {
            break;
        }
        let mut data = &buf[..n];
        if !saw_header {
            let Some(pos) = data.iter().position(|b| *b == 0) else {
                header.extend_from_slice(data);
                if header.len() > 64 {
                    bail!("loose blob {} has oversized header", path.display());
                }
                continue;
            };
            header.extend_from_slice(&data[..pos]);
            let header_text = std::str::from_utf8(&header)
                .with_context(|| format!("decode loose blob header {}", path.display()))?;
            let expected_header = format!("blob {expected_bytes}");
            if header_text != expected_header {
                bail!(
                    "loose blob {} has header {:?}, expected {:?}",
                    path.display(),
                    header_text,
                    expected_header
                );
            }
            saw_header = true;
            data = &data[pos + 1..];
        }
        content_bytes += data.len() as u64;
        hasher.update(data);
    }
    if !saw_header {
        bail!("loose blob {} is missing object header", path.display());
    }
    if content_bytes != expected_bytes {
        bail!(
            "loose blob {} has {} bytes, expected {}",
            path.display(),
            content_bytes,
            expected_bytes
        );
    }
    let actual = hasher.finalize();
    if actual != oid {
        bail!(
            "loose blob {} hashes to {}, expected {}",
            path.display(),
            actual.to_hex(),
            oid.to_hex()
        );
    }
    Ok(())
}

fn protected_cache_entries(manifest: &FastCloneManifest) -> Result<HashSet<String>> {
    let mut protected = HashSet::new();
    for pack in &manifest.packs {
        let _storage_id = Oid::from_hex(&pack.pack_id)
            .with_context(|| format!("bad fast-clone pack id {}", pack.pack_id))?;
        protected.insert(format!("pack-{}.pack", pack.pack_id));
        protected.insert(format!("pack-{}.idx", pack.pack_id));
    }
    for blob in &manifest.large_blobs {
        let oid = Oid::from_hex(&blob.oid).with_context(|| format!("bad blob oid {}", blob.oid))?;
        protected.insert(format!("blob-{}.loose", oid));
    }
    Ok(protected)
}

fn prune_cache_dir(
    cache_dir: &Path,
    max_bytes: u64,
    protected: &HashSet<String>,
) -> Result<(usize, u64)> {
    let mut total = 0u64;
    let mut candidates = Vec::new();
    for entry in match fs::read_dir(cache_dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((0, 0)),
        Err(e) => return Err(e).with_context(|| format!("read cache dir {}", cache_dir.display())),
    } {
        let entry = entry.with_context(|| format!("read cache dir {}", cache_dir.display()))?;
        let meta = entry
            .metadata()
            .with_context(|| format!("stat cache entry {}", entry.path().display()))?;
        if !meta.is_file() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(|s| s.to_string()) else {
            continue;
        };
        if !is_cache_artifact_name(&name) {
            continue;
        }
        total = total.saturating_add(meta.len());
        if !protected.contains(&name) {
            candidates.push((
                meta.modified().unwrap_or(UNIX_EPOCH),
                entry.path(),
                meta.len(),
            ));
        }
    }
    if total <= max_bytes {
        return Ok((0, 0));
    }
    candidates.sort_by_key(|(modified, _, _)| *modified);
    let mut pruned_files = 0usize;
    let mut pruned_bytes = 0u64;
    for (_, path, bytes) in candidates {
        if total <= max_bytes {
            break;
        }
        match fs::remove_file(&path) {
            Ok(()) => {
                total = total.saturating_sub(bytes);
                pruned_files += 1;
                pruned_bytes = pruned_bytes.saturating_add(bytes);
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e).with_context(|| format!("remove {}", path.display())),
        }
    }
    Ok((pruned_files, pruned_bytes))
}

fn is_cache_artifact_name(name: &str) -> bool {
    (name.starts_with("pack-") && (name.ends_with(".pack") || name.ends_with(".idx")))
        || (name.starts_with("blob-") && name.ends_with(".loose"))
}

fn authed_get(ctx: &HttpCtx, url: Url) -> reqwest::RequestBuilder {
    let req = ctx.client.get(url.clone());
    if same_origin(&ctx.origin, &url) {
        if let Some(auth) = &ctx.auth {
            return req.basic_auth(auth.username.clone(), auth.password.clone());
        }
    }
    req
}

fn same_origin(a: &Url, b: &Url) -> bool {
    a.scheme() == b.scheme()
        && a.host_str() == b.host_str()
        && a.port_or_known_default() == b.port_or_known_default()
}

fn resolve_artifact_url(repo_base: &Url, value: &str) -> Result<Url> {
    match Url::parse(value) {
        Ok(u) => Ok(u),
        Err(_) => Ok(repo_base.join(value)?),
    }
}

fn ensure_clone_target_available(dest: &Path) -> Result<()> {
    if !dest.exists() {
        return Ok(());
    }
    if !dest.is_dir() {
        bail!(
            "destination exists and is not a directory: {}",
            dest.display()
        );
    }
    if dest
        .read_dir()
        .with_context(|| format!("read destination {}", dest.display()))?
        .next()
        .is_some()
    {
        bail!("destination directory is not empty: {}", dest.display());
    }
    Ok(())
}

fn write_refs(git_dir: &Path, manifest: &FastCloneManifest) -> Result<()> {
    let mut packed = String::from("# pack-refs with: peeled fully-peeled sorted\n");
    for r in &manifest.refs {
        if let Some(name) = packed_ref_name(&r.name) {
            packed.push_str(&r.oid);
            packed.push(' ');
            packed.push_str(&name);
            packed.push('\n');
        }
    }
    std::fs::write(git_dir.join("packed-refs"), packed)
        .with_context(|| format!("write {}", git_dir.join("packed-refs").display()))?;

    let head_target = manifest
        .head
        .as_deref()
        .unwrap_or("refs/heads/main")
        .to_string();
    std::fs::write(git_dir.join("HEAD"), format!("ref: {head_target}\n"))
        .with_context(|| format!("write {}", git_dir.join("HEAD").display()))?;

    if let Some(head_ref) = manifest.refs.iter().find(|r| r.name == head_target) {
        write_loose_ref(git_dir, &head_target, &head_ref.oid)?;
    }
    Ok(())
}

fn packed_ref_name(name: &str) -> Option<String> {
    if let Some(short) = name.strip_prefix("refs/heads/") {
        Some(format!("refs/remotes/origin/{short}"))
    } else if name.starts_with("refs/tags/") || name.starts_with("refs/") {
        Some(name.to_string())
    } else {
        None
    }
}

fn write_loose_ref(git_dir: &Path, name: &str, oid: &str) -> Result<()> {
    ensure_safe_ref_name(name)?;
    let path = git_dir.join(name);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create ref dir {}", parent.display()))?;
    }
    std::fs::write(&path, format!("{oid}\n"))
        .with_context(|| format!("write loose ref {}", path.display()))?;
    Ok(())
}

fn ensure_safe_ref_name(name: &str) -> Result<()> {
    let path = Path::new(name);
    if path.is_absolute() {
        bail!("absolute ref path in manifest: {name}");
    }
    for c in path.components() {
        if matches!(
            c,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        ) {
            bail!("unsafe ref path in manifest: {name}");
        }
    }
    Ok(())
}

fn configure_remote(
    stats: &mut FastCloneStats,
    dest: &Path,
    repo_url: &str,
    manifest: &FastCloneManifest,
) -> Result<()> {
    run_git(stats, dest, &["config", "remote.origin.url", repo_url])?;
    run_git(
        stats,
        dest,
        &[
            "config",
            "remote.origin.fetch",
            "+refs/heads/*:refs/remotes/origin/*",
        ],
    )?;
    if let Some(head) = manifest.head.as_deref() {
        if let Some(branch) = head.strip_prefix("refs/heads/") {
            run_git(
                stats,
                dest,
                &["config", &format!("branch.{branch}.remote"), "origin"],
            )?;
            run_git(
                stats,
                dest,
                &["config", &format!("branch.{branch}.merge"), head],
            )?;
            run_git(
                stats,
                dest,
                &[
                    "symbolic-ref",
                    "refs/remotes/origin/HEAD",
                    &format!("refs/remotes/origin/{branch}"),
                ],
            )?;
        }
    }
    Ok(())
}

fn run_git_outside(stats: &mut FastCloneStats, args: &[&str]) -> Result<()> {
    stats
        .git_commands
        .push(args.iter().map(|s| s.to_string()).collect());
    let out = Command::new("git")
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .context("spawn git")?;
    if !out.status.success() {
        bail!(
            "git {} failed:\n{}{}",
            args.join(" "),
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(())
}

fn run_git(stats: &mut FastCloneStats, cwd: &Path, args: &[&str]) -> Result<()> {
    stats
        .git_commands
        .push(args.iter().map(|s| s.to_string()).collect());
    let out = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .context("spawn git")?;
    if !out.status.success() {
        bail!(
            "git {} failed in {}:\n{}{}",
            args.join(" "),
            cwd.display(),
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(())
}

fn fix_thin_pack(
    stats: &mut FastCloneStats,
    cwd: &Path,
    git_dir: &Path,
    pack_cache: &Path,
) -> Result<u64> {
    stats.git_commands.push(vec![
        "index-pack".to_string(),
        "--fix-thin".to_string(),
        "--stdin".to_string(),
        format!("<{}", pack_cache.display()),
    ]);
    let input = fs::File::open(pack_cache)
        .with_context(|| format!("open thin pack cache {}", pack_cache.display()))?;
    let out = Command::new("git")
        .args(["index-pack", "--fix-thin", "--stdin"])
        .current_dir(cwd)
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::from(input))
        .output()
        .context("spawn git")?;
    if !out.status.success() {
        bail!(
            "git index-pack --fix-thin --stdin failed in {} for {}:\n{}{}",
            cwd.display(),
            pack_cache.display(),
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let pack_hash = parse_index_pack_output(&out.stdout)?;
    let pack_dir = git_dir.join("objects/pack");
    let pack_dst = pack_dir.join(format!("pack-{pack_hash}.pack"));
    let idx_dst = pack_dir.join(format!("pack-{pack_hash}.idx"));
    Ok(file_size(&pack_dst)?.unwrap_or(0) + file_size(&idx_dst)?.unwrap_or(0))
}

fn parse_index_pack_output(stdout: &[u8]) -> Result<String> {
    let text = String::from_utf8_lossy(stdout);
    for line in text.lines() {
        let line = line.trim();
        if let Some(hash) = line
            .strip_prefix("pack")
            .map(str::trim_start)
            .filter(|hash| !hash.is_empty())
        {
            if hash.len() == 40 && hash.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Ok(hash.to_string());
            }
        }
    }
    bail!("git index-pack did not report a pack hash: {text:?}")
}

fn link_or_copy(src: &Path, dst: &Path) -> Result<()> {
    if file_size(dst)? == Some(file_size(src)?.unwrap_or(0)) {
        return Ok(());
    }
    if let Some(parent) = dst.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create install parent {}", parent.display()))?;
    }
    match std::fs::hard_link(src, dst) {
        Ok(()) => Ok(()),
        Err(_) => {
            std::fs::copy(src, dst)
                .with_context(|| format!("copy {} to {}", src.display(), dst.display()))?;
            Ok(())
        }
    }
}

fn install_loose_object(src: &Path, git_dir: &Path, oid: &str) -> Result<()> {
    if oid.len() != 40 || !oid.bytes().all(|b| b.is_ascii_hexdigit()) {
        bail!("bad loose object oid: {oid}");
    }
    let (dir, file) = oid.split_at(2);
    let dst = git_dir.join("objects").join(dir).join(file);
    if dst.exists() {
        return Ok(());
    }
    link_or_copy(src, &dst)
}

fn file_size(path: &Path) -> Result<Option<u64>> {
    match std::fs::metadata(path) {
        Ok(m) => Ok(Some(m.len())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("stat {}", path.display())),
    }
}

/// Default pack/idx/blob cache dir: `~/.cache/tensorlake/git-fast-clone` (or the platform cache dir).
fn default_cache_dir() -> PathBuf {
    dirs::cache_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("tensorlake")
        .join("git-fast-clone")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_dest_uses_repo_segment_from_project_scoped_url() {
        assert_eq!(
            default_dest_from_url("http://localhost:8080/demo/myrepo"),
            PathBuf::from("myrepo")
        );
        assert_eq!(
            default_dest_from_url("https://git.example.com/demo/myrepo.git"),
            PathBuf::from("myrepo")
        );
    }

    #[test]
    fn parse_cache_max_bytes_accepts_suffixes() {
        assert_eq!(parse_cache_max_bytes("512").unwrap(), 512);
        assert_eq!(
            parse_cache_max_bytes("2GiB").unwrap(),
            2 * 1024 * 1024 * 1024
        );
        assert_eq!(parse_cache_max_bytes("10 mb").unwrap(), 10 * 1024 * 1024);
    }
}
