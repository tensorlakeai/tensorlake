//! Local workspace state for `tl fs`: the `.tlfs/` directory at a mount root.
//!
//! A mounted file system is a plain directory of files plus `.tlfs/` holding:
//! - `state.json` — which repo/workspace this directory belongs to and its base snapshot;
//! - `manifest.json` — the tracked files as of the last mount/snapshot/restore: path, git blob
//!   oid, mode, and the size/mtime pair used to skip re-hashing unchanged files on dirty scans.
//!
//! Ignore rules follow artifact_storage issue #24: `.tlfs` itself, a built-in set of build/cache
//! directory names, and any names listed in a `.tlignore` file at the root. Ignored paths are
//! workspace-local: they are never uploaded and do not survive restore elsewhere.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use gsvc_codec::BlobOidHasher;
use serde::{Deserialize, Serialize};

use crate::error::{CliError, Result};

pub const STATE_DIR: &str = ".tlfs";
const IGNORED_DIR_NAMES: &[&str] = &[
    ".git",
    ".tlfs",
    "node_modules",
    "target",
    "dist",
    ".cache",
    "__pycache__",
];

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkspaceState {
    pub project_id: String,
    pub repo: String,
    pub workspace_id: String,
    pub ref_name: String,
    /// The commit the local manifest reflects (base at mount; last snapshot/restore after).
    pub base_commit: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub oid: String,
    /// Octal git mode (100644 / 100755 / 120000).
    pub mode: u32,
    pub size: u64,
    pub mtime_ms: u64,
}

pub type Manifest = BTreeMap<String, ManifestEntry>;

/// One local change discovered by a dirty scan.
#[derive(Clone, Debug)]
pub enum Change {
    /// Added or modified: `(repo path, absolute path, mode, blob oid, size, mtime_ms)`. The
    /// oid is computed only when a manifest entry exists to compare against (to filter
    /// touched-but-unchanged files); new files skip the extra read and take their oid from the
    /// push report.
    Upsert {
        path: String,
        abs: PathBuf,
        mode: u32,
        oid: Option<String>,
        size: u64,
        mtime_ms: u64,
    },
    Delete {
        path: String,
    },
}

impl Change {
    pub fn path(&self) -> &str {
        match self {
            Change::Upsert { path, .. } => path,
            Change::Delete { path } => path,
        }
    }
}

pub fn state_dir(root: &Path) -> PathBuf {
    root.join(STATE_DIR)
}

pub fn load_state(root: &Path) -> Result<WorkspaceState> {
    let path = state_dir(root).join("state.json");
    let raw = std::fs::read(&path).map_err(|_| {
        CliError::usage(format!(
            "{} is not a tl fs mount (missing {}/state.json); run `tl fs mount` first",
            root.display(),
            STATE_DIR
        ))
    })?;
    Ok(serde_json::from_slice(&raw)?)
}

pub fn save_state(root: &Path, state: &WorkspaceState) -> Result<()> {
    let dir = state_dir(root);
    std::fs::create_dir_all(&dir)?;
    std::fs::write(dir.join("state.json"), serde_json::to_vec_pretty(state)?)?;
    Ok(())
}

pub fn load_manifest(root: &Path) -> Result<Manifest> {
    let raw = std::fs::read(state_dir(root).join("manifest.json"))?;
    Ok(serde_json::from_slice(&raw)?)
}

pub fn save_manifest(root: &Path, manifest: &Manifest) -> Result<()> {
    let dir = state_dir(root);
    std::fs::create_dir_all(&dir)?;
    std::fs::write(dir.join("manifest.json"), serde_json::to_vec(manifest)?)?;
    Ok(())
}

/// Names ignored at any depth: the built-in set plus `.tlignore` lines (comments with `#`,
/// blank lines skipped; a trailing `/` is stripped — v1 matches directory/file *names*).
pub fn ignored_names(root: &Path) -> Vec<String> {
    let mut names: Vec<String> = IGNORED_DIR_NAMES.iter().map(|s| s.to_string()).collect();
    if let Ok(raw) = std::fs::read_to_string(root.join(".tlignore")) {
        for line in raw.lines() {
            let line = line.trim().trim_end_matches('/');
            if !line.is_empty() && !line.starts_with('#') {
                names.push(line.to_string());
            }
        }
    }
    names
}

fn mtime_ms(meta: &std::fs::Metadata) -> u64 {
    meta.modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(unix)]
fn file_mode(meta: &std::fs::Metadata) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    if meta.permissions().mode() & 0o111 != 0 {
        0o100755
    } else {
        0o100644
    }
}

#[cfg(not(unix))]
fn file_mode(_meta: &std::fs::Metadata) -> u32 {
    0o100644
}

/// Git blob oid of a file's content (or of a symlink's target path).
pub fn blob_oid(abs: &Path, mode: u32) -> Result<String> {
    let bytes = if mode == 0o120000 {
        std::fs::read_link(abs)?
            .to_string_lossy()
            .into_owned()
            .into_bytes()
    } else {
        std::fs::read(abs)?
    };
    let mut hasher = BlobOidHasher::new(bytes.len() as u64);
    hasher.update(&bytes);
    Ok(hasher.finalize().to_hex())
}

/// Walk the mount and diff it against the manifest. Unchanged size+mtime skips hashing; a
/// size/mtime mismatch re-hashes and only reports a change when the blob oid actually differs
/// (the manifest entry's freshness is the caller's to update on snapshot).
pub fn scan_dirty(root: &Path, manifest: &Manifest) -> Result<Vec<Change>> {
    let ignored = ignored_names(root);
    let mut seen: BTreeMap<String, (PathBuf, u32, u64, u64)> = BTreeMap::new();
    walk(root, root, &ignored, &mut seen)?;

    let mut changes = Vec::new();
    for (path, (abs, mode, size, mtime)) in &seen {
        match manifest.get(path) {
            Some(entry)
                if entry.mode == *mode && entry.size == *size && entry.mtime_ms == *mtime => {}
            Some(entry) => {
                let oid = blob_oid(abs, *mode)?;
                if oid != entry.oid || entry.mode != *mode {
                    changes.push(Change::Upsert {
                        path: path.clone(),
                        abs: abs.clone(),
                        mode: *mode,
                        oid: Some(oid),
                        size: *size,
                        mtime_ms: *mtime,
                    });
                }
            }
            None => {
                changes.push(Change::Upsert {
                    path: path.clone(),
                    abs: abs.clone(),
                    mode: *mode,
                    oid: None,
                    size: *size,
                    mtime_ms: *mtime,
                });
            }
        }
    }
    for path in manifest.keys() {
        if !seen.contains_key(path) {
            changes.push(Change::Delete { path: path.clone() });
        }
    }
    Ok(changes)
}

fn walk(
    root: &Path,
    dir: &Path,
    ignored: &[String],
    out: &mut BTreeMap<String, (PathBuf, u32, u64, u64)>,
) -> Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if ignored.contains(&name) {
            continue;
        }
        let abs = entry.path();
        let meta = std::fs::symlink_metadata(&abs)?;
        if meta.file_type().is_symlink() {
            let rel = rel_path(root, &abs)?;
            out.insert(rel, (abs, 0o120000, meta.len(), mtime_ms(&meta)));
        } else if meta.is_dir() {
            walk(root, &abs, ignored, out)?;
        } else if meta.is_file() {
            let rel = rel_path(root, &abs)?;
            out.insert(
                rel,
                (abs.clone(), file_mode(&meta), meta.len(), mtime_ms(&meta)),
            );
        }
    }
    Ok(())
}

fn rel_path(root: &Path, abs: &Path) -> Result<String> {
    Ok(abs
        .strip_prefix(root)
        .map_err(|_| CliError::usage("path escaped the mount root"))?
        .components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/"))
}

/// Build a manifest from a freshly checked-out git worktree (`git ls-files -s`), then stat each
/// file for the size/mtime freshness pair. Used once at mount; the `.git` directory is removed
/// afterwards.
pub fn manifest_from_git_checkout(dst: &Path) -> Result<Manifest> {
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(dst)
        .args(["ls-files", "-s", "-z"])
        .output()?;
    if !out.status.success() {
        return Err(CliError::usage(format!(
            "git ls-files failed: {}",
            String::from_utf8_lossy(&out.stderr)
        )));
    }
    let mut manifest = Manifest::new();
    for record in out.stdout.split(|b| *b == 0) {
        if record.is_empty() {
            continue;
        }
        let text = String::from_utf8_lossy(record);
        // `<mode> <oid> <stage>\t<path>`
        let Some((meta_part, path)) = text.split_once('\t') else {
            continue;
        };
        let mut fields = meta_part.split_whitespace();
        let (Some(mode), Some(oid)) = (fields.next(), fields.next()) else {
            continue;
        };
        let abs = dst.join(path);
        let Ok(meta) = std::fs::symlink_metadata(&abs) else {
            continue;
        };
        manifest.insert(
            path.to_string(),
            ManifestEntry {
                oid: oid.to_string(),
                mode: u32::from_str_radix(mode, 8).unwrap_or(0o100644),
                size: meta.len(),
                mtime_ms: mtime_ms(&meta),
            },
        );
    }
    Ok(manifest)
}

/// Write one materialized entry (file, executable, or symlink) and return its manifest row.
pub fn write_entry(
    root: &Path,
    path: &str,
    mode: u32,
    oid: &str,
    bytes: &[u8],
) -> Result<ManifestEntry> {
    let abs = root.join(path);
    if let Some(parent) = abs.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if mode == 0o120000 {
        let target = String::from_utf8_lossy(bytes).into_owned();
        let _ = std::fs::remove_file(&abs);
        #[cfg(unix)]
        std::os::unix::fs::symlink(&target, &abs)?;
        #[cfg(not(unix))]
        std::fs::write(&abs, target.as_bytes())?;
    } else {
        std::fs::write(&abs, bytes)?;
        #[cfg(unix)]
        if mode == 0o100755 {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&abs, std::fs::Permissions::from_mode(0o755))?;
        }
    }
    let meta = std::fs::symlink_metadata(&abs)?;
    Ok(ManifestEntry {
        oid: oid.to_string(),
        mode,
        size: meta.len(),
        mtime_ms: mtime_ms(&meta),
    })
}
