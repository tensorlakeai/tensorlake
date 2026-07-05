//! # gsvc-mount
//!
//! The VFS-agnostic mount core for artifact-storage repos: a native-API HTTP client, immutable
//! client-side caches, and the branch-refresh state machine with open-handle pinning. FUSE (or
//! NFS-loopback, or any other) bindings live with the client distribution (`tl fs mount`), not
//! here — this crate deliberately has no `fuser` dependency so the binding layer can vary by
//! platform without touching server-coupled code.
//!
//! ## Model
//!
//! A mount is `repo : reference`, either **pinned** (reference resolved once — a branch, tag, or
//! raw commit) or **following** (a ref polled through the per-ref generation journal; the root
//! swaps to the new commit when the ref moves). All content below a commit is immutable, so every
//! cache is keyed by `(commit, path)` and never invalidates.
//!
//! Inodes are **per path and stable across refreshes** (close-to-open coherence, issue #24):
//! a refresh rebinds changed paths behind their existing inos, leaves unchanged paths (proven by
//! oid equality, whole subtrees pruned via directory oids) untouched with warm caches, and
//! stales vanished paths. Open handles snapshot their node at open time and keep serving the
//! commit they opened under.

mod cache;
mod client;
mod core;
mod watch;

pub use cache::CacheConfig;
pub use client::{FileStat, FsClient, RefStatus, TreeEntry, TreePage};
pub use core::{DirEntryOut, MountCore, NodeAttr, NodeKind, ROOT_INO};
pub use watch::spawn_ref_watcher;

use std::time::Duration;

/// Mount configuration.
#[derive(Clone, Debug)]
pub struct MountOptions {
    /// Branch (short name), full ref, tag, or — pinned mode only — a commit hex.
    pub reference: String,
    /// Follow the reference as it moves (`--shared-ro` semantics) instead of pinning at mount.
    pub follow: bool,
    /// Ref poll interval in follow mode.
    pub poll_interval: Duration,
    /// Directory listing page size.
    pub page_limit: usize,
    pub cache: CacheConfig,
}

impl Default for MountOptions {
    fn default() -> Self {
        MountOptions {
            reference: "main".to_string(),
            follow: false,
            poll_interval: Duration::from_secs(5),
            page_limit: 1000,
            cache: CacheConfig::default(),
        }
    }
}

/// Errors surfaced by the mount core. Bindings map these onto errnos
/// (`NotFound` → `ENOENT`, `NotADirectory` → `ENOTDIR`, `IsADirectory` → `EISDIR`,
/// `Exists` → `EEXIST`, `IndexNotReady` → `EAGAIN`, `ReadOnly` → `EROFS`, the rest → `EIO`).
#[derive(Debug, thiserror::Error)]
pub enum MountError {
    #[error("http error: {0}")]
    Http(reqwest::Error),
    #[error("server returned {status}: {message}")]
    Status { status: u16, message: String },
    #[error("not found: {0}")]
    NotFound(String),
    #[error("not a directory")]
    NotADirectory,
    #[error("is a directory")]
    IsADirectory,
    #[error("already exists")]
    Exists,
    #[error("stale or invalid handle")]
    BadHandle,
    #[error("index not ready: {0}")]
    IndexNotReady(String),
    #[error("read-only mount")]
    ReadOnly,
    #[error("protocol error: {0}")]
    Protocol(String),
}
