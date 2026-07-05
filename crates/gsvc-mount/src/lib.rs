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
//!
//! ## Kernel-cache contract for bindings
//!
//! Each refresh reports its exact effects as a [`RefreshDelta`] (via [`MountCore::poll_ref`] or
//! the [`spawn_ref_watcher`] callback). That precision is what lets a binding delegate caching
//! to the kernel — a kernel-cache hit is orders of magnitude cheaper than a round trip into the
//! daemon, and between deltas *nothing* a mount serves can change. A binding should:
//!
//! - Answer `lookup` and `getattr` with **long (effectively infinite) entry and attr TTLs**.
//!   Content under a commit is immutable and inos are path-stable; entries only go bad when a
//!   delta says so.
//! - On each delta, call `inval_inode` for every [`RefreshDelta::rebound`] ino — dropping its
//!   cached attrs, page cache, and (for directories) kernel readdir cache — and
//!   `inval_entry(parent_ino, name)` **plus** `inval_inode(ino)` for every
//!   [`RefreshDelta::staled`] ino. A `None` parent means the kernel holds no dentry for the
//!   name; `inval_inode` alone suffices.
//! - **Never cache negative lookups in the kernel** (reply to `ENOENT` with a zero entry TTL).
//!   The delta covers paths the mount has served, not names appearing for the first time — a
//!   kernel-cached negative dentry could outlive the file's creation.
//! - Pass `FOPEN_KEEP_CACHE` on `open` when the node's current [`NodeAttr::oid`] equals the oid
//!   the binding last opened that ino with: same oid means byte-identical content, so the page
//!   cache from previous opens — including across refreshes that never touched the path — stays
//!   valid. On a differing oid, omit it so the kernel discards stale pages.

mod cache;
mod client;
mod core;
mod watch;

pub use cache::CacheConfig;
pub use client::{
    ChangeEntry, ChangeKind, ChangesPage, FileStat, FsClient, RefStatus, TreeEntry, TreePage,
};
pub use core::{DirEntryOut, InvalEntry, MountCore, NodeAttr, NodeKind, ROOT_INO, RefreshDelta};
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
    /// Refresh through the server-side path-diff (`changes?from=&to=`) when a followed ref
    /// moves, falling back to the per-path stat walk when the diff isn't servable. Disable to
    /// force the walk (rollout safety valve; the walk is also what covers servers without the
    /// endpoint).
    pub diff_refresh: bool,
    pub cache: CacheConfig,
}

impl Default for MountOptions {
    fn default() -> Self {
        MountOptions {
            reference: "main".to_string(),
            follow: false,
            poll_interval: Duration::from_secs(5),
            page_limit: 1000,
            diff_refresh: true,
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
