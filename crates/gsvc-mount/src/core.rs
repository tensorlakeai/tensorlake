//! The VFS-agnostic mount core: inode table, lookup/readdir/read over the native API, and the
//! branch-refresh state machine.
//!
//! Everything is scoped by a commit. The root inode (1) always resolves against the mount's
//! *current* commit; every other node captures the commit it was looked up under and keeps it for
//! life. A branch-following refresh therefore only swaps the root's commit pointer: new lookups
//! descend into the new tree, while open handles and already-known inodes keep reading the commit
//! they were opened under — the open-handle pinning semantics the workspace design requires, with
//! no per-node invalidation at all.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use bytes::{Bytes, BytesMut};

use crate::cache::{MountCaches, StatOutcome};
use crate::client::{FileStat, FsClient, TreeEntry};
use crate::{MountError, MountOptions};

/// What a node is, from its git mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeKind {
    Dir,
    File,
    Symlink,
}

fn kind_of_mode(mode: u32) -> NodeKind {
    match mode {
        0o40000 => NodeKind::Dir,
        0o120000 => NodeKind::Symlink,
        _ => NodeKind::File,
    }
}

/// POSIX-ish attributes for a node, ready for a FUSE binding to translate.
#[derive(Clone, Debug)]
pub struct NodeAttr {
    pub ino: u64,
    pub kind: NodeKind,
    pub size: u64,
    /// Permission bits derived from the git mode (git stores no more than exec-or-not).
    pub perm: u16,
    /// The commit this node is pinned to (hex).
    pub commit: String,
}

/// One resolved filesystem node. Immutable once created: the (commit, path) pair it names can
/// never change content.
#[derive(Debug)]
struct Node {
    commit: Arc<str>,
    path: String, // repo-relative, "" for a root
    kind: NodeKind,
    mode: u32,
    size: u64,
    /// Object oid (hex). For directories this is the tree oid, which lets readdir descend by oid
    /// with no server-side path walk. Empty when unknown (the root, or a dir resolved through the
    /// tree-probe fallback) — those list by path instead.
    oid: String,
}

impl Node {
    fn attr(&self, ino: u64) -> NodeAttr {
        NodeAttr {
            ino,
            kind: self.kind,
            size: self.size,
            perm: match self.kind {
                NodeKind::Dir => 0o755,
                NodeKind::Symlink => 0o777,
                NodeKind::File => {
                    if self.mode == 0o100755 {
                        0o755
                    } else {
                        0o644
                    }
                }
            },
            commit: self.commit.to_string(),
        }
    }
}

/// Inode table with kernel-style lookup counting: `lookup`-family calls increment a node's count,
/// `forget` decrements, and a node is dropped at zero so the table stays bounded by what the
/// kernel actually references. Ino 1 is the root and never expires.
struct InodeTable {
    nodes: HashMap<u64, (Arc<Node>, u64)>, // node, lookup count
    index: HashMap<(Arc<str>, String), u64>,
    next: u64,
}

pub const ROOT_INO: u64 = 1;

impl InodeTable {
    fn new() -> Self {
        InodeTable {
            nodes: HashMap::new(),
            index: HashMap::new(),
            next: ROOT_INO + 1,
        }
    }

    fn intern(&mut self, node: Node) -> (u64, Arc<Node>) {
        let key = (node.commit.clone(), node.path.clone());
        if let Some(&ino) = self.index.get(&key) {
            let (existing, count) = self.nodes.get_mut(&ino).expect("indexed node");
            *count += 1;
            return (ino, existing.clone());
        }
        let ino = self.next;
        self.next += 1;
        let node = Arc::new(node);
        self.nodes.insert(ino, (node.clone(), 1));
        self.index.insert(key, ino);
        (ino, node)
    }

    fn get(&self, ino: u64) -> Option<Arc<Node>> {
        self.nodes.get(&ino).map(|(n, _)| n.clone())
    }

    fn forget(&mut self, ino: u64, nlookups: u64) {
        if ino == ROOT_INO {
            return;
        }
        let Some((node, count)) = self.nodes.get_mut(&ino) else {
            return;
        };
        *count = count.saturating_sub(nlookups);
        if *count == 0 {
            let key = (node.commit.clone(), node.path.clone());
            self.nodes.remove(&ino);
            self.index.remove(&key);
        }
    }
}

/// The mount's mutable state: which commit the root currently resolves to.
struct RootState {
    commit: Arc<str>,
    generation: u64,
}

/// An open directory handle: incrementally paged entries, so a huge directory is never fetched
/// whole unless the reader actually walks it to the end (at which point it is cached whole).
struct DirHandle {
    node: Arc<Node>,
    entries: Vec<TreeEntry>,
    next_after: Option<String>,
    exhausted: bool,
    from_cache: bool,
}

/// An open file handle, pinning its node (and therefore its commit) for the handle's lifetime.
struct FileHandle {
    node: Arc<Node>,
}

enum Handle {
    Dir(DirHandle),
    File(FileHandle),
}

/// One entry as returned to a `readdir` caller. Plain-readdir semantics, matching the kernel's:
/// entries carry **no inode and no lookup reference** — a consumer resolves a name it cares about
/// through [`MountCore::lookup`], which is the counted-reference path that `forget` balances.
/// (Returning interned inos here would leak table entries: plain FUSE readdir never generates
/// matching forgets.)
#[derive(Clone, Debug)]
pub struct DirEntryOut {
    /// Offset of the *next* entry (pass back as the resume offset).
    pub next_offset: u64,
    pub name: String,
    pub kind: NodeKind,
}

/// The read-only mount core. All methods are async and thread-safe; a FUSE binding drives them
/// from its callbacks via a runtime handle.
pub struct MountCore {
    client: FsClient,
    caches: MountCaches,
    opts: MountOptions,
    root: RwLock<RootState>,
    inodes: Mutex<InodeTable>,
    handles: Mutex<HashMap<u64, Handle>>,
    next_fh: AtomicU64,
}

impl MountCore {
    /// Resolve the mount reference and build the core. `follow` mode requires the reference to be
    /// a live ref; pinned mode also accepts a raw commit hex.
    pub async fn new(client: FsClient, opts: MountOptions) -> Result<Arc<MountCore>, MountError> {
        let (commit, generation) = match client.ref_status(&opts.reference).await {
            Ok(status) => match status.oid {
                Some(oid) => (oid, status.generation),
                None if opts.follow => {
                    return Err(MountError::NotFound(format!(
                        "ref {} does not exist",
                        opts.reference
                    )));
                }
                None => (opts.reference.clone(), 0),
            },
            // A 40-hex pinned mount may name a commit, not a ref.
            Err(MountError::NotFound(_)) | Err(MountError::Status { status: 400, .. })
                if !opts.follow && opts.reference.len() == 40 =>
            {
                (opts.reference.clone(), 0)
            }
            Err(e) => return Err(e),
        };
        let core = Arc::new(MountCore {
            client,
            caches: MountCaches::new(opts.cache),
            root: RwLock::new(RootState {
                commit: Arc::from(commit.as_str()),
                generation,
            }),
            inodes: Mutex::new(InodeTable::new()),
            handles: Mutex::new(HashMap::new()),
            next_fh: AtomicU64::new(1),
            opts,
        });
        // Validate the commit actually serves before reporting a successful mount.
        core.client
            .tree_page(&core.current_commit(), "", None, 1)
            .await?;
        Ok(core)
    }

    /// The commit the root currently resolves to (hex).
    pub fn current_commit(&self) -> String {
        self.root.read().unwrap().commit.to_string()
    }

    /// The last observed ref generation (0 for pinned-to-commit mounts).
    pub fn current_generation(&self) -> u64 {
        self.root.read().unwrap().generation
    }

    pub fn follow(&self) -> bool {
        self.opts.follow
    }

    pub fn poll_interval(&self) -> std::time::Duration {
        self.opts.poll_interval
    }

    fn root_commit(&self) -> Arc<str> {
        self.root.read().unwrap().commit.clone()
    }

    /// One poll of the followed ref. Returns `true` when the root moved to a new commit. A
    /// deleted ref keeps serving the last commit (the mount is pinned-by-force and logs it).
    pub async fn poll_ref(&self) -> Result<bool, MountError> {
        if !self.opts.follow {
            return Ok(false);
        }
        let status = self.client.ref_status(&self.opts.reference).await?;
        let mut root = self.root.write().unwrap();
        if status.generation == root.generation {
            return Ok(false);
        }
        match status.oid {
            Some(oid) => {
                tracing::info!(
                    reference = %self.opts.reference,
                    from = %root.commit,
                    to = %oid,
                    generation = status.generation,
                    "mount: ref advanced; refreshing root"
                );
                root.commit = Arc::from(oid.as_str());
                root.generation = status.generation;
                Ok(true)
            }
            None => {
                tracing::warn!(
                    reference = %self.opts.reference,
                    pinned = %root.commit,
                    "mount: followed ref deleted; serving last known commit"
                );
                root.generation = status.generation;
                Ok(false)
            }
        }
    }

    fn node_of(&self, ino: u64) -> Result<Arc<Node>, MountError> {
        if ino == ROOT_INO {
            return Ok(Arc::new(Node {
                commit: self.root_commit(),
                path: String::new(),
                kind: NodeKind::Dir,
                mode: 0o40000,
                size: 0,
                oid: String::new(),
            }));
        }
        self.inodes
            .lock()
            .unwrap()
            .get(ino)
            .ok_or_else(|| MountError::NotFound(format!("inode {ino}")))
    }

    pub fn getattr(&self, ino: u64) -> Result<NodeAttr, MountError> {
        Ok(self.node_of(ino)?.attr(ino))
    }

    /// Drop `nlookups` kernel references to `ino` (FUSE `forget`).
    pub fn forget(&self, ino: u64, nlookups: u64) {
        self.inodes.lock().unwrap().forget(ino, nlookups);
    }

    /// Resolve `name` under `parent`. The child inherits the parent's commit, which is what pins
    /// entire in-use subtrees across a branch refresh.
    pub async fn lookup(&self, parent: u64, name: &str) -> Result<NodeAttr, MountError> {
        let parent_node = self.node_of(parent)?;
        if parent_node.kind != NodeKind::Dir {
            return Err(MountError::NotADirectory);
        }
        let commit = parent_node.commit.clone();
        let child_path = join_path(&parent_node.path, name);

        match self.stat_at(&commit, &child_path).await? {
            StatOutcome::Present(stat) => {
                let node = Node {
                    commit: commit.clone(),
                    kind: kind_of_mode(stat.mode),
                    mode: stat.mode,
                    size: stat.size,
                    oid: stat.oid,
                    path: child_path,
                };
                let (ino, node) = self.inodes.lock().unwrap().intern(node);
                Ok(node.attr(ino))
            }
            StatOutcome::Absent => Err(MountError::NotFound(name.to_string())),
        }
    }

    /// Stat one `(commit, path)` with negative caching. Resolution order: stat cache, the parent
    /// directory's complete cached listing, `HEAD files/`, and finally a one-entry `tree` probe to
    /// distinguish "directory" from "absent" (the files endpoint serves blobs, not trees).
    async fn stat_at(&self, commit: &Arc<str>, path: &str) -> Result<StatOutcome, MountError> {
        if let Some(outcome) = self.caches.stat(commit, path) {
            return Ok(outcome);
        }
        if let Some((dir, name)) = split_path(path) {
            if let Some(entries) = self.caches.dir(commit, dir) {
                let outcome = match entries.iter().find(|e| e.name == name) {
                    Some(e) => StatOutcome::Present(FileStat {
                        oid: e.oid.clone(),
                        mode: e.mode,
                        // A tree entry without a cheap size resolves through HEAD below.
                        size: match (e.is_dir(), e.size) {
                            (true, _) => 0,
                            (false, Some(size)) => size,
                            (false, None) => {
                                return self.stat_via_http(commit, path).await;
                            }
                        },
                    }),
                    None => StatOutcome::Absent,
                };
                self.caches.put_stat(commit, path, outcome.clone());
                return Ok(outcome);
            }
        }
        self.stat_via_http(commit, path).await
    }

    async fn stat_via_http(
        &self,
        commit: &Arc<str>,
        path: &str,
    ) -> Result<StatOutcome, MountError> {
        let outcome = match self.client.stat(commit, path).await {
            Ok(stat) => StatOutcome::Present(stat),
            Err(MountError::NotFound(_)) => {
                // Not servable as a file: either a directory or truly absent.
                match self.client.tree_page(commit, path, None, 1).await {
                    Ok(_) => StatOutcome::Present(FileStat {
                        oid: String::new(),
                        mode: 0o40000,
                        size: 0,
                    }),
                    Err(MountError::NotFound(_)) => StatOutcome::Absent,
                    Err(e) => return Err(e),
                }
            }
            Err(e) => return Err(e),
        };
        self.caches.put_stat(commit, path, outcome.clone());
        Ok(outcome)
    }

    /// Open a directory for reading; the handle pages incrementally.
    pub fn opendir(&self, ino: u64) -> Result<u64, MountError> {
        let node = self.node_of(ino)?;
        if node.kind != NodeKind::Dir {
            return Err(MountError::NotADirectory);
        }
        let (entries, exhausted, from_cache) = match self.caches.dir(&node.commit, &node.path) {
            Some(cached) => ((*cached).clone(), true, true),
            None => (Vec::new(), false, false),
        };
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        self.handles.lock().unwrap().insert(
            fh,
            Handle::Dir(DirHandle {
                node,
                entries,
                next_after: None,
                exhausted,
                from_cache,
            }),
        );
        Ok(fh)
    }

    /// Read directory entries starting at `offset` (0 for the first call; then the last returned
    /// `next_offset`). Synthetic `.`/`..` are the binding's concern, not the core's.
    pub async fn readdir(
        &self,
        fh: u64,
        offset: u64,
        max: usize,
    ) -> Result<Vec<DirEntryOut>, MountError> {
        // Take what we need under the lock, fetch pages outside it, then write back.
        loop {
            let (commit, path, dir_oid, have, next_after, exhausted) = {
                let handles = self.handles.lock().unwrap();
                let Some(Handle::Dir(dir)) = handles.get(&fh) else {
                    return Err(MountError::BadHandle);
                };
                (
                    dir.node.commit.clone(),
                    dir.node.path.clone(),
                    dir.node.oid.clone(),
                    dir.entries.len() as u64,
                    dir.next_after.clone(),
                    dir.exhausted,
                )
            };
            if exhausted || have > offset {
                break;
            }
            // Descend by tree oid when the parent listing gave us one — the server skips version
            // resolution and the O(depth) path walk. The root (and probe-resolved dirs) page by
            // path.
            let page = if dir_oid.is_empty() {
                self.client
                    .tree_page(&commit, &path, next_after.as_deref(), self.opts.page_limit)
                    .await?
            } else {
                self.client
                    .tree_page_by_oid(&dir_oid, next_after.as_deref(), self.opts.page_limit)
                    .await?
            };
            let mut handles = self.handles.lock().unwrap();
            let Some(Handle::Dir(dir)) = handles.get_mut(&fh) else {
                return Err(MountError::BadHandle);
            };
            // The page was fetched outside the lock: a concurrent readdir on the same handle may
            // already have appended it. Append only if the handle is exactly where we left it;
            // otherwise discard and re-evaluate.
            if dir.entries.len() as u64 != have || dir.next_after != next_after {
                continue;
            }
            dir.entries.extend(page.entries);
            dir.exhausted = !page.truncated;
            dir.next_after = page.next_after;
            if dir.exhausted && !dir.from_cache {
                // The full listing is now known; publish it so lookups and later opendirs skip
                // the network entirely.
                self.caches
                    .put_dir(&commit, &path, Arc::new(dir.entries.clone()));
            }
        }

        let handles = self.handles.lock().unwrap();
        let Some(Handle::Dir(dir)) = handles.get(&fh) else {
            return Err(MountError::BadHandle);
        };
        // Plain-readdir: names and kinds only. No inode is interned and no reference is taken —
        // `lookup` is the counted path (and resolves sizes there, from the cached listing this
        // very call populates).
        Ok(dir
            .entries
            .iter()
            .enumerate()
            .skip(offset as usize)
            .take(max)
            .map(|(i, entry)| DirEntryOut {
                next_offset: (i + 1) as u64,
                name: entry.name.clone(),
                kind: kind_of_mode(entry.mode),
            })
            .collect())
    }

    pub fn releasedir(&self, fh: u64) {
        self.handles.lock().unwrap().remove(&fh);
    }

    /// Open a file (or symlink) for reading; the handle pins the node's commit.
    pub fn open(&self, ino: u64) -> Result<u64, MountError> {
        let node = self.node_of(ino)?;
        if node.kind == NodeKind::Dir {
            return Err(MountError::IsADirectory);
        }
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        self.handles
            .lock()
            .unwrap()
            .insert(fh, Handle::File(FileHandle { node }));
        Ok(fh)
    }

    pub fn release(&self, fh: u64) {
        self.handles.lock().unwrap().remove(&fh);
    }

    /// Read `[offset, offset+size)` of an open file through the block cache. Fetches are
    /// block-aligned so sequential FUSE reads coalesce into few range requests.
    pub async fn read(&self, fh: u64, offset: u64, size: u64) -> Result<Bytes, MountError> {
        let node = {
            let handles = self.handles.lock().unwrap();
            match handles.get(&fh) {
                Some(Handle::File(f)) => f.node.clone(),
                _ => return Err(MountError::BadHandle),
            }
        };
        self.read_node(&node, offset, size).await
    }

    /// Symlink target: git stores it as the blob's content.
    pub async fn readlink(&self, ino: u64) -> Result<Bytes, MountError> {
        let node = self.node_of(ino)?;
        if node.kind != NodeKind::Symlink {
            return Err(MountError::Protocol("not a symlink".to_string()));
        }
        self.read_node(&node, 0, node.size.max(4096)).await
    }

    async fn read_node(&self, node: &Node, offset: u64, size: u64) -> Result<Bytes, MountError> {
        if size == 0 || offset >= node.size {
            return Ok(Bytes::new());
        }
        let end = offset.saturating_add(size).min(node.size);
        let block_bytes = self.caches.config.block_bytes.max(1);
        let first_block = offset / block_bytes;
        let last_block = (end - 1) / block_bytes;

        let mut out = BytesMut::with_capacity((end - offset) as usize);
        for block in first_block..=last_block {
            let block_start = block * block_bytes;
            let block_end = (block_start + block_bytes).min(node.size);
            let bytes = match self.caches.block(&node.commit, &node.path, block) {
                Some(bytes) => bytes,
                None => {
                    let bytes = self
                        .client
                        .read_range(
                            &node.commit,
                            &node.path,
                            block_start,
                            block_end - block_start,
                        )
                        .await?;
                    self.caches
                        .put_block(&node.commit, &node.path, block, bytes.clone());
                    bytes
                }
            };
            let want_start = offset.max(block_start) - block_start;
            let want_end = (end.min(block_end) - block_start).min(bytes.len() as u64);
            if want_start < want_end {
                out.extend_from_slice(&bytes[want_start as usize..want_end as usize]);
            }
        }
        Ok(out.freeze())
    }
}

fn join_path(dir: &str, name: &str) -> String {
    if dir.is_empty() {
        name.to_string()
    } else {
        format!("{dir}/{name}")
    }
}

/// Split a repo-relative path into `(parent dir, name)`; `None` for the root itself.
fn split_path(path: &str) -> Option<(&str, &str)> {
    if path.is_empty() {
        return None;
    }
    match path.rfind('/') {
        Some(i) => Some((&path[..i], &path[i + 1..])),
        None => Some(("", path)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_helpers_round_trip() {
        assert_eq!(join_path("", "a"), "a");
        assert_eq!(join_path("a/b", "c"), "a/b/c");
        assert_eq!(split_path(""), None);
        assert_eq!(split_path("a"), Some(("", "a")));
        assert_eq!(split_path("a/b/c"), Some(("a/b", "c")));
    }

    #[test]
    fn inode_table_interns_and_forgets() {
        let mut table = InodeTable::new();
        let commit: Arc<str> = Arc::from("c1");
        let node = |path: &str| Node {
            commit: commit.clone(),
            path: path.to_string(),
            kind: NodeKind::File,
            mode: 0o100644,
            size: 1,
            oid: String::new(),
        };
        let (ino_a, _) = table.intern(node("a"));
        let (ino_a2, _) = table.intern(node("a"));
        assert_eq!(ino_a, ino_a2, "same (commit, path) interns to one ino");
        let (ino_b, _) = table.intern(node("b"));
        assert_ne!(ino_a, ino_b);
        // Two lookups recorded for a: one forget keeps it, the second drops it.
        table.forget(ino_a, 1);
        assert!(table.get(ino_a).is_some());
        table.forget(ino_a, 1);
        assert!(table.get(ino_a).is_none());
        // A new intern of the same path gets a fresh ino.
        let (ino_a3, _) = table.intern(node("a"));
        assert_ne!(ino_a, ino_a3);
    }
}
