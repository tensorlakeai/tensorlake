//! Writable overlay over a read-only [`gsvc_mount::MountCore`].
//!
//! The lower layer is the workspace's snapshot commit, served by the mount core (immutable,
//! cached, branch-following). The upper layer is a plain local directory of real files; a
//! parallel `wh/` tree of marker files records deletions of lower paths (whiteouts). The merged
//! view is what a FUSE binding exposes:
//!
//! - reads route to the upper file when one exists, else to the core;
//! - the first write to a lower file copies it up, then all IO is local;
//! - `unlink`/`rmdir` remove upper state and whiteout any lower presence;
//! - the upper tree **is** the dirty set: snapshot enumerates `upper/` + `wh/` and nothing else.
//!
//! Everything here is VFS-agnostic and directly testable without a kernel; the `fuser` glue is a
//! thin translation layer. Write operations are synchronous local filesystem work; only
//! lower-layer access is async (delegated to the core).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use gsvc_mount::{MountCore, MountError, NodeAttr, NodeKind, ROOT_INO};

/// One merged-namespace node. Path-keyed: the same repo-relative path keeps the same ino for as
/// long as the kernel references it, regardless of which layer currently backs it.
struct ONode {
    path: String,
    /// The core's ino for the lower node at this path, when one exists. Holds exactly one
    /// counted core lookup reference (released on forget). Mutable because the workspace ref
    /// moves: a fresh lookup after a snapshot re-resolves against the new commit and swaps the
    /// backing, while inos the kernel already holds keep serving the commit they came from.
    core_ino: Mutex<Option<u64>>,
}

impl ONode {
    fn core_ino(&self) -> Option<u64> {
        *self.core_ino.lock().expect("core ino lock")
    }
}

struct InodeTable {
    nodes: HashMap<u64, (Arc<ONode>, u64)>,
    index: HashMap<String, u64>,
    next: u64,
}

impl InodeTable {
    fn new() -> Self {
        let mut nodes = HashMap::new();
        let mut index = HashMap::new();
        nodes.insert(
            ROOT_INO,
            (
                Arc::new(ONode {
                    path: String::new(),
                    core_ino: Mutex::new(Some(ROOT_INO)),
                }),
                1,
            ),
        );
        index.insert(String::new(), ROOT_INO);
        InodeTable {
            nodes,
            index,
            next: ROOT_INO + 1,
        }
    }

    /// Intern a path, bumping its lookup count. `fresh_core` is a just-acquired counted core
    /// reference (or `None`): the table keeps exactly one core reference per node, so the return
    /// value is a core ino the **caller must release** — either the duplicate fresh one, or the
    /// stale one this lookup replaced after the workspace ref moved.
    fn intern(&mut self, path: String, fresh_core: Option<u64>) -> (u64, Arc<ONode>, Option<u64>) {
        if let Some(&ino) = self.index.get(&path) {
            let (node, count) = self.nodes.get_mut(&ino).expect("indexed node");
            *count += 1;
            let mut stored = node.core_ino.lock().expect("core ino lock");
            let release = match (*stored, fresh_core) {
                (Some(old), Some(fresh)) if old == fresh => Some(fresh),
                (Some(old), Some(fresh)) => {
                    *stored = Some(fresh);
                    Some(old)
                }
                (None, Some(fresh)) => {
                    *stored = Some(fresh);
                    None
                }
                _ => None,
            };
            drop(stored);
            return (ino, node.clone(), release);
        }
        let ino = self.next;
        self.next += 1;
        let node = Arc::new(ONode {
            path: path.clone(),
            core_ino: Mutex::new(fresh_core),
        });
        self.nodes.insert(ino, (node.clone(), 1));
        self.index.insert(path, ino);
        (ino, node, None)
    }

    fn get(&self, ino: u64) -> Option<Arc<ONode>> {
        self.nodes.get(&ino).map(|(n, _)| n.clone())
    }

    /// Decrement and drop at zero, returning the node so the caller can release its core ref.
    fn forget(&mut self, ino: u64, nlookups: u64) -> Option<Arc<ONode>> {
        if ino == ROOT_INO {
            return None;
        }
        let (node, count) = self.nodes.get_mut(&ino)?;
        *count = count.saturating_sub(nlookups);
        if *count == 0 {
            let node = node.clone();
            self.nodes.remove(&ino);
            self.index.remove(&node.path);
            Some(node)
        } else {
            None
        }
    }
}

/// An open handle in the merged namespace.
enum OHandle {
    /// Backed by a real upper file (reads and writes are positional on this descriptor).
    Upper { file: std::fs::File },
    /// Backed by the read-only core.
    Lower { core_fh: u64 },
    /// A merged directory listing, fixed at opendir time.
    Dir { entries: Vec<(String, NodeKind)> },
}

/// Attributes of a merged node, plus which layer answered.
#[derive(Clone, Debug)]
pub struct OverlayAttr {
    pub ino: u64,
    pub kind: NodeKind,
    pub size: u64,
    pub perm: u16,
    /// True when the upper layer backs this node (i.e. it is locally dirty).
    pub upper: bool,
}

#[derive(Clone, Debug)]
pub struct OverlayDirEntry {
    pub next_offset: u64,
    pub name: String,
    pub kind: NodeKind,
}

pub struct OverlayFs {
    core: Arc<MountCore>,
    upper: PathBuf,
    wh: PathBuf,
    inodes: Mutex<InodeTable>,
    handles: Mutex<HashMap<u64, OHandle>>,
    next_fh: AtomicU64,
}

fn not_found() -> MountError {
    MountError::NotFound("no such file or directory".to_string())
}

fn io_err(e: std::io::Error) -> MountError {
    MountError::Protocol(format!("overlay io: {e}"))
}

impl OverlayFs {
    pub fn new(core: Arc<MountCore>, state_dir: &Path) -> Result<Arc<OverlayFs>, MountError> {
        let upper = state_dir.join("upper");
        let wh = state_dir.join("wh");
        std::fs::create_dir_all(&upper).map_err(io_err)?;
        std::fs::create_dir_all(&wh).map_err(io_err)?;
        Ok(Arc::new(OverlayFs {
            core,
            upper,
            wh,
            inodes: Mutex::new(InodeTable::new()),
            handles: Mutex::new(HashMap::new()),
            next_fh: AtomicU64::new(1),
        }))
    }

    pub fn core(&self) -> &Arc<MountCore> {
        &self.core
    }

    fn upper_path(&self, path: &str) -> PathBuf {
        if path.is_empty() {
            self.upper.clone()
        } else {
            self.upper.join(path)
        }
    }

    fn wh_path(&self, path: &str) -> PathBuf {
        self.wh.join(path)
    }

    fn whited_out(&self, path: &str) -> bool {
        // Markers are files; a directory at a wh path is only the container for child markers
        // (wh/dir/b.txt marks dir/b.txt, not dir).
        !path.is_empty()
            && self
                .wh_path(path)
                .symlink_metadata()
                .map(|m| m.is_file())
                .unwrap_or(false)
    }

    fn upper_meta(&self, path: &str) -> Option<std::fs::Metadata> {
        self.upper_path(path).symlink_metadata().ok()
    }

    fn attr_from_meta(&self, ino: u64, meta: &std::fs::Metadata) -> OverlayAttr {
        use std::os::unix::fs::PermissionsExt;
        let kind = if meta.file_type().is_symlink() {
            NodeKind::Symlink
        } else if meta.is_dir() {
            NodeKind::Dir
        } else {
            NodeKind::File
        };
        OverlayAttr {
            ino,
            kind,
            size: meta.len(),
            perm: (meta.permissions().mode() & 0o777) as u16,
            upper: true,
        }
    }

    fn attr_from_core(&self, ino: u64, attr: &NodeAttr) -> OverlayAttr {
        OverlayAttr {
            ino,
            kind: attr.kind,
            size: attr.size,
            perm: attr.perm,
            upper: false,
        }
    }

    fn node(&self, ino: u64) -> Result<Arc<ONode>, MountError> {
        self.inodes
            .lock()
            .expect("inode lock")
            .get(ino)
            .ok_or_else(not_found)
    }

    fn child_path(parent: &str, name: &str) -> String {
        if parent.is_empty() {
            name.to_string()
        } else {
            format!("{parent}/{name}")
        }
    }

    // -------------------------------------------------------------------------------------
    // Read side
    // -------------------------------------------------------------------------------------

    pub async fn lookup(&self, parent: u64, name: &str) -> Result<OverlayAttr, MountError> {
        let parent_node = self.node(parent)?;
        let path = Self::child_path(&parent_node.path, name);

        // Upper wins; a whiteout without upper presence is a hard miss.
        if let Some(meta) = self.upper_meta(&path) {
            let (ino, _, release) = self
                .inodes
                .lock()
                .expect("inode lock")
                .intern(path.clone(), None);
            debug_assert!(release.is_none());
            return Ok(self.attr_from_meta(ino, &meta));
        }
        if self.whited_out(&path) {
            return Err(not_found());
        }
        let Some(parent_core) = parent_node.core_ino() else {
            return Err(not_found());
        };
        // Always a fresh core lookup: after a snapshot the workspace ref moved, and the node's
        // lower backing must follow it (intern swaps and hands back the stale reference).
        let attr = self.core.lookup(parent_core, name).await?;
        let (ino, release) = {
            let mut inodes = self.inodes.lock().expect("inode lock");
            let (ino, _, release) = inodes.intern(path, Some(attr.ino));
            (ino, release)
        };
        if let Some(stale) = release {
            self.core.forget(stale, 1);
        }
        Ok(self.attr_from_core(ino, &attr))
    }

    pub fn getattr(&self, ino: u64) -> Result<OverlayAttr, MountError> {
        let node = self.node(ino)?;
        if let Some(meta) = self.upper_meta(&node.path) {
            return Ok(self.attr_from_meta(ino, &meta));
        }
        if self.whited_out(&node.path) {
            return Err(not_found());
        }
        let core_ino = node.core_ino().ok_or_else(not_found)?;
        let attr = self.core.getattr(core_ino)?;
        Ok(self.attr_from_core(ino, &attr))
    }

    pub fn forget(&self, ino: u64, nlookups: u64) {
        let dropped = self
            .inodes
            .lock()
            .expect("inode lock")
            .forget(ino, nlookups);
        if let Some(node) = dropped {
            if let Some(core_ino) = node.core_ino() {
                self.core.forget(core_ino, 1);
            }
        }
    }

    pub async fn readlink(&self, ino: u64) -> Result<Bytes, MountError> {
        let node = self.node(ino)?;
        if self.upper_meta(&node.path).is_some() {
            let target = std::fs::read_link(self.upper_path(&node.path)).map_err(io_err)?;
            return Ok(Bytes::from(
                target.to_string_lossy().into_owned().into_bytes(),
            ));
        }
        let core_ino = node.core_ino().ok_or_else(not_found)?;
        self.core.readlink(core_ino).await
    }

    /// Open a directory handle over the merged listing: lower entries (paged to exhaustion via
    /// the core) merged with upper entries, minus whiteouts, upper shadowing lower duplicates.
    pub async fn opendir(&self, ino: u64) -> Result<u64, MountError> {
        let node = self.node(ino)?;
        let mut merged: Vec<(String, NodeKind)> = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

        let upper_dir = self.upper_path(&node.path);
        if let Ok(read) = std::fs::read_dir(&upper_dir) {
            for entry in read.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                let Ok(meta) = entry
                    .metadata()
                    .or_else(|_| entry.path().symlink_metadata())
                else {
                    continue;
                };
                let kind = if entry
                    .path()
                    .symlink_metadata()
                    .map(|m| m.file_type().is_symlink())
                    .unwrap_or(false)
                {
                    NodeKind::Symlink
                } else if meta.is_dir() {
                    NodeKind::Dir
                } else {
                    NodeKind::File
                };
                seen.insert(name.clone());
                merged.push((name, kind));
            }
        }

        if let Some(core_ino) = node.core_ino() {
            if !self.whited_out(&node.path) || node.path.is_empty() {
                let fh = self.core.opendir(core_ino)?;
                let mut offset = 0u64;
                loop {
                    let page = self.core.readdir(fh, offset, 4096).await;
                    let page = match page {
                        Ok(page) => page,
                        Err(e) => {
                            self.core.releasedir(fh);
                            return Err(e);
                        }
                    };
                    if page.is_empty() {
                        break;
                    }
                    offset = page.last().expect("nonempty page").next_offset;
                    for entry in page {
                        let child = Self::child_path(&node.path, &entry.name);
                        if seen.contains(&entry.name) || self.whited_out(&child) {
                            continue;
                        }
                        merged.push((entry.name, entry.kind));
                    }
                }
                self.core.releasedir(fh);
            }
        }

        merged.sort_by(|a, b| a.0.cmp(&b.0));
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        self.handles
            .lock()
            .expect("handle lock")
            .insert(fh, OHandle::Dir { entries: merged });
        Ok(fh)
    }

    pub fn readdir(
        &self,
        fh: u64,
        offset: u64,
        max: usize,
    ) -> Result<Vec<OverlayDirEntry>, MountError> {
        let handles = self.handles.lock().expect("handle lock");
        let Some(OHandle::Dir { entries }) = handles.get(&fh) else {
            return Err(not_found());
        };
        Ok(entries
            .iter()
            .enumerate()
            .skip(offset as usize)
            .take(max)
            .map(|(i, (name, kind))| OverlayDirEntry {
                next_offset: i as u64 + 1,
                name: name.clone(),
                kind: *kind,
            })
            .collect())
    }

    pub fn releasedir(&self, fh: u64) {
        self.handles.lock().expect("handle lock").remove(&fh);
    }

    // -------------------------------------------------------------------------------------
    // File handles / IO
    // -------------------------------------------------------------------------------------

    /// Open for read or write. Writing to a lower-backed file copies it up first; after that,
    /// all IO on the path is local.
    pub async fn open(&self, ino: u64, write: bool) -> Result<u64, MountError> {
        let node = self.node(ino)?;
        if write {
            self.copy_up(&node).await?;
        }
        let handle = if self.upper_meta(&node.path).is_some() {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(write)
                .open(self.upper_path(&node.path))
                .map_err(io_err)?;
            OHandle::Upper { file }
        } else {
            let core_ino = node.core_ino().ok_or_else(not_found)?;
            OHandle::Lower {
                core_fh: self.core.open(core_ino)?,
            }
        };
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        self.handles.lock().expect("handle lock").insert(fh, handle);
        Ok(fh)
    }

    pub async fn read(&self, fh: u64, offset: u64, size: u64) -> Result<Bytes, MountError> {
        let core_fh = {
            let handles = self.handles.lock().expect("handle lock");
            match handles.get(&fh) {
                Some(OHandle::Upper { file }) => {
                    use std::os::unix::fs::FileExt;
                    let mut buf = vec![0u8; size as usize];
                    let n = file.read_at(&mut buf, offset).map_err(io_err)?;
                    buf.truncate(n);
                    return Ok(Bytes::from(buf));
                }
                Some(OHandle::Lower { core_fh }) => *core_fh,
                _ => return Err(not_found()),
            }
        };
        self.core.read(core_fh, offset, size).await
    }

    pub fn write(&self, fh: u64, offset: u64, data: &[u8]) -> Result<u32, MountError> {
        use std::os::unix::fs::FileExt;
        let handles = self.handles.lock().expect("handle lock");
        match handles.get(&fh) {
            Some(OHandle::Upper { file }) => {
                file.write_all_at(data, offset).map_err(io_err)?;
                Ok(data.len() as u32)
            }
            // open(write=true) always yields an upper handle; a write on a lower handle means
            // the kernel opened read-only, which it won't for writes.
            Some(OHandle::Lower { .. }) => Err(MountError::Protocol(
                "write on read-only handle".to_string(),
            )),
            _ => Err(not_found()),
        }
    }

    pub fn fsync(&self, fh: u64) -> Result<(), MountError> {
        let handles = self.handles.lock().expect("handle lock");
        if let Some(OHandle::Upper { file }) = handles.get(&fh) {
            file.sync_all().map_err(io_err)?;
        }
        Ok(())
    }

    pub fn release(&self, fh: u64) {
        let handle = self.handles.lock().expect("handle lock").remove(&fh);
        if let Some(OHandle::Lower { core_fh }) = handle {
            self.core.release(core_fh);
        }
    }

    /// Materialize a lower file into the upper layer (no-op when upper already has the path).
    async fn copy_up(&self, node: &ONode) -> Result<(), MountError> {
        if self.upper_meta(&node.path).is_some() {
            return Ok(());
        }
        let core_ino = node.core_ino().ok_or_else(not_found)?;
        let attr = self.core.getattr(core_ino)?;
        match attr.kind {
            NodeKind::Dir => {
                std::fs::create_dir_all(self.upper_path(&node.path)).map_err(io_err)?;
                return Ok(());
            }
            NodeKind::Symlink => {
                let target = self.core.readlink(core_ino).await?;
                let dest = self.upper_path(&node.path);
                if let Some(parent) = dest.parent() {
                    std::fs::create_dir_all(parent).map_err(io_err)?;
                }
                std::os::unix::fs::symlink(String::from_utf8_lossy(&target).as_ref(), &dest)
                    .map_err(io_err)?;
                return Ok(());
            }
            NodeKind::File => {}
        }
        let dest = self.upper_path(&node.path);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent).map_err(io_err)?;
        }
        let tmp = dest.with_extension("tlfs-copyup");
        {
            use std::io::Write;
            let mut out = std::fs::File::create(&tmp).map_err(io_err)?;
            let fh = self.core.open(core_ino)?;
            let mut offset = 0u64;
            loop {
                let chunk = match self.core.read(fh, offset, 4 * 1024 * 1024).await {
                    Ok(chunk) => chunk,
                    Err(e) => {
                        self.core.release(fh);
                        let _ = std::fs::remove_file(&tmp);
                        return Err(e);
                    }
                };
                if chunk.is_empty() {
                    break;
                }
                offset += chunk.len() as u64;
                out.write_all(&chunk).map_err(io_err)?;
            }
            self.core.release(fh);
            if attr.perm & 0o111 != 0 {
                use std::os::unix::fs::PermissionsExt;
                out.set_permissions(std::fs::Permissions::from_mode(0o755))
                    .map_err(io_err)?;
            }
        }
        std::fs::rename(&tmp, &dest).map_err(io_err)?;
        Ok(())
    }

    // -------------------------------------------------------------------------------------
    // Write side (namespace mutations)
    // -------------------------------------------------------------------------------------

    /// Whether the lower layer has a node at `path` (via a counted lookup that is immediately
    /// balanced — used to decide whether a whiteout is needed).
    async fn lower_has(&self, parent_core: Option<u64>, name: &str) -> bool {
        let Some(parent_core) = parent_core else {
            return false;
        };
        match self.core.lookup(parent_core, name).await {
            Ok(attr) => {
                self.core.forget(attr.ino, 1);
                true
            }
            Err(_) => false,
        }
    }

    fn clear_whiteout(&self, path: &str) {
        let _ = std::fs::remove_file(self.wh_path(path));
    }

    fn set_whiteout(&self, path: &str) -> Result<(), MountError> {
        let marker = self.wh_path(path);
        if let Some(parent) = marker.parent() {
            std::fs::create_dir_all(parent).map_err(io_err)?;
        }
        // A container of child markers may sit where a directory whiteout goes; the dir-level
        // marker supersedes them.
        if marker.is_dir() {
            std::fs::remove_dir_all(&marker).map_err(io_err)?;
        }
        std::fs::write(&marker, b"").map_err(io_err)?;
        Ok(())
    }

    pub async fn create(
        &self,
        parent: u64,
        name: &str,
        exec: bool,
    ) -> Result<(OverlayAttr, u64), MountError> {
        let parent_node = self.node(parent)?;
        let path = Self::child_path(&parent_node.path, name);
        let dest = self.upper_path(&path);
        if let Some(dir) = dest.parent() {
            std::fs::create_dir_all(dir).map_err(io_err)?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&dest)
            .map_err(io_err)?;
        if exec {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(std::fs::Permissions::from_mode(0o755))
                .map_err(io_err)?;
        }
        self.clear_whiteout(&path);
        let meta = dest.symlink_metadata().map_err(io_err)?;
        let (ino, _, _) = self.inodes.lock().expect("inode lock").intern(path, None);
        let attr = self.attr_from_meta(ino, &meta);
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        self.handles
            .lock()
            .expect("handle lock")
            .insert(fh, OHandle::Upper { file });
        Ok((attr, fh))
    }

    pub fn mkdir(&self, parent: u64, name: &str) -> Result<OverlayAttr, MountError> {
        let parent_node = self.node(parent)?;
        let path = Self::child_path(&parent_node.path, name);
        let dest = self.upper_path(&path);
        std::fs::create_dir_all(&dest).map_err(io_err)?;
        self.clear_whiteout(&path);
        let meta = dest.symlink_metadata().map_err(io_err)?;
        let (ino, _, _) = self.inodes.lock().expect("inode lock").intern(path, None);
        Ok(self.attr_from_meta(ino, &meta))
    }

    pub fn symlink(
        &self,
        parent: u64,
        name: &str,
        target: &str,
    ) -> Result<OverlayAttr, MountError> {
        let parent_node = self.node(parent)?;
        let path = Self::child_path(&parent_node.path, name);
        let dest = self.upper_path(&path);
        if let Some(dir) = dest.parent() {
            std::fs::create_dir_all(dir).map_err(io_err)?;
        }
        let _ = std::fs::remove_file(&dest);
        std::os::unix::fs::symlink(target, &dest).map_err(io_err)?;
        self.clear_whiteout(&path);
        let meta = dest.symlink_metadata().map_err(io_err)?;
        let (ino, _, _) = self.inodes.lock().expect("inode lock").intern(path, None);
        Ok(self.attr_from_meta(ino, &meta))
    }

    pub async fn unlink(&self, parent: u64, name: &str) -> Result<(), MountError> {
        let parent_node = self.node(parent)?;
        let path = Self::child_path(&parent_node.path, name);
        let dest = self.upper_path(&path);
        let had_upper = dest.symlink_metadata().is_ok();
        if had_upper {
            std::fs::remove_file(&dest).map_err(io_err)?;
        }
        if self.lower_has(parent_node.core_ino(), name).await {
            self.set_whiteout(&path)?;
        } else if !had_upper {
            return Err(not_found());
        }
        Ok(())
    }

    pub async fn rmdir(&self, parent: u64, name: &str) -> Result<(), MountError> {
        let parent_node = self.node(parent)?;
        let path = Self::child_path(&parent_node.path, name);
        // Merged-empty check: opendir the directory and require zero entries.
        let attr = self.lookup(parent, name).await?;
        let fh = self.opendir(attr.ino).await?;
        let empty = self.readdir(fh, 0, 1)?.is_empty();
        self.releasedir(fh);
        // lookup() above took a reference; balance it.
        self.forget(attr.ino, 1);
        if !empty {
            return Err(MountError::Protocol("directory not empty".to_string()));
        }
        let dest = self.upper_path(&path);
        if dest.symlink_metadata().is_ok() {
            std::fs::remove_dir_all(&dest).map_err(io_err)?;
        }
        if self.lower_has(parent_node.core_ino(), name).await {
            self.set_whiteout(&path)?;
        }
        Ok(())
    }

    /// Rename. Upper-only sources rename in place; a lower-backed file copies up, writes the
    /// destination, and whiteouts the source. Renaming a directory with lower presence is not
    /// supported (v1) — copy-up of a whole subtree belongs in a snapshot/promote flow, not a
    /// syscall.
    pub async fn rename(
        &self,
        parent: u64,
        name: &str,
        new_parent: u64,
        new_name: &str,
    ) -> Result<(), MountError> {
        let parent_node = self.node(parent)?;
        let new_parent_node = self.node(new_parent)?;
        let src = Self::child_path(&parent_node.path, name);
        let dst = Self::child_path(&new_parent_node.path, new_name);
        let src_upper = self.upper_path(&src);
        let dst_upper = self.upper_path(&dst);
        if let Some(dir) = dst_upper.parent() {
            std::fs::create_dir_all(dir).map_err(io_err)?;
        }

        let src_meta = src_upper.symlink_metadata().ok();
        let lower_src = self.lower_has(parent_node.core_ino(), name).await;
        match src_meta {
            Some(_) => {
                std::fs::rename(&src_upper, &dst_upper).map_err(io_err)?;
            }
            None if lower_src => {
                // Copy the lower file up directly at the destination.
                let attr = self.lookup(parent, name).await?;
                let node = self.node(attr.ino)?;
                if matches!(attr.kind, NodeKind::Dir) {
                    self.forget(attr.ino, 1);
                    return Err(MountError::Protocol(
                        "renaming a committed directory is not supported; snapshot first"
                            .to_string(),
                    ));
                }
                self.copy_up(&node).await?;
                self.forget(attr.ino, 1);
                std::fs::rename(self.upper_path(&src), &dst_upper).map_err(io_err)?;
            }
            None => return Err(not_found()),
        }
        if lower_src {
            self.set_whiteout(&src)?;
        }
        self.clear_whiteout(&dst);
        // The destination path may already be interned (overwrite): its ino now serves upper
        // content automatically since attribute resolution is dynamic.
        Ok(())
    }

    /// Truncate and exec-bit changes; anything else is accepted and ignored (git versions
    /// neither timestamps nor ownership).
    pub async fn setattr(
        &self,
        ino: u64,
        size: Option<u64>,
        mode: Option<u32>,
    ) -> Result<OverlayAttr, MountError> {
        let node = self.node(ino)?;
        if size.is_some() || mode.is_some() {
            self.copy_up(&node).await?;
            let dest = self.upper_path(&node.path);
            if let Some(size) = size {
                let file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&dest)
                    .map_err(io_err)?;
                file.set_len(size).map_err(io_err)?;
            }
            if let Some(mode) = mode {
                use std::os::unix::fs::PermissionsExt;
                let perm = if mode & 0o111 != 0 { 0o755 } else { 0o644 };
                std::fs::set_permissions(&dest, std::fs::Permissions::from_mode(perm))
                    .map_err(io_err)?;
            }
        }
        self.getattr(ino)
    }

    // -------------------------------------------------------------------------------------
    // Snapshot support
    // -------------------------------------------------------------------------------------

    /// Drop all upper state (after a successful snapshot has sealed it into a commit, or a
    /// restore replaced it). Open upper handles keep their descriptors (unix semantics); new
    /// opens see the lower layer.
    pub fn clear_upper(&self) -> Result<(), MountError> {
        for dir in [&self.upper, &self.wh] {
            for entry in std::fs::read_dir(dir).map_err(io_err)?.flatten() {
                let path = entry.path();
                if path.is_dir() && !path.is_symlink() {
                    std::fs::remove_dir_all(&path).map_err(io_err)?;
                } else {
                    std::fs::remove_file(&path).map_err(io_err)?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // Overlay integration tests: drive `OverlayFs` — the exact surface the FUSE glue
    // translates — against a live local artifact-storage server, proving merged-namespace
    // semantics (copy-up, whiteouts, merged readdir, snapshot round-trip through the workspace
    // ref) without a kernel. Requires `gsvc-server` on 127.0.0.1:8080 in open-auth mode:
    // `cargo test -p tensorlake-cli overlay -- --ignored --nocapture`.

    use super::*;
    use gsvc_mount::NodeKind;

    // Integration tests for the `tl fs` overlay: the writable layer over the read-only mount core.
    //
    // These drive `OverlayFs` directly — the exact surface the FUSE glue translates — against a
    // live local artifact-storage server, so every merged-namespace semantic (copy-up, whiteouts,
    // merged readdir, snapshot round-trip through the workspace ref) is proven without a kernel.
    //
    // Requires `gsvc-server` on 127.0.0.1:8080 in open-auth mode (skips cleanly when absent):
    // `cargo run -p gsvc-server` in an artifact_storage checkout, then
    // `cargo test -p tensorlake-cli --test overlay_fs -- --ignored --nocapture`.
    use std::sync::Arc;

    use gsvc_mount::{FsClient, MountCore, MountOptions, ROOT_INO};
    use tensorlake::ClientBuilder;

    use tensorlake::artifact_storage::ArtifactStorageClient;
    use tensorlake::artifact_storage::ingest::{PushFile, PushOptions, PushSource};
    use tensorlake::artifact_storage::workspaces::CreateWorkspaceRequest;

    const BASE: &str = "http://127.0.0.1:8080";
    const PROJECT: &str = "overlaytest";
    const TOKEN: &str = "devtoken";

    fn server_up() -> bool {
        std::net::TcpStream::connect_timeout(
            &"127.0.0.1:8080".parse().unwrap(),
            std::time::Duration::from_millis(500),
        )
        .is_ok()
    }

    fn sdk() -> ArtifactStorageClient {
        let client = ClientBuilder::new(BASE)
            .bearer_token("dummy")
            .build()
            .unwrap();
        ArtifactStorageClient::new(client, BASE).unwrap()
    }

    fn push_file(path: &str, bytes: &[u8], mode: u32) -> PushFile {
        PushFile {
            repo_path: path.to_string(),
            source: PushSource::Bytes(bytes.to_vec()),
            mode: Some(mode),
            delete: false,
        }
    }

    async fn read_all(fs: &OverlayFs, parent: u64, name: &str) -> Vec<u8> {
        let attr = fs.lookup(parent, name).await.unwrap();
        let fh = fs.open(attr.ino, false).await.unwrap();
        let mut out = Vec::new();
        loop {
            let chunk = fs.read(fh, out.len() as u64, 1 << 20).await.unwrap();
            if chunk.is_empty() {
                break;
            }
            out.extend_from_slice(&chunk);
        }
        fs.release(fh);
        fs.forget(attr.ino, 1);
        out
    }

    async fn dir_names(fs: &OverlayFs, ino: u64) -> Vec<String> {
        let fh = fs.opendir(ino).await.unwrap();
        let entries = fs.readdir(fh, 0, 10_000).unwrap();
        fs.releasedir(fh);
        entries.into_iter().map(|e| e.name).collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires a local artifact-storage server on 127.0.0.1:8080"]
    async fn overlay_merges_copies_up_whiteouts_and_snapshots() {
        if !server_up() {
            eprintln!("skipping: no local artifact-storage server");
            return;
        }
        let sdk = sdk();
        let repo = format!(
            "ofs-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let project_repo = format!("{PROJECT}/{repo}");
        let _ = project_repo;
        sdk.create_repo_with_credential(PROJECT, &repo, None, "t", TOKEN)
            .await
            .unwrap();
        sdk.push_files(
            PROJECT,
            &repo,
            "t",
            TOKEN,
            vec![
                push_file("README.md", b"# seed\n", 0o100644),
                push_file("dir/a.txt", b"alpha\n", 0o100644),
                push_file("dir/b.txt", b"beta\n", 0o100644),
                push_file("bin/run.sh", b"#!/bin/sh\n", 0o100755),
            ],
            PushOptions {
                message: "seed".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let ws = sdk
            .create_workspace(
                PROJECT,
                &repo,
                "t",
                TOKEN,
                &CreateWorkspaceRequest::default(),
            )
            .await
            .unwrap()
            .into_inner();

        // The mount core follows the workspace ref; the overlay sits on a temp state dir.
        let client = FsClient::new(BASE, PROJECT, &repo, Some(TOKEN.to_string())).unwrap();
        let core = MountCore::new(
            client,
            MountOptions {
                reference: ws.ref_name.clone(),
                follow: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let state = tempfile::tempdir().unwrap();
        let fs: Arc<OverlayFs> = OverlayFs::new(core.clone(), state.path()).unwrap();

        // 1. Lower reads through the merged view.
        assert_eq!(read_all(&fs, ROOT_INO, "README.md").await, b"# seed\n");
        let names = dir_names(&fs, ROOT_INO).await;
        assert_eq!(names, vec!["README.md", "bin", "dir"]);
        let dir = fs.lookup(ROOT_INO, "dir").await.unwrap();
        assert_eq!(dir_names(&fs, dir.ino).await, vec!["a.txt", "b.txt"]);
        let run = fs.lookup(ROOT_INO, "bin").await.unwrap();
        let run_sh = fs.lookup(run.ino, "run.sh").await.unwrap();
        assert_eq!(run_sh.perm & 0o111, 0o111, "exec bit survives the lower");
        assert!(!run_sh.upper);

        // 2. Copy-up on first write; content merges; the upper layer holds exactly the dirty file.
        let readme = fs.lookup(ROOT_INO, "README.md").await.unwrap();
        let fh = fs.open(readme.ino, true).await.unwrap();
        fs.write(fh, 0, b"# edited\n").unwrap();
        fs.setattr(readme.ino, Some(9), None).await.unwrap();
        fs.release(fh);
        assert_eq!(read_all(&fs, ROOT_INO, "README.md").await, b"# edited\n");
        assert!(state.path().join("upper/README.md").is_file());
        assert!(
            !state.path().join("upper/dir").exists(),
            "reads never copy up"
        );
        let readme2 = fs.lookup(ROOT_INO, "README.md").await.unwrap();
        assert!(readme2.upper, "dirty file is upper-backed");

        // 3. Create / mkdir / symlink land in upper and merge into readdir.
        let (created, cfh) = fs.create(ROOT_INO, "new.txt", false).await.unwrap();
        fs.write(cfh, 0, b"fresh\n").unwrap();
        fs.release(cfh);
        fs.forget(created.ino, 1);
        fs.mkdir(ROOT_INO, "made").unwrap();
        fs.symlink(ROOT_INO, "lnk", "README.md").unwrap();
        let names = dir_names(&fs, ROOT_INO).await;
        assert_eq!(
            names,
            vec!["README.md", "bin", "dir", "lnk", "made", "new.txt"]
        );
        let lnk = fs.lookup(ROOT_INO, "lnk").await.unwrap();
        assert!(matches!(lnk.kind, NodeKind::Symlink));
        assert_eq!(fs.readlink(lnk.ino).await.unwrap().as_ref(), b"README.md");

        // 4. Whiteouts: unlink a lower file — gone from lookup and readdir, marker recorded.
        fs.unlink(dir.ino, "b.txt").await.unwrap();
        assert!(fs.lookup(dir.ino, "b.txt").await.is_err());
        assert_eq!(dir_names(&fs, dir.ino).await, vec!["a.txt"]);
        assert!(state.path().join("wh/dir/b.txt").is_file());

        // 5. rmdir refuses non-empty; empties then whiteouts a lower dir.
        assert!(fs.rmdir(ROOT_INO, "dir").await.is_err());
        fs.unlink(dir.ino, "a.txt").await.unwrap();
        fs.rmdir(ROOT_INO, "dir").await.unwrap();
        assert!(fs.lookup(ROOT_INO, "dir").await.is_err());

        // 6. Rename of a lower file: copy-up at destination + whiteout at source.
        fs.rename(run.ino, "run.sh", ROOT_INO, "run2.sh")
            .await
            .unwrap();
        assert!(fs.lookup(run.ino, "run.sh").await.is_err());
        assert_eq!(read_all(&fs, ROOT_INO, "run2.sh").await, b"#!/bin/sh\n");

        // 7. Snapshot round-trip: push the overlay's dirty set to the workspace ref, follow the
        //    ref, clear the overlay — the merged view must be unchanged, now served by the lower.
        let before_commit = core.current_commit();
        let mut files = vec![
            PushFile {
                repo_path: "README.md".into(),
                source: PushSource::Path(state.path().join("upper/README.md")),
                mode: Some(0o100644),
                delete: false,
            },
            push_file("new.txt", b"fresh\n", 0o100644),
            PushFile {
                repo_path: "lnk".into(),
                source: PushSource::Bytes(b"README.md".to_vec()),
                mode: Some(0o120000),
                delete: false,
            },
            push_file("run2.sh", b"#!/bin/sh\n", 0o100755),
        ];
        for gone in ["dir/a.txt", "dir/b.txt", "bin/run.sh"] {
            files.push(PushFile {
                repo_path: gone.into(),
                source: PushSource::Bytes(Vec::new()),
                mode: None,
                delete: true,
            });
        }
        sdk.push_files(
            PROJECT,
            &repo,
            "t",
            TOKEN,
            files,
            PushOptions {
                message: "overlay snapshot".into(),
                workspace_snapshot: Some(ws.id.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(core.poll_ref().await.unwrap(), "ref moved to the snapshot");
        assert_ne!(core.current_commit(), before_commit);
        fs.clear_upper().unwrap();
        assert!(!state.path().join("upper/README.md").exists());

        // Fresh lookups now serve the snapshot from the lower layer, identically.
        assert_eq!(read_all(&fs, ROOT_INO, "README.md").await, b"# edited\n");
        assert_eq!(read_all(&fs, ROOT_INO, "new.txt").await, b"fresh\n");
        assert_eq!(read_all(&fs, ROOT_INO, "run2.sh").await, b"#!/bin/sh\n");
        let readme3 = fs.lookup(ROOT_INO, "README.md").await.unwrap();
        assert!(!readme3.upper, "post-snapshot content is lower-backed");
        assert!(
            fs.lookup(ROOT_INO, "dir").await.is_err(),
            "deletes committed"
        );
        // git drops empty trees: bin/ vanished with its only file.
        let names = dir_names(&fs, ROOT_INO).await;
        assert_eq!(names, vec!["README.md", "lnk", "new.txt", "run2.sh"]);

        sdk.delete_workspace(PROJECT, &repo, "t", TOKEN, &ws.id)
            .await
            .unwrap();
    }
}
