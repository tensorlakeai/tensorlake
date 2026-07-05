//! The `fuser` binding over [`super::overlay::OverlayFs`].
//!
//! Deliberately thin: every callback translates arguments, `block_on`s the overlay (whose write
//! side is synchronous local IO anyway), and maps [`gsvc_mount::MountError`] onto errnos. All
//! filesystem semantics live in the overlay, which is what the integration tests drive — this
//! layer is only reachable on a real kernel (Linux `/dev/fuse`, or macOS with the `macfuse`
//! feature and macFUSE installed).

use std::ffi::OsStr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use gsvc_mount::{MountError, NodeKind};

use super::overlay::{OverlayAttr, OverlayFs};

/// Kernel entry/attr TTL. Effectively infinite (the EdenFS posture): nothing a mount serves can
/// change between refresh deltas, and the daemon pushes exact invalidations for those
/// (`Notifier::inval_entry`/`inval_inode`, handed out by [`WorkspaceFuse::run`]) — so repeat
/// stats are kernel-cache hits instead of daemon round trips. Negative lookups are never
/// kernel-cached (`reply.error(ENOENT)` carries no TTL), so newly created names are always
/// discoverable. Bounded rather than literally infinite as a backstop for a missed
/// invalidation edge.
const TTL: Duration = Duration::from_secs(3600);

fn errno(e: &MountError) -> i32 {
    match e {
        MountError::NotFound(_) => libc::ENOENT,
        MountError::NotADirectory => libc::ENOTDIR,
        MountError::IsADirectory => libc::EISDIR,
        MountError::Exists => libc::EEXIST,
        MountError::IndexNotReady(_) => libc::EAGAIN,
        MountError::BadHandle => libc::EBADF,
        MountError::ReadOnly => libc::EROFS,
        _ => libc::EIO,
    }
}

fn file_type(kind: NodeKind) -> fuser::FileType {
    match kind {
        NodeKind::Dir => fuser::FileType::Directory,
        NodeKind::File => fuser::FileType::RegularFile,
        NodeKind::Symlink => fuser::FileType::Symlink,
    }
}

fn file_attr(attr: &OverlayAttr) -> fuser::FileAttr {
    // The overlay's content timestamp (see OverlayAttr::mtime): stable while content is
    // unchanged, moves when it can have changed — what kernel cache revalidation needs.
    let mtime = attr.mtime;
    fuser::FileAttr {
        ino: attr.ino,
        size: attr.size,
        blocks: attr.size.div_ceil(512),
        atime: mtime,
        mtime,
        ctime: mtime,
        crtime: mtime,
        kind: file_type(attr.kind),
        perm: attr.perm,
        nlink: 1,
        uid: unsafe { libc::getuid() },
        gid: unsafe { libc::getgid() },
        rdev: 0,
        blksize: 4096,
        flags: 0,
    }
}

pub struct WorkspaceFuse {
    fs: Arc<OverlayFs>,
    rt: tokio::runtime::Handle,
}

impl WorkspaceFuse {
    pub fn new(fs: Arc<OverlayFs>, rt: tokio::runtime::Handle) -> WorkspaceFuse {
        WorkspaceFuse { fs, rt }
    }

    /// Establish the kernel mount (fails fast when FUSE is unavailable), then serve until
    /// unmounted. `mounted` fires exactly once, after the session exists — the daemon publishes
    /// its control socket only then, so `tl fs mount` can't observe a live socket for a mount
    /// that never attached. It carries the session's [`fuser::Notifier`], which is what the
    /// daemon drives refresh-delta invalidation through — the contract that makes the long
    /// [`TTL`] sound.
    pub fn run(
        self,
        mountpoint: &Path,
        mounted: tokio::sync::oneshot::Sender<fuser::Notifier>,
    ) -> std::io::Result<()> {
        let options = vec![
            fuser::MountOption::FSName("tlfs".to_string()),
            fuser::MountOption::DefaultPermissions,
            fuser::MountOption::NoAtime,
        ];
        let mut session = fuser::Session::new(self, mountpoint, &options)?;
        let _ = mounted.send(session.notifier());
        session.run()
    }
}

impl fuser::Filesystem for WorkspaceFuse {
    fn lookup(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        reply: fuser::ReplyEntry,
    ) {
        let name = name.to_string_lossy();
        match self.rt.block_on(self.fs.lookup(parent, &name)) {
            Ok(attr) => reply.entry(&TTL, &file_attr(&attr), 0),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn forget(&mut self, _req: &fuser::Request<'_>, ino: u64, nlookup: u64) {
        self.fs.forget(ino, nlookup);
    }

    fn getattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: Option<u64>,
        reply: fuser::ReplyAttr,
    ) {
        match self.rt.block_on(self.fs.getattr(ino)) {
            Ok(attr) => reply.attr(&TTL, &file_attr(&attr)),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn setattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: fuser::ReplyAttr,
    ) {
        match self.rt.block_on(self.fs.setattr(ino, size, mode)) {
            Ok(attr) => reply.attr(&TTL, &file_attr(&attr)),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn readlink(&mut self, _req: &fuser::Request<'_>, ino: u64, reply: fuser::ReplyData) {
        match self.rt.block_on(self.fs.readlink(ino)) {
            Ok(target) => reply.data(&target),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn opendir(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _flags: i32,
        reply: fuser::ReplyOpen,
    ) {
        match self.rt.block_on(self.fs.opendir(ino)) {
            Ok(fh) => reply.opened(fh, 0),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn readdir(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        mut reply: fuser::ReplyDirectory,
    ) {
        match self.fs.readdir(fh, offset.max(0) as u64, 1024) {
            Ok(entries) => {
                for entry in entries {
                    // Ino 0 is invalid to the kernel; use a synthetic non-zero value. Real inos
                    // arrive through lookup, which is the counted path.
                    if reply.add(
                        u64::MAX,
                        entry.next_offset as i64,
                        file_type(entry.kind),
                        &entry.name,
                    ) {
                        break;
                    }
                }
                reply.ok();
            }
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn releasedir(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        reply: fuser::ReplyEmpty,
    ) {
        self.fs.releasedir(fh);
        reply.ok();
    }

    fn open(&mut self, _req: &fuser::Request<'_>, ino: u64, flags: i32, reply: fuser::ReplyOpen) {
        let write = (flags & libc::O_ACCMODE) != libc::O_RDONLY;
        match self.rt.block_on(self.fs.open(ino, write)) {
            // keep_cache: the overlay proved this open serves byte-identical content to the
            // previous one (same lower oid), so the kernel page cache survives the reopen.
            Ok((fh, keep_cache)) => reply.opened(
                fh,
                if keep_cache {
                    fuser::consts::FOPEN_KEEP_CACHE
                } else {
                    0
                },
            ),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn read(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyData,
    ) {
        match self
            .rt
            .block_on(self.fs.read(fh, offset.max(0) as u64, size as u64))
        {
            Ok(data) => reply.data(&data),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn write(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        match self.fs.write(fh, offset.max(0) as u64, data) {
            Ok(n) => reply.written(n),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn flush(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        fh: u64,
        _lock_owner: u64,
        reply: fuser::ReplyEmpty,
    ) {
        match self.fs.fsync(fh) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn fsync(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        fh: u64,
        _datasync: bool,
        reply: fuser::ReplyEmpty,
    ) {
        match self.fs.fsync(fh) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn release(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        self.fs.release(fh);
        reply.ok();
    }

    fn create(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        let name = name.to_string_lossy();
        let exec = mode & 0o111 != 0;
        match self.rt.block_on(self.fs.create(parent, &name, exec)) {
            Ok((attr, fh)) => reply.created(&TTL, &file_attr(&attr), 0, fh, 0),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn mkdir(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: fuser::ReplyEntry,
    ) {
        let name = name.to_string_lossy();
        match self.rt.block_on(self.fs.mkdir(parent, &name)) {
            Ok(attr) => reply.entry(&TTL, &file_attr(&attr), 0),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn symlink(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        link_name: &OsStr,
        target: &Path,
        reply: fuser::ReplyEntry,
    ) {
        let name = link_name.to_string_lossy();
        let target = target.to_string_lossy();
        match self.rt.block_on(self.fs.symlink(parent, &name, &target)) {
            Ok(attr) => reply.entry(&TTL, &file_attr(&attr), 0),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn unlink(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        let name = name.to_string_lossy();
        match self.rt.block_on(self.fs.unlink(parent, &name)) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn rmdir(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        let name = name.to_string_lossy();
        match self.rt.block_on(self.fs.rmdir(parent, &name)) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn rename(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: fuser::ReplyEmpty,
    ) {
        let name = name.to_string_lossy();
        let newname = newname.to_string_lossy();
        match self
            .rt
            .block_on(self.fs.rename(parent, &name, newparent, &newname))
        {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn statfs(&mut self, _req: &fuser::Request<'_>, _ino: u64, reply: fuser::ReplyStatfs) {
        reply.statfs(1 << 40, 1 << 39, 1 << 39, 0, 0, 4096, 255, 4096);
    }
}
