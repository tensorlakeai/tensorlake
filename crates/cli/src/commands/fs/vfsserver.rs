//! The mount VFS wire protocol and its server: how the macOS FSKit extension reaches the
//! overlay.
//!
//! On Linux the FUSE glue calls [`super::overlay::OverlayFs`] in-process. On macOS the kernel
//! talks to a sandboxed FSKit app extension instead, and a sandboxed appex can neither link our
//! Rust stack usefully (it cannot read the daemon's state directories) nor share its caches —
//! so the extension stays a thin Swift proxy and the daemon serves the overlay over localhost
//! TCP. The mount URL carries the endpoint and a per-mount secret:
//!
//! ```text
//! tlfs://127.0.0.1:<port>/<secret>
//! ```
//!
//! Framing: every message is `u32-le length | u8 opcode | payload`; every response is
//! `u32-le length | i32-le errno | payload` (errno 0 = success, else POSIX). Integers are
//! little-endian; strings and byte blobs are `u32-le length | bytes` (strings UTF-8). One
//! connection serves one request at a time; the extension opens a small pool for concurrency.
//! The first message on every connection must be `Hello` with the secret and protocol version.
//!
//! Attribute encoding (`attr`): `u64 ino | u8 kind (0 dir, 1 file, 2 symlink) | u64 size |
//! u16 perm | u8 upper | u64 mtime_sec | u32 mtime_nsec` (mtime as a Unix timestamp; it is
//! the content timestamp the kernel uses for cache revalidation, see `OverlayAttr::mtime`).

use std::sync::Arc;

use gsvc_mount::{MountError, NodeKind};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::overlay::{OverlayAttr, OverlayFs};

pub const PROTOCOL_VERSION: u32 = 2;

pub mod op {
    pub const HELLO: u8 = 0;
    pub const GETATTR: u8 = 1;
    pub const LOOKUP: u8 = 2;
    pub const FORGET: u8 = 3;
    pub const OPENDIR: u8 = 4;
    pub const READDIR: u8 = 5;
    pub const RELEASEDIR: u8 = 6;
    pub const OPEN: u8 = 7;
    pub const READ: u8 = 8;
    pub const WRITE: u8 = 9;
    pub const RELEASE: u8 = 10;
    pub const FSYNC: u8 = 11;
    pub const CREATE: u8 = 12;
    pub const MKDIR: u8 = 13;
    pub const SYMLINK: u8 = 14;
    pub const READLINK: u8 = 15;
    pub const UNLINK: u8 = 16;
    pub const RMDIR: u8 = 17;
    pub const RENAME: u8 = 18;
    pub const SETATTR: u8 = 19;
    pub const STATFS: u8 = 20;
}

const MAX_FRAME: u32 = 4 * 1024 * 1024 + 512;

/// Op tracing to stderr, for foreground-daemon debugging (`tl fs mount --foreground`).
pub static TRACE_OPS: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn errno(e: &MountError) -> i32 {
    match e {
        MountError::NotFound(_) => libc::ENOENT,
        MountError::NotADirectory => libc::ENOTDIR,
        MountError::IsADirectory => libc::EISDIR,
        MountError::Exists => libc::EEXIST,
        MountError::NotEmpty => libc::ENOTEMPTY,
        MountError::Unsupported(_) => libc::ENOTSUP,
        MountError::IndexNotReady(_) => libc::EAGAIN,
        MountError::BadHandle => libc::EBADF,
        MountError::ReadOnly => libc::EROFS,
        _ => libc::EIO,
    }
}

fn kind_byte(kind: NodeKind) -> u8 {
    match kind {
        NodeKind::Dir => 0,
        NodeKind::File => 1,
        NodeKind::Symlink => 2,
    }
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Reader<'a> {
        Reader { buf, pos: 0 }
    }
    fn u8(&mut self) -> Result<u8, i32> {
        let v = *self.buf.get(self.pos).ok_or(libc::EINVAL)?;
        self.pos += 1;
        Ok(v)
    }
    fn u32(&mut self) -> Result<u32, i32> {
        let end = self.pos + 4;
        let raw = self.buf.get(self.pos..end).ok_or(libc::EINVAL)?;
        self.pos = end;
        Ok(u32::from_le_bytes(raw.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, i32> {
        let end = self.pos + 8;
        let raw = self.buf.get(self.pos..end).ok_or(libc::EINVAL)?;
        self.pos = end;
        Ok(u64::from_le_bytes(raw.try_into().unwrap()))
    }
    fn bytes(&mut self) -> Result<&'a [u8], i32> {
        let len = self.u32()? as usize;
        let end = self.pos + len;
        let raw = self.buf.get(self.pos..end).ok_or(libc::EINVAL)?;
        self.pos = end;
        Ok(raw)
    }
    fn str(&mut self) -> Result<&'a str, i32> {
        std::str::from_utf8(self.bytes()?).map_err(|_| libc::EINVAL)
    }
}

struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    fn ok() -> Writer {
        let mut w = Writer { buf: Vec::new() };
        w.i32(0);
        w
    }
    fn err(errno: i32) -> Writer {
        let mut w = Writer { buf: Vec::new() };
        w.i32(errno);
        w
    }
    fn i32(&mut self, v: i32) -> &mut Self {
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }
    fn u8v(&mut self, v: u8) -> &mut Self {
        self.buf.push(v);
        self
    }
    fn u16v(&mut self, v: u16) -> &mut Self {
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }
    fn u32v(&mut self, v: u32) -> &mut Self {
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }
    fn u64v(&mut self, v: u64) -> &mut Self {
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }
    fn bytes(&mut self, v: &[u8]) -> &mut Self {
        self.u32v(v.len() as u32);
        self.buf.extend_from_slice(v);
        self
    }
    fn attr(&mut self, a: &OverlayAttr) -> &mut Self {
        let mtime = a
            .mtime
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        self.u64v(a.ino)
            .u8v(kind_byte(a.kind))
            .u64v(a.size)
            .u16v(a.perm)
            .u8v(a.upper as u8)
            .u64v(mtime.as_secs())
            .u32v(mtime.subsec_nanos())
    }
}

/// A running VFS server bound to 127.0.0.1. Dropping the handle stops accepting (existing
/// connections run until the daemon exits).
pub struct VfsServer {
    pub port: u16,
    pub secret: String,
}

/// Bind and serve the overlay on a localhost ephemeral port with a fresh per-mount secret.
pub async fn serve(fs: Arc<OverlayFs>) -> std::io::Result<VfsServer> {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let port = listener.local_addr()?.port();
    let secret: String = {
        // 32 bytes of getentropy-backed randomness, hex.
        let mut raw = [0u8; 32];
        getrandom(&mut raw)?;
        raw.iter().map(|b| format!("{b:02x}")).collect()
    };
    let accept_secret = secret.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let fs = fs.clone();
            let secret = accept_secret.clone();
            tokio::spawn(async move {
                let _ = connection(stream, fs, &secret).await;
            });
        }
    });
    Ok(VfsServer { port, secret })
}

fn getrandom(buf: &mut [u8]) -> std::io::Result<()> {
    // getentropy(2) is available on every platform tl targets.
    let rc = unsafe { libc::getentropy(buf.as_mut_ptr().cast(), buf.len()) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

async fn connection(
    mut stream: tokio::net::TcpStream,
    fs: Arc<OverlayFs>,
    secret: &str,
) -> std::io::Result<()> {
    stream.set_nodelay(true)?;
    let mut hello_done = false;
    loop {
        let len = match stream.read_u32_le().await {
            Ok(len) => len,
            Err(_) => return Ok(()), // peer closed
        };
        if len == 0 || len > MAX_FRAME {
            return Ok(());
        }
        let mut frame = vec![0u8; len as usize];
        stream.read_exact(&mut frame).await?;
        let opcode = frame[0];
        let payload = &frame[1..];

        if !hello_done {
            let resp = if opcode == op::HELLO {
                let mut r = Reader::new(payload);
                match (r.str(), r.u32()) {
                    (Ok(s), Ok(v)) if s == secret && v == PROTOCOL_VERSION => {
                        hello_done = true;
                        Writer::ok()
                    }
                    _ => Writer::err(libc::EACCES),
                }
            } else {
                Writer::err(libc::EACCES)
            };
            write_frame(&mut stream, resp).await?;
            if !hello_done {
                return Ok(());
            }
            continue;
        }

        let resp = handle(&fs, opcode, payload).await;
        if TRACE_OPS.load(std::sync::atomic::Ordering::Relaxed) {
            let errno = i32::from_le_bytes(resp.buf[..4].try_into().unwrap_or_default());
            let mut r = Reader::new(payload);
            let arg = r.u64().unwrap_or(0);
            let name = match opcode {
                op::LOOKUP
                | op::CREATE
                | op::MKDIR
                | op::SYMLINK
                | op::UNLINK
                | op::RMDIR
                | op::RENAME => r.str().unwrap_or("?").to_string(),
                op::READDIR => format!(
                    "off={} -> {} entries",
                    r.u64().unwrap_or(0),
                    resp.buf
                        .get(4..8)
                        .map(|b| u32::from_le_bytes(b.try_into().unwrap()))
                        .unwrap_or(0)
                ),
                _ => String::new(),
            };
            eprintln!(
                "[vfs {:>9.3}] op={opcode:<2} arg={arg} {name} errno={errno}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs_f64()
                    % 1000.0,
            );
        }
        write_frame(&mut stream, resp).await?;
    }
}

async fn write_frame(stream: &mut tokio::net::TcpStream, w: Writer) -> std::io::Result<()> {
    stream.write_u32_le(w.buf.len() as u32).await?;
    stream.write_all(&w.buf).await?;
    Ok(())
}

macro_rules! try_req {
    ($e:expr) => {
        match $e {
            Ok(v) => v,
            Err(errno) => return Writer::err(errno),
        }
    };
}

fn map<T>(result: Result<T, MountError>) -> Result<T, i32> {
    result.map_err(|e| errno(&e))
}

async fn handle(fs: &Arc<OverlayFs>, opcode: u8, payload: &[u8]) -> Writer {
    let mut r = Reader::new(payload);
    match opcode {
        op::GETATTR => {
            let ino = try_req!(r.u64());
            let attr = try_req!(map(fs.getattr(ino).await));
            let mut w = Writer::ok();
            w.attr(&attr);
            w
        }
        op::LOOKUP => {
            let parent = try_req!(r.u64());
            let name = try_req!(r.str());
            let attr = try_req!(map(fs.lookup(parent, name).await));
            let mut w = Writer::ok();
            w.attr(&attr);
            w
        }
        op::FORGET => {
            let ino = try_req!(r.u64());
            let n = try_req!(r.u64());
            fs.forget(ino, n);
            Writer::ok()
        }
        op::OPENDIR => {
            let ino = try_req!(r.u64());
            let fh = try_req!(map(fs.opendir(ino).await));
            let mut w = Writer::ok();
            w.u64v(fh);
            w
        }
        op::READDIR => {
            let fh = try_req!(r.u64());
            let offset = try_req!(r.u64());
            let max = try_req!(r.u32());
            let entries = try_req!(map(fs.readdir(fh, offset, max.min(10_000) as usize)));
            let mut w = Writer::ok();
            w.u32v(entries.len() as u32);
            for entry in entries {
                w.u64v(entry.next_offset);
                w.u8v(kind_byte(entry.kind));
                w.bytes(entry.name.as_bytes());
            }
            w
        }
        op::RELEASEDIR => {
            let fh = try_req!(r.u64());
            fs.releasedir(fh);
            Writer::ok()
        }
        op::OPEN => {
            let ino = try_req!(r.u64());
            let write = try_req!(r.u8()) != 0;
            // The keep-cache hint has no slot in the FSKit wire protocol (FSKit does its own
            // attribute-driven revalidation); the overlay still records the open identity.
            let (fh, _keep_cache) = try_req!(map(fs.open(ino, write).await));
            let mut w = Writer::ok();
            w.u64v(fh);
            w
        }
        op::READ => {
            let fh = try_req!(r.u64());
            let off = try_req!(r.u64());
            let len = try_req!(r.u32());
            let data = try_req!(map(fs.read(fh, off, len.min(4 * 1024 * 1024) as u64).await));
            let mut w = Writer::ok();
            w.bytes(&data);
            w
        }
        op::WRITE => {
            let fh = try_req!(r.u64());
            let off = try_req!(r.u64());
            let data = try_req!(r.bytes());
            let n = try_req!(map(fs.write(fh, off, data)));
            let mut w = Writer::ok();
            w.u32v(n);
            w
        }
        op::RELEASE => {
            let fh = try_req!(r.u64());
            fs.release(fh);
            Writer::ok()
        }
        op::FSYNC => {
            let fh = try_req!(r.u64());
            try_req!(map(fs.fsync(fh)));
            Writer::ok()
        }
        op::CREATE => {
            let parent = try_req!(r.u64());
            let name = try_req!(r.str());
            let exec = try_req!(r.u8()) != 0;
            let (attr, fh) = try_req!(map(fs.create(parent, name, exec).await));
            let mut w = Writer::ok();
            w.attr(&attr).u64v(fh);
            w
        }
        op::MKDIR => {
            let parent = try_req!(r.u64());
            let name = try_req!(r.str());
            let attr = try_req!(map(fs.mkdir(parent, name).await));
            let mut w = Writer::ok();
            w.attr(&attr);
            w
        }
        op::SYMLINK => {
            let parent = try_req!(r.u64());
            let name = try_req!(r.str());
            let target = try_req!(r.str());
            let attr = try_req!(map(fs.symlink(parent, name, target).await));
            let mut w = Writer::ok();
            w.attr(&attr);
            w
        }
        op::READLINK => {
            let ino = try_req!(r.u64());
            let target = try_req!(map(fs.readlink(ino).await));
            let mut w = Writer::ok();
            w.bytes(&target);
            w
        }
        op::UNLINK => {
            let parent = try_req!(r.u64());
            let name = try_req!(r.str());
            try_req!(map(fs.unlink(parent, name).await));
            Writer::ok()
        }
        op::RMDIR => {
            let parent = try_req!(r.u64());
            let name = try_req!(r.str());
            try_req!(map(fs.rmdir(parent, name).await));
            Writer::ok()
        }
        op::RENAME => {
            let parent = try_req!(r.u64());
            let name = try_req!(r.str());
            let new_parent = try_req!(r.u64());
            let new_name = try_req!(r.str());
            try_req!(map(fs.rename(parent, name, new_parent, new_name).await));
            Writer::ok()
        }
        op::SETATTR => {
            let ino = try_req!(r.u64());
            let has_size = try_req!(r.u8()) != 0;
            let size = try_req!(r.u64());
            let has_mode = try_req!(r.u8()) != 0;
            let mode = try_req!(r.u32());
            let attr = try_req!(map(fs
                .setattr(ino, has_size.then_some(size), has_mode.then_some(mode))
                .await));
            let mut w = Writer::ok();
            w.attr(&attr);
            w
        }
        op::STATFS => {
            // Synthetic: the backing store is remote and effectively unbounded.
            let mut w = Writer::ok();
            w.u64v(1 << 40).u64v(1 << 39).u64v(1 << 20);
            w
        }
        _ => Writer::err(libc::ENOSYS),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_primitives_round_trip() {
        let mut w = Writer::ok();
        w.u64v(42).u8v(7).bytes(b"name").u16v(0o755).u32v(9);
        // Skip the errno prefix, then read back.
        let mut r = Reader::new(&w.buf[4..]);
        assert_eq!(r.u64().unwrap(), 42);
        assert_eq!(r.u8().unwrap(), 7);
        assert_eq!(r.bytes().unwrap(), b"name");
        // u16 is not directly readable via Reader (server never reads one) — check raw.
        let raw = &w.buf[4 + 8 + 1 + 4 + 4..];
        assert_eq!(u16::from_le_bytes(raw[..2].try_into().unwrap()), 0o755);
        assert_eq!(u32::from_le_bytes(raw[2..6].try_into().unwrap()), 9);
    }

    #[test]
    fn truncated_payload_yields_einval_not_panic() {
        let mut r = Reader::new(&[1, 2]);
        assert!(r.u64().is_err());
        let mut r = Reader::new(&[255, 255, 255, 255]);
        assert!(r.bytes().is_err());
    }

    #[test]
    fn not_empty_maps_to_enotempty_not_eio() {
        // rmdir of a non-empty directory must surface ENOTEMPTY; the old Protocol("directory
        // not empty") fell through to the EIO catch-all and `rm` reported "Input/output error".
        assert_eq!(errno(&MountError::NotEmpty), libc::ENOTEMPTY);
        assert_ne!(errno(&MountError::NotEmpty), libc::EIO);
    }

    #[test]
    fn unsupported_maps_to_enotsup_not_eio() {
        // Renaming a committed directory is unsupported; it must surface ENOTSUP, not the
        // Protocol->EIO catch-all that read as a bewildering "Input/output error".
        assert_eq!(errno(&MountError::Unsupported(String::new())), libc::ENOTSUP);
        assert_ne!(errno(&MountError::Unsupported(String::new())), libc::EIO);
    }
}
