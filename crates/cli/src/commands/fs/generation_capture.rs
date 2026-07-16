//! Immutable local sources for one frozen filesystem generation.
//!
//! The live upper layer cannot itself be a prepared generation's byte authority: applications
//! may keep writing it while hashing/compression/upload runs.  Capture each dirty entry under the
//! generation-owned staging directory first.  Filesystems with copy-on-write cloning make this a
//! metadata operation; the portable fallback copies bytes and validates the source identity on
//! both sides of the copy.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use tensorlake::artifact_storage::native_fs::NativeLocalUpsert;

use crate::error::{CliError, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CaptureStrategy {
    Independent,
    SharedHardlink,
}

/// Capture `upserts` into an immutable, generation-owned tree and return the sources the native
/// preparer must read. Existing captures are replaced atomically, which makes a retry after a
/// process crash conservative: the generation is rebuilt from the current journal-resolved view
/// before it can become publishable.
#[cfg(unix)]
pub(crate) fn capture_generation_upserts(
    state_dir: &Path,
    generation: u64,
    upserts: impl IntoIterator<Item = (String, PathBuf)>,
) -> Result<Vec<NativeLocalUpsert>> {
    capture_generation_upserts_internal(state_dir, generation, upserts, false)
}

/// Managed mounts route every later write through the overlay's hardlink-breaking COW fence.
/// Therefore a filesystem without reflink support can freeze through metadata-only hardlinks;
/// the first generation-N+1 write copies and retargets the live inode before changing it.
#[cfg(unix)]
pub(crate) fn capture_managed_generation_upserts(
    state_dir: &Path,
    generation: u64,
    upserts: impl IntoIterator<Item = (String, PathBuf)>,
) -> Result<Vec<NativeLocalUpsert>> {
    capture_generation_upserts_internal(state_dir, generation, upserts, true)
}

#[cfg(unix)]
fn capture_generation_upserts_internal(
    state_dir: &Path,
    generation: u64,
    upserts: impl IntoIterator<Item = (String, PathBuf)>,
    allow_shared_hardlinks: bool,
) -> Result<Vec<NativeLocalUpsert>> {
    let root = state_dir
        .join("staging")
        .join("generations")
        .join(generation.to_string())
        .join("upper");
    std::fs::create_dir_all(&root)?;

    let mut captured = Vec::new();
    for (path, source) in upserts {
        let destination = root.join(&path);
        capture_entry(&source, &destination, allow_shared_hardlinks)?;
        captured.push(NativeLocalUpsert {
            path,
            source: destination,
        });
    }
    Ok(captured)
}

/// Retire all local byte sources owned by one generation. Safe to call repeatedly after a crash.
#[cfg(unix)]
pub(crate) fn retire_generation_capture(state_dir: &Path, generation: u64) -> Result<()> {
    let generation_dir = state_dir
        .join("staging")
        .join("generations")
        .join(generation.to_string());
    remove_any(&generation_dir)?;
    Ok(())
}

#[cfg(unix)]
pub(crate) fn generation_capture_bytes(state_dir: &Path, generation: u64) -> Result<u64> {
    let generation_dir = state_dir
        .join("staging")
        .join("generations")
        .join(generation.to_string());
    match directory_bytes_without_following_symlinks(&generation_dir) {
        Ok(bytes) => Ok(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error.into()),
    }
}

/// Remove generation-owned captures that have no corresponding durable artifact ownership row.
///
/// The ownership row is committed before capture construction starts. A crash can therefore
/// leave an empty or partial *owned* directory, which recovery retains and the same generation
/// rebuilds idempotently. A directory without a row is an orphan from an interrupted
/// create/retire boundary and is reclaimable. The directory name keeps discovery bounded to the
/// staging namespace rather than the user overlay.
#[cfg(unix)]
pub(crate) fn reclaim_orphan_generation_captures(
    state_dir: &Path,
    owned_generations: &BTreeSet<u64>,
) -> Result<(usize, u64)> {
    let root = state_dir.join("staging").join("generations");
    let entries = match std::fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok((0, 0)),
        Err(error) => return Err(error.into()),
    };
    let mut reclaimed = 0usize;
    let mut reclaimed_bytes = 0u64;
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let Some(generation) = name.to_str().and_then(|name| name.parse::<u64>().ok()) else {
            // An unknown name is evidence for support/repair, not ours to delete.
            continue;
        };
        if owned_generations.contains(&generation) {
            continue;
        }
        reclaimed_bytes = reclaimed_bytes
            .saturating_add(directory_bytes_without_following_symlinks(&entry.path())?);
        remove_any(&entry.path())?;
        reclaimed += 1;
    }
    Ok((reclaimed, reclaimed_bytes))
}

#[cfg(unix)]
fn directory_bytes_without_following_symlinks(path: &Path) -> std::io::Result<u64> {
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Ok(metadata.len());
    }
    let mut bytes = 0u64;
    for entry in std::fs::read_dir(path)? {
        bytes = bytes.saturating_add(directory_bytes_without_following_symlinks(&entry?.path())?);
    }
    Ok(bytes)
}

#[cfg(unix)]
fn capture_entry(source: &Path, destination: &Path, allow_shared_hardlinks: bool) -> Result<()> {
    let before = std::fs::symlink_metadata(source).map_err(|error| {
        CliError::usage(format!(
            "capturing dirty path {} failed before preparation: {error}",
            source.display()
        ))
    })?;
    if let Some(parent) = destination.parent() {
        std::fs::create_dir_all(parent)?;
    }
    remove_any(destination)?;
    let temporary = destination.with_extension(format!(
        "tl-generation-capture-{:016x}",
        rand::random::<u64>()
    ));
    remove_any(&temporary)?;

    let file_type = before.file_type();
    let capture_result = if file_type.is_dir() && !file_type.is_symlink() {
        std::fs::create_dir(&temporary).map(|_| CaptureStrategy::Independent)
    } else if file_type.is_symlink() {
        std::fs::read_link(source)
            .and_then(|target| std::os::unix::fs::symlink(target, &temporary))
            .map(|_| CaptureStrategy::Independent)
    } else if file_type.is_file() {
        clone_or_capture_file(source, &temporary, allow_shared_hardlinks)
    } else {
        return Err(CliError::usage(format!(
            "{} is not a regular file, directory, or symlink",
            source.display()
        )));
    };
    let strategy = match capture_result {
        Ok(strategy) => strategy,
        Err(error) => {
            let _ = remove_any(&temporary);
            return Err(error.into());
        }
    };

    if strategy == CaptureStrategy::Independent
        && let Err(error) = preserve_metadata(source, &temporary, &before)
    {
        let _ = remove_any(&temporary);
        return Err(error);
    }
    if strategy == CaptureStrategy::Independent
        && let Err(error) = sync_capture_entry(&temporary, &before)
    {
        let _ = remove_any(&temporary);
        return Err(error.into());
    }
    if strategy == CaptureStrategy::Independent {
        let after = std::fs::symlink_metadata(source)?;
        if !same_capture_identity(&before, &after) {
            let _ = remove_any(&temporary);
            return Err(CliError::usage(format!(
                "{} changed while its snapshot generation was being captured; retrying preparation",
                source.display()
            )));
        }
    }
    std::fs::rename(&temporary, destination)?;
    if let Some(parent) = destination.parent() {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[cfg(unix)]
fn sync_capture_entry(path: &Path, metadata: &std::fs::Metadata) -> std::io::Result<()> {
    if metadata.file_type().is_symlink() {
        // Symlink contents cannot be opened portably; the containing directory fsync after the
        // atomic rename is the durability boundary for the link entry.
        return Ok(());
    }
    std::fs::File::open(path)?.sync_all()
}

#[cfg(unix)]
fn remove_any(path: &Path) -> std::io::Result<()> {
    let Ok(metadata) = std::fs::symlink_metadata(path) else {
        return Ok(());
    };
    if metadata.is_dir() && !metadata.file_type().is_symlink() {
        std::fs::remove_dir_all(path)
    } else {
        std::fs::remove_file(path)
    }
}

#[cfg(target_os = "macos")]
fn clone_or_capture_file(
    source: &Path,
    destination: &Path,
    allow_shared_hardlink: bool,
) -> std::io::Result<CaptureStrategy> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let source_c = CString::new(source.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::from_raw_os_error(libc::EINVAL))?;
    let destination_c = CString::new(destination.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::from_raw_os_error(libc::EINVAL))?;
    // SAFETY: both C strings are alive for the call and contain no interior NUL.
    if unsafe { libc::clonefile(source_c.as_ptr(), destination_c.as_ptr(), 0) } == 0 {
        return Ok(CaptureStrategy::Independent);
    }
    let clone_error = std::io::Error::last_os_error();
    if !matches!(
        clone_error.raw_os_error(),
        Some(code) if code == libc::ENOTSUP || code == libc::EXDEV || code == libc::EINVAL
    ) {
        return Err(clone_error);
    }
    if allow_shared_hardlink {
        std::fs::hard_link(source, destination).map(|_| CaptureStrategy::SharedHardlink)
    } else {
        std::fs::copy(source, destination).map(|_| CaptureStrategy::Independent)
    }
}

#[cfg(target_os = "linux")]
fn clone_or_capture_file(
    source: &Path,
    destination: &Path,
    allow_shared_hardlink: bool,
) -> std::io::Result<CaptureStrategy> {
    use std::os::fd::AsRawFd;

    let source_file = std::fs::File::open(source)?;
    let destination_file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(destination)?;
    // SAFETY: both descriptors remain open for the ioctl and FICLONE consumes the source fd as
    // an integer argument.
    if unsafe {
        libc::ioctl(
            destination_file.as_raw_fd(),
            libc::FICLONE as _,
            source_file.as_raw_fd(),
        )
    } == 0
    {
        return Ok(CaptureStrategy::Independent);
    }
    let clone_error = std::io::Error::last_os_error();
    drop(destination_file);
    let _ = std::fs::remove_file(destination);
    if !matches!(
        clone_error.raw_os_error(),
        Some(code)
            if code == libc::EOPNOTSUPP
                || code == libc::EXDEV
                || code == libc::EINVAL
                || code == libc::ENOTTY
    ) {
        return Err(clone_error);
    }
    if allow_shared_hardlink {
        std::fs::hard_link(source, destination).map(|_| CaptureStrategy::SharedHardlink)
    } else {
        std::fs::copy(source, destination).map(|_| CaptureStrategy::Independent)
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn clone_or_capture_file(
    source: &Path,
    destination: &Path,
    allow_shared_hardlink: bool,
) -> std::io::Result<CaptureStrategy> {
    if allow_shared_hardlink {
        std::fs::hard_link(source, destination).map(|_| CaptureStrategy::SharedHardlink)
    } else {
        std::fs::copy(source, destination).map(|_| CaptureStrategy::Independent)
    }
}

#[cfg(unix)]
fn preserve_metadata(
    source: &Path,
    destination: &Path,
    metadata: &std::fs::Metadata,
) -> Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    if !metadata.file_type().is_symlink() {
        std::fs::set_permissions(
            destination,
            std::fs::Permissions::from_mode(metadata.mode()),
        )?;
    }
    let modified =
        filetime::FileTime::from_unix_time(metadata.mtime(), metadata.mtime_nsec() as u32);
    if metadata.file_type().is_symlink() {
        filetime::set_symlink_file_times(destination, modified, modified)?;
    } else {
        filetime::set_file_mtime(destination, modified)?;
    }
    for name in xattr::list(source)? {
        if let Some(value) = xattr::get(source, &name)? {
            xattr::set(destination, &name, &value)?;
        }
    }
    Ok(())
}

#[cfg(unix)]
fn same_capture_identity(before: &std::fs::Metadata, after: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;

    before.dev() == after.dev()
        && before.ino() == after.ino()
        && before.size() == after.size()
        && before.mtime() == after.mtime()
        && before.mtime_nsec() == after.mtime_nsec()
        && before.ctime() == after.ctime()
        && before.ctime_nsec() == after.ctime_nsec()
        && before.mode() == after.mode()
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn capture_is_independent_from_later_source_writes() {
        let state = tempfile::tempdir().unwrap();
        let source_root = tempfile::tempdir().unwrap();
        let source = source_root.path().join("file.txt");
        std::fs::write(&source, b"before").unwrap();

        let captured =
            capture_generation_upserts(state.path(), 7, [("file.txt".to_string(), source.clone())])
                .unwrap();
        std::fs::write(&source, b"after").unwrap();

        assert_eq!(std::fs::read(&captured[0].source).unwrap(), b"before");
        assert_eq!(std::fs::read(&source).unwrap(), b"after");
    }

    #[test]
    fn capture_preserves_symlink_without_following_it() {
        let state = tempfile::tempdir().unwrap();
        let source_root = tempfile::tempdir().unwrap();
        let source = source_root.path().join("link");
        std::os::unix::fs::symlink("missing-target", &source).unwrap();

        let captured =
            capture_generation_upserts(state.path(), 3, [("link".to_string(), source)]).unwrap();

        assert_eq!(
            std::fs::read_link(&captured[0].source).unwrap(),
            PathBuf::from("missing-target")
        );
    }

    #[test]
    fn orphan_reclamation_keeps_only_live_generation_directories() {
        let state = tempfile::tempdir().unwrap();
        for generation in [2, 3, 9] {
            let root = state
                .path()
                .join("staging")
                .join("generations")
                .join(generation.to_string())
                .join("upper");
            std::fs::create_dir_all(&root).unwrap();
            std::fs::write(
                root.join("data"),
                vec![generation as u8; generation as usize],
            )
            .unwrap();
        }
        std::fs::create_dir_all(
            state
                .path()
                .join("staging")
                .join("generations")
                .join("support-bundle"),
        )
        .unwrap();

        let (count, bytes) =
            reclaim_orphan_generation_captures(state.path(), &[3, 9].into_iter().collect())
                .unwrap();

        assert_eq!(count, 1);
        assert_eq!(bytes, 2);
        assert!(!state.path().join("staging/generations/2").exists());
        assert!(state.path().join("staging/generations/3").exists());
        assert!(state.path().join("staging/generations/9").exists());
        assert!(
            state
                .path()
                .join("staging/generations/support-bundle")
                .exists(),
            "unknown names are retained for explicit support/repair"
        );
    }
}
