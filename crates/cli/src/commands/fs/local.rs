//! Local helpers for materializing `tl fs` tree entries into a directory.
//!
//! The FUSE overlay owns all mount state; what remains here is shared by restore paths that write
//! fetched entries into the overlay's upper layer.

use std::path::Path;

use crate::error::Result;

/// Write one fetched entry (file, executable, or symlink) under `root`.
pub fn write_entry(root: &Path, path: &str, mode: u32, bytes: &[u8]) -> Result<()> {
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
    Ok(())
}
