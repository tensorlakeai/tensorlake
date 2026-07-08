//! Local helpers for `tl fs`: ignore rules and materializing tree entries into a directory.
//!
//! The FUSE overlay owns all mount state; what remains here is shared by snapshot enumeration
//! (which paths never upload) and restore (writing fetched entries into the overlay's upper
//! layer).

use std::path::Path;

use crate::error::Result;

const IGNORED_DIR_NAMES: &[&str] = &[
    ".git",
    "node_modules",
    "target",
    "dist",
    ".cache",
    "__pycache__",
];

/// Whether a name is inherently workspace-local: macOS metadata turds the kernel writes onto
/// filesystems without native xattr support. They serve reads from the overlay but never
/// version.
pub fn is_metadata_turd(name: &str) -> bool {
    name.starts_with("._") || name == ".DS_Store"
}

/// Names ignored at any depth: the built-in set plus `.tlignore` lines read from the mount root
/// (comments with `#`, blank lines skipped; a trailing `/` is stripped — v1 matches
/// directory/file *names*). Ignored paths are workspace-local: never uploaded, never restored.
pub fn ignored_names(mount_root: &Path) -> Vec<String> {
    let mut names: Vec<String> = IGNORED_DIR_NAMES.iter().map(|s| s.to_string()).collect();
    if let Ok(raw) = std::fs::read_to_string(mount_root.join(".tlignore")) {
        for line in raw.lines() {
            let line = line.trim().trim_end_matches('/');
            if !line.is_empty() && !line.starts_with('#') {
                names.push(line.to_string());
            }
        }
    }
    names
}

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
