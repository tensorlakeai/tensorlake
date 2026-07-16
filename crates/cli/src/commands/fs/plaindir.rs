//! Cleanup-only reader for pre-release Git-backed plain-directory bindings.
//!
//! Native tracked directories now use the common redb generation engine in `fs.rs`. This module
//! intentionally retains only enough of the old local format to find, fence, and remove
//! development bindings without discarding their directory contents. It contains no snapshot,
//! hashing, upload, status, or publication path.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use console::style;
use serde::{Deserialize, Serialize};

use crate::error::{CliError, Result};

use super::{canonical_mountpoint, short_id};

#[derive(Debug, Default, Serialize, Deserialize)]
struct BindingRegistry {
    #[serde(default)]
    bindings: BTreeMap<String, PathBuf>,
}

fn registry_path() -> PathBuf {
    crate::config::files::config_dir().join("bindings.json")
}

fn state_dir_root() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".local")
        .join("share")
        .join("tensorlake")
        .join("bindings")
}

fn registry_load() -> Result<BindingRegistry> {
    registry_load_at(&registry_path())
}

fn registry_load_at(path: &Path) -> Result<BindingRegistry> {
    match std::fs::read(path) {
        Ok(raw) => serde_json::from_slice(&raw).map_err(|error| {
            CliError::usage(format!(
                "the removed-binding registry {} is corrupt ({error}); refusing to guess. Its \
                 state directories under {} are untouched.",
                path.display(),
                state_dir_root().display(),
            ))
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(BindingRegistry::default())
        }
        Err(error) => Err(CliError::usage(format!(
            "cannot read the removed-binding registry {}: {error}",
            path.display()
        ))),
    }
}

static REGISTRY_WARNED: std::sync::Once = std::sync::Once::new();
static REGISTRY_CORRUPT: std::sync::OnceLock<String> = std::sync::OnceLock::new();

pub(crate) fn registry_corruption_note() -> Option<String> {
    REGISTRY_CORRUPT.get().map(|path| {
        format!(
            "note: the removed-binding registry at {path} is unreadable; repair or remove that \
             file before cleaning up a pre-release binding"
        )
    })
}

fn registry_lenient<T>(result: Result<T>) -> Option<T> {
    match result {
        Ok(value) => Some(value),
        Err(error) => {
            let _ = REGISTRY_CORRUPT.set(registry_path().display().to_string());
            REGISTRY_WARNED.call_once(|| {
                eprintln!(
                    "{} {error}\n         (native mount commands remain available; cleanup of \
                     removed bindings fails closed until the registry is repaired)",
                    style("warning:").yellow(),
                );
            });
            None
        }
    }
}

fn registry_save(registry: &BindingRegistry) -> Result<()> {
    write_atomic(&registry_path(), &serde_json::to_vec_pretty(registry)?)
}

fn binding_state_live(state_dir: &Path) -> bool {
    state_dir.join("binding.json").exists()
}

fn prune_dangling(bindings: &mut BTreeMap<String, PathBuf>) {
    bindings.retain(|_, state_dir| binding_state_live(state_dir));
}

fn registry_mutate(mutate: impl FnOnce(&mut BindingRegistry) -> Result<()>) -> Result<()> {
    let config_dir = crate::config::files::config_dir();
    std::fs::create_dir_all(&config_dir)?;
    let _lock = flock_exclusive(&config_dir.join("bindings.lock"), true)?
        .ok_or_else(|| CliError::usage("could not lock the removed-binding registry"))?;
    let mut registry = registry_load()?;
    prune_dangling(&mut registry.bindings);
    mutate(&mut registry)?;
    registry_save(&registry)
}

pub fn binding_for(path: &Path) -> Result<Option<(String, PathBuf)>> {
    let root = canonical_mountpoint(path)?;
    Ok(registry_load()?
        .bindings
        .get(&root)
        .filter(|state_dir| binding_state_live(state_dir))
        .map(|state_dir| (root, state_dir.clone())))
}

pub fn binding_for_lenient(path: &Path) -> Option<(String, PathBuf)> {
    registry_lenient(binding_for(path)).flatten()
}

pub fn binding_roots_lenient() -> Vec<String> {
    registry_lenient(registry_load())
        .map(|registry| registry.bindings.keys().cloned().collect())
        .unwrap_or_default()
}

fn deepest_containing<'a>(roots: impl Iterator<Item = &'a String>, path: &Path) -> Option<String> {
    roots
        .filter(|root| {
            let root_path = Path::new(root);
            path.starts_with(root_path)
                || root_path
                    .canonicalize()
                    .is_ok_and(|canonical| path.starts_with(canonical))
        })
        .max_by_key(|root| Path::new(root).components().count())
        .cloned()
}

fn binding_containing(path: &Path) -> Result<Option<(String, PathBuf)>> {
    let registry = registry_load()?;
    Ok(deepest_containing(
        registry
            .bindings
            .iter()
            .filter(|(_, state_dir)| binding_state_live(state_dir))
            .map(|(root, _)| root),
        path,
    )
    .map(|root| {
        let state_dir = registry.bindings[&root].clone();
        (root, state_dir)
    }))
}

pub(crate) fn bound_workspaces() -> Vec<(String, String)> {
    let Some(registry) = registry_lenient(registry_load()) else {
        return Vec::new();
    };
    registry
        .bindings
        .iter()
        .filter_map(|(root, state_dir)| {
            let binding = load_binding(state_dir).ok()?;
            Some((binding.workspace_id, root.clone()))
        })
        .collect()
}

pub(crate) fn bound_binding_repos() -> Vec<(String, String)> {
    let Some(registry) = registry_lenient(registry_load()) else {
        return Vec::new();
    };
    registry
        .bindings
        .iter()
        .filter_map(|(root, state_dir)| {
            let binding = load_binding(state_dir).ok()?;
            Some((binding.repo, root.clone()))
        })
        .collect()
}

pub(crate) fn binding_using_workspace(workspace_id: &str) -> Result<Option<String>> {
    let read = match std::fs::read_dir(state_dir_root()) {
        Ok(read) => read,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(CliError::usage(format!(
                "cannot read removed-binding state: {error}"
            )));
        }
    };
    for entry in read {
        let state_dir = entry?.path();
        let path = state_dir.join("binding.json");
        let raw = match std::fs::read(&path) {
            Ok(raw) => raw,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(CliError::usage(format!(
                    "cannot read removed binding {}: {error}; refusing to treat workspace \
                     {workspace_id} as unattached",
                    path.display()
                )));
            }
        };
        let binding: Binding = serde_json::from_slice(&raw).map_err(|error| {
            CliError::usage(format!(
                "removed binding {} is corrupt ({error}); refusing to treat workspace \
                 {workspace_id} as unattached",
                path.display()
            ))
        })?;
        if binding.workspace_id == workspace_id {
            return Ok(Some(binding.root.to_string_lossy().into_owned()));
        }
    }
    Ok(None)
}

pub(crate) fn assert_no_overlap(root: &str) -> Result<()> {
    if let Some(message) = mount_overlap_error(root) {
        return Err(CliError::usage(message));
    }
    if let Some(message) = binding_overlap_error(root, &registry_load()?.bindings) {
        return Err(CliError::usage(message));
    }
    Ok(())
}

fn mount_overlap_error(root: &str) -> Option<String> {
    let candidate = Path::new(root);
    super::registry_load().keys().find_map(|mountpoint| {
        let mount_path = Path::new(mountpoint);
        (candidate.starts_with(mount_path) || mount_path.starts_with(candidate)).then(|| {
            format!(
                "{root} overlaps the mount at {mountpoint}; a directory cannot be both mounted \
                 and tracked"
            )
        })
    })
}

fn binding_overlap_error(root: &str, bindings: &BTreeMap<String, PathBuf>) -> Option<String> {
    let candidate = Path::new(root);
    bindings.keys().find_map(|bound| {
        let bound_path = Path::new(bound);
        (candidate.starts_with(bound_path) || bound_path.starts_with(candidate)).then(|| {
            format!(
                "{root} overlaps removed pre-release binding {bound}; clean it up with `tl fs \
                 unmount {bound}` first"
            )
        })
    })
}

pub fn assert_no_binding_overlap(mountpoint: &str) -> Result<()> {
    let candidate = Path::new(mountpoint);
    for bound in registry_load()?.bindings.keys() {
        let bound_path = Path::new(bound);
        if candidate.starts_with(bound_path) || bound_path.starts_with(candidate) {
            return Err(CliError::usage(format!(
                "{mountpoint} overlaps removed pre-release binding {bound}; clean it up with `tl \
                 fs unmount {bound}` first"
            )));
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Binding {
    pub project_id: String,
    #[serde(default)]
    pub organization_id: Option<String>,
    pub repo: String,
    pub workspace_id: String,
    pub ref_name: String,
    pub root: PathBuf,
    pub created_at_secs: u64,
    #[serde(default)]
    pub publish: bool,
    #[serde(default)]
    pub local_state_uuid: Option<String>,
}

pub(crate) fn load_binding(state_dir: &Path) -> Result<Binding> {
    let path = state_dir.join("binding.json");
    let raw = std::fs::read(&path).map_err(|error| {
        CliError::usage(format!(
            "cannot read removed binding {}: {error}",
            path.display()
        ))
    })?;
    serde_json::from_slice(&raw).map_err(|error| {
        CliError::usage(format!(
            "removed binding {} is corrupt ({error}); refusing to guess",
            path.display()
        ))
    })
}

pub(crate) fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write as _;
    let parent = path
        .parent()
        .ok_or_else(|| CliError::usage(format!("{} has no parent", path.display())))?;
    std::fs::create_dir_all(parent)?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| CliError::usage(format!("{} has no UTF-8 file name", path.display())))?;
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let temporary = parent.join(format!(
        "{file_name}.{}.{}.tmp",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    {
        let mut file = std::fs::File::create(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    if let Err(error) = std::fs::rename(&temporary, path) {
        let _ = std::fs::remove_file(&temporary);
        return Err(error.into());
    }
    if let Ok(directory) = std::fs::File::open(parent) {
        let _ = directory.sync_all();
    }
    Ok(())
}

#[cfg(unix)]
pub(crate) fn flock_exclusive(path: &Path, block: bool) -> Result<Option<std::fs::File>> {
    use std::os::unix::io::AsRawFd as _;
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)?;
    let flags = libc::LOCK_EX | if block { 0 } else { libc::LOCK_NB };
    if unsafe { libc::flock(file.as_raw_fd(), flags) } != 0 {
        if block {
            return Err(CliError::usage(format!(
                "could not lock {}: {}",
                path.display(),
                std::io::Error::last_os_error()
            )));
        }
        return Ok(None);
    }
    Ok(Some(file))
}

#[cfg(not(unix))]
pub(crate) fn flock_exclusive(_path: &Path, _block: bool) -> Result<Option<std::fs::File>> {
    Err(CliError::usage(
        "local filesystem attachment state is supported on Unix only",
    ))
}

fn orphan_state_dir_for(root: &str) -> Option<PathBuf> {
    let read = std::fs::read_dir(state_dir_root()).ok()?;
    for entry in read.flatten() {
        if let Ok(binding) = load_binding(&entry.path())
            && binding.root == Path::new(root)
        {
            return Some(entry.path());
        }
    }
    None
}

/// Remove only the local metadata of a pre-release binding. Directory bytes and server data are
/// never touched.
pub async fn unbind(path: Option<PathBuf>) -> Result<()> {
    let (root, state_dir) = match path {
        Some(path) => match binding_for(&path)? {
            Some(found) => found,
            None => {
                let root = canonical_mountpoint(&path)?;
                if let Some(state_dir) = orphan_state_dir_for(&root) {
                    let workspace = load_binding(&state_dir)
                        .map(|binding| binding.workspace_id)
                        .unwrap_or_default();
                    std::fs::remove_dir_all(&state_dir)?;
                    println!(
                        "Removed orphaned pre-release binding state for {root}. Workspace {} \
                         survives on the server.",
                        short_id(&workspace)
                    );
                    return Ok(());
                }
                return Err(CliError::usage(format!(
                    "{} is not a tracked directory",
                    path.display()
                )));
            }
        },
        None => {
            let cwd = std::env::current_dir()?;
            let cwd = cwd.canonicalize().unwrap_or(cwd);
            binding_containing(&cwd)?.ok_or_else(|| {
                CliError::usage(format!(
                    "{} is not inside a removed pre-release binding",
                    cwd.display()
                ))
            })?
        }
    };
    let workspace = load_binding(&state_dir)
        .map(|binding| binding.workspace_id)
        .unwrap_or_default();
    std::fs::remove_dir_all(&state_dir)?;
    registry_mutate(|registry| {
        registry.bindings.remove(&root);
        Ok(())
    })?;
    println!(
        "Stopped tracking {root}. Local files were not changed; workspace {} survives on the \
         server.",
        short_id(&workspace)
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overlap_is_symmetric() {
        let mut bindings = BTreeMap::new();
        bindings.insert("/tmp/root/child".to_string(), PathBuf::from("/tmp/state"));
        assert!(binding_overlap_error("/tmp/root", &bindings).is_some());
        assert!(binding_overlap_error("/tmp/root/child/grandchild", &bindings).is_some());
        assert!(binding_overlap_error("/tmp/elsewhere", &bindings).is_none());
    }

    #[test]
    fn corrupt_registry_fails_closed() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("bindings.json");
        std::fs::write(&path, b"{").unwrap();
        assert!(
            registry_load_at(&path)
                .unwrap_err()
                .to_string()
                .contains("corrupt")
        );
    }
}
