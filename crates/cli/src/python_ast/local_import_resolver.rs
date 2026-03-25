use std::collections::HashSet;
use std::path::{Path, PathBuf};

use rustpython_parser::{Parse, ast};

use super::image_extractor::{ImageDef, extract_images_from_source};

/// Collect all `Image` definitions reachable from `entry_file` by following
/// local-project imports only (installed packages are skipped).
///
/// `app_dir` is used as the base directory for resolving absolute imports.
pub fn collect_images(entry_file: &Path, app_dir: &Path) -> Vec<ImageDef> {
    let mut visited: HashSet<PathBuf> = HashSet::new();
    let mut images: Vec<ImageDef> = Vec::new();
    collect_recursive(entry_file, app_dir, &mut visited, &mut images);

    // Deduplicate: keep first occurrence of each image name.
    let mut seen_names: HashSet<String> = HashSet::new();
    images.retain(|img| seen_names.insert(img.name.clone()));
    images
}

fn collect_recursive(
    file: &Path,
    app_dir: &Path,
    visited: &mut HashSet<PathBuf>,
    images: &mut Vec<ImageDef>,
) {
    // Use canonical path to avoid re-visiting via different path representations.
    let canonical = file.canonicalize().unwrap_or_else(|_| file.to_path_buf());

    if !visited.insert(canonical) {
        return; // Already visited — circular import guard.
    }

    let source = match std::fs::read_to_string(file) {
        Ok(s) => s,
        Err(_) => return, // Skip unreadable files silently.
    };

    // Extract Image definitions from this file.
    images.extend(extract_images_from_source(&source));

    // Parse imports and follow local ones.
    let stmts = match ast::Suite::parse(&source, file.to_str().unwrap_or("<unknown>")) {
        Ok(s) => s,
        Err(_) => return,
    };

    let local_files = resolve_local_imports(&stmts, app_dir, file);
    for local_file in local_files {
        collect_recursive(&local_file, app_dir, visited, images);
    }
}

/// Resolve all import statements in `stmts` to local `.py` files.
///
/// Only files that actually exist under `app_dir` (for absolute imports) or
/// relative to `current_file` (for relative imports) are returned.
fn resolve_local_imports(stmts: &[ast::Stmt], app_dir: &Path, current_file: &Path) -> Vec<PathBuf> {
    let current_dir = current_file.parent().unwrap_or(current_file);
    let mut result = Vec::new();

    for stmt in stmts {
        match stmt {
            // `import foo` / `import foo.bar`
            ast::Stmt::Import(import_stmt) => {
                for alias in &import_stmt.names {
                    result.extend(resolve_absolute_module(alias.name.as_str(), app_dir));
                }
            }

            // `from X import Y` / `from . import Y` / `from .X import Y`
            ast::Stmt::ImportFrom(import_from) => {
                let level = import_from.level.as_ref().map(|l| l.to_u32()).unwrap_or(0);

                if level == 0 {
                    // Absolute import: `from module import X`
                    if let Some(ref module) = import_from.module {
                        result.extend(resolve_absolute_module(module.as_str(), app_dir));
                    }
                } else {
                    // Relative import: climb `level - 1` directories above current_dir.
                    let base = climb_dirs(current_dir, (level - 1) as usize);
                    let module_path = import_from.module.as_deref().map(|m| m.replace('.', "/"));

                    if let Some(rel_path) = module_path {
                        // `from .foo import X` or `from ..foo import X`
                        for candidate in module_candidates(&base, &rel_path) {
                            if candidate.is_file() {
                                result.push(candidate);
                            }
                        }
                    } else {
                        // `from . import X` — each name could be a sub-module.
                        for alias in &import_from.names {
                            for candidate in module_candidates(&base, alias.name.as_str()) {
                                if candidate.is_file() {
                                    result.push(candidate);
                                }
                            }
                        }
                    }
                }
            }

            _ => {}
        }
    }

    result
}

/// Try to resolve an absolute module name to a local `.py` file.
///
/// Returns files that exist under `app_dir`; non-existent paths (installed
/// packages) are filtered out.
fn resolve_absolute_module(module: &str, app_dir: &Path) -> Vec<PathBuf> {
    let rel_path = module.replace('.', "/");
    module_candidates(app_dir, &rel_path)
        .into_iter()
        .filter(|p| p.is_file())
        .collect()
}

/// Return the two candidate paths for a module under `base`:
///   `base/rel.py` and `base/rel/__init__.py`.
fn module_candidates(base: &Path, rel: &str) -> Vec<PathBuf> {
    vec![
        base.join(rel).with_extension("py"),
        base.join(rel).join("__init__.py"),
    ]
}

/// Climb `n` directories up from `dir`.
fn climb_dirs(dir: &Path, n: usize) -> PathBuf {
    let mut current = dir.to_path_buf();
    for _ in 0..n {
        current = current.parent().map(Path::to_path_buf).unwrap_or(current);
    }
    current
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    /// Write a file relative to `dir`.
    fn write(dir: &TempDir, rel: &str, content: &str) {
        let path = dir.path().join(rel);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, content).unwrap();
    }

    fn collect(dir: &TempDir, entry: &str) -> Vec<ImageDef> {
        collect_images(&dir.path().join(entry), dir.path())
    }

    // ── Single-file app ───────────────────────────────────────────────────────

    #[test]
    fn test_single_file_simple_image() {
        let dir = TempDir::new().unwrap();
        write(&dir, "app.py", r#"IMG = Image(name="single")"#);

        let imgs = collect(&dir, "app.py");
        assert_eq!(imgs.len(), 1);
        assert_eq!(imgs[0].name, "single");
    }

    #[test]
    fn test_single_file_no_images() {
        let dir = TempDir::new().unwrap();
        write(&dir, "app.py", "x = 1 + 2");

        let imgs = collect(&dir, "app.py");
        assert!(imgs.is_empty());
    }

    #[test]
    fn test_single_file_multiple_images() {
        let dir = TempDir::new().unwrap();
        write(
            &dir,
            "app.py",
            r#"
A = Image(name="alpha")
B = Image(name="beta")
"#,
        );

        let imgs = collect(&dir, "app.py");
        assert_eq!(imgs.len(), 2);
    }

    // ── Absolute imports ──────────────────────────────────────────────────────

    #[test]
    fn test_absolute_import_from_module() {
        let dir = TempDir::new().unwrap();
        write(&dir, "app.py", "from images import MY_IMAGE");
        write(&dir, "images.py", r#"MY_IMAGE = Image(name="from-module")"#);

        let imgs = collect(&dir, "app.py");
        assert_eq!(imgs.len(), 1);
        assert_eq!(imgs[0].name, "from-module");
    }

    #[test]
    fn test_absolute_import_statement() {
        let dir = TempDir::new().unwrap();
        write(&dir, "app.py", "import images");
        write(&dir, "images.py", r#"MY_IMAGE = Image(name="via-import")"#);

        let imgs = collect(&dir, "app.py");
        assert_eq!(imgs.len(), 1);
        assert_eq!(imgs[0].name, "via-import");
    }

    #[test]
    fn test_dotted_module_import() {
        let dir = TempDir::new().unwrap();
        write(&dir, "app.py", "from mypkg.images import IMG");
        write(&dir, "mypkg/images.py", r#"IMG = Image(name="dotted")"#);

        let imgs = collect(&dir, "app.py");
        assert_eq!(imgs.len(), 1);
        assert_eq!(imgs[0].name, "dotted");
    }

    #[test]
    fn test_installed_package_skipped() {
        let dir = TempDir::new().unwrap();
        // `PIL` is not in app_dir → no PIL.py or PIL/__init__.py → skipped.
        write(
            &dir,
            "app.py",
            r#"
from PIL import Image as PILImage
MY_IMAGE = Image(name="real")
"#,
        );

        let imgs = collect(&dir, "app.py");
        assert_eq!(imgs.len(), 1);
        assert_eq!(imgs[0].name, "real");
    }

    // ── Package with __init__.py ──────────────────────────────────────────────

    #[test]
    fn test_package_init_py() {
        let dir = TempDir::new().unwrap();
        write(&dir, "app.py", "from mypackage import MY_IMAGE");
        write(
            &dir,
            "mypackage/__init__.py",
            r#"MY_IMAGE = Image(name="pkg-image")"#,
        );

        let imgs = collect(&dir, "app.py");
        assert_eq!(imgs.len(), 1);
        assert_eq!(imgs[0].name, "pkg-image");
    }

    #[test]
    fn test_package_submodule() {
        let dir = TempDir::new().unwrap();
        write(&dir, "app.py", "from mypackage.images import IMG");
        write(&dir, "mypackage/__init__.py", "");
        write(
            &dir,
            "mypackage/images.py",
            r#"IMG = Image(name="sub-image")"#,
        );

        let imgs = collect(&dir, "app.py");
        assert_eq!(imgs.len(), 1);
        assert_eq!(imgs[0].name, "sub-image");
    }

    // ── Relative imports ──────────────────────────────────────────────────────

    #[test]
    fn test_relative_import_dot_module() {
        let dir = TempDir::new().unwrap();
        // subdir/app.py imports from subdir/images.py
        write(&dir, "subdir/app.py", "from . import images");
        write(
            &dir,
            "subdir/images.py",
            r#"IMAGE = Image(name="rel-image")"#,
        );

        let entry = dir.path().join("subdir/app.py");
        let imgs = collect_images(&entry, dir.path());
        assert_eq!(imgs.len(), 1);
        assert_eq!(imgs[0].name, "rel-image");
    }

    #[test]
    fn test_relative_import_from_dot_module() {
        let dir = TempDir::new().unwrap();
        write(&dir, "subdir/app.py", "from .images import IMAGE");
        write(&dir, "subdir/images.py", r#"IMAGE = Image(name="dotrel")"#);

        let entry = dir.path().join("subdir/app.py");
        let imgs = collect_images(&entry, dir.path());
        assert_eq!(imgs.len(), 1);
        assert_eq!(imgs[0].name, "dotrel");
    }

    #[test]
    fn test_relative_import_two_dots() {
        let dir = TempDir::new().unwrap();
        // deep/sub/app.py imports from deep/images.py via `from .. import images`
        write(&dir, "deep/sub/app.py", "from .. import images");
        write(&dir, "deep/images.py", r#"IMAGE = Image(name="twodot")"#);

        let entry = dir.path().join("deep/sub/app.py");
        let imgs = collect_images(&entry, dir.path());
        assert_eq!(imgs.len(), 1);
        assert_eq!(imgs[0].name, "twodot");
    }

    #[test]
    fn test_relative_import_two_dots_with_module() {
        let dir = TempDir::new().unwrap();
        // a/b/app.py: from ..images import IMG  →  a/images.py
        write(&dir, "a/b/app.py", "from ..images import IMG");
        write(&dir, "a/images.py", r#"IMG = Image(name="twodot-mod")"#);

        let entry = dir.path().join("a/b/app.py");
        let imgs = collect_images(&entry, dir.path());
        assert_eq!(imgs.len(), 1);
        assert_eq!(imgs[0].name, "twodot-mod");
    }

    // ── Deep import chains ────────────────────────────────────────────────────

    #[test]
    fn test_deep_import_chain() {
        let dir = TempDir::new().unwrap();
        write(&dir, "app.py", "from a import x");
        write(&dir, "a.py", "from b import y");
        write(&dir, "b.py", "from c import z");
        write(&dir, "c.py", r#"z = Image(name="deep")"#);

        let imgs = collect(&dir, "app.py");
        assert_eq!(imgs.len(), 1);
        assert_eq!(imgs[0].name, "deep");
    }

    // ── Circular import protection ────────────────────────────────────────────

    #[test]
    fn test_circular_imports_do_not_loop() {
        let dir = TempDir::new().unwrap();
        write(
            &dir,
            "app.py",
            r#"
from b import x
IMG = Image(name="circ-main")
"#,
        );
        write(
            &dir,
            "b.py",
            r#"
from app import IMG
x = 1
"#,
        );

        // Should terminate without infinite recursion.
        let imgs = collect(&dir, "app.py");
        // At least the image from app.py should be found.
        assert!(!imgs.is_empty());
    }

    #[test]
    fn test_self_import_does_not_loop() {
        let dir = TempDir::new().unwrap();
        write(
            &dir,
            "app.py",
            r#"
from app import IMG  # imports itself
IMG = Image(name="self-import")
"#,
        );

        let imgs = collect(&dir, "app.py");
        assert_eq!(imgs.len(), 1);
    }

    // ── Deduplication ────────────────────────────────────────────────────────

    #[test]
    fn test_same_image_in_two_imports_deduplicated() {
        let dir = TempDir::new().unwrap();
        // Both a.py and b.py import from images.py — images.py is visited once.
        write(
            &dir,
            "app.py",
            r#"
from a import x
from b import y
"#,
        );
        write(&dir, "a.py", "from images import IMG");
        write(&dir, "b.py", "from images import IMG");
        write(&dir, "images.py", r#"IMG = Image(name="shared")"#);

        let imgs = collect(&dir, "app.py");
        assert_eq!(imgs.len(), 1);
        assert_eq!(imgs[0].name, "shared");
    }

    // ── Operation extraction via import chain ─────────────────────────────────

    #[test]
    fn test_operations_preserved_across_import() {
        let dir = TempDir::new().unwrap();
        write(&dir, "app.py", "from images import IMG");
        write(
            &dir,
            "images.py",
            r#"
IMG = (
    Image(name="rich")
    .run("pip install rich")
    .env("TERM", "xterm")
)
"#,
        );

        let imgs = collect(&dir, "app.py");
        assert_eq!(imgs.len(), 1);
        let img = &imgs[0];
        assert_eq!(img.operations.len(), 2);
        assert_eq!(img.operations[0].op_type, "RUN");
        assert_eq!(img.operations[1].op_type, "ENV");
    }

    // ── Missing entry file ────────────────────────────────────────────────────

    #[test]
    fn test_missing_entry_file_returns_empty() {
        let dir = TempDir::new().unwrap();
        let imgs = collect_images(&dir.path().join("nonexistent.py"), dir.path());
        assert!(imgs.is_empty());
    }
}
