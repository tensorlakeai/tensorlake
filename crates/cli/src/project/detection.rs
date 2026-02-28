use std::path::{Path, PathBuf};

/// Find the project root by looking for common markers.
/// Priority: .tensorlake/config.toml > .git > pyproject.toml/setup.py/etc > cwd
pub fn find_project_root(start_path: Option<&Path>) -> PathBuf {
    let current = start_path
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
    let current = current.canonicalize().unwrap_or(current);

    // Strategy 1: existing .tensorlake/config.toml
    if let Some(root) = search_upward(&current, |dir| {
        dir.join(".tensorlake").join("config.toml").exists()
    }) {
        return root;
    }

    // Strategy 2: .git directory
    if let Some(root) = search_upward(&current, |dir| dir.join(".git").is_dir()) {
        return root;
    }

    // Strategy 3: Python project markers
    let python_markers = ["pyproject.toml", "setup.py", "setup.cfg", "requirements.txt"];
    if let Some(root) = search_upward(&current, |dir| {
        python_markers.iter().any(|m| dir.join(m).exists())
    }) {
        return root;
    }

    // Strategy 4: fall back to current directory
    current
}

/// Get a human-readable reason for why this directory was chosen.
pub fn get_detection_reason(path: &Path) -> &'static str {
    if path.join(".tensorlake").join("config.toml").exists() {
        return "Found existing .tensorlake/config.toml";
    }
    if path.join(".git").is_dir() {
        return "Found .git directory";
    }
    if path.join("pyproject.toml").exists() {
        return "Found pyproject.toml";
    }
    if path.join("setup.py").exists() {
        return "Found setup.py";
    }
    if path.join("setup.cfg").exists() {
        return "Found setup.cfg";
    }
    if path.join("requirements.txt").exists() {
        return "Found requirements.txt";
    }
    "Using current directory (no project markers found)"
}

fn search_upward(start: &Path, predicate: impl Fn(&Path) -> bool) -> Option<PathBuf> {
    let mut dir = start;
    loop {
        if predicate(dir) {
            return Some(dir.to_path_buf());
        }
        match dir.parent() {
            Some(parent) => dir = parent,
            None => return None,
        }
    }
}
