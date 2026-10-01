import os
import subprocess
from pathlib import Path
from typing import List, Optional, Set

from ...interface import SDKUsageError

# Marker file present in the root of every Python virtualenv.
_VENV_MARKER_FILE = "pyvenv.cfg"


def ignored_code_paths(root_dir: str) -> Set[str]:
    """Returns a set of absolute paths to be ignored when loading or zipping application code.

    All returned paths use os.path.abspath (not Path.resolve) to stay consistent
    with walk_code() which compares paths using os.path.join / os.path.abspath.
    Using Path.resolve() would follow symlinks and produce different strings on
    macOS where /var -> /private/var, causing exclusion checks to silently fail.
    """
    root = Path(os.path.abspath(root_dir))
    _raise_if_virtualenv_root(root)

    exclude_paths = set()

    gitignore_path = root / ".gitignore"
    if gitignore_path.exists():
        git_ignored = _git_ignored_paths(root)
        if git_ignored is not None:
            exclude_paths.update(git_ignored)
        else:
            exclude_paths.update(_parse_gitignore(root, gitignore_path))

    # Computed after the gitignored paths so the scan can skip descending into them.
    exclude_paths.update(_virtualenv_paths(root, exclude_paths))

    return exclude_paths


def _is_inside(path: Path, root: Path) -> bool:
    """Returns True if path is root itself or below it."""
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _raise_if_virtualenv_root(root: Path) -> None:
    """Rejects deploying an application file that sits in the root of a virtualenv.

    The directory holding the application file is what gets deployed as the application
    code. When that directory is a virtualenv there is no way to ship the application
    without shipping the virtualenv around it, so the deploy has to fail here rather than
    succeed and leave the application failing to load in the Function Executor.
    """
    if not (root / _VENV_MARKER_FILE).is_file():
        return

    raise SDKUsageError(
        f"The application file is in `{root}`, which is the root directory of a Python "
        "virtualenv. The directory that holds the application file is deployed as the "
        "application code, so deploying from here would ship the virtualenv instead of the "
        "application and the application would fail to load. Please move the application "
        "file into its own directory outside of the virtualenv and deploy it from there."
    )


def _virtualenv_paths(root: Path, already_excluded: Set[str]) -> Set[str]:
    """Returns absolute paths of all virtualenvs inside the root directory, at any depth.

    Virtualenvs are identified by the pyvenv.cfg marker file that every Python venv has.
    A virtualenv must never end up in the application code ZIP: it is large, its contents
    are platform specific, and its modules shadow the application modules once the ZIP is
    unpacked.

    Symlinks are not followed, so a virtualenv reachable only through a symlink is found
    only when it is the active one.
    """
    venv_paths: Set[str] = set()

    # The active virtualenv is checked explicitly because it can be reached through a
    # symlink that the walk below does not follow. Excluding the root itself is
    # meaningless because walk_code only compares the paths under the root, so a
    # virtualenv that is the root is rejected by _raise_if_virtualenv_root() instead.
    active_venv: Optional[str] = os.environ.get("VIRTUAL_ENV")
    if active_venv:
        active_venv_path = Path(os.path.abspath(active_venv))
        if active_venv_path != root and _is_inside(active_venv_path, root):
            venv_paths.add(str(active_venv_path))

    for dir_path, dir_names, _ in os.walk(root):
        kept: List[str] = []
        for dir_name in dir_names:
            child: str = os.path.abspath(os.path.join(dir_path, dir_name))
            # Don't descend into directories that are already excluded or already known
            # to be virtualenvs.
            if child in already_excluded or child in venv_paths:
                continue
            if os.path.isfile(os.path.join(child, _VENV_MARKER_FILE)):
                venv_paths.add(child)
                continue
            kept.append(dir_name)
        dir_names[:] = kept

    return venv_paths


def _git_ignored_paths(root: Path) -> Optional[Set[str]]:
    """Use git to find ignored paths. Returns None if git is not available or the directory is not a git repo."""
    try:
        result = subprocess.run(
            [
                "git",
                "ls-files",
                "--ignored",
                "--exclude-standard",
                "--others",
                "--directory",
            ],
            cwd=str(root),
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        # git not installed or timed out
        return None

    if result.returncode != 0:
        # Not a git repo or other git error
        return None

    paths = set()
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        # git outputs paths relative to cwd, with optional trailing '/' for dirs
        line = line.rstrip("/")
        abs_path = os.path.abspath(root / line)
        paths.add(abs_path)
    return paths


def _parse_gitignore(root: Path, gitignore_path: Path) -> Set[str]:
    """Fallback .gitignore parser when git is not available.

    Handles the key .gitignore semantics:
    - Leading '/' anchors to root (stripped since root.glob is already rooted)
    - Trailing '/' means directory-only match
    - Patterns without '/' match at any depth (prepend '**/')
    - '!' negation patterns are skipped (unsupported)
    - '#' lines are comments, blank lines are ignored
    """
    exclude_paths = set()
    patterns = []
    with gitignore_path.open("r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            patterns.append(line)

    for pattern in patterns:
        # Skip negation patterns (unsupported by Path.glob)
        if pattern.startswith("!"):
            continue

        # Track whether the original pattern was anchored to root (had leading '/')
        anchored = pattern.startswith("/")

        # Strip leading '/' — in .gitignore it anchors the pattern to the
        # repo root, which is already what root.glob() does.
        pattern = pattern.lstrip("/")

        if not pattern:
            continue

        # Determine if this is a directory-only pattern (trailing '/')
        dir_only = pattern.endswith("/")
        pattern = pattern.rstrip("/")

        # In .gitignore, patterns without a '/' match at any depth.
        # Anchored patterns (had leading '/') or patterns containing '/'
        # only match relative to root. Prepend '**/' for unanchored,
        # slash-free patterns so Path.glob searches recursively.
        if not anchored and "/" not in pattern:
            glob_pattern = f"**/{pattern}"
        else:
            glob_pattern = pattern

        try:
            for match in root.glob(glob_pattern):
                if dir_only and not match.is_dir():
                    continue
                if match.exists():
                    abs_path = os.path.abspath(match)
                    exclude_paths.add(abs_path)
        except (NotImplementedError, ValueError):
            # Ignore unrecognized or unsupported patterns
            continue

    return exclude_paths
