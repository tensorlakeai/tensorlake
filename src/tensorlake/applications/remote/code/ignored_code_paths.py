import os
import subprocess
from pathlib import Path
from typing import Optional, Set

import click


def ignored_code_paths(root_dir: str) -> Set[str]:
    """Returns a set of absolute paths to be ignored when loading or zipping application code.

    All returned paths use os.path.abspath (not Path.resolve) to stay consistent
    with walk_code() which compares paths using os.path.join / os.path.abspath.
    Using Path.resolve() would follow symlinks and produce different strings on
    macOS where /var -> /private/var, causing exclusion checks to silently fail.
    """
    root = Path(os.path.abspath(root_dir))
    exclude_paths = set()

    # Exclude the active virtualenv if it's inside the root directory.
    venv_path = os.environ.get("VIRTUAL_ENV")
    if venv_path:
        venv_path = Path(os.path.abspath(venv_path))
        try:
            venv_path.relative_to(root)
            exclude_paths.add(str(venv_path))
            click.echo(f"skipping active virtualenv: {venv_path}")
        except ValueError:
            # venv is not inside root_dir, ignore
            pass

    # Exclude any other virtualenvs inside the root directory by looking for
    # pyvenv.cfg — the standard marker file present in every Python venv.
    for child in root.iterdir():
        if child.is_dir() and (child / "pyvenv.cfg").exists():
            abs_child = str(Path(os.path.abspath(child)))
            if abs_child not in exclude_paths:
                exclude_paths.add(abs_child)
                click.echo(f"skipping detected virtualenv: {abs_child}")

    gitignore_path = root / ".gitignore"
    if gitignore_path.exists():
        click.echo("detected .gitignore, trying to resolve ignored paths using git")
        git_ignored = _git_ignored_paths(root)
        if git_ignored is not None:
            exclude_paths.update(git_ignored)
        else:
            click.echo(
                "git is not available or this is not a git repository, "
                "falling back to manual .gitignore parsing"
            )
            exclude_paths.update(_parse_gitignore(root, gitignore_path))

    return exclude_paths


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
        is_dir = line.endswith("/")
        line = line.rstrip("/")
        abs_path = os.path.abspath(root / line)
        paths.add(abs_path)
        if is_dir:
            click.echo(f"skipping directory: {abs_path}")
        else:
            click.echo(f"skipping file: {abs_path}")
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
                    if match.is_dir():
                        click.echo(f"skipping directory: {abs_path}")
                    else:
                        click.echo(f"skipping file: {abs_path}")
        except (NotImplementedError, ValueError) as e:
            click.echo(
                f"skipping unsupported .gitignore pattern '{pattern}': {e}. "
                f"install git for full .gitignore support.",
                err=True,
            )

    return exclude_paths
