import os
import subprocess
from pathlib import Path
from typing import Optional, Set

import click


def ignored_code_paths(root_dir: str) -> Set[str]:
    """Returns a set of absolute paths to be ignored when loading or zipping application code."""
    root = Path(root_dir).resolve()
    exclude_paths = set()

    # Exclude the active virtualenv if it's inside the root directory.
    venv_path = os.environ.get("VIRTUAL_ENV")
    if venv_path:
        venv_path = Path(venv_path).resolve()
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
            resolved = str(child.resolve())
            if resolved not in exclude_paths:
                exclude_paths.add(resolved)
                click.echo(f"skipping detected virtualenv: {resolved}")

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
        resolved = str((root / line).resolve())
        paths.add(resolved)
        if is_dir:
            click.echo(f"skipping directory: {resolved}")
        else:
            click.echo(f"skipping file: {resolved}")
    return paths


def _parse_gitignore(root: Path, gitignore_path: Path) -> Set[str]:
    """Fallback .gitignore parser when git is not available."""
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

        # Strip leading '/' — in .gitignore it anchors the pattern to the
        # repo root, which is already what root.glob() does.
        pattern = pattern.lstrip("/")

        if not pattern:
            continue

        try:
            if pattern.endswith("/"):
                pattern = pattern.rstrip("/")
                for match in root.glob(pattern):
                    if match.is_dir():
                        resolved = str(match.resolve())
                        exclude_paths.add(resolved)
                        click.echo(f"skipping directory: {resolved}")
            else:
                for match in root.glob(pattern):
                    if match.exists():
                        resolved = str(match.resolve())
                        exclude_paths.add(resolved)
                        if match.is_dir():
                            click.echo(f"skipping directory: {resolved}")
                        else:
                            click.echo(f"skipping file: {resolved}")
        except (NotImplementedError, ValueError) as e:
            click.echo(
                f"skipping unsupported .gitignore pattern '{pattern}': {e}. "
                f"install git for full .gitignore support.",
                err=True,
            )

    return exclude_paths
