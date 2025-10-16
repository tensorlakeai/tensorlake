"""Project root detection utilities for TensorLake CLI."""

from pathlib import Path
from typing import Optional

import click


def find_project_root(start_path: Optional[Path] = None) -> Path:
    """
    Find the project root directory by looking for common project markers.

    Priority order:
    1. Existing .tensorlake/config.toml in current or parent directories
    2. .git directory (most common indicator)
    3. Python project files (pyproject.toml, setup.py, etc.)
    4. Fall back to current directory

    Args:
        start_path: Starting directory (defaults to current working directory)

    Returns:
        Path to the detected project root
    """
    if start_path is None:
        start_path = Path.cwd()

    current = start_path.resolve()

    # Strategy 1: Look for existing .tensorlake/config.toml
    # This ensures we don't create multiple config files in different directories
    for parent in [current] + list(current.parents):
        if (parent / ".tensorlake" / "config.toml").exists():
            return parent

    # Strategy 2: Look for .git directory (most reliable indicator of project root)
    for parent in [current] + list(current.parents):
        if (parent / ".git").is_dir():
            return parent

    # Strategy 3: Look for Python project markers
    python_markers = [
        "pyproject.toml",
        "setup.py",
        "setup.cfg",
        "requirements.txt",
    ]

    for parent in [current] + list(current.parents):
        for marker in python_markers:
            if (parent / marker).exists():
                return parent

    # Strategy 4: Fall back to current directory
    return current


def find_project_root_interactive(start_path: Optional[Path] = None) -> Path:
    """
    Find project root with user confirmation in interactive mode.

    Args:
        start_path: Starting directory (defaults to current working directory)

    Returns:
        Path to the confirmed project root
    """
    detected = find_project_root(start_path)

    # Show what was detected and why
    reason = get_detection_reason(detected)

    click.echo(f"Detected project root: {detected}")
    if reason:
        click.echo(f"Reason: {reason}")

    if click.confirm("Is this correct?", default=True):
        return detected

    # Allow user to specify different directory
    custom_path = click.prompt(
        "Enter project root directory",
        type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
        default=str(detected),
    )
    return Path(custom_path).resolve()


def get_detection_reason(path: Path) -> str:
    """
    Get human-readable reason for why this directory was chosen.

    Args:
        path: The detected project root path

    Returns:
        String explaining why this path was selected
    """
    if (path / ".tensorlake" / "config.toml").exists():
        return "Found existing .tensorlake/config.toml"
    if (path / ".git").is_dir():
        return "Found .git directory"
    if (path / "pyproject.toml").exists():
        return "Found pyproject.toml"
    if (path / "setup.py").exists():
        return "Found setup.py"
    if (path / "setup.cfg").exists():
        return "Found setup.cfg"
    if (path / "requirements.txt").exists():
        return "Found requirements.txt"
    return "Using current directory (no project markers found)"


def check_for_nested_configs(project_root: Path) -> list[Path]:
    """
    Check for multiple .tensorlake/config.toml files in the current path hierarchy.

    Args:
        project_root: The project root to check from

    Returns:
        List of paths to all .tensorlake/config.toml files found
    """
    current = Path.cwd().resolve()
    configs = []

    for parent in [current] + list(current.parents):
        config = parent / ".tensorlake" / "config.toml"
        if config.exists():
            configs.append(config)

    return configs


def warn_if_nested_configs(project_root: Path) -> None:
    """
    Warn if there are multiple .tensorlake/config.toml files in the hierarchy.

    Args:
        project_root: The project root being used
    """
    configs = check_for_nested_configs(project_root)

    if len(configs) > 1:
        click.echo("\nWarning: Multiple .tensorlake/config.toml files found:")
        for config in configs:
            marker = " (will be used)" if config.parent.parent == project_root else ""
            click.echo(f"  - {config}{marker}")


def find_gitignore_path(start_path: Optional[Path] = None) -> Optional[Path]:
    """
    Find the most relevant .gitignore file by locating the git repository root.

    Args:
        start_path: Starting directory (defaults to current working directory)

    Returns:
        Path to .gitignore at git root, or None if no git repo found
    """
    if start_path is None:
        start_path = Path.cwd()

    current = start_path.resolve()

    # Find the .git directory by traversing upward
    for parent in [current] + list(current.parents):
        if (parent / ".git").is_dir():
            # Found git root, return .gitignore path at this level
            return parent / ".gitignore"

    # No git repository found
    return None


def add_to_gitignore(gitignore_path: Path, entry: str) -> None:
    """
    Add an entry to a .gitignore file if it doesn't already exist.

    Creates the .gitignore file if it doesn't exist.

    Args:
        gitignore_path: Path to the .gitignore file
        entry: Entry to add (e.g., ".tensorlake/")
    """
    # Check if entry already exists
    if gitignore_path.exists():
        with open(gitignore_path, "r", encoding="utf-8") as f:
            content = f.read()
            lines = content.splitlines()

            # Check if entry already exists (exact match or as pattern)
            for line in lines:
                line = line.strip()
                if line == entry or line == f"/{entry}":
                    # Entry already exists, nothing to do
                    return

        # Entry doesn't exist, append it
        # Ensure file ends with newline before appending
        with open(gitignore_path, "a", encoding="utf-8") as f:
            if content and not content.endswith("\n"):
                f.write("\n")
            f.write(f"{entry}\n")
    else:
        # Create new .gitignore file with the entry
        with open(gitignore_path, "w", encoding="utf-8") as f:
            f.write(f"{entry}\n")
