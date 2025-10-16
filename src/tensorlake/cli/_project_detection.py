"""Project root detection utilities for TensorLake CLI."""

from pathlib import Path
from typing import Optional

import click


def find_project_root(start_path: Optional[Path] = None) -> Path:
    """
    Find the project root directory by looking for common project markers.

    Priority order:
    1. Existing .tensorlake.toml in current or parent directories
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

    # Strategy 1: Look for existing .tensorlake.toml
    # This ensures we don't create multiple config files in different directories
    for parent in [current] + list(current.parents):
        if (parent / ".tensorlake.toml").exists():
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
    if (path / ".tensorlake.toml").exists():
        return "Found existing .tensorlake.toml"
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
    Check for multiple .tensorlake.toml files in the current path hierarchy.

    Args:
        project_root: The project root to check from

    Returns:
        List of paths to all .tensorlake.toml files found
    """
    current = Path.cwd().resolve()
    configs = []

    for parent in [current] + list(current.parents):
        config = parent / ".tensorlake.toml"
        if config.exists():
            configs.append(config)

    return configs


def warn_if_nested_configs(project_root: Path) -> None:
    """
    Warn if there are multiple .tensorlake.toml files in the hierarchy.

    Args:
        project_root: The project root being used
    """
    configs = check_for_nested_configs(project_root)

    if len(configs) > 1:
        click.echo("\nWarning: Multiple .tensorlake.toml files found:")
        for config in configs:
            marker = " (will be used)" if config.parent == project_root else ""
            click.echo(f"  - {config}{marker}")
