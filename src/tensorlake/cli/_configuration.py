import os
from pathlib import Path
from typing import Any

import click
from tomlkit import document, dumps, parse, table

from tensorlake.cli._project_detection import add_to_gitignore, find_gitignore_path

CONFIG_DIR = Path.home() / ".config" / "tensorlake"

CONFIG_FILE = CONFIG_DIR / ".tensorlake_config"

CREDENTIALS_PATH = CONFIG_DIR / "credentials.toml"

# Local project configuration file
LOCAL_CONFIG_FILE = Path.cwd() / ".tensorlake" / "config.toml"


def load_config() -> dict[str, Any]:
    """Load configuration from the TOML file."""
    if not CONFIG_FILE.exists():
        return {}

    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return parse(f.read())


def save_config(config: dict[str, Any]) -> None:
    """Save configuration to the TOML file."""
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        f.write(dumps(config))

    # Set restrictive permissions (0600) to protect sensitive data like API keys
    os.chmod(CONFIG_FILE, 0o600)


def load_local_config() -> dict[str, Any]:
    """
    Load configuration from the local project .tensorlake/config.toml file.
    Searches current directory and parent directories for .tensorlake/config.toml.

    If LOCAL_CONFIG_FILE is set (e.g., in tests), uses that path directly.
    Otherwise, searches upward from current directory.
    """
    # If LOCAL_CONFIG_FILE is set to a specific path (e.g., by tests), use it directly
    if LOCAL_CONFIG_FILE != Path.cwd() / ".tensorlake" / "config.toml":
        if LOCAL_CONFIG_FILE.exists():
            with open(LOCAL_CONFIG_FILE, "r", encoding="utf-8") as f:
                return parse(f.read())
        return {}

    # Normal operation: search upward from current directory
    current = Path.cwd()

    for parent in [current] + list(current.parents):
        config_file = parent / ".tensorlake" / "config.toml"
        if config_file.exists():
            with open(config_file, "r", encoding="utf-8") as f:
                return parse(f.read())

    return {}


def save_local_config(config: dict[str, Any], project_root: Path) -> None:
    """
    Save configuration to the local project .tensorlake/config.toml file.

    Args:
        config: Configuration dictionary to save
        project_root: Project root directory where .tensorlake/ directory will be created

    Raises:
        click.ClickException: If .tensorlake exists as a file (not a directory)
    """
    config_dir = project_root / ".tensorlake"
    config_path = config_dir / "config.toml"

    # Check if .tensorlake exists as a file (not a directory)
    if config_dir.exists() and not config_dir.is_dir():
        raise click.ClickException(
            f"Cannot create configuration directory: '{config_dir}' exists as a file.\n"
            f"Please rename or remove this file and try again."
        )

    # Create .tensorlake directory if it doesn't exist
    config_dir.mkdir(parents=True, exist_ok=True)

    # Write configuration file
    with open(config_path, "w", encoding="utf-8") as f:
        f.write(dumps(config))

    # Set restrictive permissions (0600) to protect sensitive data
    os.chmod(config_path, 0o600)

    # Add .tensorlake/ to .gitignore
    try:
        # Try to find .gitignore at git root
        gitignore_path = find_gitignore_path(project_root)

        # If no git repository found, create .gitignore next to .tensorlake/
        if gitignore_path is None:
            gitignore_path = project_root / ".gitignore"

        # Add the directory entry
        add_to_gitignore(gitignore_path, ".tensorlake/")
    except Exception:
        # Silently ignore any errors - this is a non-critical operation
        pass


def load_credentials(api_url: str) -> str | None:
    """
    Load the personal access token from the credentials file if it exists and is valid.
    """
    if CREDENTIALS_PATH.exists():
        try:
            with open(CREDENTIALS_PATH, "r", encoding="utf-8") as f:
                credentials = parse(f.read())

                scoped = credentials.get(api_url)
                if scoped is None:
                    return None

                return scoped.get("token")
        except Exception:
            return None

    return None


def save_credentials(api_url: str, token: str):
    """
    Save the personal access token in the credentials file.
    """
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)

    # Read existing credentials first if file exists
    if CREDENTIALS_PATH.exists():
        with open(CREDENTIALS_PATH, "r", encoding="utf-8") as f:
            config = parse(f.read())
    else:
        config = document()

    # Update config with new endpoint credentials
    section = table()
    section["token"] = token
    config[api_url] = section

    # Write updated config back to file
    with open(CREDENTIALS_PATH, "w", encoding="utf-8") as f:
        f.write(dumps(config))

    os.chmod(CREDENTIALS_PATH, 0o600)


def set_nested_value(config: dict[str, Any], key: str, value: str) -> None:
    """Set a nested configuration value using dot notation."""
    keys = key.split(".")
    current = config

    for k in keys[:-1]:
        if k not in current:
            current[k] = {}
        current = current[k]

    current[keys[-1]] = value


def get_nested_value(config: dict[str, Any], key: str) -> Any:
    """Get a nested configuration value using dot notation."""
    keys = key.split(".")
    value = config
    for k in keys:
        if isinstance(value, dict) and k in value:
            value = value[k]
        else:
            return None
    return value
