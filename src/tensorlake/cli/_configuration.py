import json
import os
from pathlib import Path
from typing import Any

from tomlkit import document, dumps, parse, table

CONFIG_DIR = Path.home() / ".config" / "tensorlake"

CONFIG_FILE = CONFIG_DIR / ".tensorlake_config"

CREDENTIALS_PATH = CONFIG_DIR / "credentials.toml"

# Legacy credentials file (pre-endpoint-scoping)
LEGACY_CREDENTIALS_PATH = CONFIG_DIR / "credentials.json"

# Local project configuration file
LOCAL_CONFIG_FILE = Path.cwd() / ".tensorlake.toml"


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
    Load configuration from the local project .tensorlake.toml file.
    Searches current directory and parent directories for .tensorlake.toml.

    If LOCAL_CONFIG_FILE is set (e.g., in tests), uses that path directly.
    Otherwise, searches upward from current directory.
    """
    # If LOCAL_CONFIG_FILE is set to a specific path (e.g., by tests), use it directly
    if LOCAL_CONFIG_FILE != Path.cwd() / ".tensorlake.toml":
        if LOCAL_CONFIG_FILE.exists():
            with open(LOCAL_CONFIG_FILE, "r", encoding="utf-8") as f:
                return parse(f.read())
        return {}

    # Normal operation: search upward from current directory
    current = Path.cwd()

    for parent in [current] + list(current.parents):
        config_file = parent / ".tensorlake.toml"
        if config_file.exists():
            with open(config_file, "r", encoding="utf-8") as f:
                return parse(f.read())

    return {}


def save_local_config(config: dict[str, Any], project_root: Path) -> None:
    """
    Save configuration to the local project .tensorlake.toml file.

    Args:
        config: Configuration dictionary to save
        project_root: Project root directory where .tensorlake.toml will be created
    """
    config_path = project_root / ".tensorlake.toml"

    with open(config_path, "w", encoding="utf-8") as f:
        f.write(dumps(config))

    # Set restrictive permissions (0600) to protect sensitive data
    os.chmod(config_path, 0o600)

    # Add .tensorlake.toml to .gitignore
    try:
        from tensorlake.cli._project_detection import (
            add_to_gitignore,
            find_gitignore_path,
        )

        # Try to find .gitignore at git root
        gitignore_path = find_gitignore_path(project_root)

        # If no git repository found, create .gitignore next to .tensorlake.toml
        if gitignore_path is None:
            gitignore_path = project_root / ".gitignore"

        # Add the entry
        add_to_gitignore(gitignore_path, ".tensorlake.toml")
    except Exception:
        # Silently ignore any errors - this is a non-critical operation
        pass


def load_credentials(base_url: str) -> str | None:
    """
    Load the personal access token from the credentials file if it exists and is valid.

    Performs one-time migration from legacy credentials.json to credentials.toml format.
    """
    # Check if new TOML format exists
    if CREDENTIALS_PATH.exists():
        try:
            with open(CREDENTIALS_PATH, "r", encoding="utf-8") as f:
                credentials = parse(f.read())

                scoped = credentials.get(base_url)
                if scoped is None:
                    return None

                return scoped.get("token")
        except Exception:
            return None

    # One-time migration: If old JSON format exists, migrate to new TOML format
    if LEGACY_CREDENTIALS_PATH.exists():
        try:
            with open(LEGACY_CREDENTIALS_PATH, "r", encoding="utf-8") as f:
                old_credentials = json.load(f)

            token = old_credentials.get("token")
            if token:
                # Migrate to new endpoint-scoped format
                save_credentials(base_url, token)

                # Delete the old credentials file
                LEGACY_CREDENTIALS_PATH.unlink()

                return token
        except Exception:
            # If migration fails, don't delete the old file
            pass

    return None


def save_credentials(base_url: str, token: str):
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
    config[base_url] = section

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
