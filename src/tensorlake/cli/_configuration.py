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
