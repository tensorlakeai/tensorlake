import os
from pathlib import Path
from typing import Any, Dict, Optional

import click
import tomli
import tomli_w

CONFIG_FILE = Path.home() / ".tensorlake_config"


def load_config() -> Dict[str, Any]:
    """Load configuration from the TOML file."""
    if not CONFIG_FILE.exists():
        return {}

    with open(CONFIG_FILE, "rb") as f:
        return tomli.load(f)


def save_config(config: Dict[str, Any]) -> None:
    """Save configuration to the TOML file."""
    with open(CONFIG_FILE, "wb") as f:
        tomli_w.dump(config, f)

    # Set restrictive permissions (0600) to protect sensitive data like API keys
    os.chmod(CONFIG_FILE, 0o600)


def get_nested_value(config: Dict[str, Any], key: str) -> Any:
    """Get a nested configuration value using dot notation."""
    keys = key.split(".")
    value = config
    for k in keys:
        if isinstance(value, dict) and k in value:
            value = value[k]
        else:
            return None
    return value


def set_nested_value(config: Dict[str, Any], key: str, value: str) -> None:
    """Set a nested configuration value using dot notation."""
    keys = key.split(".")
    current = config

    for k in keys[:-1]:
        if k not in current:
            current[k] = {}
        current = current[k]

    current[keys[-1]] = value


@click.group()
def config():
    """Manage tensorlake configuration."""
    pass


@config.command()
@click.argument("key")
@click.argument("value")
def set(key: str, value: str):
    """Set a configuration value."""
    try:
        config_data = load_config()
        set_nested_value(config_data, key, value)
        save_config(config_data)
        click.echo(f"Set {key} = {value}")
    except Exception as e:
        click.echo(f"Error setting configuration: {e}", err=True)


@config.command()
@click.argument("key")
def get(key: str):
    """Get a configuration value."""
    try:
        config_data = load_config()
        value = get_nested_value(config_data, key)
        if value is None:
            click.echo(f"Configuration key '{key}' not found", err=True)
        else:
            click.echo(value)
    except Exception as e:
        click.echo(f"Error getting configuration: {e}", err=True)


@config.command()
def list():
    """List all configuration values."""
    try:
        config_data = load_config()
        if not config_data:
            click.echo("No configuration found")
            return

        def print_dict(d: Dict[str, Any], prefix: str = ""):
            for key, value in d.items():
                full_key = f"{prefix}.{key}" if prefix else key
                if isinstance(value, dict):
                    print_dict(value, full_key)
                else:
                    click.echo(f"{full_key} = {value}")

        print_dict(config_data)
    except Exception as e:
        click.echo(f"Error listing configuration: {e}", err=True)
