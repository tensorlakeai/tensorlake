import os
from pathlib import Path
from typing import Any, Dict

import click

CONFIG_FILE = Path.home() / ".tensorlake_config"


def _parse_toml(content: str) -> Dict[str, Any]:
    """Simple TOML parser for basic key-value pairs and nested sections."""
    result = {}
    current_section = result
    section_stack = []

    for line_num, line in enumerate(content.splitlines(), 1):
        line = line.strip()

        # Skip empty lines and comments
        if not line or line.startswith("#"):
            continue

        # Handle section headers [section] or [section.subsection]
        if line.startswith("[") and line.endswith("]"):
            section_path = line[1:-1].strip()
            if not section_path:
                raise ValueError(f"Empty section name at line {line_num}")

            # Reset to root and navigate to section
            current_section = result
            section_stack = []

            for section_name in section_path.split("."):
                section_name = section_name.strip()
                if not section_name:
                    raise ValueError(
                        f"Empty section name in path '{section_path}' at line {line_num}"
                    )

                if section_name not in current_section:
                    current_section[section_name] = {}
                elif not isinstance(current_section[section_name], dict):
                    raise ValueError(
                        f"Section '{section_name}' conflicts with existing value at line {line_num}"
                    )

                current_section = current_section[section_name]
                section_stack.append(section_name)
            continue

        # Handle key-value pairs
        if "=" in line:
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()

            if not key:
                raise ValueError(f"Empty key at line {line_num}")

            # Parse value
            parsed_value = _parse_toml_value(value)
            current_section[key] = parsed_value
        else:
            raise ValueError(f"Invalid TOML syntax at line {line_num}: {line}")

    return result


def _parse_toml_value(value: str) -> Any:
    """Parse a TOML value string into appropriate Python type."""
    value = value.strip()

    # Handle quoted strings
    if (value.startswith('"') and value.endswith('"')) or (
        value.startswith("'") and value.endswith("'")
    ):
        return value[1:-1]

    # Handle booleans
    if value.lower() == "true":
        return True
    elif value.lower() == "false":
        return False

    # Handle integers
    try:
        if "." not in value:
            return int(value)
    except ValueError:
        pass

    # Handle floats
    try:
        return float(value)
    except ValueError:
        pass

    # Return as string if nothing else matches
    return value


def _serialize_toml(data: Dict[str, Any]) -> str:
    """Simple TOML serializer for basic key-value pairs and nested sections."""
    lines = []

    def _serialize_section(section_data: Dict[str, Any], section_path: str = ""):
        # First, write simple key-value pairs
        for key, value in section_data.items():
            if not isinstance(value, dict):
                lines.append(f"{key} = {_serialize_toml_value(value)}")

        # Then, write nested sections
        for key, value in section_data.items():
            if isinstance(value, dict):
                full_path = f"{section_path}.{key}" if section_path else key
                lines.append(f"\n[{full_path}]")
                _serialize_section(value, full_path)

    _serialize_section(data)
    return "\n".join(lines)


def _serialize_toml_value(value: Any) -> str:
    """Serialize a Python value to TOML format."""
    if isinstance(value, bool):
        return str(value).lower()
    elif isinstance(value, str):
        # Quote strings that contain special characters or spaces
        if any(c in value for c in [" ", "\t", "\n", '"', "'", "=", "#", "[", "]"]):
            return f'"{value}"'
        return value
    elif isinstance(value, (int, float)):
        return str(value)
    else:
        return f'"{str(value)}"'


def load_config() -> Dict[str, Any]:
    """Load configuration from the TOML file."""
    if not CONFIG_FILE.exists():
        return {}

    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        content = f.read()
        return _parse_toml(content)


def save_config(config: Dict[str, Any]) -> None:
    """Save configuration to the TOML file."""
    content = _serialize_toml(config)
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        f.write(content)

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
