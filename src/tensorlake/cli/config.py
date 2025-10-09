import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import click
import httpx

CONFIG_DIR = Path.home() / ".config" / "tensorlake"
CONFIG_FILE = CONFIG_DIR / ".tensorlake_config"
CREDENTIALS_PATH = CONFIG_DIR / "credentials.json"


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


def load_credentials_token() -> str | None:
    """
    Load the personal access token from the credentials file if it exists and is valid.
    """
    try:
        with open(CREDENTIALS_PATH, "r", encoding="utf-8") as f:
            credentials = json.load(f)

            if "token" not in credentials:
                return None

            return credentials.get("token")
    except (FileNotFoundError, json.JSONDecodeError):
        return None


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
def init():
    """Initialize the configuration."""
    personal_access_token = load_credentials_token()
    if not personal_access_token:
        click.echo("No valid credentials found. Please log in first.", err=True)
        return

    organizations_response = httpx.get(
        "https://api.tensorlake.ai/platform/v1/organizations",
        headers={"Authorization": f"Bearer {personal_access_token}"},
    )

    if organizations_response.status_code != 200:
        click.echo(
            f"Failed to fetch organizations: {organizations_response.text}", err=True
        )
        return

    organizations_page = organizations_response.json()
    if not organizations_page.get("items"):
        click.echo("No organizations found for the provided token.", err=True)
        return

    organizations = organizations_page["items"]
    if len(organizations) == 1:
        organization = organizations[0]
        organization_id = organization["id"]
        click.echo(
            f"Only one organization found. Using organization ID: {organization_id}"
        )
    else:
        click.echo("Multiple organizations found:")
        for idx, org in enumerate(organizations, 1):
            click.echo(f"{idx}. {org['name']} (ID: {org['id']})")

        choice = click.prompt(
            "Select an organization by number",
            type=click.IntRange(1, len(organizations)),
        )
        organization_id = organizations[choice - 1]["id"]

    projects_response = httpx.get(
        f"https://api.tensorlake.ai/platform/v1/organizations/{organization_id}/projects",
        headers={"Authorization": f"Bearer {personal_access_token}"},
    )
    if projects_response.status_code != 200:
        click.echo(f"Failed to fetch projects: {projects_response.text}", err=True)
        return
    projects_page = projects_response.json()
    if not projects_page.get("items"):
        click.echo("No projects found in the selected organization.", err=True)
        return

    projects = projects_page["items"]
    if len(projects) == 1:
        project = projects[0]
        project_id = project["id"]
        click.echo(f"Only one project found. Using project ID: {project_id}")
    else:
        click.echo("Multiple projects found:")
        for idx, proj in enumerate(projects, 1):
            click.echo(f"{idx}. {proj['name']} (ID: {proj['id']})")

        choice = click.prompt(
            "Select a project by number", type=click.IntRange(1, len(projects))
        )
        project_id = projects[choice - 1]["id"]

    config_data = load_config()
    set_nested_value(config_data, "default.organization", organization_id)
    set_nested_value(config_data, "default.project", project_id)
    save_config(config_data)
    click.echo("Configuration initialized successfully.")


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
