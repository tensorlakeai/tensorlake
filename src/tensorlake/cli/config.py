import json
import os
from pathlib import Path
from typing import Any, Dict

import click
import httpx

from tensorlake.cli._common import Context, pass_auth
from tensorlake.cli._configuration import (
    load_config,
    load_credentials,
    save_config,
    set_nested_value,
)


@click.group()
def config():
    """Manage tensorlake configuration."""
    pass


@config.command()
@pass_auth
def init(ctx: Context):
    """Initialize the configuration."""
    personal_access_token = load_credentials(ctx.base_url)
    if not personal_access_token:
        click.echo("No valid credentials found. Please run 'tensorlake login' first.", err=True)
        return

    organizations_response = httpx.get(
        f"{ctx.base_url}/platform/v1/organizations",
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
        f"{ctx.base_url}/platform/v1/organizations/{organization_id}/projects",
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
