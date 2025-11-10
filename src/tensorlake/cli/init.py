from pathlib import Path
from typing import Optional, Tuple

import click
import httpx

from tensorlake.cli._common import Context, pass_auth
from tensorlake.cli._configuration import (
    load_credentials,
    load_local_config,
    save_local_config,
)


def run_init_flow(
    ctx: Context,
    interactive: bool = True,
    create_local_config: bool = True,
    skip_if_provided: bool = True,
    project_root: Path = Path.cwd(),
) -> Tuple[str, str]:
    """
    Run the init flow to select organization and project.

    Args:
        ctx: Context object with authentication and configuration
        interactive: If True, display messages and prompts to user
        create_local_config: If True, save selections to .tensorlake/config.toml
        skip_if_provided: If True, skip if org/project already in ctx (from CLI/env)
        project_root: Project root directory where .tensorlake/ will be created

    Returns:
        Tuple of (organization_id, project_id)

    Raises:
        click.Abort: If authentication missing or API calls fail
    """
    # Check if we should skip (values already provided via CLI/env)
    if skip_if_provided and ctx.has_org_and_project():
        if interactive:
            click.echo("Organization and project already configured.")
        return ctx.organization_id, ctx.project_id

    # Check if local config already exists
    local_config = load_local_config()
    if local_config.get("organization") and local_config.get("project"):
        if interactive:
            click.echo("Local configuration already exists in .tensorlake/config.toml")
        return local_config["organization"], local_config["project"]

    personal_access_token = load_credentials(ctx.api_url)
    if not personal_access_token:
        if interactive:
            click.echo(
                "No valid credentials found. Please run 'tensorlake login' first.",
                err=True,
            )
        raise click.Abort()

    if interactive:
        click.echo("Initializing TensorLake configuration...\n")

    # Step 1: Fetch and select organization
    organizations_response = httpx.get(
        f"{ctx.api_url}/platform/v1/organizations",
        headers={"Authorization": f"Bearer {personal_access_token}"},
    )

    if organizations_response.status_code != 200:
        if interactive:
            click.echo(
                f"Failed to fetch organizations: {organizations_response.text}",
                err=True,
            )
        raise click.Abort()

    organizations_page = organizations_response.json()
    if not organizations_page.get("items"):
        if interactive:
            click.echo("No organizations found for the provided token.", err=True)
        raise click.Abort()

    organizations = organizations_page["items"]
    if len(organizations) == 1:
        organization = organizations[0]
        organization_id = organization["id"]
        if interactive:
            click.echo(
                f"Found organization: {organization['name']} ({organization_id})"
            )
    else:
        if interactive:
            click.echo("Multiple organizations found:")
            for idx, org in enumerate(organizations, 1):
                click.echo(f"  {idx}. {org['name']} (ID: {org['id']})")

        choice = click.prompt(
            "\nSelect an organization by number",
            type=click.IntRange(1, len(organizations)),
        )
        organization = organizations[choice - 1]
        organization_id = organization["id"]
        if interactive:
            click.echo(f"Selected: {organization['name']}")

    # Step 2: Fetch and select project
    if interactive:
        click.echo()
    projects_response = httpx.get(
        f"{ctx.api_url}/platform/v1/organizations/{organization_id}/projects",
        headers={"Authorization": f"Bearer {personal_access_token}"},
    )

    if projects_response.status_code != 200:
        if interactive:
            click.echo(f"Failed to fetch projects: {projects_response.text}", err=True)
        raise click.Abort()

    projects_page = projects_response.json()
    if not projects_page.get("items"):
        if interactive:
            click.echo("No projects found in the selected organization.", err=True)
        raise click.Abort()

    projects = projects_page["items"]
    if len(projects) == 1:
        project = projects[0]
        project_id = project["id"]
        if interactive:
            click.echo(f"Found project: {project['name']} ({project_id})")
    else:
        if interactive:
            click.echo("Multiple projects found:")
            for idx, proj in enumerate(projects, 1):
                click.echo(f"  {idx}. {proj['name']} (ID: {proj['id']})")

        choice = click.prompt(
            "\nSelect a project by number", type=click.IntRange(1, len(projects))
        )
        project = projects[choice - 1]
        project_id = project["id"]
        if interactive:
            click.echo(f"Selected: {project['name']}")

    # Step 3: Save to local .tensorlake/config.toml (if requested)
    if create_local_config:
        if interactive:
            click.echo()

        config_data = {
            "organization": organization_id,
            "project": project_id,
        }
        save_local_config(config_data, project_root)

        if interactive:
            config_path = project_root / ".tensorlake" / "config.toml"
            click.echo(f"Configuration saved to {config_path}")
            click.echo(
                "\nYou can now use TensorLake commands in this project without specifying --organization and --project flags."
            )

    return organization_id, project_id


@click.command()
@click.option(
    "--directory",
    "-d",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
    help="Project directory (default: auto-detect from current directory)",
)
@click.option(
    "--no-confirm",
    is_flag=True,
    help="Skip confirmation of detected project directory",
)
@pass_auth
def init(ctx: Context, directory: Optional[Path], no_confirm: bool):
    """Initialize TensorLake configuration for this project."""
    from tensorlake.cli._project_detection import (
        find_project_root,
        find_project_root_interactive,
        get_detection_reason,
    )

    # Determine project root
    if directory:
        # User explicitly specified directory
        project_root = directory.resolve()
        click.echo(f"Using specified directory: {project_root}")
    else:
        # Auto-detect project root
        if no_confirm:
            project_root = find_project_root()
            reason = get_detection_reason(project_root)
            click.echo(f"Using project root: {project_root} ({reason})")
        else:
            project_root = find_project_root_interactive()

    run_init_flow(
        ctx,
        interactive=True,
        create_local_config=True,
        skip_if_provided=False,
        project_root=project_root,
    )
