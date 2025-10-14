import click
import httpx

from tensorlake.cli._common import Context, pass_auth
from tensorlake.cli._configuration import (
    load_credentials,
    save_local_config,
    set_nested_value,
)


@click.command()
@pass_auth
def init(ctx: Context):
    """Initialize TensorLake configuration for this project."""
    personal_access_token = load_credentials(ctx.base_url)
    if not personal_access_token:
        click.echo(
            "No valid credentials found. Please run 'tensorlake login' first.", err=True
        )
        raise click.Abort()

    click.echo("Initializing TensorLake configuration...\n")

    # Step 1: Fetch and select organization
    organizations_response = httpx.get(
        f"{ctx.base_url}/platform/v1/organizations",
        headers={"Authorization": f"Bearer {personal_access_token}"},
    )

    if organizations_response.status_code != 200:
        click.echo(
            f"Failed to fetch organizations: {organizations_response.text}", err=True
        )
        raise click.Abort()

    organizations_page = organizations_response.json()
    if not organizations_page.get("items"):
        click.echo("No organizations found for the provided token.", err=True)
        raise click.Abort()

    organizations = organizations_page["items"]
    if len(organizations) == 1:
        organization = organizations[0]
        organization_id = organization["id"]
        click.echo(f"Found organization: {organization['name']} ({organization_id})")
    else:
        click.echo("Multiple organizations found:")
        for idx, org in enumerate(organizations, 1):
            click.echo(f"  {idx}. {org['name']} (ID: {org['id']})")

        choice = click.prompt(
            "\nSelect an organization by number",
            type=click.IntRange(1, len(organizations)),
        )
        organization = organizations[choice - 1]
        organization_id = organization["id"]
        click.echo(f"Selected: {organization['name']}")

    # Step 2: Fetch and select project
    click.echo()
    projects_response = httpx.get(
        f"{ctx.base_url}/platform/v1/organizations/{organization_id}/projects",
        headers={"Authorization": f"Bearer {personal_access_token}"},
    )

    if projects_response.status_code != 200:
        click.echo(f"Failed to fetch projects: {projects_response.text}", err=True)
        raise click.Abort()

    projects_page = projects_response.json()
    if not projects_page.get("items"):
        click.echo("No projects found in the selected organization.", err=True)
        raise click.Abort()

    projects = projects_page["items"]
    if len(projects) == 1:
        project = projects[0]
        project_id = project["id"]
        click.echo(f"Found project: {project['name']} ({project_id})")
    else:
        click.echo("Multiple projects found:")
        for idx, proj in enumerate(projects, 1):
            click.echo(f"  {idx}. {proj['name']} (ID: {proj['id']})")

        choice = click.prompt(
            "\nSelect a project by number", type=click.IntRange(1, len(projects))
        )
        project = projects[choice - 1]
        project_id = project["id"]
        click.echo(f"Selected: {project['name']}")

    # Step 3: Save to local .tensorlake.toml
    click.echo()
    config_data = {
        "organization": organization_id,
        "project": project_id,
    }
    save_local_config(config_data)

    click.echo("Configuration saved to .tensorlake.toml")
    click.echo(
        "\nYou can now use TensorLake commands in this project without specifying --organization and --project flags."
    )
