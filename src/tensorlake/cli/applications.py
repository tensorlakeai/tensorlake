from datetime import datetime, timezone

import click
from rich.console import Console
from rich.table import Table

from tensorlake.cli._common import Context, require_auth_and_project


@click.command()
@require_auth_and_project
def ls(ctx: Context):
    """
    List all applications in the current project.
    """
    applications = ctx.api_client.applications()

    # Filter out tombstoned applications
    active_applications = [app for app in applications if not app.tombstoned]

    if len(active_applications) == 0:
        click.echo("No applications found")
        return

    table = Table()

    table.add_column("Name", no_wrap=True)
    table.add_column("Description")
    table.add_column("Deployed At", style="green")

    for i, app in enumerate(active_applications):
        # Format the created_at timestamp
        deployed_at = ""
        if app.created_at:
            # created_at is Unix timestamp in milliseconds, convert to seconds
            dt = datetime.fromtimestamp(app.created_at / 1000, tz=timezone.utc)
            # Convert to local time
            local_dt = dt.astimezone()
            deployed_at = local_dt.strftime("%Y-%m-%d %H:%M:%S")

        table.add_row(
            app.name,
            app.description or "",
            deployed_at,
        )

        # Add spacing between rows (but not after the last row)
        if i < len(active_applications) - 1:
            table.add_row("", "", "")

    console = Console()
    console.print(table)

    if len(active_applications) == 1:
        click.echo("1 application")
    else:
        click.echo(f"{len(active_applications)} applications")

    # Show link to applications page
    applications_url = f"{ctx.cloud_url}/organizations/{ctx.organization_id}/projects/{ctx.project_id}/applications"
    click.echo(f"\nView all applications: {applications_url}")
