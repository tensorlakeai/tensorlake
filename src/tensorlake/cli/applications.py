import json
from typing import List

import click
from pydantic.json import pydantic_encoder
from rich import print, print_json
from rich.table import Table

from tensorlake.applications.remote.manifests.application import ApplicationManifest
from tensorlake.applications.remote.manifests.function import FunctionManifest
from tensorlake.cli._common import Context, LogFormat, pass_auth, print_application_logs


@click.group()
def application():
    """
    Serverless Application Management
    """
    pass


@application.command()
@click.option(
    "--verbose", "-v", is_flag=True, help="Include all application information"
)
@click.option(
    "--json",
    "use_json",
    "-j",
    is_flag=True,
    help="Export all application information as JSON-encoded data",
)
@pass_auth
def list(ctx: Context, verbose: bool, use_json: bool):
    """
    List remote applications
    """
    if verbose and use_json:
        raise click.UsageError("--verbose and --json are incompatible")

    applications: List[ApplicationManifest] = ctx.api_client.applications()

    if use_json:
        all_graphs = json.dumps(applications, default=pydantic_encoder)
        print_json(all_graphs)

    elif verbose:
        print(applications)

    else:
        table = Table(title="Applications")
        table.add_column("Name")
        table.add_column("Description")

        for application in applications:
            application: ApplicationManifest
            table.add_row(application.name, application.description)

        print(table)


@application.command(
    epilog="""
\b
Use 'tensorlake config set default.application <name>' to set a default application name.
"""
)
@click.option(
    "--json",
    "-j",
    is_flag=True,
    help="Export application information as JSON-encoded data",
)
@click.argument("application-name", required=False)
@pass_auth
def info(ctx: Context, json: bool, application_name: str):
    """
    Info about a remote application
    """
    if not application_name:
        if ctx.default_application:
            application_name = ctx.default_application
            click.echo(f"Using default application from config: {application_name}")
        else:
            raise click.UsageError(
                "No application name provided and no default.application configured"
            )

    app: ApplicationManifest = ctx.api_client.application(application_name)

    if json:
        print_json(app.model_dump_json())
        return

    print(f"[bold][red]Application:[/red][/bold] {app.name}")
    if app.description:
        print(f"[bold][red]Description:[/red][/bold] {app.description}")
    print(f"[bold][red]Version:[/red][/bold] {app.version}")
    if app.tags:
        print(f"[bold][red]Tags:[/red][/bold] {app.tags}")

    table = Table(title="Functions")
    table.add_column("Name")
    table.add_column("Description")
    table.add_column("is_api")

    for function in app.functions.values():
        function: FunctionManifest
        table.add_row(function.name, function.description, str(function.is_api))

    print(table)


@application.command(
    epilog="""
\b
Use 'tensorlake config set default.application <name>' to set a default application name.
"""
)
@click.option("--json", "-j", is_flag=True, help="Format output as JSON")
@click.option(
    "--function",
    "-f",
    default=None,
    help="Name of the function to filter logs by",
)
@click.option(
    "--request",
    "-r",
    default=None,
    help="Request ID to filter logs by",
)
@click.option(
    "--container",
    "-c",
    default=None,
    help="Container ID to filter logs by",
)
@click.option(
    "--format",
    "-F",
    default="compact",
    help="Format of the logs",
    type=click.Choice(["compact", "expanded", "long", "json"]),
)
@click.argument("application-name", required=False)
@pass_auth
def logs(
    ctx: Context,
    json: bool,
    application_name: str,
    function: str | None,
    request: str | None,
    container: str | None,
    format: str,
):
    """
    View logs for a remote application
    """
    if not application_name:
        if ctx.default_application:
            application_name = ctx.default_application
            click.echo(f"Using default application from config: {application_name}")
        else:
            raise click.UsageError(
                "No application name provided and no default.application configured"
            )

    logs = ctx.api_client.application_logs(
        application_name, function, request, container
    )

    print_application_logs(logs, LogFormat(format))
