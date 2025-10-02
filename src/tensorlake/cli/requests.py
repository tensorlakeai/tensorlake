import json
from typing import List

import click
from pydantic.json import pydantic_encoder
from rich import print, print_json
from rich.table import Table

from tensorlake.applications.remote.api_client import FunctionRun, RequestMetadata
from tensorlake.cli._common import (
    Context,
    LogFormat,
    pass_auth,
    print_application_logs,
)


@click.group()
def request():
    """
    Serverless Graph Management
    """
    pass


@request.command(
    epilog="""
\b
Use 'tensorlake config set default.application <name>' to set a default application name.
"""
)
@pass_auth
@click.option(
    "--verbose", "-v", is_flag=True, help="Include all application information"
)
@click.option(
    "--json",
    "use_json",
    "-j",
    is_flag=True,
    help="Export request information as JSON-encoded data",
)
@click.argument("application-name", required=False)
def list(ctx: Context, verbose: bool, use_json: bool, application_name: str):
    """
    List remote invocations
    """
    if verbose and use_json:
        raise click.UsageError("--verbose and --json are incompatible")

    if not application_name:
        if ctx.default_application:
            application_name = ctx.default_application
            click.echo(f"Using default application from config: {application_name}")
        else:
            raise click.UsageError(
                "No application name provided and no default.application configured"
            )

    requests: List[RequestMetadata] = ctx.api_client.requests(application_name)

    if use_json:
        all_requests = json.dumps(requests, default=pydantic_encoder)
        print_json(all_requests)
        return

    if verbose:
        print(requests)
        return

    table = Table(title="Requests")
    table.add_column("Request")
    table.add_column("Created At")
    table.add_column("Outcome")

    for request in requests:
        table.add_row(
            request.id,
            str(request.created_at),
            str(request.outcome),
        )

    print(table)


@request.command(
    epilog="""
\b
Arguments:
  tensorlake request info <request-id>              # Uses default application
  tensorlake request info <application-name> <request-id> # Explicit application name
\b
Use 'tensorlake config set default.application <name>' to set a default application name.
Use 'tensorlake config set default.request <id>' to set a default request ID.
"""
)
@click.option(
    "--json",
    "use_json",
    "-j",
    is_flag=True,
    help="Export request information as JSON-encoded data",
)
@click.argument("args", nargs=-1, required=False)
@click.option("--function-runs", "-t", is_flag=True, help="Show function runs")
@pass_auth
def info(
    ctx: Context,
    use_json: bool,
    function_runs: bool,
    args: tuple,
):
    """
    Info about a request
    """
    (application_name, request_id) = parse_args(ctx, args)

    request: RequestMetadata = ctx.api_client.request(application_name, request_id)

    if use_json:
        print_json(request.model_dump_json())
        return

    print(f"[bold][red]Request:[/red][/bold] {request.id}")
    print(f"[bold][red]Application Version:[/red][/bold] {request.application_version}")
    print(f"[bold][red]Created At:[/red][/bold] {request.created_at}")
    print(f"[bold][red]Outcome:[/red][/bold] {request.outcome}")
    if request.request_error:
        print(
            f"[bold][red]Error:[/red][/bold] {request.request_error.function_name}: {request.request_error.message}"
        )

    if function_runs:
        function_runs: List[FunctionRun] = ctx.api_client.function_runs(
            application_name, request_id
        )

        for function_run in function_runs:
            print(f"[bold][red]ID:[/red][/bold] {function_run.id}")
            print(f"[bold][red]Function:[/red][/bold] {function_run.function_name}")
            print(f"[bold][red]Status:[/red][/bold] {function_run.status}")
            print(f"[bold][red]Outcome:[/red][/bold] {function_run.outcome}")
            print(f"[bold][red]Created At:[/red][/bold] {function_run.created_at}")

            allocations_table = Table(title="[bold][red]Allocations[/red][/bold]")
            allocations_table.add_column("Allocation ID")
            allocations_table.add_column("Server ID")
            allocations_table.add_column("Container ID")
            allocations_table.add_column("Created At")
            allocations_table.add_column("Outcome")
            allocations_table.add_column("Attempt Number")
            for allocation in function_run.allocations:
                allocations_table.add_row(
                    allocation.id,
                    allocation.server_id,
                    allocation.container_id,
                    str(allocation.created_at),
                    allocation.outcome,
                    str(allocation.attempt_number),
                )

            print(allocations_table)


@request.command(
    epilog="""
\b
Arguments:
  tensorlake request info <request-id>              # Uses default application
  tensorlake request info <application-name> <request-id> # Explicit application name
\b
Use 'tensorlake config set default.application <name>' to set a default application name.
Use 'tensorlake config set default.request <id>' to set a default request ID.
"""
)
@click.option(
    "--format",
    "-F",
    default="compact",
    help="Format of the logs",
    type=click.Choice(["compact", "expanded", "long", "json"]),
)
@click.argument("args", nargs=-1, required=False)
@pass_auth
def logs(ctx: Context, format: str, args: tuple):
    """
    View logs for a remote request
    """
    (application_name, request) = parse_args(ctx, args)

    logs = ctx.api_client.application_logs(application_name, None, request, None)

    print_application_logs(logs, LogFormat(format))


def parse_args(ctx: Context, args: tuple) -> tuple[str, str]:
    """
    Parse arguments: if one arg provided, treat it as request_id
    If two args provided, treat them as application_name and request_id
    """
    if len(args) == 1:
        application_name = None
        request_id = args[0]
    elif len(args) == 2:
        application_name = args[0]
        request_id = args[1]
    else:
        application_name = None
        request_id = None

    if not application_name:
        if ctx.default_application:
            application_name = ctx.default_application
            click.echo(f"Using default application from config: {application_name}")
        else:
            raise click.UsageError(
                "No application name provided and no default.application configured"
            )

    if not request_id:
        if ctx.default_request:
            request_id = ctx.default_request
            click.echo(f"Using default request from config: {request_id}")
        else:
            raise click.UsageError(
                "No request ID provided and no default.request configured"
            )

    return (application_name, request_id)
