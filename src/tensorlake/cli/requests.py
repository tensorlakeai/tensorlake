import json
from typing import List

import click
from pydantic.json import pydantic_encoder
from rich import print, print_json
from rich.table import Table

from tensorlake.cli._common import Context, pass_auth
from tensorlake.functions_sdk.http_client import RequestMetadata, Task


@click.group()
def request():
    """
    Serverless Graph Management
    """
    pass


@request.command(
    epilog="""
\b
Use 'tensorlake config set default.graph <name>' to set a default graph name.
"""
)
@pass_auth
@click.option("--verbose", "-v", is_flag=True, help="Include all graph information")
@click.option(
    "--json",
    "use_json",
    "-j",
    is_flag=True,
    help="Export invocation information as JSON-encoded data",
)
@click.argument("graph-name", required=False)
def list(ctx: Context, verbose: bool, use_json: bool, graph_name: str):
    """
    List remote invocations
    """
    if verbose and use_json:
        raise click.UsageError("--verbose and --json are incompatible")

    if not graph_name:
        if ctx.default_graph:
            graph_name = ctx.default_graph
            click.echo(f"Using default graph from config: {graph_name}")
        else:
            raise click.UsageError(
                "No graph name provided and no default.graph configured"
            )

    invocations: List[RequestMetadata] = ctx.tensorlake_client.requests(graph_name)

    if use_json:
        all_invocations = json.dumps(invocations, default=pydantic_encoder)
        print_json(all_invocations)
        return

    if verbose:
        print(invocations)
        return

    table = Table(title="Requests")
    table.add_column("Request")
    table.add_column("Created At")
    table.add_column("Status")
    table.add_column("Outcome")

    for invocation in invocations:
        table.add_row(
            invocation.id,
            str(invocation.created_at),
            invocation.status,
            invocation.outcome,
        )

    print(table)


@request.command(
    epilog="""
\b
Arguments:
  tensorlake request info <request-id>              # Uses default graph
  tensorlake request info <graph-name> <request-id> # Explicit graph name
\b
Use 'tensorlake config set default.graph <name>' to set a default graph name.
Use 'tensorlake config set default.request <id>' to set a default request ID.
"""
)
@click.option(
    "--json",
    "use_json",
    "-j",
    is_flag=True,
    help="Export invocation information as JSON-encoded data",
)
@click.argument("args", nargs=-1, required=False)
@click.option("--tasks", "-t", is_flag=True, help="Show tasks")
@click.option("--outputs", "-o", is_flag=True, help="Show outputs")
@pass_auth
def info(
    ctx: Context,
    use_json: bool,
    tasks: bool,
    outputs: bool,
    args: tuple,
):
    """
    Info about a remote request
    """
    # Parse arguments: if one arg provided, treat it as request_id
    # If two args provided, treat them as graph_name and request_id
    if len(args) == 1:
        graph_name = None
        request_id = args[0]
    elif len(args) == 2:
        graph_name = args[0]
        request_id = args[1]
    else:
        graph_name = None
        request_id = None

    if not graph_name:
        if ctx.default_graph:
            graph_name = ctx.default_graph
            click.echo(f"Using default graph from config: {graph_name}")
        else:
            raise click.UsageError(
                "No graph name provided and no default.graph configured"
            )

    if not request_id:
        if ctx.default_request:
            request_id = ctx.default_request
            click.echo(f"Using default request from config: {request_id}")
        else:
            raise click.UsageError(
                "No request ID provided and no default.request configured"
            )

    request: RequestMetadata = ctx.tensorlake_client.request(graph_name, request_id)

    if use_json:
        print_json(request.model_dump_json())
        return

    print(f"[bold][red]Request:[/red][/bold] {request.id}")
    print(f"[bold][red]Graph Version:[/red][/bold] {request.graph_version}")
    print(f"[bold][red]Created At:[/red][/bold] {request.created_at}")
    print(f"[bold][red]Status:[/red][/bold] {request.status}")
    print(f"[bold][red]Outcome:[/red][/bold] {request.outcome}")
    if request.failure_reason:
        print(f"[bold][red]Failure Reason:[/red][/bold] {request.failure_reason}")
    if request.request_error:
        print(f"[bold][red]Error:[/red][/bold] {request.request_error.message}")

    progress_table = Table(title="[bold][red]Progress[/red][/bold]")
    progress_table.add_column("Function")
    progress_table.add_column("Pending")
    progress_table.add_column("Successful")
    progress_table.add_column("Failed")

    for function, progress in request.request_progress.items():
        progress_table.add_row(
            function,
            str(progress.pending_tasks),
            str(progress.successful_tasks),
            str(progress.failed_tasks),
        )

    print(progress_table)

    if outputs:
        outputs_table = Table(title="[bold][red]Outputs[/red][/bold]")
        outputs_table.add_column("Function")
        outputs_table.add_column("Output ID")
        outputs_table.add_column("Num Outputs")

        for output in request.outputs:
            outputs_table.add_row(
                output.compute_fn, str(output.id), str(output.num_outputs)
            )

        print(outputs_table)

    if tasks:
        tasks: List[Task] = ctx.tensorlake_client.tasks(graph_name, request_id)

        for task in tasks:
            print(f"[bold][red]Task:[/red][/bold] {task.id}")
            print(f"[bold][red]Status:[/red][/bold] {task.status}")
            print(f"[bold][red]Outcome:[/red][/bold] {task.outcome}")
            print(f"[bold][red]Created At:[/red][/bold] {task.created_at}")

            allocations_table = Table(title="[bold][red]Allocations[/red][/bold]")
            allocations_table.add_column("Allocation ID")
            allocations_table.add_column("Server ID")
            allocations_table.add_column("Container ID")
            allocations_table.add_column("Created At")
            allocations_table.add_column("Outcome")
            allocations_table.add_column("Attempt Number")
            for allocation in task.allocations:
                allocations_table.add_row(
                    allocation.id,
                    allocation.server_id,
                    allocation.container_id,
                    str(allocation.created_at),
                    allocation.outcome,
                    str(allocation.attempt_number),
                )

            print(allocations_table)
