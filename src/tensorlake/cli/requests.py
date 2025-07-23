import json
from typing import List

import click
from pydantic.json import pydantic_encoder
from rich import print, print_json
from rich.table import Table

from tensorlake.cli._common import Context, pass_auth
from tensorlake.functions_sdk.http_client import RequestMetadata


@click.group()
def request():
    """
    Serverless Graph Management
    """
    pass


@request.command()
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

    elif verbose:
        print(invocations)

    else:
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


@request.command()
@click.option(
    "--json",
    "use_json",
    "-j",
    is_flag=True,
    help="Export invocation information as JSON-encoded data",
)
@click.argument("graph-name", required=False)
@click.argument("request-id", required=False)
@pass_auth
def info(ctx: Context, use_json: bool, graph_name: str, request_id: str):
    """
    Info about a remote request
    """
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

    else:
        print(request)
