import json
from typing import List

import click
from pydantic.json import pydantic_encoder
from rich import print, print_json
from rich.table import Table

from tensorlake.cli._common import AuthContext, pass_auth
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
@click.argument("graph-name")
def list(auth: AuthContext, verbose: bool, use_json: bool, graph_name: str):
    """
    List remote invocations
    """
    if verbose and use_json:
        raise click.UsageError("--verbose and --json are incompatible")

    invocations: List[RequestMetadata] = auth.tensorlake_client.requests(graph_name)

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
@click.argument("graph-name")
@click.argument("request-id")
@pass_auth
def info(auth: AuthContext, use_json: bool, graph_name: str, request_id: str):
    """
    Info about a remote request
    """
    request: RequestMetadata = auth.tensorlake_client.request(graph_name, request_id)

    if use_json:
        print_json(request.model_dump_json())

    else:
        print(request)
