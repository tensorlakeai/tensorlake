import json
from typing import List

import click
from pydantic.json import pydantic_encoder
from rich import print, print_json
from rich.table import Table

from tensorlake.cli._common import AuthContext, pass_auth
from tensorlake.functions_sdk.runtime_definition import InvocationMetadata


@click.group()
def invocation():
    """
    Serverless Graph Management
    """
    pass


@invocation.command()
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

    invocations: List[InvocationMetadata] = auth.tensorlake_client.invocations(
        graph_name
    )

    if use_json:
        all_invocations = json.dumps(invocations, default=pydantic_encoder)
        print_json(all_invocations)

    elif verbose:
        print(invocations)

    else:
        table = Table(title="Invocations")
        table.add_column("Invocation")
        table.add_column("Created At")
        table.add_column("Status")
        table.add_column("Outcome")
        table.add_column("Invocation Error function")
        table.add_column("Invocation Error message")

        for invocation in invocations:
            table.add_row(
                invocation.id,
                str(invocation.created_at),
                invocation.status,
                invocation.outcome,
                (
                    ""
                    if invocation.invocation_error is None
                    else invocation.invocation_error.function_name
                ),
                (
                    ""
                    if invocation.invocation_error is None
                    else invocation.invocation_error.message
                ),
            )

        print(table)


@invocation.command()
@click.option(
    "--json",
    "use_json",
    "-j",
    is_flag=True,
    help="Export invocation information as JSON-encoded data",
)
@click.argument("graph-name")
@click.argument("invocation-id")
@pass_auth
def info(auth: AuthContext, use_json: bool, graph_name: str, invocation_id: str):
    """
    Info about a remote invocation
    """
    invocation: InvocationMetadata = auth.tensorlake_client.invocation(
        graph_name, invocation_id
    )

    if use_json:
        print_json(invocation.model_dump_json())

    else:
        print(invocation)
