import json

import click
from pydantic.json import pydantic_encoder
from rich import print, print_json
from rich.table import Table

from tensorlake.cli._common import AuthContext, pass_auth


@click.group()
def graph():
    """
    Serverless Graph Management
    """
    pass


@graph.command()
@click.option("--verbose", "-v", is_flag=True, help="Include all graph information")
@click.option(
    "--json",
    "use_json",
    "-j",
    is_flag=True,
    help="Export all graph information as JSON-encoded data",
)
@pass_auth
def list(auth: AuthContext, verbose: bool, use_json: bool):
    """
    List remote graphs
    """
    if verbose and use_json:
        raise click.UsageError("--verbose and --json are incompatible")

    graphs = auth.tensorlake_client.graphs()

    if use_json:
        all_graphs = json.dumps(graphs, default=pydantic_encoder)
        print_json(all_graphs)

    elif verbose:
        print(graphs)

    else:
        table = Table(title="Graphs")
        table.add_column("Name")
        table.add_column("Description")

        for graph in graphs:
            table.add_row(graph.name, graph.description)

        print(table)


@graph.command()
@click.option(
    "--json", "-j", is_flag=True, help="Export graph information as JSON-encoded data"
)
@click.argument("graph-name")
@pass_auth
def info(auth: AuthContext, json: bool, graph_name: str):
    """
    Info about a remote graph
    """
    g = auth.tensorlake_client.graph(graph_name)

    if json:
        print_json(g.model_dump_json())

    else:
        print(g)
