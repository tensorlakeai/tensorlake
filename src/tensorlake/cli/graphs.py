import json

import click
import rich
from pydantic.json import pydantic_encoder
from rich import print, print_json
from rich.table import Table

from tensorlake.cli._common import Context, pass_auth


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
def list(ctx: Context, verbose: bool, use_json: bool):
    """
    List remote graphs
    """
    if verbose and use_json:
        raise click.UsageError("--verbose and --json are incompatible")

    graphs = ctx.tensorlake_client.graphs()

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


@graph.command(
    epilog="""
\b
Use 'tensorlake config set default.graph <name>' to set a default graph name.
"""
)
@click.option(
    "--json", "-j", is_flag=True, help="Export graph information as JSON-encoded data"
)
@click.argument("graph-name", required=False)
@pass_auth
def info(ctx: Context, json: bool, graph_name: str):
    """
    Info about a remote graph
    """
    if not graph_name:
        if ctx.default_graph:
            graph_name = ctx.default_graph
            click.echo(f"Using default graph from config: {graph_name}")
        else:
            raise click.UsageError(
                "No graph name provided and no default.graph configured"
            )

    g = ctx.tensorlake_client.graph(graph_name)

    if json:
        print_json(g.model_dump_json())
        return

    print(f"[bold][red]Graph:[/red][/bold] {g.name}")
    if g.description:
        print(f"[bold][red]Description:[/red][/bold] {g.description}")
    print(f"[bold][red]Version:[/red][/bold] {g.version}")
    if g.tags:
        print(f"[bold][red]Tags:[/red][/bold] {g.tags}")

    print(f"[bold][red]Entrypoint Function:[/red][/bold] {g.entrypoint.name}")
    print(
        f"[bold][red]Entrypoint Function Encoding:[/red][/bold] {g.entrypoint.input_encoder}"
    )

    table = Table()
    table.add_column("Name")
    table.add_column("Edges")

    for function in g.functions:
        edges = g.edges.get(function, [])
        edge_names = [edge for edge in edges]
        table.add_row(function, ", ".join(edge_names))

    print(table)
