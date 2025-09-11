import click

from tensorlake.cli._common import Context, pass_auth, print_signals


@click.group()
def container():
    """
    Container Management
    """
    pass


@container.command(
    epilog="""
\b
Arguments:
  tensorlake container logs <container-id>              # Uses default graph
  tensorlake container logs <graph-name> <container-id> # Explicit graph name
\b
Use 'tensorlake config set default.graph <name>' to set a default graph name.
"""
)
@click.option(
    "--json",
    "use_json",
    "-j",
    is_flag=True,
    help="Export container log information as JSON-encoded data",
)
@click.option("--verbose", "-v", is_flag=True, help="Include all log information")
@click.argument("args", nargs=-1, required=False)
@pass_auth
def logs(
    ctx: Context,
    use_json: bool,
    verbose: bool,
    args: tuple,
):
    """
    Show logs for a specific container
    """
    # Parse arguments: if one arg provided, treat it as container_id
    # If two args provided, treat them as graph_name and container_id
    if len(args) == 1:
        graph_name = None
        container_id = args[0]
    elif len(args) == 2:
        graph_name = args[0]
        container_id = args[1]
    else:
        raise click.UsageError(
            "Container ID is required. Usage: logs [graph-name] <container-id>"
        )

    if not graph_name:
        if ctx.default_graph:
            graph_name = ctx.default_graph
            click.echo(f"Using default graph from config: {graph_name}")
        else:
            raise click.UsageError(
                "No graph name provided and no default.graph configured"
            )

    print_signals(
        f"Logs for Container {container_id}",
        ctx.tensorlake_client.container_logs(graph_name, container_id),
        use_json=use_json,
        verbose=verbose,
    )
