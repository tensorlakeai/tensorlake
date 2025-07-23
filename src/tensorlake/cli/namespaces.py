import click
from rich import print
from rich.table import Table

from tensorlake.cli._common import Context, pass_auth


@click.group()
def namespace():
    """
    Serverless Namespace Management
    """
    pass


@namespace.command()
@pass_auth
def list(ctx: Context):
    """
    List remote namespaces
    """
    namespaces = ctx.tensorlake_client.namespaces()

    table = Table(title="Namespaces")
    table.add_column(" ", justify="center")
    table.add_column("Name")

    for namespace in namespaces:
        table.add_row(
            "*" if namespace == ctx.tensorlake_client.namespace else "", namespace
        )

    print(table)


@namespace.command()
@click.argument("namespace-name")
@pass_auth
def create(ctx: Context, namespace_name: str):
    """
    Create a remote namespace
    """
    ctx.tensorlake_client.create_namespace(namespace_name)
    click.echo(f"Created namespace: {namespace_name}")
