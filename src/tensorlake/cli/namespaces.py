import click
from rich import print
from rich.table import Table

from tensorlake.cli._common import AuthContext, pass_auth


@click.group()
def namespace():
    """
    Serverless Namespace Management
    """
    pass


@namespace.command()
@pass_auth
def list(auth: AuthContext):
    """
    List remote namespaces
    """
    namespaces = auth.tensorlake_client.namespaces()

    table = Table(title="Namespaces")
    table.add_column(" ", justify="center")
    table.add_column("Name")

    for namespace in namespaces:
        table.add_row(
            "*" if namespace == auth.tensorlake_client.namespace else "", namespace
        )

    print(table)


@namespace.command()
@click.argument("namespace")
@pass_auth
def create(auth: AuthContext, namespace: str):
    """
    Create a remote namespace
    """
    auth.tensorlake_client.create_namespace(namespace)
    click.echo(f"Created namespace: {namespace}")
