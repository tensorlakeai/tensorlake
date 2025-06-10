import click

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

    for namespace in namespaces:
        print(namespace)


@namespace.command()
@click.argument("namespace")
@pass_auth
def create(auth: AuthContext, namespace: str):
    """
    Create a remote namespace
    """
    auth.tensorlake_client.create_namespace(namespace)
    click.echo(f"Created namespace: {namespace}")
