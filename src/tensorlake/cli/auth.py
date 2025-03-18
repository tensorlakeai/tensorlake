import click

from tensorlake.cli._common import AuthContext, with_auth


@click.group()
def auth():
    """
    Authentication commands
    """
    pass


@auth.command(help="Print authentication status")
@with_auth
def status(auth: AuthContext):
    click.echo(f"Organization ID: {auth.organization_id}")
    click.echo(f"Project ID     : {auth.project_id}")
    click.echo(f"API Key ID     : {auth.api_key_id}")
