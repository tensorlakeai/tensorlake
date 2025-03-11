import click

from tensorlake.cli._common import AuthContext, with_auth


@click.command(help="Return the project ID")
@with_auth
def get_project_id(auth: AuthContext):
    click.echo(auth.project_id)
