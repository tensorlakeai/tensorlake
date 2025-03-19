import json

import click

from tensorlake.cli._common import AuthContext, with_auth


@click.group()
def auth():
    """
    Authentication commands
    """
    pass


@auth.command(help="Print authentication status")
@click.option(
    "--output",
    "-o",
    type=click.Choice(["text", "json"]),
    default="text",
    help="Output format",
)
@with_auth
def status(auth: AuthContext, output: str):
    if output == "json":
        print(
            json.dumps(
                {
                    "organizationId": auth.organization_id,
                    "projectId": auth.project_id,
                    "apiKeyId": auth.api_key_id,
                }
            )
        )
        return
    click.echo(f"Organization ID: {auth.organization_id}")
    click.echo(f"Project ID     : {auth.project_id}")
    click.echo(f"API Key ID     : {auth.api_key_id}")
