import json

import click

from tensorlake.cli._common import Context, pass_auth


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
@pass_auth
def status(ctx: Context, output: str):
    if output == "json":
        print(
            json.dumps(
                {
                    "organizationId": ctx.organization_id,
                    "projectId": ctx.project_id,
                    "apiKeyId": ctx.api_key_id,
                }
            )
        )
        return
    click.echo(f"Organization ID: {ctx.organization_id}")
    click.echo(f"Project ID     : {ctx.project_id}")
    click.echo(f"API Key ID     : {ctx.api_key_id}")
