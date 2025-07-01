from typing import Optional

import click

from . import _common, auth, deploy, graphs, invocations, namespaces, secrets


@click.group()
@click.version_option(
    version=_common.VERSION, package_name="tensorlake", prog_name="tensorlake"
)
@click.option(
    "--indexify-url",
    "base_url",
    envvar="INDEXIFY_URL",
    help="The Indexify server URL",
    default="https://api.tensorlake.ai",
)
@click.option(
    "--api-key",
    envvar="TENSORLAKE_API_KEY",
    help="The Tensorlake Indexify server API key",
)
@click.option(
    "--namespace",
    envvar="INDEXIFY_NAMESPACE",
    help="The namespace to use",
    default="default",
)
@click.pass_context
def cli(ctx: click.Context, base_url: str, api_key: Optional[str], namespace: str):
    """
    Tensorlake CLI to manage and deploy workflows to Tensorlake Serverless Workflows.
    """
    ctx.obj = _common.AuthContext(
        base_url=base_url, api_key=api_key, namespace=namespace
    )


cli.add_command(auth.auth)
cli.add_command(deploy.deploy)
cli.add_command(graphs.graph)
cli.add_command(invocations.invocation)
cli.add_command(namespaces.namespace)
cli.add_command(secrets.secrets)
