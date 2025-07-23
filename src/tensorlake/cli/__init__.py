from typing import Optional

import click

from . import _common, auth, config, deploy, graphs, requests, secrets


@click.group(
    epilog="""
\b
Use 'tensorlake config' to manage settings:
  tensorlake.apikey     - API key for authentication
  indexify.url          - Server URL (default: 'https://api.tensorlake.ai')
  indexify.namespace    - Namespace (default: 'default')
  default.graph         - Default graph name for commands
  default.request       - Default request ID for request info
"""
)
@click.version_option(
    version=_common.VERSION, package_name="tensorlake", prog_name="tensorlake"
)
@click.option(
    "--indexify-url",
    "base_url",
    envvar="INDEXIFY_URL",
    help="The Indexify server URL",
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
)
@click.pass_context
def cli(
    ctx: click.Context,
    base_url: Optional[str],
    api_key: Optional[str],
    namespace: Optional[str],
):
    """
    Tensorlake CLI to manage and deploy workflows to Tensorlake Serverless Workflows.
    """
    ctx.obj = _common.Context.default(
        base_url=base_url, api_key=api_key, namespace=namespace
    )


cli.add_command(auth.auth)
cli.add_command(config.config)
cli.add_command(deploy.deploy)
cli.add_command(graphs.graph)
cli.add_command(requests.request)
cli.add_command(secrets.secrets)
