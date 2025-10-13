import click

from . import _common, applications, auth, config, deploy, parse, requests, secrets


@click.group(
    epilog="""
\b
Use 'tensorlake config' to manage settings:
  tensorlake.apikey     - API key for authentication
  indexify.url          - Server URL (default: 'https://api.tensorlake.ai')
  tensorlake.cloud_url  - Cloud URL (default: 'https://cloud.tensorlake.ai')
  indexify.namespace    - Namespace (default: 'default')
  default.application   - Default application name for commands
  default.request       - Default request ID for request info
  default.project       - Default project ID for requests and deployments
  default.organization  - Default organization ID for requests and deployments

\b
Authentication:
  Use --api-key or TENSORLAKE_API_KEY for API key authentication
  Use --pat or TENSORLAKE_PAT for Personal Access Token authentication
  Use 'tensorlake auth login' to obtain a PAT interactively
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
    "--cloud-url",
    "cloud_url",
    envvar="TENSORLAKE_CLOUD_URL",
    help="The Tensorlake Cloud URL",
)
@click.option(
    "--api-key",
    envvar="TENSORLAKE_API_KEY",
    help="The Tensorlake Indexify server API key",
)
@click.option(
    "--pat",
    "personal_access_token",
    envvar="TENSORLAKE_PAT",
    help="The Tensorlake Personal Access Token",
)
@click.option(
    "--namespace",
    envvar="INDEXIFY_NAMESPACE",
    help="The namespace to use",
)
@click.pass_context
def cli(
    ctx: click.Context,
    base_url: str | None,
    cloud_url: str | None,
    api_key: str | None,
    personal_access_token: str | None,
    namespace: str | None,
):
    """
    Tensorlake CLI for Tensorlake Cloud.
    """
    ctx.obj = _common.Context.default(
        base_url=base_url,
        cloud_url=cloud_url,
        api_key=api_key,
        personal_access_token=personal_access_token,
        namespace=namespace,
    )


cli.add_command(auth.auth)
cli.add_command(config.config)
cli.add_command(deploy.deploy)
cli.add_command(applications.application)
cli.add_command(requests.request)
cli.add_command(secrets.secrets)
cli.add_command(parse.parse)
