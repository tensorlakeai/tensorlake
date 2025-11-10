import click

from . import (
    _common,
    applications,
    auth,
    deploy,
    init,
    new,
    parse,
    secrets,
)


@click.group(
    cls=_common.AliasedGroup,
    epilog="""
\b
Authentication:
  Use --api-key or TENSORLAKE_API_KEY for API key authentication
  Use --pat or TENSORLAKE_PAT for Personal Access Token authentication
  Use 'tensorlake login' to obtain a PAT interactively
""",
)
@click.version_option(
    version=_common.VERSION, package_name="tensorlake", prog_name="tensorlake"
)
@click.option(
    "--debug",
    is_flag=True,
    envvar="TENSORLAKE_DEBUG",
    help="Show detailed error information and stack traces",
)
@click.option(
    "--api-url",
    "api_url",
    envvar="TENSORLAKE_API_URL",
    help="The TensorLake API server URL",
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
@click.option(
    "--organization",
    "organization_id",
    envvar="TENSORLAKE_ORGANIZATION_ID",
    help="The organization ID to use",
)
@click.option(
    "--project",
    "project_id",
    envvar="TENSORLAKE_PROJECT_ID",
    help="The project ID to use",
)
@click.pass_context
def cli(
    ctx: click.Context,
    debug: bool,
    api_url: str | None,
    cloud_url: str | None,
    api_key: str | None,
    personal_access_token: str | None,
    namespace: str | None,
    organization_id: str | None,
    project_id: str | None,
):
    """
    Tensorlake CLI.
    """
    ctx.obj = _common.Context.default(
        api_url=api_url,
        cloud_url=cloud_url,
        api_key=api_key,
        personal_access_token=personal_access_token,
        namespace=namespace,
        organization_id=organization_id,
        project_id=project_id,
        debug=debug,
    )


cli.add_command(auth.login)
cli.add_command(auth.whoami)
cli.add_command(init.init)
cli.add_command(new.new)
cli.add_command(deploy.deploy)
cli.add_command(secrets.secrets)
cli.add_command(parse.parse)
cli.add_command(applications.ls)
