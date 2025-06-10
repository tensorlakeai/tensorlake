import click

from . import _common, auth, deploy, graphs, images, invocations, namespaces, secrets


@click.group()
@click.version_option(
    version=_common.VERSION, package_name="tensorlake", prog_name="tensorlake"
)
@click.pass_context
def cli(ctx: click.Context):
    """
    Tensorlake CLI to manage and deploy workflows to Tensorlake Serverless Workflows.
    """
    ctx.obj = _common.AuthContext()


cli.add_command(auth.auth)
cli.add_command(deploy.deploy)
cli.add_command(graphs.graph)
cli.add_command(images.image_logs)
cli.add_command(invocations.invocation)
cli.add_command(namespaces.namespace)
cli.add_command(secrets.secrets)
