import click

from . import _common, deploy, get_project, images, prepare, secrets


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
    pass


cli.add_command(get_project.get_project_id)
cli.add_command(prepare.prepare)
cli.add_command(deploy.deploy)
cli.add_command(images.get_image_uri)
cli.add_command(images.show_logs)
cli.add_command(secrets.secrets)
