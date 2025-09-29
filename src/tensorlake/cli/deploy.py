import asyncio
import os
import traceback
from typing import Dict, Set

import click

from tensorlake.applications import Application, Image
from tensorlake.applications.application import get_user_defined_or_default_application
from tensorlake.applications.image import (
    ImageInformation,
    image_infos,
)
from tensorlake.applications.remote.application.ignored_code_paths import (
    ignored_code_paths,
)
from tensorlake.applications.remote.application.loader import load_application
from tensorlake.applications.remote.deploy import deploy as tl_deploy
from tensorlake.applications.secrets import list_secret_names
from tensorlake.builder.client_v2 import BuildContext, ImageBuilderV2Client
from tensorlake.cli._common import Context, pass_auth
from tensorlake.cli.secrets import warning_missing_secrets


@click.command(
    short_help="Deploys application defined in <application-dir-path> directory to Tensorlake Cloud"
)
@click.option("-p", "--parallel-builds", is_flag=True, default=False)
@click.option(
    "-u",
    "--upgrade-running-requests",
    is_flag=True,
    default=False,
    help="Upgrade requests that are already queued or running to use the new deployed version of the application",
)
@click.argument(
    "application-dir-path",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
)
@pass_auth
def deploy(
    auth: Context,
    application_dir_path: str,
    # TODO: implement with image builder v2
    parallel_builds: bool,
    upgrade_running_requests: bool,
):
    """Deploys application to tensorlake."""

    click.echo(f"Preparing deployment for application from {application_dir_path}")
    builder_v2 = ImageBuilderV2Client.from_env()

    try:
        application_dir_path: str = os.path.abspath(application_dir_path)

        ignored_absolute_paths: Set[str] = ignored_code_paths(application_dir_path)
        load_application(application_dir_path, ignored_absolute_paths)
    except Exception as e:
        click.secho(
            f"Failed to load the application modules, please check the error message: {e}",
            fg="red",
        )
        traceback.print_exception(e)
        raise click.Abort

    application: Application = get_user_defined_or_default_application()
    # warning_missing_secrets(auth, list(list_secret_names()))

    asyncio.run(_prepare_images_v2(builder_v2, application))

    click.secho("Everything looks good, deploying now", fg="green")

    _deploy_application(
        application=application,
        application_dir_path=application_dir_path,
        upgrade_running_requests=upgrade_running_requests,
    )


async def _prepare_images_v2(
    builder_v2: ImageBuilderV2Client,
    app: Application,
):
    images: Dict[Image, ImageInformation] = image_infos()
    for image_info in images.values():
        image_info: ImageInformation
        for function in image_info.functions:
            click.secho(f"Building image {image_info.image.name}...", fg="yellow")
            try:
                await builder_v2.build(
                    BuildContext(
                        application_name=app.name,
                        application_version=app.version,
                        function_name=function.function_config.function_name,
                    ),
                    image_info.image,
                )
            except Exception as e:
                click.secho(
                    f"Failed to build image {image_info.image.name}, please check the error message: {e}",
                    fg="red",
                )
                traceback.print_exception(e)
                raise click.Abort

    click.secho(f"Built {len(images)} images", fg="green")


def _deploy_application(
    application: Application, application_dir_path: str, upgrade_running_requests: bool
):
    try:
        tl_deploy(
            application_source_dir_or_file_path=application_dir_path,
            upgrade_running_requests=upgrade_running_requests,
            load_application_modules=False,  # Already loaded
        )
    except Exception as e:
        click.secho(
            f"Application {application.name} could not be deployed, please check the error message: {e}",
            fg="red",
        )
        traceback.print_exception(e)
        raise click.Abort

    click.secho(f"Deployed {application.name}", fg="green")
