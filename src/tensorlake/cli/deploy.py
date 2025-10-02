import asyncio
import os
import traceback
from typing import Dict, List, Set

import click

from tensorlake.applications import Function, Image
from tensorlake.applications.applications import filter_applications
from tensorlake.applications.image import (
    ImageInformation,
    image_infos,
)
from tensorlake.applications.interface.function import (
    _ApplicationConfiguration,
    _FunctionConfiguration,
)
from tensorlake.applications.registry import get_functions
from tensorlake.applications.remote.code.ignored_code_paths import (
    ignored_code_paths,
)
from tensorlake.applications.remote.code.loader import load_code
from tensorlake.applications.remote.deploy import deploy_applications
from tensorlake.applications.secrets import list_secret_names
from tensorlake.builder.client_v2 import BuildContext, ImageBuilderV2Client
from tensorlake.cli._common import Context, pass_auth
from tensorlake.cli.secrets import warning_missing_secrets


@click.command(
    short_help="Deploys applications defined in <code-dir-path> directory to Tensorlake Cloud"
)
@click.option("-p", "--parallel-builds", is_flag=True, default=False)
@click.option(
    "-u",
    "--upgrade-running-requests",
    is_flag=True,
    default=False,
    help="Upgrade requests that are already queued or running to use the new deployed version of the applications",
)
@click.argument(
    "code-dir-path",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
)
@pass_auth
def deploy(
    auth: Context,
    code_dir_path: str,
    # TODO: implement with image builder v2
    parallel_builds: bool,
    upgrade_running_requests: bool,
):
    """Deploys applications to Tensorlake Cloud."""

    click.echo(f"Preparing deployment for applications from {code_dir_path}")
    builder_v2 = ImageBuilderV2Client.from_env()

    try:
        code_dir_path: str = os.path.abspath(code_dir_path)

        ignored_absolute_paths: Set[str] = ignored_code_paths(code_dir_path)
        load_code(code_dir_path, ignored_absolute_paths)
    except Exception as e:
        click.secho(
            f"Failed to load the code directory modules, please check the error message: {e}",
            fg="red",
        )
        traceback.print_exception(e)
        raise click.Abort

    warning_missing_secrets(auth, list(list_secret_names()))

    functions: List[Function] = get_functions()
    asyncio.run(_prepare_images_v2(builder_v2, functions))

    click.secho("Everything looks good, deploying now", fg="green")

    _deploy_applications(
        code_dir_path=code_dir_path,
        upgrade_running_requests=upgrade_running_requests,
    )


async def _prepare_images_v2(builder: ImageBuilderV2Client, functions: List[Function]):
    images: Dict[Image, ImageInformation] = image_infos()
    for application in filter_applications(functions):
        fn_config: _FunctionConfiguration = application.function_config
        app_config: _ApplicationConfiguration = application.application_config

        for image_info in images.values():
            image_info: ImageInformation
            for function in image_info.functions:
                click.secho(
                    f"Building image {image_info.image.name} for application {fn_config.function_name} ...",
                    fg="yellow",
                )
                try:
                    await builder.build(
                        BuildContext(
                            application_name=fn_config.function_name,
                            application_version=app_config.version,
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

    click.secho(f"Built all images", fg="green")


def _deploy_applications(code_dir_path: str, upgrade_running_requests: bool):
    try:
        deploy_applications(
            source_dir_or_file_path=code_dir_path,
            upgrade_running_requests=upgrade_running_requests,
            load_source_dir_modules=False,  # Already loaded
        )
    except Exception as e:
        click.secho(
            f"Applications could not be deployed, please check the error message: {e}",
            fg="red",
        )
        traceback.print_exception(e)
        raise click.Abort

    click.secho(f"Deployed all applications", fg="green")
