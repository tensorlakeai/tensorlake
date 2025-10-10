import asyncio
import os
import traceback
from typing import Dict, List

import click

from tensorlake.applications import Function, Image
from tensorlake.applications.applications import filter_applications
from tensorlake.applications.image import ImageInformation, image_infos
from tensorlake.applications.interface.function import (
    _ApplicationConfiguration,
    _FunctionConfiguration,
)
from tensorlake.applications.registry import get_functions
from tensorlake.applications.remote.code.loader import load_code
from tensorlake.applications.remote.deploy import deploy_applications
from tensorlake.applications.secrets import list_secret_names
from tensorlake.builder.client_v2 import BuildContext, ImageBuilderV2Client
from tensorlake.cli._common import Context, pass_auth
from tensorlake.cli.secrets import warning_missing_secrets


@click.command(
    short_help="Deploys applications defined in <application-file-path> .py file to Tensorlake Cloud"
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
    "application-file-path",
    type=click.Path(exists=True, file_okay=True, dir_okay=False),
)
@pass_auth
def deploy(
    auth: Context,
    application_file_path: str,
    # TODO: implement with image builder v2
    parallel_builds: bool,
    upgrade_running_requests: bool,
):
    """Deploys applications to Tensorlake Cloud."""

    click.echo(f"Preparing deployment for applications from {application_file_path}")
    builder_v2 = ImageBuilderV2Client.from_env()

    try:
        application_file_path: str = os.path.abspath(application_file_path)
        load_code(application_file_path)
    except Exception as e:
        click.secho(
            f"Failed to load the application file, please check the error message: {e}",
            fg="red",
        )
        traceback.print_exception(e)
        raise click.Abort

    warning_missing_secrets(auth, list(list_secret_names()))

    functions: List[Function] = get_functions()
    asyncio.run(_prepare_images_v2(builder_v2, functions))

    click.secho("Everything looks good, deploying now", fg="green")

    _deploy_applications(
        auth=auth,
        application_file_path=application_file_path,
        upgrade_running_requests=upgrade_running_requests,
        functions=functions,
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

    click.secho("Built all images", fg="green")


def _deploy_applications(
    auth: Context,
    application_file_path: str,
    upgrade_running_requests: bool,
    functions: List[Function],
):
    try:
        deploy_applications(
            applications_file_path=application_file_path,
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

    # Display success message with deployment information
    deployed_apps = filter_applications(functions)

    try:
        application = next(deployed_apps)
        fn_config: _FunctionConfiguration = application.function_config

        # Get parameter type from function signature
        import inspect
        sig = inspect.signature(application.original_function)
        first_param = next(iter(sig.parameters.values()), None)

        if first_param and first_param.annotation != inspect.Parameter.empty:
            type_map = {int: "<integer>", str: "<string>", float: "<float>", bool: "<boolean>"}
            param_type = type_map.get(first_param.annotation, f"<{first_param.annotation.__name__}>")
        else:
            param_type = "<value>"

        click.secho(
            f"""Deployed application: {fn_config.function_name}
curl -X POST {auth.base_url}/v1/namespaces/{auth.namespace}/applications/{fn_config.function_name} \\
-H "Authorization: Bearer $TENSORLAKE_API_KEY" \\
-H "accept: application/json" \\
-H "Content-Type: application/json" \\
-d '{param_type}'
""",
            fg="green",
        )
    except StopIteration:
        click.secho("Deployed application", fg="green")
        click.secho(
            "Error generating curl command\n", fg="yellow"
        )
