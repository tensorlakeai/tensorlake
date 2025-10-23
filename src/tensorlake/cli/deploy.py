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
from tensorlake.applications.remote.manifests.function import (
    FunctionManifest,
    create_function_manifest,
)
from tensorlake.applications.secrets import list_secret_names
from tensorlake.builder.client_v2 import BuildContext, ImageBuilderV2Client
from tensorlake.cli._common import Context, require_auth_and_project
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
@require_auth_and_project
def deploy(
    auth: Context,
    application_file_path: str,
    # TODO: implement with image builder v2
    parallel_builds: bool,
    upgrade_running_requests: bool,
):
    """Deploys applications to Tensorlake Cloud."""
    click.echo(f"Preparing deployment for applications from {application_file_path}")

    # Create builder client with proper authentication
    # If using API key, don't pass org/project IDs (they come from introspection)
    # If using PAT, pass org/project IDs for X-Forwarded headers
    bearer_token = auth.api_key or auth.personal_access_token
    builder_v2 = ImageBuilderV2Client(
        build_service=os.getenv("TENSORLAKE_BUILD_SERVICE")
        or f"{auth.base_url}/images/v2",
        api_key=bearer_token,
        organization_id=auth.organization_id if not auth.api_key else None,
        project_id=auth.project_id if not auth.api_key else None,
    )

    try:
        application_file_path: str = os.path.abspath(application_file_path)
        load_code(application_file_path)
    except Exception as e:
        click.echo(
            f"Failed to load the application file, please check the error message: {e}",
            err=True,
        )
        traceback.print_exception(e)
        raise click.Abort

    warning_missing_secrets(auth, list(list_secret_names()))

    functions: List[Function] = get_functions()
    asyncio.run(_prepare_images_v2(builder_v2, functions))

    click.echo("Everything looks good, deploying now\n")

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
                click.echo(
                    f"Building image {image_info.image.name} for application {fn_config.function_name} ...",
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
                except (
                    asyncio.CancelledError,
                    KeyboardInterrupt,
                    click.Abort,
                    click.UsageError,
                ) as error:
                    # Re-raise cancellation errors. Return early to skip printing the success message
                    raise error
                except Exception as error:
                    click.echo(error, err=True)
                    raise click.Abort

    click.secho("\nAll images built successfully")


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
            api_client=auth.api_client,  # Use the authenticated API client from context
        )

        for application_function in filter_applications(functions):
            app_func_manifest: FunctionManifest = create_function_manifest(
                application_function,
                application_function.application_config.version,
                application_function,
            )
            func_name = app_func_manifest.name
            click.echo(f"Deployed application: {func_name}\n")
            # TODO: update after parameterless function support
            if len(app_func_manifest.parameters) > 0:
                param_type = app_func_manifest.parameters[0].data_type
                click.echo(
                    f"""To invoke the application, use the following curl command:
curl -X POST {auth.base_url}/v1/namespaces/{auth.namespace}/applications/{func_name} \\
-H "Authorization: Bearer $TENSORLAKE_API_KEY" \\
-H "accept: application/json" \\
-H "Content-Type: application/json" \\
-d '{param_type}'
""",
                )
        return
    except click.UsageError as error:
        raise error
    except Exception as e:
        click.echo(
            f"Applications could not be deployed, please check the error message: {e}",
            err=True,
        )
        traceback.print_exception(e)
        raise click.Abort
