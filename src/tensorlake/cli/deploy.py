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
        click.echo(
            f"Failed to load the application file, please check the error message: {e}",
            err=True
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
                except Exception as e:
                    click.echo(
                        f"Failed to build image {image_info.image.name}, please check the error message: {e}",
                        err=True,
                    )
                    traceback.print_exception(e)
                    raise click.Abort

    click.secho("\nBuilt all images")


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
        click.echo(
            f"Applications could not be deployed, please check the error message: {e}", err=True
        )
        traceback.print_exception(e)
        raise click.Abort

    deployed_apps = filter_applications(functions)
    try:
        application = next(deployed_apps)
        fn_config: _FunctionConfiguration = application.function_config

        # Get parameter type from function signature
        import inspect
        sig = inspect.signature(application.original_function)
        first_param = next(iter(sig.parameters.values()), None)

        if first_param and first_param.annotation != inspect.Parameter.empty:
            param_annotation = first_param.annotation

            if hasattr(param_annotation, 'model_json_schema') or hasattr(param_annotation, 'schema'):
                schema = getattr(param_annotation, 'model_json_schema', lambda: None)() or getattr(param_annotation, 'schema', lambda: {})()
                properties = schema.get('properties', {})
                field_examples = []

                for field_name, field_schema in properties.items():
                    # Handle different schema formats
                    if 'type' in field_schema:
                        field_type_name = field_schema['type']
                    elif 'anyOf' in field_schema:
                        # Show all types in the union
                        types = [item.get('type') for item in field_schema['anyOf'] if item.get('type')]
                        field_type_name = ' | '.join(types) if types else 'value'
                    else:
                        field_type_name = 'value'
                    field_examples.append(f'"{field_name}": <{field_type_name}>')

                param_type = '{' + ', '.join(field_examples) + '}'
            else:
                type_name = getattr(param_annotation, '__name__', str(param_annotation))
                param_type = f"<{type_name}>"
        else:
            param_type = "<value>"

        click.echo(f"Deployed application: {fn_config.function_name}\n")
        click.echo(
            f"""To invoke the application, use the following curl command:
curl -X POST {auth.base_url}/v1/namespaces/{auth.namespace}/applications/{fn_config.function_name} \\
  -H "Authorization: Bearer $TENSORLAKE_API_KEY" \\
  -H "accept: application/json" \\
  -H "Content-Type: application/json" \\
  -d '{param_type}'
""",
        )
    except StopIteration:
        click.echo("Successfully deployed application")
        click.echo("Error generating curl command\n", err=True)
