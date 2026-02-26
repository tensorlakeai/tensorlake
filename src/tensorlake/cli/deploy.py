import asyncio
import os

import click

from tensorlake.applications import Function, Image, SDKUsageError, TensorlakeError
from tensorlake.applications.applications import filter_applications
from tensorlake.applications.image import ImageInformation, image_infos
from tensorlake.applications.interface.function import (
    _ApplicationConfiguration,
    _FunctionConfiguration,
)
from tensorlake.applications.registry import get_functions
from tensorlake.applications.remote.code.ignored_code_paths import CodePathsSummary
from tensorlake.applications.remote.code.loader import load_code
from tensorlake.applications.remote.curl_command import example_application_curl_command
from tensorlake.applications.remote.deploy import deploy_applications
from tensorlake.applications.secrets import list_secret_names
from tensorlake.applications.validation import (
    ValidationMessage,
    has_error_message,
    print_validation_messages,
    validate_loaded_applications,
)
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
    ctx: Context,
    application_file_path: str,
    # TODO: implement with image builder v2
    parallel_builds: bool,
    upgrade_running_requests: bool,
):
    """Deploys applications to Tensorlake Cloud."""
    click.echo(f"⚙️  Preparing deployment for applications from {application_file_path}")

    # Create builder client with proper authentication
    # If using API key, don't pass org/project IDs (they come from introspection)
    # If using PAT, pass org/project IDs for X-Forwarded headers
    bearer_token = ctx.api_key or ctx.personal_access_token
    builder_v2 = ImageBuilderV2Client(
        build_service=os.getenv("TENSORLAKE_BUILD_SERVICE")
        or f"{ctx.api_url}/images/v2",
        api_key=bearer_token,
        organization_id=ctx.organization_id if not ctx.api_key else None,
        project_id=ctx.project_id if not ctx.api_key else None,
    )

    try:
        application_file_path: str = os.path.abspath(application_file_path)
        load_code(application_file_path)
    except SyntaxError as e:
        raise click.ClickException(
            f"syntax error in {e.filename}, line {e.lineno}: {e.msg}"
        ) from None
    except ImportError as e:
        raise click.ClickException(
            f"failed to import application file: {e}. "
            f"make sure all dependencies are installed in your current environment."
        ) from None
    except Exception as e:
        raise click.ClickException(
            f"failed to load {application_file_path}: {type(e).__name__}: {e}"
        ) from None

    validation_messages: list[ValidationMessage] = validate_loaded_applications()
    print_validation_messages(validation_messages)
    if has_error_message(validation_messages):
        click.echo(
            "‼️  Deployment aborted due to code validation errors, please address them before deploying.",
            err=True,
        )
        raise click.Abort

    warning_missing_secrets(ctx, list(list_secret_names()))

    functions: list[Function] = get_functions()
    asyncio.run(_prepare_images_v2(builder_v2, functions))

    _deploy_applications(
        ctx=ctx,
        application_file_path=application_file_path,
        upgrade_running_requests=upgrade_running_requests,
        functions=functions,
    )


async def _prepare_images_v2(builder: ImageBuilderV2Client, functions: list[Function]):
    images: dict[Image, ImageInformation] = image_infos()
    for application in filter_applications(functions):
        fn_config: _FunctionConfiguration = application._function_config
        app_config: _ApplicationConfiguration = application._application_config

        for image_info in images.values():
            image_info: ImageInformation
            for function in image_info.functions:
                click.echo(
                    f"📦 Building `{image_info.image.name}` image...",
                )
                try:
                    await builder.build(
                        BuildContext(
                            application_name=fn_config.function_name,
                            application_version=app_config.version,
                            function_name=function._function_config.function_name,
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
                    raise click.ClickException(
                        f"image '{image_info.image.name}' build failed: {error}. "
                        f"check your Image() configuration and try again."
                    ) from None

    click.secho("\n✅ All images built successfully")


def _deploy_applications(
    ctx: Context,
    application_file_path: str,
    upgrade_running_requests: bool,
    functions: list[Function],
):
    click.echo("⚙️  Deploying applications...\n")

    try:
        paths_summary = deploy_applications(
            applications_file_path=application_file_path,
            upgrade_running_requests=upgrade_running_requests,
            load_source_dir_modules=False,  # Already loaded
            api_client=ctx.api_client,  # Use the authenticated API client from context
        )

        # Always show ignored paths
        _print_ignored_paths(paths_summary)

        # Show skipped patterns only in debug mode
        if ctx.debug:
            _print_skipped_patterns(paths_summary)

        for application_function in filter_applications(functions):
            application_function: Function
            click.echo(
                f"🚀 Application `{application_function._name}` deployed successfully\n"
            )
            curl_command: str | None = example_application_curl_command(
                api_url=ctx.api_url,
                application=application_function,
                file_paths=None,
            )
            if curl_command is not None:
                click.echo(
                    f"💡 To invoke it, you can use the following cURL command:\n\n{curl_command}"
                )
    except SDKUsageError as e:
        raise click.UsageError(str(e)) from None
    except TensorlakeError as e:
        raise click.ClickException(f"failed to deploy applications: {e}") from e
    except Exception as e:
        raise click.ClickException(
            f"failed to deploy applications: {type(e).__name__}: {e}"
        ) from None

    click.echo(
        "\n📚 Visit our documentation if you need more information about invoking applications: "
        "https://docs.tensorlake.ai/applications/quickstart#calling-applications\n\n"
    )


def _print_ignored_paths(summary: CodePathsSummary) -> None:
    """Print ignored paths summary (always shown)."""
    click.echo("\n" + "=" * 80)
    click.echo("📋 Ignored Paths Summary")
    click.echo("=" * 80)

    click.echo(
        f"\nThe following paths were excluded from the deployment ({len(summary.ignored_paths)} total):"
    )
    if summary.ignored_paths:
        for path in sorted(summary.ignored_paths):
            click.echo(f"  - {path}")
    else:
        click.echo("  (none)")

    click.echo("\n" + "=" * 80 + "\n")


def _print_skipped_patterns(summary: CodePathsSummary) -> None:
    """Print skipped patterns (debug mode only)."""
    if not summary.skipped_patterns:
        return

    click.echo("\n" + "=" * 80)
    click.echo("🔍 DEBUG: Skipped .gitignore Patterns")
    click.echo("=" * 80)

    click.echo(
        f"\nThe following patterns could not be processed ({len(summary.skipped_patterns)} total):"
    )
    for pattern, reason in summary.skipped_patterns:
        click.echo(f"  - {pattern}: {reason}")

    click.echo("\n" + "=" * 80 + "\n")
