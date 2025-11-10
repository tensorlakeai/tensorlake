import json
import os
import time
import webbrowser
from pathlib import Path

import click
import httpx

from tensorlake.cli._common import Context, pass_auth
from tensorlake.cli._configuration import save_credentials


@click.command(help="Print authentication status")
@click.option(
    "--output",
    "-o",
    type=click.Choice(["text", "json"]),
    default="text",
    help="Output format",
)
@pass_auth
def whoami(ctx: Context, output: str):
    # Check if user is authenticated
    if not ctx.has_authentication():
        if output == "json":
            print(
                json.dumps(
                    {
                        "authenticated": False,
                        "message": "Not logged in and no API key provided",
                    }
                )
            )
        else:
            click.echo("You are not logged in and have not provided an API key.")
            click.echo(
                "Run 'tensorlake login' to authenticate, or see 'tensorlake --help' for API key options."
            )
        return

    data = {
        "endpoint": ctx.api_url,
        "organizationId": ctx.organization_id,
        "projectId": ctx.project_id,
    }

    if ctx.api_key_id is not None:
        data["apiKeyId"] = ctx.api_key_id

    if ctx.personal_access_token is not None:
        replacement = "*" * len(ctx.personal_access_token[:-6])
        data["personalAccessToken"] = replacement + ctx.personal_access_token[-6:]

    if output == "json":
        print(json.dumps(data))
        return

    click.echo(f"Dashboard Endpoint    : {ctx.cloud_url}")
    click.echo(f"API Endpoint          : {data['endpoint']}")
    click.echo(f"Organization ID       : {data['organizationId']}")
    click.echo(f"Project ID            : {data['projectId']}")
    if data.get("apiKeyId") is not None:
        click.echo(f"API Key ID            : {data['apiKeyId']}")
    if data.get("personalAccessToken") is not None:
        click.echo(f"Personal Access Token : {data['personalAccessToken']}")


def run_login_flow(ctx: Context, auto_init: bool = True) -> str:
    """
    Run the interactive login flow.

    Args:
        ctx: Context object with configuration
        auto_init: If True, automatically run init flow after login if org/project are missing

    Returns:
        The access token obtained from successful login

    Raises:
        click.ClickException: If login fails at any step
        click.Abort: If user cancels the login process
    """
    login_start_url = f"{ctx.api_url}/platform/cli/login/start"

    start_response = httpx.post(login_start_url)

    if not start_response.is_success:
        raise click.ClickException(
            f"Failed to start login process: {start_response.text}"
        )

    start_response_body = start_response.json()
    device_code = start_response_body["device_code"]
    user_code = start_response_body["user_code"]

    click.echo("We're going to open a web browser for you to enter a one-time code.")
    click.echo(f"Your code is: {user_code}")

    verification_uri = f"{ctx.cloud_url}/cli/login"
    click.echo(f"URL: {verification_uri}")
    click.echo("Opening web browser...")

    # Give people time to read the messages above
    time.sleep(5)

    try:
        webbrowser.open(verification_uri)
    except webbrowser.Error:
        click.echo(
            "Failed to open web browser. Please open the URL above manually and enter the code.",
            err=True,
        )

    click.echo("Waiting for the code to be processed...")

    poll_url = f"{ctx.api_url}/platform/cli/login/poll?device_code={device_code}"

    while True:
        poll_response = httpx.get(poll_url)

        if not poll_response.is_success:
            raise click.ClickException(
                f"Failed to poll login status: {poll_response.text}"
            )

        poll_response_body = poll_response.json()
        status = poll_response_body["status"]

        match status:
            case "pending":
                wait_time = 5
            case "expired":
                wait_time = 0
            case "failed":
                wait_time = 0
            case "approved":
                wait_time = 0
            case _:
                raise click.ClickException(f"Unknown status: {status}")

        if wait_time > 0:
            time.sleep(wait_time)

        if status == "expired":
            raise click.ClickException("Login request has expired. Please try again.")

        if status == "failed":
            raise click.ClickException("Login request has failed. Please try again.")

        if status == "approved":
            break

    exchange_token_url = f"{ctx.api_url}/platform/cli/login/exchange"

    exchange_response = httpx.post(
        exchange_token_url, json={"device_code": device_code}
    )

    if not exchange_response.is_success:
        raise click.ClickException(
            f"Failed to exchange token: {exchange_response.text}"
        )

    exchange_response_body = exchange_response.json()

    access_token = exchange_response_body["access_token"]
    save_credentials(ctx.api_url, access_token)
    click.echo("Login successful!")

    if auto_init:
        # After successful login, check if we need to run init
        # Recreate context with the new PAT to check if org/project are available
        updated_ctx = Context.default(
            api_url=ctx.api_url,
            cloud_url=ctx.cloud_url,
            personal_access_token=access_token,
            # Preserve CLI flags and env vars if they were provided
            organization_id=ctx.organization_id_from_cli,
            project_id=ctx.project_id_from_cli,
        )

        # Check if org/project are available from ANY source (CLI, env, local config)
        if not updated_ctx.has_org_and_project():
            click.echo(
                "\nNo organization and project configuration found. Let's set up your project.\n"
            )

            # Import here to avoid circular dependency
            from tensorlake.cli._project_detection import find_project_root
            from tensorlake.cli.init import run_init_flow

            # Detect project root automatically for login flow
            project_root = find_project_root()

            try:
                run_init_flow(
                    updated_ctx,
                    interactive=True,
                    create_local_config=True,
                    skip_if_provided=False,
                    project_root=project_root,
                )
            except click.Abort:
                click.echo(
                    "\nYou can run 'tensorlake init' later to complete the setup.",
                    err=True,
                )

    return access_token


@click.command(help="Login to TensorLake")
@pass_auth
def login(ctx: Context):
    """Login to TensorLake using device code flow."""
    run_login_flow(ctx, auto_init=True)
