import json
import os
import time
import webbrowser
from pathlib import Path

import click
import httpx

from tensorlake.cli._common import Context, pass_auth
from tensorlake.cli._configuration import save_credentials


@click.group()
def auth():
    """
    Authentication commands
    """
    pass


@auth.command(help="Print authentication status")
@click.option(
    "--output",
    "-o",
    type=click.Choice(["text", "json"]),
    default="text",
    help="Output format",
)
@pass_auth
def status(ctx: Context, output: str):
    data = {
        "endpoint": ctx.base_url,
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


@auth.command(help="Login to TensorLake")
@pass_auth
def login(ctx: Context):
    login_start_url = f"{ctx.base_url}/platform/cli/login/start"

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
    click.echo("Opening web browser...")

    # Give people time to read the messages above
    time.sleep(5)

    verification_uri = f"{ctx.cloud_url}/cli/login"

    try:
        webbrowser.open(verification_uri)
    except webbrowser.Error:
        click.echo(
            "Failed to open web browser. Please open the following URL manually and enter the code:"
        )
        click.echo(verification_uri)

    click.echo("Waiting for the code to be processed...")

    poll_url = f"{ctx.base_url}/platform/cli/login/poll?device_code={device_code}"

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

    exchange_token_url = f"{ctx.base_url}/platform/cli/login/exchange"

    exchange_response = httpx.post(
        exchange_token_url, json={"device_code": device_code}
    )

    if not exchange_response.is_success:
        raise click.ClickException(
            f"Failed to exchange token: {exchange_response.text}"
        )

    exchange_response_body = exchange_response.json()

    access_token = exchange_response_body["access_token"]
    save_credentials(ctx.base_url, access_token)
    click.echo("Login successful!")
    click.echo(
        "Next, run `tensorlake config init` if you want to configure your CLI experience."
    )
