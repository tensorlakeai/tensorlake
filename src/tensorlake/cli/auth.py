import json
import os
import time
import webbrowser
from datetime import datetime, timedelta
from pathlib import Path

import click
import httpx

from tensorlake.cli._common import Context, pass_auth

CONFIG_DIR = Path.home() / ".config" / "tensorlake"
CREDENTIALS_PATH = CONFIG_DIR / "credentials.json"


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
    if output == "json":
        print(
            json.dumps(
                {
                    "organizationId": ctx.organization_id,
                    "projectId": ctx.project_id,
                    "apiKeyId": ctx.api_key_id,
                }
            )
        )
        return
    click.echo(f"Organization ID: {ctx.organization_id}")
    click.echo(f"Project ID     : {ctx.project_id}")
    click.echo(f"API Key ID     : {ctx.api_key_id}")


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

    click.echo(f"Your code is: {user_code}")
    click.echo("Opening web browser...")

    verification_uri = f"{ctx.cloud_url}/cli/login"

    try:
        webbrowser.open(verification_uri)
    except webbrowser.Error:
        click.echo(
            "Failed to open web browser. Please open the following URL manually:"
        )
        click.echo(verification_uri)
        click.echo(f"Enter the code: {user_code}")

    click.echo("A web browser has been opened for you to log in.")

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
            click.echo(
                f"Waiting for approval... (checking again in {wait_time} seconds)"
            )

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
    _save_credentials(access_token)
    click.echo("Login successful!")


def _save_credentials(token: str):
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)

    config = {"token": token}
    with open(CREDENTIALS_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f)

    os.chmod(CREDENTIALS_PATH, 0o600)
