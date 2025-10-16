from datetime import datetime, timezone
from typing import Dict, List, Tuple

import click
import httpx
from rich.console import Console
from rich.table import Table

from tensorlake.cli._common import Context, require_auth_and_project
from tensorlake.cli._errors import handle_http_error


@click.group()
def secrets():
    """
    Serverless Secrets Management

    Secrets are provided to applications as environment variables. Names are case sensitive.
    """
    pass


@secrets.command()
@require_auth_and_project
def list(ctx: Context):
    """
    List all secrets in the current project.
    """

    secrets = _get_all_existing_secrets(ctx)
    if len(secrets) == 0:
        click.echo("No secrets found")
        return

    table = Table()

    table.add_column("Name", no_wrap=True)
    table.add_column("Created At", style="green")

    for secret in secrets:
        # FIXME: format createdAt using iso format.
        # created_at = datetime.fromisoformat(secret["createdAt"])
        # # Convert to local time
        # local_created_at = created_at.astimezone(
        #     datetime.now(timezone.utc).astimezone().tzinfo
        # )
        # local_created_at_iso = local_created_at.isoformat()

        table.add_row(secret["name"], secret["createdAt"])

    console = Console()
    console.print(table)
    if len(secrets) == 1:
        click.echo("1 secret")
    else:
        click.echo(f"{len(secrets)} secrets")


@secrets.command()
@click.argument("secrets", nargs=-1)
@require_auth_and_project
def set(ctx: Context, secrets: str):
    """
    Set one of many secrets in the current project.

    Example:
        tensorlake secrets set MY_NAME=MY_VALUE OTHER_NAME=OTHER_VALUE "NAME3=VALUE WITH SPACES"
    """

    # Validate secrets
    upsert_secrets: List[Dict] = []
    for set_str in secrets:
        if "=" not in set_str:
            raise click.UsageError(f"Invalid secret format {set_str}, missing '='")

        [name, value] = set_str.split("=", maxsplit=1)

        if not name or len(name) == 0:
            raise click.UsageError(f"Invalid secret format {set_str}, missing name")

        if " " in name:
            raise click.UsageError(
                f"Invalid secret name {name}, spaces are not allowed"
            )

        if name in [s["name"] for s in upsert_secrets]:
            raise click.UsageError(f"Duplicate secret name: {name}")

        upsert_secrets.append({"name": name, "value": value})

    # Upsert secrets
    resp = ctx.client.put(
        f"/platform/v1/organizations/{ctx.organization_id}/projects/{ctx.project_id}/secrets",
        json=upsert_secrets,
    )
    if resp.status_code >= 400 and resp.status_code < 500:
        error_message = resp.json().get("message", "Unknown error")
        click.echo(f"Error: {error_message}")
        return
    resp.raise_for_status()

    if len(upsert_secrets) == 1:
        click.echo("1 secret set")
    else:
        click.echo(f"{len(upsert_secrets)} secrets set")


@secrets.command()
@click.argument("secret_names", nargs=-1)
@require_auth_and_project
def unset(ctx: Context, secret_names: str):
    """
    Unset one or many secrets in the current project.

    Example:
        tensorlake secrets unset NAME1 NAME2
    """
    secrets = _get_all_existing_secrets(ctx)
    secrets_dict = {s["name"]: s for s in secrets}
    num = 0
    for name in secret_names:
        if name not in secrets_dict:
            continue
        secret_id = secrets_dict[name]["id"]
        resp = ctx.client.delete(
            f"/platform/v1/organizations/{ctx.organization_id}/projects/{ctx.project_id}/secrets/{secret_id}"
        )
        resp.raise_for_status()
        num += 1

    if num == 1:
        click.echo("1 secret unset")
    else:
        click.echo(f"{num} secrets unset")


def _get_all_existing_secrets(ctx: Context) -> List[dict]:
    try:
        resp = ctx.client.get(
            f"/platform/v1/organizations/{ctx.organization_id}/projects/{ctx.project_id}/secrets?pageSize=100"
        )
        resp.raise_for_status()
        return resp.json()["items"]
    except httpx.HTTPStatusError as e:
        handle_http_error(e, ctx, "fetching secrets")
        return []  # Unreachable, but satisfies type checker


def warning_missing_secrets(
    auth: Context, secrets: List[str]
) -> Tuple[bool, List[str]]:
    existing_secrets = _get_all_existing_secrets(auth)
    existing_secret_names = [s["name"] for s in existing_secrets]
    missing_secrets = [s for s in secrets if s not in existing_secret_names]

    if len(missing_secrets) > 0:
        click.echo(
            f"Your Tensorlake project has missing secrets: {', '.join(missing_secrets)}. Application invocations may fail until these secrets are set.",
        )

    return len(missing_secrets) == 0, missing_secrets
