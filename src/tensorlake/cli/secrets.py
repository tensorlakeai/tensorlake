from datetime import datetime, timezone
from typing import Dict, List, Tuple

import click
from rich.console import Console
from rich.table import Table

from tensorlake.cli._common import AuthContext, with_auth


@click.group()
def secrets():
    """
    Serverless Secrets Management

    Secrets are provided to compute graphs as environment variables. Names are case sensitive.
    """
    pass


@secrets.command()
@with_auth
def list(auth: AuthContext):
    """
    List all secrets in the current project.
    """

    secrets = _get_all_existing_secrets(auth)
    if len(secrets) == 0:
        click.echo("No secrets found")
        return

    table = Table()

    table.add_column("Name", no_wrap=True)
    table.add_column("Created At", style="green")

    for secret in secrets:
        created_at = datetime.fromisoformat(secret["createdAt"])
        # Convert to local time
        local_created_at = created_at.astimezone(
            datetime.now(timezone.utc).astimezone().tzinfo
        )
        local_created_at_iso = local_created_at.isoformat()

        table.add_row(secret["name"], local_created_at_iso)

    console = Console()
    console.print(table)
    if len(secrets) == 1:
        click.echo("1 secret")
    else:
        click.echo(f"{len(secrets)} secrets")


@secrets.command()
@click.argument("secrets", nargs=-1)
@with_auth
def set(auth: AuthContext, secrets: str):
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
    resp = auth.client.put(
        f"/platform/v1/organizations/{auth.organization_id}/projects/{auth.project_id}/secrets",
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
@with_auth
def unset(auth: AuthContext, secret_names: str):
    """
    Unset one or many secrets in the current project.

    Example:
        tensorlake secrets unset NAME1 NAME2
    """
    secrets = _get_all_existing_secrets(auth)
    secrets_dict = {s["name"]: s for s in secrets}
    num = 0
    for name in secret_names:
        if name not in secrets_dict:
            continue
        secret_id = secrets_dict[name]["id"]
        resp = auth.client.delete(
            f"/platform/v1/organizations/{auth.organization_id}/projects/{auth.project_id}/secrets/{secret_id}"
        )
        resp.raise_for_status()
        num += 1

    if num == 1:
        click.echo("1 secret unset")
    else:
        click.echo(f"{num} secrets unset")


def _get_all_existing_secrets(auth: AuthContext) -> List[dict]:
    resp = auth.client.get(
        f"/platform/v1/organizations/{auth.organization_id}/projects/{auth.project_id}/secrets?pageSize=100"
    )
    resp.raise_for_status()
    return resp.json()["items"]


def warning_missing_secrets(
    auth: AuthContext, secrets: List[str]
) -> Tuple[bool, List[str]]:
    existing_secrets = _get_all_existing_secrets(auth)
    existing_secret_names = [s["name"] for s in existing_secrets]
    missing_secrets = [s for s in secrets if s not in existing_secret_names]

    if len(missing_secrets) > 0:
        click.secho(
            f"Your Tensorlake project has missing secrets: {', '.join(missing_secrets)}. Graph invocations may fail until these secrets are set.",
            fg="yellow",
        )

    return len(missing_secrets) == 0, missing_secrets
