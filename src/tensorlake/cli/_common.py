import importlib.metadata
import json
import sys
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

import click
import httpx
from rich import print, print_json

try:
    VERSION = importlib.metadata.version("tensorlake")
except importlib.metadata.PackageNotFoundError:
    VERSION = "unknown"


from tensorlake.applications.remote.api_client import APIClient, LogEntry, LogsPayload
from tensorlake.cli._configuration import (
    get_nested_value,
    load_config,
    load_credentials,
    load_local_config,
)


@dataclass
class Context:
    """Class for CLI context."""

    base_url: str
    cloud_url: str
    namespace: str
    api_key: str | None = None
    personal_access_token: str | None = None
    default_application: str | None = None
    default_request: str | None = None
    default_project: str | None = None
    default_organization: str | None = None
    version: str = VERSION
    _client: httpx.Client | None = None
    _introspect_response: httpx.Response | None = None
    _api_client: APIClient | None = None

    @property
    def client(self) -> httpx.Client:
        if self._client is None:
            headers = {
                "Accept": "application/json",
                "User-Agent": f"Tensorlake CLI (python/{sys.version_info[0]}.{sys.version_info[1]} sdk/{self.version})",
            }
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"
            elif self.personal_access_token:
                headers["Authorization"] = f"Bearer {self.personal_access_token}"
                # Add X-Forwarded headers when using PAT (not needed for API keys)
                if self.organization_id:
                    headers["X-Forwarded-Organization-Id"] = self.organization_id
                if self.project_id:
                    headers["X-Forwarded-Project-Id"] = self.project_id
            else:
                raise click.UsageError(
                    "Missing API key or personal access token. Please run `tensorlake login` to authenticate."
                )

            self._client = httpx.Client(base_url=self.base_url, headers=headers)
        return self._client

    @property
    def api_client(self) -> APIClient:
        if self._api_client is None:
            bearer_token = self.api_key
            if not bearer_token:
                bearer_token = self.personal_access_token

            # Pass organization and project IDs to API client for X-Forwarded headers
            self._api_client = APIClient(
                namespace=self.namespace,
                api_url=self.base_url,
                api_key=bearer_token,
                organization_id=self.organization_id if not self.api_key else None,
                project_id=self.project_id if not self.api_key else None,
            )

        return self._api_client

    @property
    def api_key_id(self):
        if self.api_key:
            return self._introspect().json().get("id")
        else:
            return None

    @property
    def project_id(self):
        """
        Get the project ID associated with the API key, or from config if no API key is set.
        """
        if self.api_key:
            return self._introspect().json().get("projectId")

        return self.default_project

    @property
    def organization_id(self):
        """
        Get the organization ID associated with the API key, or from config if no API key is set.
        """
        if self.api_key:
            return self._introspect().json().get("organizationId")

        return self.default_organization

    def _introspect(self) -> httpx.Response:
        if self._introspect_response is None:
            introspect_response = self.client.post("/platform/v1/keys/introspect")
            if introspect_response.status_code == 401:
                raise click.UsageError(
                    "The Tensorlake API key is not valid. Please supply the API key with the `--api-key` flag, or run `tensorlake login` to authenticate you."
                )
            if introspect_response.status_code == 404:
                raise click.ClickException(
                    f"The server at {self.base_url} doesn't support Tensorlake API introspection"
                )
            introspect_response.raise_for_status()
            self._introspect_response = introspect_response
        return self._introspect_response

    def has_authentication(self) -> bool:
        """Check if any form of authentication is available."""
        return self.api_key is not None or self.personal_access_token is not None

    def has_org_and_project(self) -> bool:
        """Check if organization and project IDs are available from any source."""
        return self.organization_id is not None and self.project_id is not None

    def needs_init(self) -> bool:
        """
        Check if init flow should run.
        Init is needed if we have PAT but no org/project IDs.
        If using API key, org/project come from introspection, so init not needed.
        """
        if self.api_key:
            # API key provides org/project via introspection
            return False

        if not self.has_authentication():
            # No auth at all - need login first, not init
            return False

        # Have PAT but missing org/project
        return not self.has_org_and_project()

    @classmethod
    def default(
        cls,
        base_url: str | None = None,
        cloud_url: str | None = None,
        api_key: str | None = None,
        personal_access_token: str | None = None,
        namespace: str | None = None,
        organization_id: str | None = None,
        project_id: str | None = None,
    ) -> "Context":
        """Create a Context with values from CLI args, environment, saved config, or defaults."""
        # Load both local and global config
        local_config_data = load_local_config()
        global_config_data = load_config()

        # Use CLI/env values first, then local config, then global config, then hardcoded defaults
        final_base_url = (
            base_url
            or get_nested_value(local_config_data, "tensorlake.api_url")
            or get_nested_value(global_config_data, "tensorlake.api_url")
            or "https://api.tensorlake.ai"
        )

        final_cloud_url = (
            cloud_url
            or get_nested_value(local_config_data, "tensorlake.cloud_url")
            or get_nested_value(global_config_data, "tensorlake.cloud_url")
            or "https://cloud.tensorlake.ai"
        )

        final_api_key = (
            api_key
            or get_nested_value(local_config_data, "tensorlake.apikey")
            or get_nested_value(global_config_data, "tensorlake.apikey")
        )

        # Load PAT from credentials file (endpoint-scoped) if not provided via CLI/env
        file_personal_access_token = load_credentials(final_base_url)

        # Priority: CLI/env PAT > credentials file PAT
        final_personal_access_token = (
            personal_access_token or file_personal_access_token
        )

        final_namespace = (
            namespace
            or get_nested_value(local_config_data, "indexify.namespace")
            or get_nested_value(global_config_data, "indexify.namespace")
            or "default"
        )
        final_default_app = get_nested_value(
            local_config_data, "default.application"
        ) or get_nested_value(global_config_data, "default.application")
        final_default_request = get_nested_value(
            local_config_data, "default.request"
        ) or get_nested_value(global_config_data, "default.request")

        # Priority: CLI/env > local config > None
        # Note: Organization and project IDs are NOT loaded from global config
        # They must come from CLI flags, env vars, or local .tensorlake.toml only
        final_default_project = project_id or get_nested_value(
            local_config_data, "project"
        )
        final_default_organization = organization_id or get_nested_value(
            local_config_data, "organization"
        )

        return cls(
            base_url=final_base_url,
            cloud_url=final_cloud_url,
            api_key=final_api_key,
            personal_access_token=final_personal_access_token,
            namespace=final_namespace,
            default_application=final_default_app,
            default_request=final_default_request,
            default_project=final_default_project,
            default_organization=final_default_organization,
        )


"""Pass the Context object to the click command"""
pass_auth = click.make_pass_decorator(Context)


def require_auth_and_project(f):
    """
    Decorator that ensures authentication and org/project IDs are available.

    This decorator:
    1. Checks if authentication exists (API key or PAT)
    2. If using API key, org/project come from introspection automatically
    3. If using PAT but no org/project, automatically runs init flow
    4. If no auth at all, shows error message to run 'tensorlake login'

    Usage:
        @click.command()
        @require_auth_and_project
        def my_command(ctx: Context):
            # ctx.organization_id and ctx.project_id are guaranteed to be available
            pass
    """
    import functools

    @pass_auth
    @functools.wraps(f)
    def wrapper(ctx: Context, *args, **kwargs):
        # Check if we have any authentication
        if not ctx.has_authentication():
            # No authentication - automatically run login flow
            click.echo("It seems like you're not logged in. Let's log you in...\n")

            # Import here to avoid circular dependency
            from tensorlake.cli.auth import run_login_flow

            try:
                # Run login flow, which will also run init if needed
                run_login_flow(ctx, auto_init=True)

                # Reload context with new credentials
                # The login flow will have saved credentials and potentially created local config
                updated_ctx = Context.default(
                    base_url=ctx.base_url,
                    namespace=ctx.namespace,
                    # Let Context.default reload everything from saved credentials and config
                )

                # Verify authentication is now available
                if not updated_ctx.has_authentication():
                    raise click.UsageError(
                        "Authentication failed. Please try running 'tensorlake login' manually."
                    )

                # Verify org/project are now available
                if not updated_ctx.has_org_and_project():
                    raise click.UsageError(
                        "Organization and project configuration missing. Please run 'tensorlake init'."
                    )

                # Continue with the command using the updated context
                return f(updated_ctx, *args, **kwargs)

            except click.Abort:
                click.echo(
                    "\nAuthentication cancelled. Please run 'tensorlake login' to authenticate.",
                    err=True,
                )
                raise

        # If using API key, org/project come from introspection
        # If already have org/project from any source, we're good
        if ctx.has_org_and_project():
            return f(ctx, *args, **kwargs)

        # At this point: we have auth but no org/project
        # If using API key, something is wrong (should have gotten them from introspection)
        if ctx.api_key:
            raise click.UsageError(
                "API key is set but could not determine organization and project. "
                "Please check your API key or provide --organization and --project flags."
            )

        # We have PAT but no org/project - need to run init
        click.echo("Organization and project IDs are required for this command.")
        click.echo("Running initialization flow to set up your project...\n")

        # Import here to avoid circular dependency
        from tensorlake.cli._project_detection import find_project_root
        from tensorlake.cli.init import run_init_flow

        # Detect project root automatically
        project_root = find_project_root()

        try:
            org_id, proj_id = run_init_flow(
                ctx,
                interactive=True,
                create_local_config=True,
                skip_if_provided=False,
                project_root=project_root,
            )

            # Create a new context with the org/project IDs
            updated_ctx = Context.default(
                base_url=ctx.base_url,
                api_key=ctx.api_key,
                personal_access_token=ctx.personal_access_token,
                namespace=ctx.namespace,
                organization_id=org_id,
                project_id=proj_id,
            )

            return f(updated_ctx, *args, **kwargs)
        except click.Abort:
            click.echo(
                "\nInitialization aborted. Please run 'tensorlake init' to complete setup.",
                err=True,
            )
            raise

    return wrapper


class AliasedGroup(click.Group):
    """
    A Click Group that supports command aliases through prefix matching.

    This allows users to type abbreviated commands as long as they are unambiguous.
    For example, 'application' can be invoked as 'app', 'request' as 'req', etc.

    Example:
        tensorlake app list  -> tensorlake application list
        tensorlake req info  -> tensorlake request info
        tensorlake sec set   -> tensorlake secrets set
    """

    def get_command(self, ctx: click.Context, cmd_name: str) -> click.Command | None:
        """
        Get a command by name or prefix.

        First tries exact match, then falls back to prefix matching.
        If multiple commands match the prefix, returns None (ambiguous).
        """
        # Try exact match first
        rv = super().get_command(ctx, cmd_name)
        if rv is not None:
            return rv

        # Try prefix matching
        matches = [
            x for x in self.list_commands(ctx) if x.lower().startswith(cmd_name.lower())
        ]

        if not matches:
            return None

        if len(matches) == 1:
            return super().get_command(ctx, matches[0])

        # Multiple matches - ambiguous
        ctx.fail(
            f"Ambiguous command '{cmd_name}'. Could be: {', '.join(sorted(matches))}"
        )

    def resolve_command(
        self, ctx: click.Context, args: list[str]
    ) -> tuple[str, click.Command, list[str]]:
        """
        Resolve command name to always return the full command name, not the alias.

        This ensures that when users type 'tensorlake app list', the help text
        and error messages show 'application' instead of 'app'.
        """
        _, cmd, args = super().resolve_command(ctx, args)
        return cmd.name, cmd, args


START_LINE = "┏"
LINE = "┃"
END_LINE = "┗"


class LogFormat(Enum):
    COMPACT = "compact"
    EXPANDED = "expanded"
    LONG = "long"
    JSON = "json"


def print_application_logs(logs: LogsPayload, format: LogFormat):
    if format == LogFormat.LONG:
        print_text_logs(logs.logs)
    elif format == LogFormat.JSON:
        print_json_logs(logs.logs)
    elif format == LogFormat.COMPACT:
        print_pretty_logs(logs.logs)
    elif format == LogFormat.EXPANDED:
        print_pretty_logs(logs.logs, full=True)


def print_text_logs(logs: list[LogEntry]):
    if len(logs) == 0:
        return

    for log in logs:
        print(format_log_entry(log))


def print_json_logs(logs: list[LogEntry]):
    if len(logs) == 0:
        return

    for line in logs:
        print_json(line.model_dump_json(), sort_keys=True)


def format_log_entry(log: LogEntry) -> str:
    timestamp = format_timestamp(log.timestamp)
    keys = [
        "ai.tensorlake.function_name",
        "ai.tensorlake.container.id",
        "ai.tensorlake.request.id",
    ]
    attrs = {key: value for key, value in log.resource_attributes if key in keys}
    return f"{timestamp} {log.body} {attrs} {log.log_attributes}"


def print_pretty_logs(logs: list[LogEntry], full: bool = False):
    if len(logs) == 0:
        return

    for line in logs:
        sys.stdout.write(format_pretty_log_entry(line, full=full))


def format_timestamp(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp / 1_000_000_000).strftime(
        "%Y-%m-%dT%H:%M:%S%z"
    )


def format_pretty_log_entry(log: LogEntry, full: bool = False) -> str:
    """
    Format a single LogEntry in a human-friendly, colorized style.
    """
    ts = format_timestamp(log.timestamp)

    # extract common resource attributes
    resource = dict(log.resource_attributes or [])
    function_name = resource.get("ai.tensorlake.function_name")
    container_id = resource.get("ai.tensorlake.container.id")
    request_id = resource.get("ai.tensorlake.request.id")

    source = f"at {log.application}/{function_name}"
    if request_id:
        source += f":{request_id}"
    if container_id:
        source += f" [{container_id}]"

    attrs = (
        json.dumps(json.loads(log.log_attributes), indent=2, sort_keys=True)
        if full
        else None
    )

    second_line_prefix = LINE if attrs else END_LINE
    ts_dim = click.style(ts, dim=True)
    src_dim = click.style(source, dim=True, italic=True)

    message = f"{START_LINE} {log.body} {ts_dim}\n"
    message += f"{second_line_prefix} {src_dim}\n"

    if attrs:
        lines = attrs.splitlines()
        line_number = 1
        for line in lines[:-1]:
            line_dim = click.style(f"[{line_number:2d}] {line}", dim=True)
            message += f"{LINE} {line_dim}\n"
            line_number += 1
        line_dim = click.style(f"[{line_number:2d}] {lines[-1]}", dim=True)
        message += f"{END_LINE} {line_dim}\n"

    return message
