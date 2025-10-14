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
            if self.personal_access_token:
                headers["Authorization"] = f"Bearer {self.personal_access_token}"
            elif self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"
            else:
                raise click.UsageError(
                    "Missing API key or personal access token. Please run `tensorlake login` to authenticate."
                )

            self._client = httpx.Client(base_url=self.base_url, headers=headers)
        return self._client

    @property
    def api_client(self) -> APIClient:
        if self._api_client is None:
            bearer_token = self.personal_access_token
            if self.api_key:
                bearer_token = self.api_key

            self._api_client = APIClient(
                namespace=self.namespace,
                api_url=self.base_url,
                api_key=bearer_token,
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
        # Check new config key first, then fall back to old key for backward compatibility
        final_base_url = (
            base_url
            or get_nested_value(local_config_data, "tensorlake.api_url")
            or get_nested_value(global_config_data, "tensorlake.api_url")
            or get_nested_value(global_config_data, "indexify.url")
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

        # Priority: CLI/env > local config > global config > None
        # Note: local config uses root-level keys, global config uses default.* keys
        final_default_project = (
            project_id
            or get_nested_value(local_config_data, "project")
            or get_nested_value(global_config_data, "default.project")
        )
        final_default_organization = (
            organization_id
            or get_nested_value(local_config_data, "organization")
            or get_nested_value(global_config_data, "default.organization")
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
            x for x in self.list_commands(ctx)
            if x.lower().startswith(cmd_name.lower())
        ]

        if not matches:
            return None

        if len(matches) == 1:
            return super().get_command(ctx, matches[0])

        # Multiple matches - ambiguous
        ctx.fail(f"Ambiguous command '{cmd_name}'. Could be: {', '.join(sorted(matches))}")

    def resolve_command(self, ctx: click.Context, args: list[str]) -> tuple[str, click.Command, list[str]]:
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
