import importlib.metadata
import json
import os
import sys
from asyncio.unix_events import SelectorEventLoop
from cmath import e
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

import click
import httpx
from rich import print, print_json

try:
    VERSION = importlib.metadata.version("tensorlake")
except importlib.metadata.PackageNotFoundError:
    VERSION = "unknown"


from tensorlake.applications.remote.api_client import APIClient, LogEntry, LogsPayload

from .config import get_nested_value, load_config


@dataclass
class Context:
    """Class for CLI context."""

    base_url: str
    namespace: str
    api_key: str | None = None
    default_application: str | None = None
    default_request: str | None = None
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
            self._client = httpx.Client(base_url=self.base_url, headers=headers)
        return self._client

    @property
    def api_client(self) -> APIClient:
        if self._api_client is None:
            self._api_client = APIClient(
                namespace=self.namespace,
                api_url=self.base_url,
                api_key=self.api_key,
            )
        return self._api_client

    @property
    def api_key_id(self):
        return self._introspect().json().get("id")

    @property
    def project_id(self):
        return self._introspect().json().get("projectId")

    @property
    def organization_id(self):
        return self._introspect().json().get("organizationId")

    def _introspect(self) -> httpx.Response:
        if self._introspect_response is None:
            introspect_response = self.client.post("/platform/v1/keys/introspect")
            if introspect_response.status_code == 401:
                raise click.UsageError(
                    "The Tensorlake API key is not valid. Please supply the API key to use, either via the TENSORLAKE_API_KEY environment variable or the --api-key command-line argument."
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
        api_key: str | None = None,
        namespace: str | None = None,
    ) -> "Context":
        """Create a Context with values from CLI args, environment, saved config, or defaults."""
        config_data = load_config()

        # Use CLI/env values first, then saved config, then hardcoded defaults
        final_base_url = (
            base_url
            or get_nested_value(config_data, "indexify.url")
            or "https://api.tensorlake.ai"
        )
        final_api_key = api_key or get_nested_value(config_data, "tensorlake.apikey")
        final_namespace = (
            namespace
            or get_nested_value(config_data, "indexify.namespace")
            or "default"
        )
        final_default_app = get_nested_value(config_data, "default.application")
        final_default_request = get_nested_value(config_data, "default.request")

        return cls(
            base_url=final_base_url,
            api_key=final_api_key,
            namespace=final_namespace,
            default_application=final_default_app,
            default_request=final_default_request,
        )


"""Pass the Context object to the click command"""
pass_auth = click.make_pass_decorator(Context)


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
