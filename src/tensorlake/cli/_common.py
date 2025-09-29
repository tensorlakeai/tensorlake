import importlib.metadata
import json
import sys
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

import click
import httpx
from pydantic.json import pydantic_encoder
from rich import print, print_json

try:
    VERSION = importlib.metadata.version("tensorlake")
except importlib.metadata.PackageNotFoundError:
    VERSION = "unknown"


from tensorlake.applications.remote.api_client import APIClient
from tensorlake.functions_sdk.http_client import LogEntry, LogsPayload, TensorlakeClient

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


class LogFormat(Enum):
    TEXT = "text"
    JSON = "json"


def print_application_logs(logs: LogsPayload, format: LogFormat):
    if format == LogFormat.TEXT:
        print_text_logs(logs.logs)
    elif format == LogFormat.JSON:
        print_json_logs(logs.logs)


def print_text_logs(logs: list[LogEntry]):
    if len(logs) == 0:
        return

    for log in logs:
        print(f"{format_log_entry(log)}")


def print_json_logs(logs: list[LogEntry]):
    if len(logs) == 0:
        return

    print_json([json.dumps(log, default=pydantic_encoder) for log in logs])


def format_log_entry(log: LogEntry) -> str:
    timestamp = format_timestamp(log.timestamp)
    func = [
        attr
        for attr in log.resource_attributes
        if attr[0] == "ai.tensorlake.function_name"
    ][0][1]
    container = [
        attr
        for attr in log.resource_attributes
        if attr[0] == "ai.tensorlake.container.id"
    ][0][1]
    request = [
        attr
        for attr in log.resource_attributes
        if attr[0] == "ai.tensorlake.request.id"
    ]

    if request:
        return f"{timestamp} [{func} on {container}]@{request[0][1]} {log.body} {log.log_attributes}"
    else:
        return f"{timestamp} [{func} on {container}] {log.body} {log.log_attributes}"


def format_timestamp(timestamp: int) -> str:
    datetime.fromtimestamp(timestamp / 1_000_000_000).strftime("%Y-%m-%dT%H:%M:%S.%f%z")
