import datetime
import importlib.metadata
import sys
from dataclasses import dataclass
from typing import Optional

import click
import httpx
from pydantic.json import pydantic_encoder
from rich import print, print_json
from rich.json import JSON
from rich.table import Table

from tensorlake.functions_sdk.http_client import Severity, Signal

try:
    VERSION = importlib.metadata.version("tensorlake")
except importlib.metadata.PackageNotFoundError:
    VERSION = "unknown"


from tensorlake.functions_sdk.http_client import TensorlakeClient

from .config import get_nested_value, load_config


@dataclass
class Context:
    """Class for CLI context."""

    base_url: str
    namespace: str
    api_key: Optional[str] = None
    default_graph: Optional[str] = None
    default_request: Optional[str] = None
    version: str = VERSION
    _client: Optional[httpx.Client] = None
    _introspect_response: Optional[httpx.Response] = None
    _tensorlake_client: Optional[TensorlakeClient] = None

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
    def tensorlake_client(self) -> TensorlakeClient:
        if self._tensorlake_client is None:
            self._tensorlake_client = TensorlakeClient(
                service_url=self.base_url,
                api_key=self.api_key,
                namespace=self.namespace,
            )
        return self._tensorlake_client

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
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        namespace: Optional[str] = None,
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
        final_default_graph = get_nested_value(config_data, "default.graph")
        final_default_request = get_nested_value(config_data, "default.request")

        return cls(
            base_url=final_base_url,
            api_key=final_api_key,
            namespace=final_namespace,
            default_graph=final_default_graph,
            default_request=final_default_request,
        )


"""Pass the Context object to the click command"""
pass_auth = click.make_pass_decorator(Context)


def print_signals(title: str, signals: list[Signal], use_json: bool, verbose: bool):
    if use_json:
        all_signals = json.dumps(signals, default=pydantic_encoder)
        print_json(all_signals)

    elif verbose:
        print(signals)

    else:
        if not signals:
            print("No logs found")
            return

        logs_table = Table(title=title)
        logs_table.add_column("Timestamp")
        logs_table.add_column("Severity")
        logs_table.add_column("Message")
        logs_table.add_column("Attributes")
        logs_table.add_column("Resource")

        for signal in signals:
            if hasattr(signal, "scope_logs"):
                # Format resource attributes
                resource_attrs = []
                if signal.resource is not None:
                    for attr in signal.resource.attributes:
                        if attr.value.string_value:
                            resource_attrs.append(
                                f"{attr.key}={attr.value.string_value}"
                            )
                        elif attr.value.int_value is not None:
                            resource_attrs.append(f"{attr.key}={attr.value.int_value}")
                        elif attr.value.bool_value is not None:
                            resource_attrs.append(f"{attr.key}={attr.value.bool_value}")

                for scope_logs in signal.scope_logs:
                    for log_record in scope_logs.log_records:
                        if (
                            log_record.time_unix_nano
                            and log_record.time_unix_nano != "0"
                        ):
                            log_timestamp = datetime.datetime.fromtimestamp(
                                int(log_record.time_unix_nano) / 1_000_000_000
                            ).strftime("%Y-%m-%dT%H:%M:%S.%f%z")

                        # Extract message from body
                        message = (
                            log_record.body.string_value if log_record.body else ""
                        )

                        # Format attributes
                        attrs = []
                        for attr in log_record.attributes:
                            if attr.value.string_value:
                                attrs.append(f"{attr.key}={attr.value.string_value}")
                            elif attr.value.int_value is not None:
                                attrs.append(f"{attr.key}={attr.value.int_value}")
                            elif attr.value.bool_value is not None:
                                attrs.append(f"{attr.key}={attr.value.bool_value}")

                        if isinstance(log_record.severity_number, Severity):
                            severity = log_record.severity_number.name
                        elif log_record.severity_number:
                            severity = log_record.severity_number
                        elif log_record.severity_text:
                            severity = log_record.severity_text
                        else:
                            severity = 0

                        logs_table.add_row(
                            log_timestamp,
                            severity,
                            message,
                            ", ".join(attrs) if attrs else "",
                            ", ".join(resource_attrs) if resource_attrs else "",
                        )

        print(logs_table)
