import importlib.metadata
import os
import sys
from dataclasses import dataclass
from typing import Optional

import click
import httpx

try:
    VERSION = importlib.metadata.version("tensorlake")
except importlib.metadata.PackageNotFoundError:
    VERSION = "unknown"


from tensorlake.http_client import TensorlakeClient


@dataclass
class AuthContext:
    """Class for CLI authentication context."""

    api_key: Optional[str] = None
    base_url: str = None
    version: str = VERSION
    _client: Optional[httpx.Client] = None
    _introspect_response: Optional[httpx.Response] = None
    _tensorlake_client: Optional[TensorlakeClient] = None

    def __post_init__(self):
        self.api_key = os.getenv("TENSORLAKE_API_KEY")
        self.base_url = os.getenv("INDEXIFY_URL", "https://api.tensorlake.ai")

    @property
    def client(self) -> httpx.Client:
        if self._client is None:
            self._client = httpx.Client(
                base_url=self.base_url,
                headers={
                    "Authorization": f"Bearer {self.valid_api_key()}",
                    "Accept": "application/json",
                    "User-Agent": f"Tensorlake CLI (python/{sys.version_info[0]}.{sys.version_info[1]} sdk/{self.version})",
                },
            )
        return self._client

    def valid_api_key(self) -> str:
        if not self.api_key:
            raise click.UsageError(
                "API key is not configured properly. The TENSORLAKE_API_KEY environment variable is required."
            )
        return self.api_key

    @property
    def tensorlake_client(self) -> TensorlakeClient:
        if self._tensorlake_client is None:
            self._tensorlake_client = TensorlakeClient(
                service_url=self.base_url,
                api_key=self.valid_api_key(),
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
                    "API key is not valid. Please check the TENSORLAKE_API_KEY environment variable."
                )
            introspect_response.raise_for_status()
            self._introspect_response = introspect_response
        return self._introspect_response


"""Pass the AuthContext object to the click command"""
pass_auth = click.make_pass_decorator(AuthContext)
