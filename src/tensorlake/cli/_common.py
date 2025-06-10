import importlib.metadata
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

    base_url: str
    namespace: str
    api_key: Optional[str] = None
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


"""Pass the AuthContext object to the click command"""
pass_auth = click.make_pass_decorator(AuthContext)
