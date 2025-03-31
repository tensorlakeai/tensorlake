import importlib.metadata
import os
import sys
from dataclasses import dataclass

import click
import httpx

try:
    VERSION = importlib.metadata.version("tensorlake")
except importlib.metadata.PackageNotFoundError:
    VERSION = "unknown"


@dataclass
class AuthContext:
    """Class for CLI authentication context"""

    api_key: str | None = None
    version: str = VERSION
    _client: httpx.Client | None = None
    _introspect_response: httpx.Response | None = None

    def __post_init__(self):
        self.api_key = os.getenv("TENSORLAKE_API_KEY")

    @property
    def client(self) -> httpx.Client:
        if self._client is None:
            if not self.api_key:
                raise click.UsageError(
                    "API key is not configured properly. The INDEXIFY_URL environment variable is required."
                )

            base_url = os.getenv("INDEXIFY_URL", "https://api.tensorlake.ai")

            self._client = httpx.Client(
                base_url=base_url,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Accept": "application/json",
                    "User-Agent": f"Tensorlake CLI (python/{sys.version_info[0]}.{sys.version_info[1]} sdk/{self.version})",
                },
            )
        return self._client

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
with_auth = click.make_pass_decorator(AuthContext)
