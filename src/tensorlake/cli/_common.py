import importlib.metadata
import sys
from dataclasses import dataclass

import httpx

from tensorlake.applications.remote.api_client import APIClient

try:
    VERSION = importlib.metadata.version("tensorlake")
except importlib.metadata.PackageNotFoundError:
    VERSION = "unknown"


def _cli_error(msg):
    print(f"Error: {msg}", file=sys.stderr)
    sys.exit(1)


def raise_on_authn_authz(response: httpx.Response):
    if response.status_code == 401:
        _cli_error("The credentials to access Tensorlake's API are not valid")
    elif response.status_code == 403:
        _cli_error(
            "The credentials to access Tensorlake's API are not authorized for this operation"
        )


async def raise_on_authn_authz_async(response: httpx.Response):
    raise_on_authn_authz(response)


HTTP_EVENT_HOOKS = {
    "response": [raise_on_authn_authz],
}

ASYNC_HTTP_EVENT_HOOKS = {
    "response": [raise_on_authn_authz_async],
}


@dataclass
class Context:
    """Class for CLI context."""

    api_url: str
    cloud_url: str
    namespace: str
    api_key: str | None = None
    personal_access_token: str | None = None
    version: str = VERSION
    debug: bool = False
    _client: httpx.Client | None = None
    _introspect_response: httpx.Response | None = None
    _api_client: APIClient | None = None
    organization_id_value: str | None = None
    project_id_value: str | None = None

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
                if self.organization_id:
                    headers["X-Forwarded-Organization-Id"] = self.organization_id
                if self.project_id:
                    headers["X-Forwarded-Project-Id"] = self.project_id
            else:
                _cli_error(
                    "Missing API key or personal access token. Please run `tensorlake login` to authenticate."
                )

            self._client = httpx.Client(
                base_url=self.api_url, headers=headers, event_hooks=HTTP_EVENT_HOOKS
            )
        return self._client

    @property
    def api_client(self) -> APIClient:
        if self._api_client is None:
            if self.api_key:
                bearer_token = self.api_key
                org_id = None
                proj_id = None
            else:
                bearer_token = self.personal_access_token
                org_id = self.organization_id
                proj_id = self.project_id

            self._api_client = APIClient(
                api_url=self.api_url,
                api_key=bearer_token,
                organization_id=org_id,
                project_id=proj_id,
                namespace=self.namespace,
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
        if self.api_key:
            return self._introspect().json().get("projectId")
        return self.project_id_value

    @property
    def organization_id(self):
        if self.api_key:
            return self._introspect().json().get("organizationId")
        return self.organization_id_value

    def _introspect(self) -> httpx.Response:
        if self._introspect_response is None:
            try:
                introspect_response = self.client.post("/platform/v1/keys/introspect")
                if introspect_response.status_code == 401:
                    print(
                        "The TensorLake API key is not valid.",
                        file=sys.stderr,
                    )
                    print(
                        "Please supply a valid API key with the `--api-key` flag, or run `tensorlake login` to authenticate.",
                        file=sys.stderr,
                    )
                    _cli_error("Invalid API key")
                if introspect_response.status_code == 404:
                    print(
                        f"The server at {self.api_url} doesn't support TensorLake API introspection.",
                        file=sys.stderr,
                    )
                    print(
                        "Please check your API URL or contact support.",
                        file=sys.stderr,
                    )
                    _cli_error("API introspection not supported")
                introspect_response.raise_for_status()
                self._introspect_response = introspect_response
            except httpx.HTTPStatusError as e:
                status_code = e.response.status_code
                print(f"Error validating API key: HTTP {status_code}", file=sys.stderr)

                if self.debug:
                    print("", file=sys.stderr)
                    print("Technical details:", file=sys.stderr)
                    print(
                        f"  Status: {status_code} {e.response.reason_phrase}",
                        file=sys.stderr,
                    )
                    print(f"  URL: {e.request.url}", file=sys.stderr)
                    if e.response.text:
                        print(f"  Response: {e.response.text}", file=sys.stderr)
                else:
                    print("", file=sys.stderr)
                    print(
                        "For technical details, run with --debug or set TENSORLAKE_DEBUG=1",
                        file=sys.stderr,
                    )

                _cli_error(f"API key validation failed with status {status_code}")
        return self._introspect_response

    def has_authentication(self) -> bool:
        return self.api_key is not None or self.personal_access_token is not None

    def has_org_and_project(self) -> bool:
        return self.organization_id is not None and self.project_id is not None

    @classmethod
    def _resolve_cloud_url_from_api_url(cls, api_url: str) -> str:
        return (
            api_url.replace("https://api.tensorlake.", "https://cloud.tensorlake.")
            if api_url.startswith("https://api.tensorlake.")
            else "https://cloud.tensorlake.ai"
        )

    @classmethod
    def default(
        cls,
        api_url: str | None = None,
        cloud_url: str | None = None,
        api_key: str | None = None,
        personal_access_token: str | None = None,
        namespace: str | None = None,
        organization_id: str | None = None,
        project_id: str | None = None,
        debug: bool = False,
    ) -> "Context":
        """Create a Context from arguments (typically env vars set by the Rust CLI)."""
        final_api_url = api_url or "https://api.tensorlake.ai"
        final_cloud_url = cloud_url or cls._resolve_cloud_url_from_api_url(
            final_api_url
        )
        final_namespace = namespace or "default"

        return cls(
            api_url=final_api_url,
            cloud_url=final_cloud_url,
            api_key=api_key,
            personal_access_token=personal_access_token,
            namespace=final_namespace,
            debug=debug,
            organization_id_value=organization_id,
            project_id_value=project_id,
        )
