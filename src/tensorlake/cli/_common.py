import importlib.metadata
import json
import sys
from dataclasses import dataclass
from typing import Any

import httpx

from tensorlake.applications.interface.exceptions import InternalError
from tensorlake.applications.remote.api_client import APIClient

try:
    from tensorlake_rust_cloud_sdk import CloudApiClient as RustCloudApiClient
    from tensorlake_rust_cloud_sdk import CloudApiClientError as RustCloudApiClientError

    _RUST_CLOUD_CLIENT_AVAILABLE = True
except Exception:
    RustCloudApiClient = None
    RustCloudApiClientError = None
    _RUST_CLOUD_CLIENT_AVAILABLE = False

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
    _introspect_response: dict[str, Any] | None = None
    _api_client: APIClient | None = None
    _rust_cloud_client: Any | None = None
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
    def rust_cloud_client(self):
        if self._rust_cloud_client is None:
            if not _RUST_CLOUD_CLIENT_AVAILABLE:
                _cli_error(
                    "Rust Cloud SDK client is required but unavailable. Run `make build_rust_py_client`."
                )
            bearer_token = self.api_key or self.personal_access_token
            if bearer_token is None:
                _cli_error(
                    "Missing API key or personal access token. Please run `tensorlake login` to authenticate."
                )
            try:
                self._rust_cloud_client = RustCloudApiClient(
                    api_url=self.api_url,
                    api_key=bearer_token,
                )
            except Exception as e:
                raise InternalError(str(e)) from e
        return self._rust_cloud_client

    @property
    def api_key_id(self):
        if self.api_key:
            return self._introspect().get("id")
        else:
            return None

    @property
    def project_id(self):
        if self.api_key:
            return self._introspect().get("projectId")
        return self.project_id_value

    @property
    def organization_id(self):
        if self.api_key:
            return self._introspect().get("organizationId")
        return self.organization_id_value

    def _introspect(self) -> dict[str, Any]:
        if self._introspect_response is None:
            try:
                response_json = self.rust_cloud_client.introspect_api_key_json()
                self._introspect_response = json.loads(response_json)
            except Exception as e:
                status_code: int | None = None
                if (
                    RustCloudApiClientError is not None
                    and isinstance(e, RustCloudApiClientError)
                    and len(e.args) > 0
                ):
                    if len(e.args) == 3:
                        _, status_code, _ = e.args
                    elif (
                        len(e.args) == 1
                        and isinstance(e.args[0], tuple)
                        and len(e.args[0]) == 3
                    ):
                        _, status_code, _ = e.args[0]

                if status_code == 401:
                    print(
                        "The TensorLake API key is not valid.",
                        file=sys.stderr,
                    )
                    print(
                        "Please supply a valid API key with the `--api-key` flag, or run `tensorlake login` to authenticate.",
                        file=sys.stderr,
                    )
                    _cli_error("Invalid API key")
                if status_code == 404:
                    print(
                        f"The server at {self.api_url} doesn't support TensorLake API introspection.",
                        file=sys.stderr,
                    )
                    print(
                        "Please check your API URL or contact support.",
                        file=sys.stderr,
                    )
                    _cli_error("API introspection not supported")

                if status_code is None:
                    status_code = 500
                print(f"Error validating API key: HTTP {status_code}", file=sys.stderr)

                if self.debug:
                    print("", file=sys.stderr)
                    print("Technical details:", file=sys.stderr)
                    print(
                        f"  URL: {self.api_url}/platform/v1/keys/introspect",
                        file=sys.stderr,
                    )
                    print(f"  Error: {e}", file=sys.stderr)
                else:
                    print("", file=sys.stderr)
                    print(
                        "For technical details, run with --debug or set TENSORLAKE_DEBUG=1",
                        file=sys.stderr,
                    )

                _cli_error(f"API key validation failed with status {status_code}")
        return self._introspect_response

    def list_secret_names(self, page_size: int = 100) -> list[str]:
        org_id = self.organization_id
        project_id = self.project_id
        if org_id is None or project_id is None:
            return []

        try:
            response_json = self.rust_cloud_client.list_secrets_json(
                organization_id=org_id,
                project_id=project_id,
                page_size=page_size,
            )
            payload = json.loads(response_json)
            return [item["name"] for item in payload.get("items", []) if "name" in item]
        except Exception:
            return []

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
