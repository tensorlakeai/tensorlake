import importlib.metadata
import json
import sys
from dataclasses import dataclass
from typing import Any

from tensorlake.applications.interface.exceptions import RemoteAPIError, SDKUsageError
from tensorlake.cloud_client import CloudClient

try:
    VERSION = importlib.metadata.version("tensorlake")
except importlib.metadata.PackageNotFoundError:
    VERSION = "unknown"


def _cli_error(msg):
    print(f"Error: {msg}", file=sys.stderr)
    sys.exit(1)


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
    _introspect_response: dict[str, Any] | None = None
    _cloud_client: CloudClient | None = None
    organization_id_value: str | None = None
    project_id_value: str | None = None

    @property
    def cloud_client(self) -> CloudClient:
        if self._cloud_client is None:
            bearer_token = self.api_key or self.personal_access_token
            if bearer_token is None:
                _cli_error(
                    "Missing API key or personal access token. Please run `tensorlake login` to authenticate."
                )
            self._cloud_client = CloudClient(
                api_url=self.api_url,
                api_key=bearer_token,
                organization_id=self.organization_id_value,
                project_id=self.project_id_value,
                namespace=self.namespace,
            )
        return self._cloud_client

    # Keep for backward compatibility with code that accesses this property.
    @property
    def rust_cloud_client(self) -> CloudClient:
        return self.cloud_client

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
                response_json = self.cloud_client.introspect_api_key_json()
                self._introspect_response = json.loads(response_json)
            except SDKUsageError:
                print(
                    "The TensorLake API key is not valid.",
                    file=sys.stderr,
                )
                print(
                    "Please supply a valid API key with the `--api-key` flag, or run `tensorlake login` to authenticate.",
                    file=sys.stderr,
                )
                _cli_error("Invalid API key")
            except RemoteAPIError as e:
                status_code = e.status_code
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
            except Exception as e:
                if self.debug:
                    print(f"Error: {e}", file=sys.stderr)
                _cli_error("API key validation failed")
        return self._introspect_response

    def list_secret_names(self, page_size: int = 100) -> list[str]:
        org_id = self.organization_id
        project_id = self.project_id
        if org_id is None or project_id is None:
            return []

        try:
            response_json = self.cloud_client.list_secrets_json(
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
