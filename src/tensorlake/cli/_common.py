import importlib.metadata
import json
import sys
from dataclasses import dataclass

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
            # Match Rust CLI auth behavior:
            # - API key auth: no forwarded org/project scope headers
            # - PAT auth: include forwarded org/project scope headers when available
            use_scope_headers = self.personal_access_token is not None and self.api_key is None
            self._cloud_client = CloudClient(
                api_url=self.api_url,
                api_key=bearer_token,
                organization_id=(
                    self.organization_id_value if use_scope_headers else None
                ),
                project_id=(self.project_id_value if use_scope_headers else None),
                namespace=self.namespace,
            )
        return self._cloud_client

    # Keep for backward compatibility with code that accesses this property.
    @property
    def rust_cloud_client(self) -> CloudClient:
        return self.cloud_client

    @property
    def api_key_id(self):
        return None

    @property
    def project_id(self):
        return self.project_id_value

    @property
    def organization_id(self):
        return self.organization_id_value

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
