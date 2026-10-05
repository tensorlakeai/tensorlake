"""Explicit production region selection for Git repositories and filesystems."""

from typing import Literal

ArtifactStorageRegion = Literal["us-east-1", "eu-central-1"]

_API_URLS = {
    "us-east-1": "https://api.tensorlake.ai",
    "eu-central-1": "https://api.eu-central-1.tensorlake.ai",
}


def resolve_artifact_storage_api_url(
    api_url: str, region: ArtifactStorageRegion | None = None
) -> str:
    """Route credentials and data together; never fall back to a different region."""
    if region is None:
        return api_url
    if region not in _API_URLS:
        raise ValueError(
            f"Unsupported Artifact Storage region: {region}; expected us-east-1 or eu-central-1"
        )
    if api_url.rstrip("/") not in _API_URLS.values():
        raise ValueError(
            "Artifact Storage region cannot be combined with a custom or development API URL"
        )
    return _API_URLS[region]
