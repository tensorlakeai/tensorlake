"""Client for interacting with the image builder service."""

import logging
import os
from dataclasses import dataclass, field
from typing import Any, AsyncGenerator
from urllib.parse import quote

from nanoid import generate as nanoid_generate
import click
import httpx
from httpx_sse import aconnect_sse
from pydantic import BaseModel

from tensorlake.cli._common import ASYNC_HTTP_EVENT_HOOKS

# Enable httpx debug logging if requested via environment variable
if os.getenv("TENSORLAKE_HTTPX_DEBUG", "").lower() in ("1", "true", "yes"):
    # Configure logging for httpx and httpcore
    httpx_logger = logging.getLogger("httpx")
    httpcore_logger = logging.getLogger("httpcore")

    # Set log level to DEBUG
    httpx_logger.setLevel(logging.DEBUG)
    httpcore_logger.setLevel(logging.DEBUG)

    # Ensure there's a handler to output the logs
    if not httpx_logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        httpx_logger.addHandler(handler)

    if not httpcore_logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        httpcore_logger.addHandler(handler)

# ============================================================================
# Options
# ============================================================================


# Sentinel object to distinguish between "not provided" and "explicitly None"
class _NotProvided:
    pass


_NOT_PROVIDED = _NotProvided()


@dataclass
class ImageBuilderClientV3Options:
    """Options for configuring the ImageBuilderClientV3."""

    _base_url: str = field(init=False)
    _api_key: str | None = field(default=None, init=False)
    _pat: str | None = field(default=None, init=False)
    _organization_id: str | None = field(default=None, init=False)
    _project_id: str | None = field(default=None, init=False)

    def __init__(
        self,
        base_url: str,
        api_key: str | None = None,
        pat: str | None = None,
        organization_id: str | None = None,
        project_id: str | None = None,
    ):
        """Initialize ImageBuilderClientV3Options."""
        self._base_url = base_url
        self._api_key = api_key
        self._pat = pat
        self._organization_id = organization_id
        self._project_id = project_id

    @property
    def base_url(self) -> str:
        """The base URL of the build service."""
        return self._base_url

    @property
    def api_key(self) -> str | None:
        """The API key for authentication."""
        return self._api_key

    @property
    def pat(self) -> str | None:
        """The Personal Access Token for authentication."""
        return self._pat

    def replace(
        self,
        *,
        base_url: str | None | Any = _NOT_PROVIDED,
        api_key: str | None | Any = _NOT_PROVIDED,
        pat: str | None | Any = _NOT_PROVIDED,
        organization_id: str | None | Any = _NOT_PROVIDED,
        project_id: str | None | Any = _NOT_PROVIDED,
    ) -> "ImageBuilderClientV3Options":
        """Create a new instance with updated values."""
        return ImageBuilderClientV3Options(
            base_url=base_url if base_url is not _NOT_PROVIDED else self._base_url,
            api_key=api_key if api_key is not _NOT_PROVIDED else self._api_key,
            pat=pat if pat is not _NOT_PROVIDED else self._pat,
            organization_id=(
                organization_id
                if organization_id is not _NOT_PROVIDED
                else self._organization_id
            ),
            project_id=(
                project_id if project_id is not _NOT_PROVIDED else self._project_id
            ),
        )

    def validate(self) -> None:
        """Validate the options configuration."""
        # Access underlying private fields for validation
        api_key = self._api_key
        pat = self._pat
        organization_id = self._organization_id
        project_id = self._project_id

        # Must provide exactly one authentication method
        has_api_key = api_key is not None
        has_pat = pat is not None

        if not has_api_key and not has_pat:
            raise ValueError(
                "Either api_key or pat must be provided for authentication"
            )

        if has_api_key and has_pat:
            raise ValueError(
                "Cannot provide both api_key and pat. Use one authentication method."
            )

        # Check if org/project IDs are both None or both provided (not mixed)
        org_id_is_none = organization_id is None
        project_id_is_none = project_id is None

        if org_id_is_none != project_id_is_none:
            # Mixed state: one is None, one is not
            raise ValueError(
                "organization_id and project_id must both be None (API key auth) "
                "or both be provided (PAT auth). Cannot mix None and non-None values."
            )

        if has_api_key:
            # API key authentication: org/project IDs must be None
            if not org_id_is_none:
                raise ValueError(
                    "When using API key authentication, both organization_id and project_id "
                    "must be None. API keys already contain org/project info via introspection."
                )
        else:
            # PAT authentication: org/project IDs must be provided
            if org_id_is_none:
                raise ValueError(
                    "When using PAT authentication, both organization_id and project_id "
                    "must be provided (non-empty strings)."
                )
            if not organization_id or not project_id:
                raise ValueError(
                    "When using PAT authentication, both organization_id and project_id "
                    "must be non-empty strings."
                )

    @property
    def bearer_token(self) -> str:
        """The bearer token for authentication."""
        token = self._api_key or self._pat
        assert (
            token is not None
        ), "bearer_token should always be set (call validate() first)"
        return token

    @property
    def organization_id(self) -> str | None:
        """The organization ID."""
        if self._api_key is not None:
            return None
        return self._organization_id

    @property
    def project_id(self) -> str | None:
        """The project ID."""
        if self._api_key is not None:
            return None
        return self._project_id

    @classmethod
    def from_env(cls) -> "ImageBuilderClientV3Options":
        """Create an instance from environment variables."""
        api_key = os.getenv("TENSORLAKE_API_KEY")
        pat = None
        organization_id = os.getenv("TENSORLAKE_ORGANIZATION_ID")
        project_id = os.getenv("TENSORLAKE_PROJECT_ID")

        # If no API key, try PAT
        if not api_key:
            pat = os.getenv("TENSORLAKE_PAT")

        server_url = os.getenv("TENSORLAKE_API_URL", "https://api.tensorlake.ai")
        base_url = os.getenv("TENSORLAKE_BUILD_SERVICE", f"{server_url}/images/v3")
        return cls(
            base_url=base_url,
            api_key=api_key,
            pat=pat,
            organization_id=organization_id,
            project_id=project_id,
        )


# ============================================================================
# Type Aliases (for documentation purposes)
# ============================================================================
# Public Response Models (Dataclasses)
# ============================================================================


@dataclass
class ImageBuildInfoV3:
    """Image build information."""

    id: str
    application_version_id: str
    key: str | None = None
    name: str | None = None
    function_names: list[str] = field(default_factory=list)
    status: str = ""
    created_at: str = ""
    updated_at: str = ""
    finished_at: str | None = None
    error_message: str | None = None


class ImageBuildsMap:
    """Dictionary-like container for image builds accessible by ID or client key."""

    def __init__(self, builds: list[ImageBuildInfoV3]):
        """Initialize the ImageBuildsMap from a list of builds."""
        self._by_id: dict[str, ImageBuildInfoV3] = {}
        self._by_key: dict[str, ImageBuildInfoV3] = {}

        for build in builds:
            self._by_id[build.id] = build
            if build.key is not None:
                self._by_key[build.key] = build

    def get_by_id(self, build_id: str) -> ImageBuildInfoV3 | None:
        """Get a build by its image build ID."""
        return self._by_id.get(build_id)

    def get_by_key(self, key: str) -> ImageBuildInfoV3 | None:
        """Get a build by its client key."""
        return self._by_key.get(key)

    def get(
        self, identifier: str | None
    ) -> ImageBuildInfoV3 | None:
        """Get a build by either ID or key."""
        if identifier is None:
            return None
        # Try ID first (more specific)
        if identifier in self._by_id:
            return self._by_id[identifier]
        # Then try key
        if identifier in self._by_key:
            return self._by_key.get(identifier)
        return None

    def values(self):
        """Return all image builds."""
        return self._by_id.values()

    def items(self):
        """Return all image builds as (id, build) pairs."""
        return self._by_id.items()

    def __getitem__(
        self, identifier: str
    ) -> ImageBuildInfoV3:
        """Get a build by either ID or key using dictionary syntax."""
        result = self.get(identifier)
        if result is None:
            raise KeyError(f"Image build not found: {identifier}")
        return result

    def __contains__(self, identifier: str) -> bool:
        """Check if a build exists with the given identifier."""
        if identifier in self._by_id:
            return True
        if identifier in self._by_key:
            return True
        return False

    def __len__(self) -> int:
        """Return the number of builds."""
        return len(self._by_id)

    def __iter__(self):
        """Iterate over image build IDs."""
        return iter(self._by_id)


# ============================================================================
# Public Request Models (Dataclasses)
# ============================================================================


@dataclass
class ImageBuildRequestV3:
    """Request to build an image."""

    key: str
    name: str | None
    description: str | None
    context_tar_content: bytes
    function_names: list[str]


@dataclass
class ApplicationVersionBuildRequestV3:
    """Request for building an application version."""

    name: str
    version: str
    images: list[ImageBuildRequestV3]


# ============================================================================
# Public Response Models (Dataclasses)
# ============================================================================


@dataclass
class ApplicationVersionBuildInfoV3:
    """Application version build information."""

    id: str
    name: str
    version: str
    image_builds: ImageBuildsMap


# ============================================================================
# Public Event Models (Dataclasses)
# ============================================================================


@dataclass
class ImageBuildLogEventV3:
    """Log event from the image builder service."""

    image_build_id: str
    timestamp: str
    stream: str
    message: str
    sequence_number: int
    build_status: str


# ============================================================================
# Internal Pydantic Models (Payload)
# ============================================================================


class _ImageBuildInfoPayload(BaseModel):
    """Internal Pydantic model for API deserialization."""

    id: str
    app_version_id: str | None = None
    name: str | None
    description: str | None = None
    key: str | None = None
    function_names: list[str]
    status: str
    error_message: str | None = None
    created_at: str
    updated_at: str
    finished_at: str | None


class _ImageBuildRequestPayload(BaseModel):
    """Internal Pydantic model for API serialization."""

    key: str
    name: str | None
    description: str | None
    context_tar_part_name: str
    function_names: list[str]

    @classmethod
    def from_request(cls, req: ImageBuildRequestV3) -> "_ImageBuildRequestPayload":
        """Create from public ImageBuildRequestV3."""
        return cls(
            key=req.key,
            name=req.name,
            description=req.description,
            context_tar_part_name=nanoid_generate(),
            function_names=req.function_names,
        )


class _ApplicationVersionBuildRequestPayload(BaseModel):
    """Internal Pydantic model for API serialization."""

    name: str
    version: str
    images: list[_ImageBuildRequestPayload]

    @classmethod
    def from_request(
        cls, req: ApplicationVersionBuildRequestV3
    ) -> "_ApplicationVersionBuildRequestPayload":
        """Create from public ApplicationVersionBuildRequestV3."""
        return cls(
            name=req.name,
            version=req.version,
            images=[_ImageBuildRequestPayload.from_request(img) for img in req.images],
        )


class _ApplicationVersionBuildInfoPayload(BaseModel):
    """Internal Pydantic model for API deserialization."""

    id: str
    organization_id: str
    project_id: str
    name: str
    version: str
    image_builds: list[_ImageBuildInfoPayload]


# ============================================================================
# Custom Exceptions
# ============================================================================


class ImageBuilderClientV3Error(Exception):
    """Base exception for image builder v3 client errors."""

    request_id: str | None

    def __init__(self, message: str, request_id: str | None = None):
        """Initialize ImageBuilderClientV3Error."""
        super().__init__(message)
        self.message = message
        self.request_id = request_id

    def __str__(self) -> str:
        """Return error message with request ID if available."""
        if self.request_id:
            return f"{self.message} (Request ID: {self.request_id})"
        return self.message


class ImageBuilderClientV3NetworkError(ImageBuilderClientV3Error):
    """Exception for network errors."""

    def __init__(self, original_error: Exception, request_id: str | None = None):
        """Initialize ImageBuilderClientV3NetworkError."""
        message = f"Network error while communicating with image builder service: {original_error}"
        super().__init__(message, request_id=request_id)
        self.original_error = original_error


class ImageBuilderClientV3NotFoundError(ImageBuilderClientV3Error):
    """Exception for when a requested resource is not found."""

    def __init__(
        self, resource_type: str, resource_id: str, request_id: str | None = None
    ):
        """Initialize ImageBuilderClientV3NotFoundError."""
        message = f"{resource_type} not found: {resource_id}"
        super().__init__(message, request_id=request_id)
        self.resource_type = resource_type
        self.resource_id = resource_id


class ImageBuilderClientV3BadRequestError(ImageBuilderClientV3Error):
    """Exception for when the request is invalid."""

    def __init__(self, message: str, request_id: str | None = None):
        """Initialize ImageBuilderClientV3BadRequestError."""
        super().__init__(message, request_id=request_id)


# ============================================================================
# Helper Functions
# ============================================================================


def _image_build_info_from_payload(
    data: _ImageBuildInfoPayload,
    key: str | None = None,
    app_version_id: str | None = None,
) -> ImageBuildInfoV3:
    """Convert internal API model to public dataclass."""
    # Use app_version_id from parameter if provided, otherwise from payload, or raise if neither
    version_id = app_version_id or data.app_version_id
    if version_id is None:
        raise ValueError(
            "app_version_id must be provided either in payload or as parameter"
        )

    return ImageBuildInfoV3(
        id=data.id,
        application_version_id=version_id,
        key=key if key else None,
        name=data.name,
        status=data.status,
        function_names=data.function_names,
        created_at=data.created_at,
        updated_at=data.updated_at,
        finished_at=data.finished_at,
        error_message=data.error_message,
    )


def _application_version_build_info_from_payload(
    data: _ApplicationVersionBuildInfoPayload,
) -> ApplicationVersionBuildInfoV3:
    """Convert internal API model to public dataclass."""
    builds = [
        _image_build_info_from_payload(img, key=img.key, app_version_id=data.id)
        for img in data.image_builds
    ]
    return ApplicationVersionBuildInfoV3(
        id=data.id,
        name=data.name,
        version=data.version,
        image_builds=ImageBuildsMap(builds),
    )


def _image_build_log_event_from_json(data: dict) -> ImageBuildLogEventV3:
    """Parse JSON dict and create ImageBuildLogEventV3 dataclass."""
    return ImageBuildLogEventV3(
        image_build_id=data["image_build_id"],
        timestamp=data["timestamp"],
        stream=data["stream"],
        message=data["message"],
        sequence_number=data["sequence_number"],
        build_status=data["build_status"],
    )


# ============================================================================
# Client
# ============================================================================


class ImageBuilderClientV3:
    """Client for interacting with the image builder service."""

    def __init__(self, options: ImageBuilderClientV3Options):
        """Initialize the ImageBuilderClientV3."""
        # Validate the options
        options.validate()

        self._client = httpx.AsyncClient(
            base_url=options.base_url, event_hooks=ASYNC_HTTP_EVENT_HOOKS
        )
        self._base_headers = {}

        # Set Authorization header with bearer token
        if options.bearer_token:
            self._base_headers["Authorization"] = f"Bearer {options.bearer_token}"
        if options.organization_id:
            self._base_headers["X-Forwarded-Organization-Id"] = options.organization_id
        if options.project_id:
            self._base_headers["X-Forwarded-Project-Id"] = options.project_id

    def _generate_request_id(self) -> str:
        """Generate a new request ID."""
        return nanoid_generate()

    def _get_headers_with_request_id(self, request_id: str) -> dict[str, str]:
        """Get headers with the provided X-Request-Id."""
        headers = self._base_headers.copy()
        headers["X-Request-Id"] = request_id
        return headers

    async def build_app(
        self, request: ApplicationVersionBuildRequestV3
    ) -> ApplicationVersionBuildInfoV3:
        """Build an application version and its images."""
        request_payload = _ApplicationVersionBuildRequestPayload.from_request(request)

        image_requests_by_key = {r.key: r for r in request.images}
        image_request_payloads_by_key = {r.key: r for r in request_payload.images}

        files = {
            "app_version": (
                "app_version",
                request_payload.model_dump_json().encode("utf-8"),
                "application/json",
            )
        }

        for key, image_request in image_requests_by_key.items():
            image_request_payload = image_request_payloads_by_key[key]
            files[image_request_payload.context_tar_part_name] = (
                image_request_payload.context_tar_part_name,
                image_request.context_tar_content,
                "application/gzip",
            )

        request_id = self._generate_request_id()
        headers = self._get_headers_with_request_id(request_id)

        try:
            res = await self._client.post(
                "applications",
                files=files,
                headers=headers,
                timeout=120,
            )
        except httpx.HTTPError as e:
            raise ImageBuilderClientV3NetworkError(e, request_id=request_id) from e

        if not res.is_success:
            error_message = ""
            try:
                error_json = res.json()
                if isinstance(error_json, dict):
                    error_message = error_json.get(
                        "message", error_json.get("error", "")
                    )
                    if not error_message:
                        error_message = str(error_json)
                else:
                    error_message = str(error_json)
            except Exception:
                error_message = res.text or ""

            status_info = f"HTTP {res.status_code} {res.reason_phrase}"
            url_info = ""
            if res.request:
                url_info = f" | URL: {res.request.method} {res.request.url}"

            if error_message:
                full_error = f"{status_info}{url_info}: {error_message}"
            else:
                full_error = f"{status_info}{url_info} (no error message in response)"

            error_msg = f"Error building application version: {full_error}"
            if res.status_code == 400:
                raise ImageBuilderClientV3BadRequestError(
                    error_msg, request_id=request_id
                )
            raise ImageBuilderClientV3Error(error_msg, request_id=request_id)

        info = _ApplicationVersionBuildInfoPayload.model_validate(res.json())
        return _application_version_build_info_from_payload(info)

    async def stream_image_build_logs(
        self, image_build_id: str
    ) -> AsyncGenerator[ImageBuildLogEventV3, None]:
        """Stream logs from the image builder service."""
        request_id = self._generate_request_id()
        headers = self._get_headers_with_request_id(request_id)
        build_id_str = str(image_build_id)

        # Create a separate client for SSE streams to avoid blocking the main client
        # and to allow proper connection management for long-lived SSE connections
        try:
            async with httpx.AsyncClient(
                base_url=self._client.base_url, timeout=120
            ) as client:
                async with aconnect_sse(
                    client,
                    "GET",
                    f"builds/{quote(build_id_str)}/logs",
                    headers=headers,
                ) as event_source:
                    async for sse in event_source.aiter_sse():
                        try:
                            log_entry = _image_build_log_event_from_json(sse.json())
                            yield log_entry
                        except Exception as e:
                            click.secho(
                                f"Error parsing log event: {e}", err=True, fg="red"
                            )
                            continue
        except httpx.HTTPError as e:
            raise ImageBuilderClientV3NetworkError(e, request_id=request_id) from e

    async def image_build_info(
        self, image_build_id: str
    ) -> ImageBuildInfoV3:
        """Get information about a build."""
        request_id = self._generate_request_id()
        headers = self._get_headers_with_request_id(request_id)
        build_id_str = str(image_build_id)

        try:
            res = await self._client.get(
                f"builds/{quote(build_id_str)}",
                headers=headers,
                timeout=60,
            )
        except httpx.HTTPError as e:
            raise ImageBuilderClientV3NetworkError(e, request_id=request_id) from e

        if res.status_code == 404:
            raise ImageBuilderClientV3NotFoundError(
                "Image build", build_id_str, request_id=request_id
            )
        if not res.is_success:
            error_message = res.text
            error_msg = f"Error requesting image build info: {error_message}"
            if res.status_code == 400:
                raise ImageBuilderClientV3BadRequestError(
                    error_msg, request_id=request_id
                )
            raise ImageBuilderClientV3Error(error_msg, request_id=request_id)

        info = _ImageBuildInfoPayload.model_validate(res.json())
        return _image_build_info_from_payload(info)

    async def cancel_app_build(
        self, app_version_id: str
    ) -> ApplicationVersionBuildInfoV3 | None:
        """Cancel an application version build."""
        request_id = self._generate_request_id()
        headers = self._get_headers_with_request_id(request_id)
        app_version_id_str = str(app_version_id)

        try:
            res = await self._client.post(
                f"applications/{quote(app_version_id_str)}/cancel",
                headers=headers,
                timeout=60,
            )
        except httpx.HTTPError as e:
            raise ImageBuilderClientV3NetworkError(e, request_id=request_id) from e

        if not res.is_success:
            error_message = res.text
            error_msg = f"Error canceling application version build {app_version_id_str}: {error_message}"
            if res.status_code == 400:
                raise ImageBuilderClientV3BadRequestError(
                    error_msg, request_id=request_id
                )
            raise ImageBuilderClientV3Error(error_msg, request_id=request_id)

        info = _ApplicationVersionBuildInfoPayload.model_validate(res.json())
        return _application_version_build_info_from_payload(info)

    async def cancel_image_build(
        self, image_build_id: str
    ) -> ImageBuildInfoV3 | None:
        """Cancel an image build."""
        request_id = self._generate_request_id()
        headers = self._get_headers_with_request_id(request_id)
        build_id_str = str(image_build_id)

        try:
            res = await self._client.post(
                f"builds/{quote(build_id_str)}/cancel",
                headers=headers,
                timeout=60,
            )
        except httpx.HTTPError as e:
            raise ImageBuilderClientV3NetworkError(e, request_id=request_id) from e

        if not res.is_success:
            error_message = res.text
            error_msg = f"Error canceling image build {build_id_str}: {error_message}"
            if res.status_code == 400:
                raise ImageBuilderClientV3BadRequestError(
                    error_msg, request_id=request_id
                )
            raise ImageBuilderClientV3Error(error_msg, request_id=request_id)

        try:
            return await self.image_build_info(build_id_str)
        except ImageBuilderClientV3NotFoundError:
            return None

    async def __aenter__(self) -> "ImageBuilderClientV3":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self._client.aclose()
