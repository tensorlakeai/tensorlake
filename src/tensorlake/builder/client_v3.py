"""
# TensorLake Image Builder Client
# This code is part of the TensorLake SDK for Python.
# It provides a client for interacting with the image builder service.

# It is designed to interact with the /images/v3 API endpoint.
# The client allows users to build images, check the status of builds,
# and stream logs from the image builder service.

# The client is initialized with the build service URL and an optional API key.
# The API key is used for authentication when making requests to the service.
# The client provides methods to get build information, check if a build exists,
# and stream logs from a build.
"""

import os
from typing import AsyncGenerator
from urllib.parse import quote
from uuid import uuid4 as uuid
from dataclasses import dataclass

import click
import httpx
from httpx_sse import aconnect_sse
from pydantic import BaseModel

from tensorlake.cli._common import ASYNC_HTTP_EVENT_HOOKS


# ============================================================================
# Public Request Models (Dataclasses)
# ============================================================================

@dataclass
class ImageBuildRequestV3:
    """
    ImageBuildRequestV3 represents an image to be built.

    Attributes:
        key (str): The key of the image build request.
        name (str | None): The name of the image.
        description (str | None): The description of the image.
        context_tar_content (bytes): The content of the context tar file.
        function_names (list[str]): The names of the functions to be built for this image.

    Example:
        req = ImageBuildRequestV3(
            key="image_1",
            name="image_1",
            description="Image 1",
            context_tar_content=b"context_tar_content",
            function_names=["func1", "func2"]
        )
    """
    
    key: str
    name: str | None
    description: str | None
    context_tar_content: bytes
    function_names: list[str]


@dataclass
class ApplicationVersionBuildRequestV3:
    """
    Request for building an application version.
    This request contains information about the application, version,
    and the various images to be built.

    Attributes:
        name (str): The name of the application to be built.
        version (str): The version of the application to be built.
        images (list[ImageBuildRequestV3]): List of ImageBuildRequestV3 instances.

    Example:
        images = [...]  # List of ImageBuildRequestV3 instances

        req = ApplicationVersionBuildRequestV3(
            name="example_app",
            version="v1.0",
            images=images
        )
    """

    name: str
    version: str
    images: list[ImageBuildRequestV3]


# ============================================================================
# Public Response Models (Dataclasses)
# ============================================================================

@dataclass
class ImageBuildInfoV3:
    """
    ImageBuildInfoV3 model for the image builder service.
    This model represents the information about an image build.
    
    Attributes:
        id (str): The ID of the image build.
        application_version_id (str): The ID of the application version associated with the image build.
        name (str | None): The name of the image.
        status (str): The status of the build (e.g., "pending", "in_progress", "completed").
        created_at (str): The timestamp when the image build was created.
        updated_at (str): The timestamp when the image build was last updated.
        finished_at (str | None): The timestamp when the build was finished.
        error_message (str | None): An optional error message if the build failed.
    """

    id: str
    application_version_id: str
    name: str | None = None
    status: str = ""
    created_at: str = ""
    updated_at: str = ""
    finished_at: str | None = None
    error_message: str | None = None


@dataclass
class ApplicationVersionBuildInfoV3:
    """
    ApplicationVersionBuildInfoV3 model for the image builder service.
    This model represents the information about an application version build.
    
    Attributes:
        id (str): The ID of the application version build.
        name (str): The name of the application.
        version (str): The version of the application.
        image_builds (dict[str, ImageBuildInfoV3]): A dictionary of ImageBuildInfoV3 objects representing
                                             the builds for each image in the application version. 
                                             The key is the key of the image build request.
    """

    id: str
    name: str
    version: str
    image_builds: dict[str, ImageBuildInfoV3]


# ============================================================================
# Public Event Models (Dataclasses)
# ============================================================================

@dataclass
class ImageBuildLogEventV3:
    """
    ImageBuildLogEventV3 model for the image builder service.
    This model represents a log event from the image builder service.
    
    Attributes:
        image_build_id (str): The ID of the build associated with the log event.
        timestamp (str): The timestamp of the log event.
        stream (str): The stream from which the log event originated (stdout, stderr, info).
        message (str): The log message.
        sequence_number (int): The sequence number of the log event. Used for ordering.
        build_status (str): The current status of the build.
    """

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
    app_version_id: str
    name: str | None
    description: str | None
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
            context_tar_part_name=uuid().hex,
            function_names=req.function_names,
        )


class _ApplicationVersionBuildRequestPayload(BaseModel):
    """Internal Pydantic model for API serialization."""
    name: str
    version: str
    images: list[_ImageBuildRequestPayload]

    @classmethod
    def from_request(cls, req: ApplicationVersionBuildRequestV3) -> "_ApplicationVersionBuildRequestPayload":
        """Create from public ApplicationVersionBuildRequestV3."""
        return cls(
            name=req.name,
            version=req.version,
            images=[_ImageBuildRequestPayload.from_request(img) for img in req.images],
        )


class _ApplicationVersionImageBuildInfoPayload(_ImageBuildInfoPayload):
    """Internal Pydantic model for API deserialization."""
    key: str
    function_names: list[str]


class _ApplicationVersionBuildInfoPayload(BaseModel):
    """Internal Pydantic model for API deserialization."""
    id: str
    organization_id: str
    project_id: str
    name: str
    version: str
    image_builds: list[_ApplicationVersionImageBuildInfoPayload]


# ============================================================================
# Helper Functions
# ============================================================================

def _image_build_info_from_payload(data: _ImageBuildInfoPayload) -> ImageBuildInfoV3:
    """Convert internal API model to public dataclass."""
    return ImageBuildInfoV3(
        id=data.id,
        application_version_id=data.app_version_id,
        name=data.name,
        status=data.status,
        created_at=data.created_at,
        updated_at=data.updated_at,
        finished_at=data.finished_at,
        error_message=data.error_message,
    )


def _application_version_build_info_from_payload(data: _ApplicationVersionBuildInfoPayload) -> ApplicationVersionBuildInfoV3:
    """Convert internal API model to public dataclass."""
    return ApplicationVersionBuildInfoV3(
        id=data.id,
        name=data.name,
        version=data.version,
        image_builds={
            img.key: _image_build_info_from_payload(img)
            for img in data.image_builds
        },
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
    """
    Client for interacting with the image builder service.
    This client is used to build images, check the status of builds,
    and stream logs from the image builder service.
    """

    def __init__(
        self,
        build_service: str,
        api_key: str | None = None,
        organization_id: str | None = None,
        project_id: str | None = None,
    ):
        self._client = httpx.AsyncClient(event_hooks=ASYNC_HTTP_EVENT_HOOKS)
        self._build_service = build_service
        self._headers = {}
        if api_key:
            self._headers["Authorization"] = f"Bearer {api_key}"
            # Add X-Forwarded headers when not using an API key (i.e., using PAT)
            # API keys already contain org/project info via introspection
            if organization_id:
                self._headers["X-Forwarded-Organization-Id"] = organization_id
            if project_id:
                self._headers["X-Forwarded-Project-Id"] = project_id

    @classmethod
    def from_env(cls) -> "ImageBuilderClientV3":
        """
        Create an instance of the ImageBuilderClientV3 using environment variables.

        The API key is retrieved from the TENSORLAKE_API_KEY environment variable.
        If no API key is set, PAT authentication is assumed and organization/project IDs
        are retrieved from TENSORLAKE_ORGANIZATION_ID and TENSORLAKE_PROJECT_ID.

        The build service URL is retrieved from the TENSORLAKE_API_URL environment variable,
        defaulting to "https://api.tensorlake.ai" if not set.

        The TENSORLAKE_BUILD_SERVICE environment variable can be used to specify
        a different build service URL, mainly for debugging or local testing.

        Returns:
            ImageBuilderClientV3: An instance of the ImageBuilderClientV3.
        """
        api_key = os.getenv("TENSORLAKE_API_KEY")
        # For PAT authentication, get auth token and org/project IDs
        if not api_key:
            api_key = os.getenv("TENSORLAKE_PAT")

        organization_id = os.getenv("TENSORLAKE_ORGANIZATION_ID")
        project_id = os.getenv("TENSORLAKE_PROJECT_ID")

        server_url = os.getenv("TENSORLAKE_API_URL", "https://api.tensorlake.ai")
        build_url = os.getenv("TENSORLAKE_BUILD_SERVICE", f"{server_url}/images/v3")
        return cls(build_url, api_key, organization_id, project_id)

    async def build_app(self, request: ApplicationVersionBuildRequestV3) -> ApplicationVersionBuildInfoV3:
        """
        Build an application version and its images using the provided request.

        Args:
            request (ApplicationVersionBuildRequestV3): The application version build request.
        Returns:
            ApplicationVersionBuildInfoV3: The response from the image builder service.
        """
        request_payload = _ApplicationVersionBuildRequestPayload.from_request(request)

        image_requests_by_key = {r.key: r for r in request.images}
        image_request_payloads_by_key = {r.key: r for r in request_payload.images}

        files = {
            "app_version": (None, request_payload.model_dump_json().encode("utf-8"), "application/json")
        }

        for key, image_request in image_requests_by_key.items():
            image_request_payload = image_request_payloads_by_key[key]
            files[image_request_payload.context_tar_part_name] = (None, image_request.context_tar_content, "application/gzip")

        res = await self._client.post(
            f"{self._build_service}/builds",
            files,
            headers=self._headers,
            timeout=120,
        )

        if not res.is_success:
            error_message = res.text
            click.secho(f"Error building application version: {error_message}", err=True, fg="red")
            raise RuntimeError(f"Error building application version: {error_message}")

        info = _ApplicationVersionBuildInfoPayload.model_validate(res.json())
        return _application_version_build_info_from_payload(info)

    async def stream_image_build_logs(
        self, image_build_id: str
    ) -> AsyncGenerator[ImageBuildLogEventV3]:
        """
        Stream logs from the image builder service for the specified image build.

        Args:
            image_build_id (str): The build id to stream logs for.
        Returns:
            AsyncGenerator[ImageBuildLogEventV3]: A generator of log events.
        """
        # Create a separate client for SSE streams to avoid blocking the main client
        # and to allow proper connection management for long-lived SSE connections
        async with httpx.AsyncClient(timeout=120) as client:
            async with aconnect_sse(
                client,
                "GET",
                f"{self._build_service}/builds/{quote(image_build_id)}/logs",
                headers=self._headers,
            ) as event_source:
                async for sse in event_source.aiter_sse():
                    try:
                        log_entry = _image_build_log_event_from_json(sse.json())
                        yield log_entry
                    except Exception as e:
                        click.secho(f"Error parsing log event: {e}", err=True, fg="red")
                        continue

    async def image_build_info(self, image_build_id: str) -> ImageBuildInfoV3:
        """
        Get information about a build.

        Args:
            image_build_id (str): The build id to get information about.
        Returns:
            ImageBuildInfoV3: Information about the build.
        """
        res = await self._client.get(
            f"{self._build_service}/builds/{quote(image_build_id)}",
            headers=self._headers,
            timeout=60,
        )
        if not res.is_success:
            error_message = res.text
            click.secho(f"Error requesting image build info: {error_message}", err=True, fg="red")
            raise RuntimeError(f"Error requesting image build info: {error_message}")

        info = _ImageBuildInfoPayload.model_validate(res.json())
        return _image_build_info_from_payload(info)

    async def cancel_image_build(self, image_build_id: str) -> ImageBuildInfoV3:
        """
        Cancel an image build.

        Args:
            image_build_id (str): The build id to cancel.
        Returns:
            ImageBuildInfoV3: Information about the build.
        """
        res = await self._client.post(
            f"{self._build_service}/builds/{quote(image_build_id)}/cancel",
            headers=self._headers,
            timeout=60,
        )

        if not res.is_success:
            error_message = res.text
            click.secho(f"Error canceling image build {image_build_id}: {error_message}", err=True, fg="red")
            raise RuntimeError(f"Error canceling image build {image_build_id}: {error_message}")

        return await self.image_build_info(image_build_id)

    async def __aenter__(self) -> "ImageBuilderClientV3":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit. Closes the HTTP client."""
        await self._client.aclose()
