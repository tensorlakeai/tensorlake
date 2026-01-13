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
from uuid import uuid7
from dataclasses import dataclass

import click
import httpx
from httpx_sse import aconnect_sse
from pydantic import BaseModel

from tensorlake.cli._common import ASYNC_HTTP_EVENT_HOOKS


@dataclass
class ImageBuildRequest:
    """
    ImageBuildRequest represents an image to be built.

    Attributes:
        key (str): The key of the image build request.
        name (str | None): The name of the image.
        description (str | None): The description of the image.
        context_tar_content (bytes): The content of the context tar file.
        function_names (list[str]): The names of the functions to be built for this image.

    Example:
        req = ImageBuildRequest(
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
class ApplicationVersionBuildRequest:
    """
    Request for building an application version.
    This request contains information about the application, version,
    and the various images to be built.

    Attributes:
        name (str): The name of the application to be built.
        version (str): The version of the application to be built.
        images (list[]): The name of the function used in the build.

    Example:
        images = [...]  # List of ImageBuildRequest instances

        req = ApplicationVersionBuildContext(
            name="example_app",
            version="v1.0",
            images=images
        )
    """

    name: str
    version: str
    images: list[ImageBuildRequest]


class _ImageBuildInfo(BaseModel):
    id: str
    app_version_id: str
    name: str | None
    description: str | None
    status: str
    error_message: str | None = None
    created_at: str
    updated_at: str
    finished_at: str | None


class ImageBuildInfo:
    """
    ImageBuildInfo model for the image builder service.
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
    status: str
    created_at: str
    updated_at: str
    finished_at: str | None
    error_message: str | None = None

    def __init__(self, src: _ImageBuildInfo):
        self.id = src.id
        self.application_version_id = src.app_version_id
        self.name = src.name
        self.status = src.status
        self.created_at = src.created_at
        self.updated_at = src.updated_at
        self.finished_at = src.finished_at
        self.error_message = src.error_message


class _ImageBuildRequest(BaseModel):
    key: str
    name: str | None
    description: str | None
    context_tar_part_name: str
    function_names: list[str]

    def __init__(self, req: ImageBuildRequest):
        self.key = req.key
        self.name = req.name
        self.description = req.description
        self.context_tar_part_name = uuid7().hex
        self.function_names = req.function_names


class _ApplicationVersionBuildRequest(BaseModel):
    name: str
    version: str
    images: list[_ImageBuildRequest]

    def __init__(self, req: ApplicationVersionBuildRequest):
        self.name = req.name
        self.version = req.version
        self.images = [_ImageBuildRequest(i) for i in req.images]


class _ApplicationVersionImageBuildInfo(_ImageBuildInfo):
    key: str
    function_names: list[str]


class _ApplicationVersionBuildInfo(BaseModel):
    id: str
    organization_id: str
    project_id: str
    name: str
    version: str
    image_builds: list[_ApplicationVersionImageBuildInfo]


class ApplicationVersionBuildInfo:
    """
    ApplicationVersionBuildInfo model for the image builder service.
    This model represents the information about an application version build.
    Attributes:
        id (str): The ID of the application version build.
        name (str): The name of the application.
        version (str): The version of the application.
        image_builds (dict[str, ImageBuildInfo]): A dictionary of ImageBuildInfo objects representing
                                             the builds for each image in the application version. The key is the key of the image build request.
    """

    id: str
    name: str
    version: str
    image_builds: dict[str, ImageBuildInfo]

    def __init__(self, src: _ApplicationVersionBuildInfo):
        self.id = src.id
        self.name = src.name
        self.version = src.version
        self.image_builds = {
            image_build.key: ImageBuildInfo(image_build) for image_build in src.image_builds
        }


class ImageBuildLogEvent(BaseModel):
    """
    ImageBuildLogEvent model for the image builder service.
    This model represents a log event from the image builder service.
    Attributes:
        image_build_id (str): The ID of the build associated with the log event.
        timestamp (str): The timestamp of the log event.
        stream (str): The stream from which the log event originated (stdout, stderr, info).
        message (str): The log message.
        sequence_number (int): The sequence number of the log event. Used for ordering.
    """

    image_build_id: str
    timestamp: str
    stream: str
    message: str
    sequence_number: int
    build_status: str

class ImageBuilderV3Client:
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
    def from_env(cls) -> "ImageBuilderV3Client":
        """
        Create an instance of the ImageBuilderV3Client using environment variables.

        The API key is retrieved from the TENSORLAKE_API_KEY environment variable.
        If no API key is set, PAT authentication is assumed and organization/project IDs
        are retrieved from TENSORLAKE_ORGANIZATION_ID and TENSORLAKE_PROJECT_ID.

        The build service URL is retrieved from the TENSORLAKE_API_URL environment variable,
        defaulting to "https://api.tensorlake.ai" if not set.

        The TENSORLAKE_BUILD_SERVICE environment variable can be used to specify
        a different build service URL, mainly for debugging or local testing.

        Returns:
            ImageBuilderV3Client: An instance of the ImageBuilderV3Client.
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

    async def build_app(self, req: ApplicationVersionBuildRequest) -> ApplicationVersionBuildInfo:
        """
        Build an application version and its images using the provided request.

        Args:
            req (ApplicationVersionBuildRequest): The application version build request.
        Returns:
            ApplicationVersionBuildInfo: The response from the image builder service.
        """
        json_req = _ApplicationVersionBuildRequest(req)

        image_reqs_by_key = { r.key: r for r in req.images }
        json_image_reqs_by_key = { r.key: r for r in json_req.images }

        files = {
            "app_version": (None, json_req.model_dump_json().encode("utf-8"), "application/json")
        }

        for key, image_req in image_reqs_by_key.items():
            json_image_req = json_image_reqs_by_key[key]
            files[json_image_req.context_tar_part_name] = (None, image_req.context_tar_content, "application/gzip")

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

        info = _ApplicationVersionBuildInfo.model_validate(res.json())

        return ApplicationVersionBuildInfo(info)


    async def stream_image_build_logs(
        self, image_build_id: str
    ) -> AsyncGenerator[ImageBuildLogEvent]:
        """
        Stream logs from the image builder service for the specified image build.

        Args:
            image_build_id (str): The build id to stream logs for.
        Returns:
            AsyncGenerator[ImageBuildLogEvent]: A generator of log events.
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
                        log_entry = ImageBuildLogEvent.model_validate(sse.json())
                        yield log_entry
                    except Exception as e:
                        click.secho(f"Error parsing log event: {e}", err=True, fg="red")
                        continue


    async def image_build_info(self, image_build_id: str) -> ImageBuildInfo:
        """
        Get information about a build.

        Args:
            image_build_id (str): The build id to get information about.
        Returns:
            ImageBuildInfo: Information about the build.
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

        info = _ImageBuildInfo.model_validate(res.json())

        return ImageBuildInfo(info)


    async def cancel_image_build(self, image_build_id: str) -> ImageBuildInfo:
        """
        Cancel an image build.

        Args:
            image_build_id (str): The build id to cancel.
        Returns:
            ImageBuildInfo: Information about the build.
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

        info = await self.image_build_info(image_build_id)

        return info

    async def __aenter__(self) -> "ImageBuilderV3Client":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit. Closes the HTTP client."""
        await self._client.aclose()
