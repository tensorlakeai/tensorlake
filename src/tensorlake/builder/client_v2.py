"""
# TensorLake Image Builder Client
# This code is part of the TensorLake SDK for Python.
# It provides a client for interacting with the image builder service.

# This client is a new revision of the client found in `src/tensorlake/builder/client.py`.
# It is designed to interact with the /images/v2 API endpoint.
# The client allows users to build images, check the status of builds,
# and stream logs from the image builder service.

# The client is initialized with the build service URL and an optional API key.
# The API key is used for authentication when making requests to the service.
# The client provides methods to get build information, check if a build exists,
# and stream logs from a build.
"""

import asyncio
import os
import tempfile
from dataclasses import dataclass

import aiofiles
import click
import httpx
from httpx_sse import aconnect_sse
from pydantic import BaseModel

from tensorlake.applications import Image
from tensorlake.applications.image import create_image_context_file, image_hash
from tensorlake.cli._common import ASYNC_HTTP_EVENT_HOOKS


@dataclass
class BuildContext:
    """
    Build context for the image builder service.
    This context contains information about the application, application version,
    and function name used for building the image.

    Attributes:
        application_name (str): The name of the application to be built.
        application_version (str): The version of the application to be built.
        function_name (str): The name of the function used in the build.

    Example:
        context = BuildContext(
            application_name="example_app",
            application_version="v1.0",
            function_name="example_function"
        )
    """

    application_name: str
    application_version: str
    function_name: str


class BuildInfo(BaseModel):
    """
    BuildInfo model for the image builder service.
    This model represents the information about a build.
    Attributes:
        id (str): The ID of the build.
        status (str): The status of the build (e.g., "pending", "in_progress", "completed").
        created_at (str): The timestamp when the build was created.
        updated_at (str): The timestamp when the build was last updated.
        finished_at (str | None): The timestamp when the build was finished.
        error_message (str | None): An optional error message if the build failed.
    """

    id: str
    status: str
    created_at: str
    updated_at: str
    finished_at: str | None
    error_message: str | None = None


class BuildLogEvent(BaseModel):
    """
    BuildLogEvent model for the image builder service.
    This model represents a log event from the image builder service.
    Attributes:
        build_id (str): The ID of the build associated with the log event.
        timestamp (str): The timestamp of the log event.
        stream (str): The stream from which the log event originated (stdout, stderr, info).
        message (str): The log message.
        sequence_number (int): The sequence number of the log event. Used for ordering.
    """

    build_id: str
    timestamp: str
    stream: str
    message: str
    sequence_number: int
    build_status: str


class ImageBuilderV2Client:
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
    def from_env(cls):
        """
        Create an instance of the ImageBuilderV2Client using environment variables.

        The API key is retrieved from the TENSORLAKE_API_KEY environment variable.
        If no API key is set, PAT authentication is assumed and organization/project IDs
        are retrieved from TENSORLAKE_ORGANIZATION_ID and TENSORLAKE_PROJECT_ID.

        The build service URL is retrieved from the TENSORLAKE_API_URL environment variable,
        defaulting to "https://api.tensorlake.ai" if not set.

        The TENSORLAKE_BUILD_SERVICE environment variable can be used to specify
        a different build service URL, mainly for debugging or local testing.

        Returns:
            ImageBuilderV2Client: An instance of the ImageBuilderV2Client.
        """
        api_key = os.getenv("TENSORLAKE_API_KEY")
        # For PAT authentication, get auth token and org/project IDs
        if not api_key:
            api_key = os.getenv("TENSORLAKE_PAT")

        organization_id = os.getenv("TENSORLAKE_ORGANIZATION_ID")
        project_id = os.getenv("TENSORLAKE_PROJECT_ID")

        server_url = os.getenv("TENSORLAKE_API_URL", "https://api.tensorlake.ai")
        build_url = os.getenv("TENSORLAKE_BUILD_SERVICE", f"{server_url}/images/v2")
        return cls(build_url, api_key, organization_id, project_id)

    async def build_collection(
        self, context_collection: dict[Image, BuildContext]
    ) -> dict[str, str]:
        """
        Build a collection of images using the provided build context.

        Args:
            context_collection (dict[Image, BuildContext]): A dictionary mapping images to their build contexts.
        Returns:
            dict: A dictionary mapping image hashes to their corresponding build IDs.
        """
        click.echo("Building images...")

        builds = {}
        for image, context in context_collection.items():
            click.echo(f"Building {image.name}")
            build = await self.build(context, image)
            click.echo(f"Built {image.name} with hash {image_hash(image)}")
            builds[image_hash(image)] = build.id

        return builds

    async def build(self, context: BuildContext, image: Image) -> BuildInfo:
        """
        Build an image using the provided build context.

        Args:
            context (BuildContext): The build context containing information about the application,
                                    application version, and function name.
            image (Image): The image to be built.
        Returns:
            dict: The response from the image builder service.
        """
        _fd, context_file_path = tempfile.mkstemp()
        create_image_context_file(image, context_file_path)

        click.echo(
            f"{image.name}: Posting {os.path.getsize(context_file_path)} bytes of context to build service...."
        )

        files = {}
        async with aiofiles.open(context_file_path, "rb") as fp:
            files["context"] = await fp.read()

        os.remove(context_file_path)
        data = {
            "graph_name": context.application_name,
            "graph_version": context.application_version,
            "graph_function_name": context.function_name,
            "image_name": image.name,
            "image_id": image._id,
        }

        res = await self._client.put(
            f"{self._build_service}/builds",
            data=data,
            files=files,
            headers=self._headers,
            timeout=60,
        )

        if not res.is_success:
            error_message = res.text
            raise RuntimeError(f"Error building image {image.name}: {error_message}")

        build = BuildInfo.model_validate(res.json())

        click.echo(f"Waiting for build {build.id} of {image.name} to complete...")

        try:
            return await self.stream_logs(build)
            # Handling these 3 exceptions allows the CLI to provide a better UX
            # where the user can cancel the build and receive a graceful error message.
            #
            # For example:
            # 2025-07-31T18:35:24.333784812Z: [4/5] RUN  sleep 301
            # ^C
            # Cancelling build for image generator ...
            # Build for image generator cancelled successfully
            # Cancelled build for image generator
            # Aborted!
        except (asyncio.CancelledError, KeyboardInterrupt, click.Abort):
            await self._cancel_build(build, image)
            raise

    async def stream_logs(self, build: BuildInfo) -> BuildInfo:
        """
        Stream logs from the image builder service for the specified build.

        Args:
            build (NewBuild): The build for which to stream logs.
        """
        click.echo(f"Streaming logs for build {build.id}")
        async with httpx.AsyncClient(timeout=120) as client:
            async with aconnect_sse(
                client,
                "GET",
                f"{self._build_service}/builds/{build.id}/logs",
                headers=self._headers,
            ) as event_source:
                async for sse in event_source.aiter_sse():
                    log_entry = BuildLogEvent.model_validate(sse.json())
                    self._print_build_log_event(event=log_entry)

        return await self.build_info(build.id)

    def _print_build_log_event(self, event: BuildLogEvent):
        if event.build_status == "pending":
            click.secho("Build waiting in queue...")
        else:
            match event.stream:
                case "stdout":
                    click.secho(
                        event.message,
                        nl=False,
                        err=False,
                    )
                case "stderr":
                    click.secho(event.message, err=True)
                case "info":
                    click.secho(f"{event.timestamp}: {event.message}")

    async def build_info(self, build_id: str) -> BuildInfo:
        """
        Get information about a build.

        Args:
            build (NewBuild): The build for which to get information.
        Returns:
            BuildInfo: Information about the build.
        """
        res = await self._client.get(
            f"{self._build_service}/builds/{build_id}",
            headers=self._headers,
            timeout=60,
        )
        if not res.is_success:
            error_message = res.text
            click.secho(f"Error building image: {error_message}", fg="red")
            raise RuntimeError(f"Error building image: {error_message}")

        build_info = BuildInfo.model_validate(res.json())

        if build_info.status == "failed":
            click.secho(
                f"Build {build_info.id} failed with error: {build_info.error_message}",
                fg="red",
            )
            raise RuntimeError(
                f"Build {build_info.id} failed with error: {build_info.error_message}"
            )

        return build_info

    async def _cancel_build(self, build: BuildInfo, image: Image):
        try:
            click.secho(f"\nCancelling build for image {image.name} ...", fg="yellow")
            response = await self._client.post(
                f"{self._build_service}/builds/{build.id}/cancel",
                headers=self._headers,
                timeout=60,
            )

            if response.status_code == 202:
                click.secho(f"Build for image {image.name} cancelled successfully")
            else:
                click.secho(f"Failed to cancel build {build.id}: {response.text}")
        except Exception as e:
            click.secho(f"Failed to cancel build {build.id}: {e}")
