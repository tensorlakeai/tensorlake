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
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from pydantic import BaseModel

from tensorlake.applications import Image
from tensorlake.applications.image import create_image_context_file, image_hash

try:
    from tensorlake_rust_cloud_sdk import CloudApiClient as RustCloudApiClient

    _RUST_CLOUD_CLIENT_AVAILABLE = True
except Exception:
    RustCloudApiClient = None
    _RUST_CLOUD_CLIENT_AVAILABLE = False


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
        if not _RUST_CLOUD_CLIENT_AVAILABLE:
            raise RuntimeError(
                "Rust Cloud SDK client is required but unavailable. "
                "Build/install it with `make build_rust_py_client`."
            )

        parsed = urlparse(build_service)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError(f"Invalid build service URL: {build_service}")

        self._build_service_path = parsed.path.rstrip("/") or "/images/v2"
        api_url = f"{parsed.scheme}://{parsed.netloc}"
        self._rust_client = RustCloudApiClient(
            api_url=api_url,
            api_key=api_key,
            organization_id=organization_id,
            project_id=project_id,
        )

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
        print("Building images...", file=sys.stderr)

        builds = {}
        for image, context in context_collection.items():
            print(f"Building {image.name}", file=sys.stderr)
            build = await self.build(context, image)
            print(f"Built {image.name} with hash {image_hash(image)}", file=sys.stderr)
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

        print(
            f"{image.name}: Posting {os.path.getsize(context_file_path)} bytes of context to build service....",
            file=sys.stderr,
        )

        file_content = await asyncio.to_thread(Path(context_file_path).read_bytes)

        os.remove(context_file_path)

        try:
            build_json = await asyncio.to_thread(
                self._rust_client.start_image_build,
                self._build_service_path,
                context.application_name,
                context.application_version,
                context.function_name,
                image.name,
                image._id,
                file_content,
            )
        except Exception as e:
            raise RuntimeError(f"Error building image {image.name}: {e}") from e

        build = BuildInfo.model_validate_json(build_json)

        print(
            f"Waiting for build {build.id} of {image.name} to complete...",
            file=sys.stderr,
        )

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
        except (asyncio.CancelledError, KeyboardInterrupt):
            await self._cancel_build(build, image)
            raise

    async def stream_logs(self, build: BuildInfo) -> BuildInfo:
        """
        Stream logs from the image builder service for the specified build.

        Args:
            build (NewBuild): The build for which to stream logs.
        """
        print(f"Streaming logs for build {build.id}", file=sys.stderr)
        events_json: list[str] = await asyncio.to_thread(
            self._rust_client.stream_build_logs_json,
            self._build_service_path,
            build.id,
        )
        for event_json in events_json:
            log_entry = BuildLogEvent.model_validate_json(event_json)
            self._print_build_log_event(event=log_entry)

        return await self.build_info(build.id)

    def _print_build_log_event(self, event: BuildLogEvent):
        if event.build_status == "pending":
            print("Build waiting in queue...", file=sys.stderr)
        else:
            match event.stream:
                case "stdout":
                    print(event.message, end="", file=sys.stderr)
                case "stderr":
                    print(event.message, file=sys.stderr)
                case "info":
                    print(f"{event.timestamp}: {event.message}", file=sys.stderr)

    async def build_info(self, build_id: str) -> BuildInfo:
        """
        Get information about a build.

        Args:
            build (NewBuild): The build for which to get information.
        Returns:
            BuildInfo: Information about the build.
        """
        try:
            build_info_json = await asyncio.to_thread(
                self._rust_client.build_info_json,
                self._build_service_path,
                build_id,
            )
        except Exception as e:
            print(f"Error building image: {e}", file=sys.stderr)
            raise RuntimeError(f"Error building image: {e}") from e

        build_info = BuildInfo.model_validate_json(build_info_json)

        if build_info.status == "failed":
            print(
                f"Build {build_info.id} failed with error: {build_info.error_message}",
                file=sys.stderr,
            )
            raise RuntimeError(
                f"Build {build_info.id} failed with error: {build_info.error_message}"
            )

        return build_info

    async def _cancel_build(self, build: BuildInfo, image: Image):
        try:
            print(f"\nCancelling build for image {image.name} ...", file=sys.stderr)
            await asyncio.to_thread(
                self._rust_client.cancel_build,
                self._build_service_path,
                build.id,
            )
            print(
                f"Build for image {image.name} cancelled successfully",
                file=sys.stderr,
            )
        except Exception as e:
            print(f"Failed to cancel build {build.id}: {e}", file=sys.stderr)
