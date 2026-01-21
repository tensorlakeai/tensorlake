"""Builder module for building container images and application versions in Tensorlake.

This module provides classes and utilities for building container images that
contain Tensorlake functions and their dependencies. It handles the orchestration
of image builds, streaming build logs, and managing the build lifecycle.
"""

import asyncio
import os
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime
from typing import AsyncGenerator

from nanoid import generate as nanoid_generate
import click
import httpx

from tensorlake.applications.image import ImageInformation, create_image_context_file
from tensorlake.applications.image_builder.client_v3 import (
    ApplicationVersionBuildInfoV3,
    ApplicationVersionBuildRequestV3,
    ClientKey,
    ImageBuilderClientV3,
    ImageBuilderClientV3Error,
    ImageBuildId,
    ImageBuildInfoV3,
    ImageBuildLogEventV3,
    ImageBuildRequestV3,
)


class ImageBuildRequest:
    """Represents a request to build a container image for Tensorlake functions.

    Tensorlake functions run in function containers. This class encapsulates
    the information needed to build a container image, including the image
    definition (base image, build operations) and the list of functions
    that should be included in the build.

    Container images are used to install dependencies (Python packages, system
    packages) that functions need at runtime. Images are defined using the
    `Image` class with methods like `.run()`, `.add()`, `.copy()`, and `.env()`.

    For more information on defining images, see:
    https://docs.tensorlake.ai/applications/images

    Attributes:
        image_info: The ImageInformation object containing the image and the
            functions that use it.

    Example:
        >>> from tensorlake.applications import Image
        >>> from tensorlake.applications.image import ImageInformation
        >>> # Define an image with dependencies
        >>> image = (
        ...     Image(name="my-pdf-parser-image", base_image="ubuntu:24.04")
        ...     .run("apt update")
        ...     .run("pip install langchain")
        ... )
        >>> # Create image info with functions
        >>> image_info = ImageInformation(image=image, functions=[...])
        >>> # Create a build request
        >>> request = ImageBuildRequest(image_info)
    """

    def __init__(self, image_info: ImageInformation):
        """
        Initialize an ImageBuildRequest.

        Args:
            image_info: The ImageInformation object containing the image and
                the functions that use it.

        Raises:
            ValueError: If image_info has no functions.
        """
        if not image_info.functions:
            raise ValueError("image_info.functions cannot be empty")
        self.image_info = image_info

    async def _synthesize_v3_request(self) -> ImageBuildRequestV3:
        """Synthesize a v3 image build request from this image build request.

        Returns:
            The v3 image build request.
        """
        image = self.image_info.image
        function_names = [func.function_name for func in self.image_info.functions]

        with tempfile.NamedTemporaryFile() as tmp_file:
            context_file_path = tmp_file.name
            create_image_context_file(image, context_file_path)

            # Read file content before context manager exits to ensure file is still available
            def read_file():
                with open(context_file_path, "rb") as f:
                    return f.read()

            context_tar_content = await asyncio.to_thread(read_file)

        return ImageBuildRequestV3(
            key=nanoid_generate(),
            name=image.name,
            description=None,
            context_tar_content=context_tar_content,
            function_names=function_names,
        )


class BuildRequest:
    """Represents a request to build an application with multiple container images.

    This class is used to construct a build request for an application, which
    may consist of multiple container images. Each image can contain different
    sets of functions and their dependencies.

    Tensorlake applications can use multiple images to separate functions with
    different dependency requirements. For example, you might have one image
    for PDF processing functions (with PDF libraries) and another for ML
    functions (with ML frameworks).

    Attributes:
        name: The name of the application being built. This should match your
            application name in Tensorlake.
        version: The version string for this application build (e.g., "v1.0.0",
            "latest", "dev-2024-01-01"). Used to identify and track different
            builds of the same application.
        images: List of ImageBuildRequest objects, each representing a container
            image to be built as part of this application. Each image can have
            different base images, dependencies, and associated functions.

    Example:
        >>> from tensorlake.applications import Image
        >>> req = BuildRequest("my-app", "v1.0.0")
        >>>
        >>> # Create an image for PDF processing functions
        >>> pdf_image = (
        ...     Image(name="pdf-processor", base_image="ubuntu:24.04")
        ...     .run("apt update && apt install -y poppler-utils")
        ...     .run("pip install pdfplumber")
        ... )
        >>> req.add_image(pdf_image, ["parse_pdf", "extract_text"])
        >>>
        >>> # Create an image for ML functions
        >>> ml_image = (
        ...     Image(name="ml-processor")
        ...     .run("pip install torch transformers")
        ... )
        >>> req.add_image(ml_image, ["classify_document", "extract_entities"])
        >>>
        >>> print(len(req.images))
        2
    """

    def __init__(self, name: str, version: str):
        """Initialize a BuildRequest.

        Args:
            name: The name of the application. This should match the application
                name in your Tensorlake project.
            version: The version identifier for this application build.
                This can be any string (e.g., "v1.0.0", "latest", "dev-2024-01-01").
                The version is used to identify and track different builds of the
                same application.

        Raises:
            ValueError: If name or version is empty or None.
        """
        if not name:
            raise ValueError("name cannot be empty or None")
        if not version:
            raise ValueError("version cannot be empty or None")
        self.name = name
        self.version = version
        self.images: list[ImageBuildRequest] = []

    def add_image(self, image_info: ImageInformation) -> ImageBuildRequest:
        """Add an image build request to this application version build.

        This method creates an ImageBuildRequest for the given image information,
        adds it to the list of images to be built, and returns it for further
        configuration if needed.

        The image should be defined with all necessary build operations (e.g.,
        `.run()` commands to install dependencies) before being added to the
        build request.

        Args:
            image_info: The ImageInformation object containing the image definition
                and the functions that use it. This should be an ImageInformation
                instance with an Image that has base image and build operations
                defined (e.g., `Image(name="my-image").run("pip install package")`).
                The functions in the ImageInformation will be packaged into containers
                built from the image and will have access to all dependencies installed
                in the image.

        Returns:
            The created ImageBuildRequest object, which can be used for further
            configuration or reference.

        Raises:
            ValueError: If image_info has no functions.

        Example:
            >>> from tensorlake.applications import Image
            >>> from tensorlake.applications.image import ImageInformation
            >>> req = BuildRequest("my-app", "v1.0.0")
            >>>
            >>> # Define image with dependencies
            >>> image = (
            ...     Image(name="worker", base_image="ubuntu:24.04")
            ...     .run("apt update")
            ...     .run("pip install langchain")
            ... )
            >>>
            >>> # Create image info with functions
            >>> image_info = ImageInformation(image=image, functions=[...])
            >>> image_req = req.add_image(image_info)
            >>> print(len(image_req.image_infos))
            1
        """
        image_req = ImageBuildRequest(image_info)
        self.images.append(image_req)
        return image_req

    async def _synthesize_v3_request(self) -> ApplicationVersionBuildRequestV3:
        """Synthesize a v3 application version build request from this request.

        Returns:
            The v3 application version build request.
        """
        return ApplicationVersionBuildRequestV3(
            name=self.name,
            version=self.version,
            images=[
                await image_req._synthesize_v3_request()
                for image_req in self.images  # pylint: disable=protected-access
            ],
        )


_IMAGE_NAME_PREFIX_COLORS: list[str] = [
    "magenta",
    "cyan",
    "green",
    "yellow",
    "blue",
    "white",
    "red",
    "bright_magenta",
    "bright_cyan",
    "bright_green",
    "bright_yellow",
    "bright_blue",
    "bright_white",
    "bright_red",
]


@dataclass
class BuildSummary:
    """Summary of image build results.

    Attributes:
        total: Total number of image builds.
        succeeded: Number of successful builds.
        failed: Number of failed builds.
        canceled: Number of canceled builds.
        unknown: Number of builds with unknown status.
    """

    total: int
    succeeded: int
    failed: int
    canceled: int
    unknown: int = 0


class _ImageBuildReporter:
    _instance_count = 0

    def __init__(self, info: ImageBuildInfoV3):
        """Initialize the image build reporter.

        Args:
            info: The image build information to report on.
        """
        self._info = info
        self._last_seen_status = info.status
        self._display_name = info.name if info.name != "default" else info.id[:12]
        prefix_fg_index = _ImageBuildReporter._instance_count % len(
            _IMAGE_NAME_PREFIX_COLORS
        )
        self._color = _IMAGE_NAME_PREFIX_COLORS[prefix_fg_index]
        _ImageBuildReporter._instance_count += 1
        self._event_cache = []
        self._finished = False
        self._status_message_task: asyncio.Task[None] | None = None

    @property
    def key(self) -> ClientKey | None:
        """Get the image build request key."""
        return self._info.key

    @property
    def image_build_id(self) -> ImageBuildId:
        """Get the image build ID."""
        return self._info.id

    @property
    def last_seen_status(self) -> str:
        """Get the last seen build status from log events."""
        return self._last_seen_status

    async def process_log_events(
        self, stream: AsyncGenerator[ImageBuildLogEventV3, None]
    ):
        """Process and display log events from the build stream.

        Args:
            stream: Async generator of image build log events.
        """
        if self._finished:
            return

        # Start the periodic status message loop
        self._status_message_task = asyncio.create_task(
            self._periodic_status_message_loop()
        )

        try:
            async for event in stream:
                self._event_cache.append(event)
                self._print_log_event(event)
        finally:
            # Cancel the periodic status message task
            if self._status_message_task is not None:
                self._status_message_task.cancel()
                try:
                    await self._status_message_task
                except asyncio.CancelledError:
                    pass
                self._status_message_task = None

            # Ensure the generator is properly closed to clean up async with blocks
            # This is important for proper resource cleanup when tasks are cancelled
            # aclose() is safe to call even if the generator is already closed
            try:
                await stream.aclose()
            except Exception:
                # Ignore any exceptions during close (generator may already be closed)
                pass

    def print_final_result(self, info: ImageBuildInfoV3 | None):
        """Print the final build result.

        Args:
            info: The final image build information, or None if unavailable.
        """
        if self._finished:
            return

        # Cancel the periodic status message task if it exists
        if self._status_message_task is not None:
            self._status_message_task.cancel()
            self._status_message_task = None

        self._finished = True
        self._print_trailer(info)

    def _print_prefix(self, err: bool):
        if _ImageBuildReporter._instance_count > 1:
            click.secho(f"{self._display_name}: ", nl=False, err=err, fg=self._color)

    def _print_waiting_message(self):
        """Print the waiting in queue message."""
        self._print_prefix(False)
        click.secho("Build waiting in queue")

    async def _periodic_status_message_loop(self):
        """Periodically print status messages while build is pending or enqueued.

        Prints the first message immediately, then continues every 15 seconds
        until the status changes or the reporter is finished.
        """
        # Print first message immediately if status is pending/enqueued
        if self._last_seen_status in ("pending", "enqueued"):
            self._print_waiting_message()

        # Continue printing every 15 seconds while status remains pending/enqueued
        while not self._finished:
            try:
                await asyncio.sleep(15)
            except asyncio.CancelledError:
                break

            # Check status after each sleep
            if self._last_seen_status in ("pending", "enqueued"):
                self._print_waiting_message()
            else:
                # Status changed, exit the loop
                break

    def _print_log_event(self, event: ImageBuildLogEventV3, err: bool = False):
        self._last_seen_status = event.build_status
        msg = event.message.strip()

        # Skip printing events with pending/enqueued status since the periodic loop handles those
        if event.build_status in ("pending", "enqueued"):
            return

        match event.stream:
            case "stdout":
                pass
            case "stderr":
                err = True
            case "info":
                msg = f"{event.timestamp} - {event.message}"

        self._print_prefix(err)
        click.secho(msg, err=err)

    def _print_trailer(self, info: ImageBuildInfoV3 | None):
        err = False
        show_logs = False

        image_build_id = self._info.id
        status = info.status if info else self._last_seen_status
        error_message = info.error_message if info else None
        single_reporter = _ImageBuildReporter._instance_count == 1
        build_id_suffix = f" ({image_build_id})" if single_reporter else ""

        match status:
            case "succeeded":
                msg = f"✅ Image build{build_id_suffix} succeeded"
            case "failed":
                err = True
                show_logs = True
                msg = (
                    f"❌ Image build{build_id_suffix} failed"
                    if not error_message
                    else f"❌ Image build{build_id_suffix} failed: {error_message}"
                )
            case "canceled" | "canceling":
                msg = f"🚫 Image build{build_id_suffix} canceled"
            case _:
                msg = f"⚠️ Unexpected image build{build_id_suffix} status: {status}"

        self._print_prefix(err)
        click.secho(msg, err=err)

        if show_logs:
            self._event_cache.sort(key=lambda event: event.sequence_number)
            for event in self._event_cache:
                self._print_log_event(event, err=err)
            click.echo()


class ImageBuilder:
    """Builder for images and their associated container images.

    This class provides the main interface for building images
    in Tensorlake. It handles the orchestration of building multiple container
    images, streaming build logs, and managing build lifecycle.

    Tensorlake functions run in function containers, and container images define
    the dependencies (Python packages, system packages) available to functions.
    This builder manages the process of building these images when you deploy
    an application.

    The builder uses an ImageBuilderClientV3 to communicate with the Tensorlake
    build service. It manages the build process, including:
    - Submitting build requests for all images in an application version
    - Streaming real-time build logs for all images concurrently
    - Tracking build status and final results for each image
    - Handling cancellation and cleanup on interruption

    For more information on container images, see:
    https://docs.tensorlake.ai/applications/images

    Attributes:
        _client: The ImageBuilderClientV3 used to communicate with the build service.
            This is set during initialization and should not be modified.

    Example:
        >>> from tensorlake.applications.image_builder.client_v3 import ImageBuilderClientV3, ImageBuilderClientV3Options
        >>> from tensorlake.applications.image_builder import ImageBuilder
        >>> from tensorlake.applications import Image
        >>>
        >>> options = ImageBuilderClientV3Options.from_env()
        >>> client = ImageBuilderClientV3(options)
        >>> builder = ImageBuilder(client)
        >>>
        >>> # Create build request
        >>> req = BuildRequest("my-app", "v1.0.0")
        >>>
        >>> # Define image with dependencies
        >>> image = (
        ...     Image(name="my-pdf-parser-image", base_image="ubuntu:24.04")
        ...     .run("apt update")
        ...     .run("pip install langchain")
        ... )
        >>> req.add_image(image, ["parse_pdf", "extract_text"])
        >>>
        >>> # Build the application version
        >>> await builder.build(req)
    """

    _client: ImageBuilderClientV3

    def __init__(self, client: ImageBuilderClientV3):
        """Initialize an ImageBuilder.

        Args:
            client: An ImageBuilderClientV3 instance configured with the
                appropriate build service URL and authentication credentials.
                This client will be used for all build operations.

        Raises:
            ValueError: If client is None or invalid.

        Example:
            >>> from tensorlake.applications.image_builder.client_v3 import ImageBuilderClientV3, ImageBuilderClientV3Options
            >>> options = ImageBuilderClientV3Options.from_env()
            >>> client = ImageBuilderClientV3(options)
            >>> builder = ImageBuilder(client)
        """
        if client is None:
            raise ValueError("client cannot be None")
        self._client = client

    async def build(self, req: BuildRequest) -> None:
        """Build images and all their associated container images.

        This method orchestrates the complete build process:
        1. Synthesizes the build request from the provided BuildRequest
        2. Submits the build request to the Tensorlake build service
        3. Streams real-time build logs for all images concurrently
        4. Retrieves and displays final build results for each image

        The method handles errors gracefully, providing detailed error messages
        for network issues, build service errors, and file system problems.
        If the build is interrupted (e.g., via Ctrl+C), it attempts to cancel
        all in-flight builds.

        Build progress and results are printed to the console in real-time with
        color-coded output for each image. The method does not return a value;
        all build information is displayed via console output.

        Args:
            req: The BuildRequest containing the application name, version,
                and list of images to build. Each image should have been added
                via `add_image()` with the appropriate Image and function names.

        Returns:
            None. Build progress and results are printed to the console.

        Raises:
            ImageBuilderClientV3Error: If a network error or build service error occurs.
                The error message includes HTTP status codes, URLs, response details, and request ID.
            OSError: If a file system error occurs while preparing the build
                request (e.g., reading Dockerfile or context files).
            RuntimeError: If the build service returns an error response.
            Exception: For any other unexpected errors during the build process.

        Note:
            This method will block until all builds complete or fail. Build logs
            are streamed in real-time to the console with color-coded output
            for each image. If the process is interrupted (KeyboardInterrupt),
            the method will attempt to cancel all builds before re-raising
            the exception.

        Example:
            >>> from tensorlake.applications.image_builder.client_v3 import ImageBuilderClientV3, ImageBuilderClientV3Options
            >>> from tensorlake.applications.image_builder import ImageBuilder, BuildRequest
            >>> from tensorlake.applications import Image
            >>>
            >>> options = ImageBuilderClientV3Options.from_env()
            >>> client = ImageBuilderClientV3(options)
            >>> builder = ImageBuilder(client)
            >>>
            >>> # Create build request
            >>> req = BuildRequest("my-app", "v1.0.0")
            >>>
            >>> # Define image with dependencies
            >>> image = (
            ...     Image(name="my-pdf-parser-image", base_image="ubuntu:24.04")
            ...     .run("apt update")
            ...     .run("pip install langchain")
            ... )
            >>> req.add_image(image, ["parse_pdf", "extract_text"])
            >>>
            >>> # Build the application images (logs streamed to console)
            >>> try:
            ...     await builder.build(req)
            ...     print("Build completed successfully")
            ... except ImageBuilderClientV3Error as e:
            ...     print(f"Build error: {e}")
        """

        try:
            v3_req = (
                await req._synthesize_v3_request()  # pylint: disable=protected-access
            )
            info = await self._client.build_app(v3_req)
            reporters = {
                image_build_info.id: _ImageBuildReporter(image_build_info)
                for image_build_info in info.image_builds.values()
            }
        except ImageBuilderClientV3Error as e:
            click.secho(str(e), err=True, fg="red")
            raise
        except OSError as e:
            click.secho(
                f"File system error while preparing build request: {e}",
                err=True,
                fg="red",
            )
            raise
        except Exception as e:  # pylint: disable=broad-except
            # Fallback handler for any other unexpected errors during build request preparation
            click.secho(
                f"Unexpected error while building application version: {e}",
                err=True,
                fg="red",
            )
            raise

        process_log_events_tasks: list[asyncio.Task[None]] | None = None
        try:
            log_streams: list[AsyncGenerator[ImageBuildLogEventV3, None]] = [
                self._client.stream_image_build_logs(image_build_info.id)
                for image_build_info in info.image_builds.values()
            ]
            click.echo()
            click.secho("🏭 Image build logs:", bold=True)
            process_log_events_tasks = [
                asyncio.create_task(reporter.process_log_events(log_stream))
                for reporter, log_stream in zip(
                    reporters.values(), log_streams, strict=True
                )
            ]
            _ = await asyncio.gather(*process_log_events_tasks, return_exceptions=True)

        except (asyncio.CancelledError, KeyboardInterrupt, click.Abort):
            # User-initiated cancellation - cancel builds and print final results
            canceled_app_version_info = await self._cancel_builds(
                info, process_log_events_tasks
            )
            summary = await self._print_final_results_for_reporters(
                reporters, canceled_app_version_info
            )
            self._print_build_summary(summary)
            # Use os._exit() to bypass asyncio cleanup and avoid "unhandled exception" errors
            # This exits immediately without triggering asyncio.run() cleanup issues
            os._exit(0)
        except Exception as e:  # pylint: disable=broad-except
            # Handle all other exceptions (ImageBuilderClientV3Error and unexpected errors)
            click.secho(str(e), err=True, fg="red")
            canceled_app_version_info = await self._cancel_builds(
                info, process_log_events_tasks
            )
            await self._print_final_results_for_reporters(
                reporters, canceled_app_version_info
            )
            raise

        summary = await self._print_final_results_for_reporters(reporters)
        self._print_build_summary(summary)

        # Raise an exception if any builds failed
        if summary.failed > 0:
            raise RuntimeError("Image build(s) failed")

    def _print_build_summary(self, summary: BuildSummary):
        """Print a compiler-style summary of build results.

        Args:
            summary: BuildSummary with counts for total, succeeded, failed, canceled, and unknown.
        """
        click.echo()  # Empty line before summary
        click.secho("📊 Image build summary:", bold=True)

        click.secho(f"  📦 {summary.total} image(s) total")

        if summary.succeeded > 0:
            click.secho(f"  ✅ {summary.succeeded} succeeded", fg="green")
        else:
            click.secho(f"  ✅ {summary.succeeded} succeeded", fg="white", dim=True)

        if summary.failed > 0:
            click.secho(f"  ❌ {summary.failed} failed", fg="red", err=True)

        if summary.canceled > 0:
            click.secho(f"  🚫 {summary.canceled} canceled", fg="yellow")

        if summary.unknown > 0:
            click.secho(f"  ⚠️ {summary.unknown} unknown", fg="yellow")

        click.echo()

    def _update_summary_from_status(self, summary: BuildSummary, status: str) -> None:
        """Update summary based on build status.

        Args:
            summary: BuildSummary to update with status counts.
            status: The build status to categorize.
        """
        match status:
            case "succeeded":
                summary.succeeded += 1
            case "failed":
                summary.failed += 1
            case "canceled" | "canceling":
                summary.canceled += 1
            case _:
                # Unknown or unexpected status
                summary.unknown += 1

    def _handle_build_info_error(
        self,
        reporter: _ImageBuildReporter,
        summary: BuildSummary,
        error_message: str,
    ) -> None:
        """Handle error when getting build info by printing error and updating summary from last seen status.

        Args:
            reporter: The reporter for the build.
            summary: BuildSummary to update with status counts.
            error_message: The error message to display.
        """
        click.secho(error_message, err=True, fg="red")
        reporter.print_final_result(None)
        self._update_summary_from_status(summary, reporter.last_seen_status)

    async def _print_final_results_for_reporters(
        self,
        reporters: dict[str, _ImageBuildReporter],
        app_version_info: ApplicationVersionBuildInfoV3 | None = None,
    ) -> BuildSummary:
        """Print final results for all reporters.

        Args:
            reporters: Dictionary mapping image build IDs to their reporters.
            app_version_info: Optional application version build info to use instead of
                making individual API calls for each build.

        Returns:
            BuildSummary with counts for total, succeeded, failed, canceled, and unknown.
        """
        summary = BuildSummary(
            total=len(reporters),
            succeeded=0,
            failed=0,
            canceled=0,
            unknown=0,
        )

        click.echo()
        click.secho("🎉 Image build details:", bold=True)

        for reporter in reporters.values():
            try:
                # Use info from app_version_info if available, otherwise fetch individually
                if app_version_info is not None:
                    image_build_info = app_version_info.image_builds.get(reporter.key)
                    info = image_build_info if image_build_info else None
                else:
                    info = await self._client.image_build_info(reporter.image_build_id)
                reporter.print_final_result(info)
                status = info.status if info is not None else reporter.last_seen_status
                self._update_summary_from_status(summary, status)
            except ImageBuilderClientV3Error as e:
                self._handle_build_info_error(
                    reporter,
                    summary,
                    f"Error getting final build info for {reporter.image_build_id}: {e}",
                )
            except RuntimeError as e:
                self._handle_build_info_error(
                    reporter,
                    summary,
                    f"Build service error getting final build info for {reporter.image_build_id}: {e}",
                )
            except Exception as e:  # pylint: disable=broad-except
                self._handle_build_info_error(
                    reporter,
                    summary,
                    f"Unexpected error getting final build info for {reporter.image_build_id}: {e}",
                )

        return summary

    async def _cancel_builds(
        self,
        info: ApplicationVersionBuildInfoV3,
        process_log_events_tasks: list[asyncio.Task[None]] | None = None,
    ) -> ApplicationVersionBuildInfoV3:
        """Cancel all in-flight builds and log streaming tasks.

        Args:
            info: The application version build information.
            process_log_events_tasks: Optional list of log streaming tasks to cancel.

        Returns:
            ApplicationVersionBuildInfoV3: Information about the canceled application version build.

        Raises:
            ImageBuilderClientV3Error: If cancellation fails (network error, build service error, etc.).
        """
        if process_log_events_tasks:
            for task in process_log_events_tasks:
                try:
                    task.cancel()
                except asyncio.CancelledError:
                    pass
                except RuntimeError as e:
                    click.secho(
                        f"Runtime error while canceling log streaming task: {e}",
                        err=True,
                        fg="red",
                    )
                except Exception as e:  # pylint: disable=broad-except
                    # Fallback handler for any other unexpected errors when canceling tasks
                    click.secho(
                        f"Unexpected error while canceling log streaming task: {e}",
                        err=True,
                        fg="red",
                    )

            _ = await asyncio.gather(*process_log_events_tasks, return_exceptions=True)

        # Cancel all builds at once using cancel_app_build
        # This will raise ImageBuilderClientV3Error if cancellation fails
        return await self._client.cancel_app_build(info.id)
