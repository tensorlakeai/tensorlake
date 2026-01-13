import asyncio
import asyncio
import tempfile

import click
import httpx
from typing import AsyncGenerator
from uuid import uuid7

from tensorlake.builder.client_v3 import ImageBuilderClientV3, ApplicationVersionBuildRequestV3, ApplicationVersionBuildInfoV3, ImageBuildLogEventV3, ImageBuildRequestV3, ImageBuildInfoV3
from tensorlake.applications import Image
from tensorlake.applications.image import create_image_context_file


def _format_http_error(e: httpx.HTTPError) -> str:
    """
    Format an httpx.HTTPError with detailed information.
    
    Args:
        e: The HTTP error exception
        
    Returns:
        A formatted error message with HTTP details
    """
    error_parts = [str(e)]
    
    # Handle HTTPStatusError (has response with status code)
    if isinstance(e, httpx.HTTPStatusError):
        response = e.response
        request = e.request
        
        error_parts.append(f"Status: {response.status_code} {response.reason_phrase}")
        if request:
            error_parts.append(f"URL: {request.method} {request.url}")
        
        # Try to get response text
        # Using broad Exception catch because response.text can fail in various ways
        # (e.g., encoding errors, already read, streaming mode, etc.)
        try:
            response_text = response.text
            if response_text:
                # Truncate long responses
                if len(response_text) > 500:
                    response_text = response_text[:500] + "... (truncated)"
                error_parts.append(f"Response: {response_text}")
        except Exception:
            pass
    
    # Handle TimeoutException
    elif isinstance(e, httpx.TimeoutException):
        error_parts.append("Request timed out")
        if hasattr(e, "request") and e.request:
            error_parts.append(f"URL: {e.request.method} {e.request.url}")
    
    # Handle RequestError (network errors, etc.)
    elif isinstance(e, httpx.RequestError):
        if hasattr(e, "request") and e.request:
            error_parts.append(f"URL: {e.request.method} {e.request.url}")
    
    return " | ".join(error_parts)

class ImageBuildRequest:
    """Represents a request to build a container image for Tensorlake functions.
    
    Tensorlake functions run in function containers. This class encapsulates
    the information needed to build a container image, including the image
    definition (base image, build operations) and the list of function names
    that should be included in the build.
    
    Container images are used to install dependencies (Python packages, system
    packages) that functions need at runtime. Images are defined using the
    `Image` class with methods like `.run()`, `.add()`, `.copy()`, and `.env()`.
    
    For more information on defining images, see:
    https://docs.tensorlake.ai/applications/images
    
    Attributes:
        image: The Image object containing the image definition, base image,
            and build operations (e.g., `apt update`, `pip install` commands).
        function_names: List of function names to include in this image build.
            These are the names of functions that use this image via the
            `@function(image=image)` decorator. All functions listed here will
            run in containers built from this image.
    
    Example:
        >>> from tensorlake.applications import Image
        >>> # Define an image with dependencies
        >>> image = (
        ...     Image(name="my-pdf-parser-image", base_image="ubuntu:24.04")
        ...     .run("apt update")
        ...     .run("pip install langchain")
        ... )
        >>> # Create a build request for functions using this image
        >>> request = ImageBuildRequest(image, ["parse_pdf", "extract_text"])
        >>> print(request.function_names)
        ['parse_pdf', 'extract_text']
    """
    
    def __init__(self, image: Image, function_names: list[str]):
        """Initialize an ImageBuildRequest.
        
        Args:
            image: The Image object containing the image definition. This should
                be an Image instance created with methods like `.run()` to install
                dependencies. The image defines the base image (defaults to
                `python:{version}-slim-bookworm`) and build operations.
            function_names: List of function names (as strings) that should be
                included in this image build. These are the names of functions
                that are associated with this image using `@function(image=image)`.
                All listed functions will be packaged into containers built from
                this image and will have access to all dependencies installed
                in the image.
        
        Raises:
            ValueError: If function_names is empty or contains invalid values.
        
        Note:
            Functions are associated with images using the `@function(image=image)`
            decorator. The function names here should match the names of functions
            that use this image.
        """
        if not function_names:
            raise ValueError("function_names cannot be empty")
        if not all(isinstance(name, str) and name for name in function_names):
            raise ValueError("function_names must contain only non-empty strings")
        self.image = image
        self.function_names = function_names

    async def _synthesize_v3_request(self) -> ImageBuildRequestV3:
        with tempfile.NamedTemporaryFile() as tmp_file:
            context_file_path = tmp_file.name
            create_image_context_file(self.image, context_file_path)

            # Read file content before context manager exits to ensure file is still available
            def read_file():
                with open(context_file_path, "rb") as f:
                    return f.read()

            context_tar_content = await asyncio.to_thread(read_file)

        return ImageBuildRequestV3(
            key=uuid7().hex,
            name=self.image.name,
            description=None,
            context_tar_content=context_tar_content,
            function_names=self.function_names,
        )

class ApplicationVersionBuildRequest:
    """Represents a request to build an application version with multiple container images.
    
    This class is used to construct a build request for an entire application
    version, which may consist of multiple container images. Each image can
    contain different sets of functions and their dependencies.
    
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
            image to be built as part of this application version. Each image
            can have different base images, dependencies, and associated functions.
    
    Example:
        >>> from tensorlake.applications import Image
        >>> req = ApplicationVersionBuildRequest("my-app", "v1.0.0")
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
        """Initialize an ApplicationVersionBuildRequest.
        
        Args:
            name: The name of the application. This should match the application
                name in your Tensorlake project.
            version: The version identifier for this build. This can be any
                string (e.g., "v1.0.0", "latest", "dev-2024-01-01"). The
                version is used to identify and track different builds of the
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

    def add_image(self, image: Image, function_names: list[str]) -> ImageBuildRequest:
        """Add an image build request to this application version build.
        
        This method creates an ImageBuildRequest for the given image and function
        names, adds it to the list of images to be built, and returns it for
        further configuration if needed.
        
        The image should be defined with all necessary build operations (e.g.,
        `.run()` commands to install dependencies) before being added to the
        build request.
        
        Args:
            image: The Image object containing the image definition. This should
                be an Image instance with base image and build operations defined
                (e.g., `Image(name="my-image").run("pip install package")`).
                Functions associated with this image via `@function(image=image)`
                will run in containers built from this image.
            function_names: List of function names (as strings) that should be
                included in this image build. These are the names of functions
                that use this image via the `@function(image=image)` decorator.
                All listed functions will be packaged into containers built from
                this image and will have access to all dependencies installed
                in the image.
        
        Returns:
            The created ImageBuildRequest object, which can be used for further
            configuration or reference.
        
        Raises:
            ValueError: If function_names is empty or contains invalid values.
        
        Example:
            >>> from tensorlake.applications import Image
            >>> req = ApplicationVersionBuildRequest("my-app", "v1.0.0")
            >>> 
            >>> # Define image with dependencies
            >>> image = (
            ...     Image(name="worker", base_image="ubuntu:24.04")
            ...     .run("apt update")
            ...     .run("pip install langchain")
            ... )
            >>> 
            >>> # Add image with its associated functions
            >>> image_req = req.add_image(image, ["parse_document", "extract_data"])
            >>> print(image_req.image.name)
            worker
        """
        if not function_names:
            raise ValueError("function_names cannot be empty")
        if not all(isinstance(name, str) and name for name in function_names):
            raise ValueError("function_names must contain only non-empty strings")
        image_req = ImageBuildRequest(image, function_names)
        self.images.append(image_req)
        return image_req

    async def _synthesize_v3_request(self) -> ApplicationVersionBuildRequestV3:
        return ApplicationVersionBuildRequestV3(
            name=self.name,
            version=self.version,
            images=[await image_req._synthesize_v3_request() for image_req in self.images],  # pylint: disable=protected-access
        )

_IMAGE_NAME_PREFIX_COLORS: list[str] = [
    "white",
    "magenta",
    "cyan",
    "green",
    "yellow",
    "blue",
    "red",
    "bright_white",
    "bright_magenta",
    "bright_cyan",
    "bright_green",
    "bright_yellow",
    "bright_blue",
    "bright_red",
]


class _ImageBuildReporter:
    _instance_count = 0

    def __init__(
        self, info: ImageBuildInfoV3
    ):
        self._info = info
        self._event_cache = []
        self._last_seen_status = info.status
        self._display_name = f"{info.name} ({info.id})" if info.name != "default" else info.id
        prefix_fg_index = _ImageBuildReporter._instance_count % len(
            _IMAGE_NAME_PREFIX_COLORS
        )
        self._color = _IMAGE_NAME_PREFIX_COLORS[prefix_fg_index]
        _ImageBuildReporter._instance_count += 1
        self._finished = False

    @property
    def image_build_id(self) -> str:
        return self._info.id

    async def process_log_events(self, stream: AsyncGenerator[ImageBuildLogEventV3]):
        if self._finished:
            return

        async for event in stream:
            self._event_cache.append(event)
            self._print_log_event(event)

    def print_final_result(self, info: ImageBuildInfoV3 | None):
        if self._finished:
            return
        
        self._finished = True
        self._print_trailer(info)

    def _print_prefix(self, err: bool):
        click.secho(f"{self._display_name}: ", nl=False, err=err, fg=self._color)

    def _print_log_event(self, event: ImageBuildLogEventV3, err: bool = False):
        self._last_seen_status = event.build_status
        nl = True
        msg = event.message

        if event.build_status == "pending":
            msg = "🔄 Build waiting in queue."

        match event.stream:
            case "stdout":
                nl = False
            case "stderr":
                err = True
            case "info":
                msg = f"{event.timestamp} - {event.message}"

        self._print_prefix(err)
        click.secho(msg, nl=nl, err=err)

    def _print_trailer(self, info: ImageBuildInfoV3 | None):
        err = False
        show_logs = False

        status = info.status if info else self._last_seen_status
        error_message = info.error_message if info else None

        match status:
            case "completed":
                msg = "✅ Build completed."
            case "failed":
                err = True
                show_logs = True
                msg = "❌ Build failed." if not error_message else f"❌ Build failed: {error_message}"
            case "canceled":
                msg = "🚫 Build canceled."
            case _:
                msg = f"❓ Unexpected status: {status}"

        self._print_prefix(err)
        click.secho(msg, err=err)

        if show_logs:
            self._event_cache.sort(key=lambda event: event.sequence_number)
            for event in self._event_cache:
                self._print_log_event(event, err=err)


class ApplicationVersionBuilder:
    """Builder for application versions and their associated container images.
    
    This class provides the main interface for building application versions
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
        >>> from tensorlake.builder.client_v3 import ImageBuilderClientV3
        >>> from tensorlake.builder.builder import ApplicationVersionBuilder
        >>> from tensorlake.applications import Image
        >>> 
        >>> client = ImageBuilderClientV3.from_env()
        >>> builder = ApplicationVersionBuilder(client)
        >>> 
        >>> # Create build request
        >>> req = ApplicationVersionBuildRequest("my-app", "v1.0.0")
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
        """Initialize an ApplicationVersionBuilder.
        
        Args:
            client: An ImageBuilderClientV3 instance configured with the
                appropriate build service URL and authentication credentials.
                This client will be used for all build operations.
        
        Raises:
            ValueError: If client is None or invalid.
        
        Example:
            >>> from tensorlake.builder.client_v3 import ImageBuilderClientV3
            >>> client = ImageBuilderClientV3.from_env()
            >>> builder = ApplicationVersionBuilder(client)
        """
        if client is None:
            raise ValueError("client cannot be None")
        self._client = client


    async def build(
        self, req: ApplicationVersionBuildRequest
    ) -> None:
        """Build an application version and all its associated images.
        
        This method orchestrates the complete build process:
        1. Synthesizes the build request from the provided ApplicationVersionBuildRequest
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
            req: The ApplicationVersionBuildRequest containing the application
                name, version, and list of images to build. Each image should
                have been added via `add_image()` with the appropriate Image
                and function names.
        
        Returns:
            None. Build progress and results are printed to the console.
        
        Raises:
            httpx.HTTPError: If a network error occurs during the build request
                or log streaming. The error message includes HTTP status codes,
                URLs, and response details.
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
            >>> from tensorlake.builder.client_v3 import ImageBuilderClientV3
            >>> from tensorlake.builder.builder import ApplicationVersionBuilder, ApplicationVersionBuildRequest
            >>> from tensorlake.applications import Image
            >>> 
            >>> client = ImageBuilderClientV3.from_env()
            >>> builder = ApplicationVersionBuilder(client)
            >>> 
            >>> # Create build request
            >>> req = ApplicationVersionBuildRequest("my-app", "v1.0.0")
            >>> 
            >>> # Define image with dependencies
            >>> image = (
            ...     Image(name="my-pdf-parser-image", base_image="ubuntu:24.04")
            ...     .run("apt update")
            ...     .run("pip install langchain")
            ... )
            >>> req.add_image(image, ["parse_pdf", "extract_text"])
            >>> 
            >>> # Build the application version (logs streamed to console)
            >>> try:
            ...     await builder.build(req)
            ...     print("Build completed successfully")
            ... except httpx.HTTPError as e:
            ...     print(f"Network error: {e}")
        """

        try:
            v3_req = await req._synthesize_v3_request()  # pylint: disable=protected-access
            info = await self._client.build_app(v3_req)
            reporters = { image_build_info.id: _ImageBuildReporter(image_build_info) for image_build_info in info.image_builds.values() }
        except httpx.HTTPError as e:
            error_details = _format_http_error(e)
            click.secho(f"Network error while building application version: {error_details}", err=True, fg="red")
            raise
        except OSError as e:
            click.secho(f"File system error while preparing build request: {e}", err=True, fg="red")
            raise
        except Exception as e:
            # Fallback handler for any other unexpected errors during build request preparation
            click.secho(f"Unexpected error while building application version: {e}", err=True, fg="red")
            raise

        process_log_events_tasks: list[asyncio.Task[None]] | None = None
        try:
            log_streams: list[AsyncGenerator[ImageBuildLogEventV3]] = [ await self._client.stream_image_build_logs(image_build_info.id) for image_build_info in info.image_builds.values() ]
            process_log_events_tasks = [asyncio.create_task(reporter.process_log_events(log_stream)) for reporter, log_stream in zip(reporters.values(), log_streams, strict=True)]
            _ = await asyncio.gather(*process_log_events_tasks, return_exceptions=True)

        except (asyncio.CancelledError, KeyboardInterrupt, click.Abort):
            await self._cancel_builds(info, process_log_events_tasks)
            raise

        except httpx.HTTPError as e:
            error_details = _format_http_error(e)
            click.secho(f"Network error while streaming build logs: {error_details}", err=True, fg="red")
            await self._cancel_builds(info, process_log_events_tasks)
            raise
        except Exception as e:
            # Fallback handler for any other unexpected errors during log streaming
            click.secho(f"Unexpected error while streaming build logs: {e}", err=True, fg="red")
            await self._cancel_builds(info, process_log_events_tasks)
            raise

        for reporter in reporters.values():
            try:
                info = await self._client.image_build_info(reporter.image_build_id)
                reporter.print_final_result(info)
            except httpx.HTTPError as e:
                error_details = _format_http_error(e)
                click.secho(f"Network error getting final build info for {reporter.image_build_id}: {error_details}", err=True, fg="red")
                reporter.print_final_result(None)
            except RuntimeError as e:
                click.secho(f"Build service error getting final build info for {reporter.image_build_id}: {e}", err=True, fg="red")
                reporter.print_final_result(None)
            except Exception as e:
                # Fallback handler for any other unexpected errors when getting final build info
                click.secho(f"Unexpected error getting final build info for {reporter.image_build_id}: {e}", err=True, fg="red")
                reporter.print_final_result(None)
                

    async def _cancel_builds(self, info: ApplicationVersionBuildInfoV3, process_log_events_tasks: list[asyncio.Task[None]] | None = None):
        if process_log_events_tasks:
            for task in process_log_events_tasks:
                try:
                    task.cancel()
                except asyncio.CancelledError:
                    pass
                except RuntimeError as e:
                    click.secho(f"Runtime error while canceling log streaming task: {e}", err=True, fg="red")
                except Exception as e:
                    # Fallback handler for any other unexpected errors when canceling tasks
                    click.secho(f"Unexpected error while canceling log streaming task: {e}", err=True, fg="red")

            _ = await asyncio.gather(*process_log_events_tasks, return_exceptions=True)

        for image_build_info in info.image_builds.values():
            try:
                await self._client.cancel_image_build(image_build_info.id)
            except httpx.HTTPError as e:
                error_details = _format_http_error(e)
                click.secho(f"Network error while canceling build {image_build_info.id}: {error_details}", err=True, fg="red")
            except Exception as e:
                # Fallback handler for any other unexpected errors when canceling builds
                click.secho(f"Unexpected error while canceling build {image_build_info.id}: {e}", err=True, fg="red")

