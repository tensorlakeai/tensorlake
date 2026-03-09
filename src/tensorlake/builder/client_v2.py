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
from tensorlake.cloud_client import CloudClient


@dataclass
class BuildContext:
    application_name: str
    application_version: str
    function_name: str


class BuildInfo(BaseModel):
    id: str
    status: str
    created_at: str
    updated_at: str
    finished_at: str | None
    error_message: str | None = None


class BuildLogEvent(BaseModel):
    build_id: str
    timestamp: str
    stream: str
    message: str
    sequence_number: int
    build_status: str


class ImageBuilderV2Client:
    """Client for interacting with the image builder service."""

    def __init__(
        self,
        cloud_client: CloudClient,
        build_service_path: str = "/images/v2",
    ):
        self._cloud_client = cloud_client
        self._build_service_path = build_service_path

    @classmethod
    def from_env(cls) -> "ImageBuilderV2Client":
        api_key = os.getenv("TENSORLAKE_API_KEY")
        if not api_key:
            api_key = os.getenv("TENSORLAKE_PAT")

        organization_id = os.getenv("TENSORLAKE_ORGANIZATION_ID")
        project_id = os.getenv("TENSORLAKE_PROJECT_ID")

        server_url = os.getenv("TENSORLAKE_API_URL", "https://api.tensorlake.ai")
        build_url = os.getenv("TENSORLAKE_BUILD_SERVICE", f"{server_url}/images/v2")

        parsed = urlparse(build_url)
        api_url = f"{parsed.scheme}://{parsed.netloc}"
        build_service_path = parsed.path.rstrip("/") or "/images/v2"

        client = CloudClient(
            api_url=api_url,
            api_key=api_key,
            organization_id=organization_id,
            project_id=project_id,
        )
        return cls(cloud_client=client, build_service_path=build_service_path)

    async def build_collection(
        self,
        context_collection: dict[Image, BuildContext],
        extra_env_vars: list[tuple[str, str]] | None = None,
    ) -> dict[str, str]:
        print("Building images...", file=sys.stderr)

        builds = {}
        for image, context in context_collection.items():
            print(f"Building {image.name}", file=sys.stderr)
            build = await self.build(context, image, extra_env_vars=extra_env_vars)
            print(f"Built {image.name} with hash {image_hash(image)}", file=sys.stderr)
            builds[image_hash(image)] = build.id

        return builds

    async def build(
        self,
        context: BuildContext,
        image: Image,
        extra_env_vars: list[tuple[str, str]] | None = None,
    ) -> BuildInfo:
        _fd, context_file_path = tempfile.mkstemp()
        create_image_context_file(
            image, context_file_path, extra_env_vars=extra_env_vars
        )

        print(
            f"{image.name}: Posting {os.path.getsize(context_file_path)} bytes of context to build service....",
            file=sys.stderr,
        )

        file_content = await asyncio.to_thread(Path(context_file_path).read_bytes)

        os.remove(context_file_path)

        try:
            build_json = await asyncio.to_thread(
                self._cloud_client.start_image_build,
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
        except (asyncio.CancelledError, KeyboardInterrupt):
            await self._cancel_build(build, image)
            raise

    async def stream_logs(self, build: BuildInfo) -> BuildInfo:
        print(f"Streaming logs for build {build.id}", file=sys.stderr)
        try:
            await asyncio.to_thread(
                self._cloud_client.stream_build_logs_to_stderr,
                self._build_service_path,
                build.id,
            )
        except Exception:
            events_json: list[str] = await asyncio.to_thread(
                self._cloud_client.stream_build_logs_json,
                self._build_service_path,
                build.id,
            )
            for event_json in events_json:
                log_entry = BuildLogEvent.model_validate_json(event_json)
                self._print_build_log_event(event=log_entry)

        return await self.build_info(build.id)

    def _print_build_log_event(self, event: BuildLogEvent):
        if event.build_status == "pending":
            print("Build waiting in queue...", file=sys.stderr, flush=True)
        else:
            match event.stream:
                case "stdout":
                    print(event.message, end="", file=sys.stderr, flush=True)
                case "stderr":
                    print(event.message, file=sys.stderr, flush=True)
                case "info":
                    print(
                        f"{event.timestamp}: {event.message}",
                        file=sys.stderr,
                        flush=True,
                    )

    async def build_info(self, build_id: str) -> BuildInfo:
        try:
            build_info_json = await asyncio.to_thread(
                self._cloud_client.build_info_json,
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
                self._cloud_client.cancel_build,
                self._build_service_path,
                build_id=build.id,
            )
            print(
                f"Build for image {image.name} cancelled successfully",
                file=sys.stderr,
            )
        except Exception as e:
            print(f"Failed to cancel build {build.id}: {e}", file=sys.stderr)
