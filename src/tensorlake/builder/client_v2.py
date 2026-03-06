import asyncio
import os
import sys
from typing import Callable
from urllib.parse import urlparse

from pydantic import BaseModel

from tensorlake.builder import ApplicationBuildImageRequest, ApplicationBuildRequest
from tensorlake.builder.log_events import BuildLogEvent, emit_build_log_event
from tensorlake.cloud_client import CloudClient


class BuildInfo(BaseModel):
    id: str
    status: str
    created_at: str
    updated_at: str
    finished_at: str | None
    error_message: str | None = None


class ApplicationImageBuildError(RuntimeError):
    def __init__(self, image_name: str, error: Exception | BaseException):
        self.image_name = image_name
        self.error = error
        super().__init__(f"Error building image {image_name}: {error}")


class ImageBuilderV2Client:
    """Client for interacting with the image builder service."""

    def __init__(
        self,
        cloud_client: CloudClient,
        build_service_path: str = "/images/v2",
        on_build_start: (
            Callable[[ApplicationBuildImageRequest, str], None] | None
        ) = None,
    ):
        self._cloud_client = cloud_client
        self._build_service_path = build_service_path
        self._on_build_start = on_build_start

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

    async def build(self, request: ApplicationBuildRequest) -> None:
        print("Building images...", file=sys.stderr)
        for image_request in request.images:
            for function_name in image_request.function_names:
                if self._on_build_start is not None:
                    self._on_build_start(image_request, function_name)
                print(f"Building {image_request.name}", file=sys.stderr)
                try:
                    await self._build_single(
                        application_name=request.name,
                        application_version=request.version,
                        function_name=function_name,
                        image_name=image_request.name,
                        image_key=image_request.key,
                        context_tar_gz=image_request.context_tar_gz,
                    )
                    print(
                        f"Built {image_request.name} with context sha256 {image_request.context_sha256}",
                        file=sys.stderr,
                    )
                except (asyncio.CancelledError, KeyboardInterrupt):
                    raise
                except Exception as error:
                    raise ApplicationImageBuildError(
                        image_name=image_request.name,
                        error=error,
                    ) from error

    async def _build_single(
        self,
        application_name: str,
        application_version: str,
        function_name: str,
        image_name: str,
        image_key: str,
        context_tar_gz: bytes,
    ) -> BuildInfo:
        print(
            f"{image_name}: Posting {len(context_tar_gz)} bytes of context to build service....",
            file=sys.stderr,
        )

        try:
            build_json = await asyncio.to_thread(
                self._cloud_client.start_image_build,
                self._build_service_path,
                application_name,
                application_version,
                function_name,
                image_name,
                image_key,
                context_tar_gz,
            )
        except Exception as e:
            raise RuntimeError(f"Error building image {image_name}: {e}") from e

        build = BuildInfo.model_validate_json(build_json)

        print(
            f"Waiting for build {build.id} of {image_name} to complete...",
            file=sys.stderr,
        )

        try:
            return await self.stream_logs(build)
        except (asyncio.CancelledError, KeyboardInterrupt):
            await self._cancel_build(build, image_name)
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
        emit_build_log_event(
            event,
            emit_message=lambda message: print(message, file=sys.stderr, flush=True),
            emit_stderr_message=lambda message: print(
                message,
                file=sys.stderr,
                flush=True,
            ),
            emit_stdout_message=lambda message: print(
                message,
                end="",
                file=sys.stderr,
                flush=True,
            ),
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

    async def _cancel_build(self, build: BuildInfo, image_name: str):
        try:
            print(f"\nCancelling build for image {image_name} ...", file=sys.stderr)
            await asyncio.to_thread(
                self._cloud_client.cancel_build,
                self._build_service_path,
                build_id=build.id,
            )
            print(
                f"Build for image {image_name} cancelled successfully",
                file=sys.stderr,
            )
        except Exception as e:
            print(f"Failed to cancel build {build.id}: {e}", file=sys.stderr)
