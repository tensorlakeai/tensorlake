import asyncio
import json
import sys
from dataclasses import dataclass

import click
from pydantic import BaseModel

from tensorlake.builder import ApplicationBuildRequest
from tensorlake.builder.client_v2 import ApplicationImageBuildError
from tensorlake.cloud_client import CloudClient


class ApplicationBuildImageResult(BaseModel):
    id: str
    app_version_id: str | None = None
    key: str | None = None
    name: str | None = None
    description: str | None = None
    status: str
    error_message: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    finished_at: str | None = None
    function_names: list[str] | None = None


class ApplicationBuildResult(BaseModel):
    id: str
    organization_id: str | None = None
    project_id: str | None = None
    name: str
    version: str
    status: str | None = None
    image_builds: list[ApplicationBuildImageResult]


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
    total: int
    succeeded: int = 0
    failed: int = 0
    canceled: int = 0
    unknown: int = 0


class _ImageBuildReporter:
    _instance_count = 0

    def __init__(self, info: ApplicationBuildImageResult):
        self._info = info
        self._last_seen_status = info.status
        self._display_name = self._build_display_name(info)
        self._color = _IMAGE_NAME_PREFIX_COLORS[
            _ImageBuildReporter._instance_count % len(_IMAGE_NAME_PREFIX_COLORS)
        ]
        _ImageBuildReporter._instance_count += 1

    @staticmethod
    def _build_display_name(info: ApplicationBuildImageResult) -> str:
        if info.name and info.key:
            return f"{info.name}[{info.key}]"
        if info.name:
            return info.name
        if info.key:
            return info.key
        return info.id[:12]

    @property
    def display_name(self) -> str:
        return self._display_name

    @property
    def color(self) -> str:
        return self._color

    @property
    def image_build_id(self) -> str:
        return self._info.id

    @property
    def last_seen_status(self) -> str:
        return self._last_seen_status

    def print_final_result(self, info: ApplicationBuildImageResult | None) -> str:
        status = info.status if info is not None else self._last_seen_status
        error_message = info.error_message if info is not None else None

        if status == "succeeded":
            self._print_message("Image build succeeded")
        elif status == "failed":
            message = "Image build failed"
            if error_message:
                message = f"{message}: {error_message}"
            self._print_message(message, err=True)
        elif status in {"canceled", "canceling"}:
            self._print_message("Image build canceled", err=True)
        else:
            self._print_message(f"Image build ended with status: {status}", err=True)

        return status

    def _print_prefix(self, err: bool) -> None:
        if _ImageBuildReporter._instance_count > 1:
            click.secho(f"{self._display_name}: ", nl=False, err=err, fg=self._color)

    def _print_message(
        self,
        message: str,
        *,
        err: bool = False,
        nl: bool = True,
    ) -> None:
        self._print_prefix(err)
        click.secho(message, err=err, nl=nl)


class ImageBuilderV3Client:
    def __init__(
        self,
        cloud_client: CloudClient,
        build_service_path: str = "/images/v3/applications",
    ):
        self._cloud_client = cloud_client
        self._build_service_path = build_service_path
        self._image_service_path = self._derive_image_service_path(build_service_path)

    @staticmethod
    def _derive_image_service_path(build_service_path: str) -> str:
        normalized_path = build_service_path.rstrip("/")
        if normalized_path.endswith("/applications"):
            return normalized_path.removesuffix("/applications")
        return normalized_path

    async def build(self, request: ApplicationBuildRequest) -> ApplicationBuildResult:
        print(
            f"Python ImageBuilderV3Client.build called for {request.name}@{request.version}",
            file=sys.stderr,
        )
        request_json = json.dumps(
            {
                "name": request.name,
                "version": request.version,
                "images": [
                    {
                        "key": image.key,
                        "name": image.name,
                        "context_tar_part_name": image.key,
                        "context_sha256": image.context_sha256,
                        "function_names": image.function_names,
                    }
                    for image in request.images
                ],
            }
        )
        image_contexts = [
            (image.key, image.context_tar_gz) for image in request.images
        ]
        response_json = await asyncio.to_thread(
            self._cloud_client.create_application_build,
            self._build_service_path,
            request_json,
            image_contexts,
        )
        result = ApplicationBuildResult.model_validate_json(response_json)
        reporters = self._build_reporters(result)

        try:
            await asyncio.gather(
                *[
                    self._stream_build_logs(reporters[image_build.id])
                    for image_build in result.image_builds
                ]
            )
        except (asyncio.CancelledError, KeyboardInterrupt):
            await self._cancel_application_build(result.id)
            raise

        final_result = await self._application_build_info(result.id)
        self._print_summary(reporters, final_result)
        self._raise_for_failed_builds(final_result)
        return final_result

    @staticmethod
    def _build_reporters(
        result: ApplicationBuildResult,
    ) -> dict[str, _ImageBuildReporter]:
        _ImageBuildReporter._instance_count = 0
        return {
            image_build.id: _ImageBuildReporter(image_build)
            for image_build in result.image_builds
        }

    async def _application_build_info(
        self, application_build_id: str
    ) -> ApplicationBuildResult:
        response_json = await asyncio.to_thread(
            self._cloud_client.application_build_info_json,
            self._build_service_path,
            application_build_id,
        )
        return ApplicationBuildResult.model_validate_json(response_json)

    async def _cancel_application_build(
        self, application_build_id: str
    ) -> ApplicationBuildResult:
        response_json = await asyncio.to_thread(
            self._cloud_client.cancel_application_build,
            self._build_service_path,
            application_build_id,
        )
        return ApplicationBuildResult.model_validate_json(response_json)

    async def _stream_build_logs(self, reporter: _ImageBuildReporter) -> None:
        await asyncio.to_thread(
            self._cloud_client.stream_build_logs_to_stderr_prefixed,
            self._image_service_path,
            reporter.image_build_id,
            reporter.display_name,
            reporter.color,
        )

    @staticmethod
    def _print_summary(
        reporters: dict[str, _ImageBuildReporter],
        final_result: ApplicationBuildResult,
    ) -> None:
        summary = BuildSummary(total=len(reporters))
        final_builds = {image_build.id: image_build for image_build in final_result.image_builds}

        click.echo(file=sys.stderr)
        click.secho("Image build summary:", bold=True, err=True)
        for reporter in reporters.values():
            status = reporter.print_final_result(final_builds.get(reporter.image_build_id))
            if status == "succeeded":
                summary.succeeded += 1
            elif status == "failed":
                summary.failed += 1
            elif status in {"canceled", "canceling"}:
                summary.canceled += 1
            else:
                summary.unknown += 1

        click.echo(file=sys.stderr)
        click.secho(
            (
                f"total={summary.total} "
                f"succeeded={summary.succeeded} "
                f"failed={summary.failed} "
                f"canceled={summary.canceled} "
                f"unknown={summary.unknown}"
            ),
            err=True,
            bold=True,
        )

    @staticmethod
    def _raise_for_failed_builds(result: ApplicationBuildResult) -> None:
        for image_build in result.image_builds:
            if image_build.status == "failed":
                raise ApplicationImageBuildError(
                    image_name=image_build.name or image_build.key or image_build.id,
                    error=RuntimeError(
                        image_build.error_message
                        or f"Image build {image_build.id} failed"
                    ),
                )
            if image_build.status in {"canceled", "canceling"}:
                raise RuntimeError(
                    image_build.error_message
                    or f"Image build {image_build.id} was canceled"
                )
