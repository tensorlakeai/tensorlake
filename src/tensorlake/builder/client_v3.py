import asyncio
import json
import sys
from dataclasses import dataclass
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, model_validator

from tensorlake.builder import ApplicationBuildRequest
from tensorlake.builder.client_v2 import ApplicationImageBuildError
from tensorlake.builder.log_events import BuildLogEvent, emit_build_log_event
from tensorlake.cloud_client import CloudClient

NonEmptyString = Annotated[str, Field(min_length=1)]
Sha256Hex = Annotated[str, Field(pattern=r"^[0-9a-fA-F]{64}$")]


class CreateApplicationBuildImagePayload(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    key: NonEmptyString
    name: str | None = None
    description: str | None = None
    context_tar_part_name: NonEmptyString
    context_sha256: Sha256Hex
    function_names: Annotated[list[NonEmptyString], Field(min_length=1)]


class CreateApplicationBuildPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    name: NonEmptyString
    version: NonEmptyString
    images: Annotated[list[CreateApplicationBuildImagePayload], Field(min_length=1)]

    @model_validator(mode="after")
    def validate_unique_fields(self) -> "CreateApplicationBuildPayload":
        image_keys = [image.key for image in self.images]
        if len(image_keys) != len(set(image_keys)):
            raise ValueError("image keys must be unique within an application build")

        context_part_names = [image.context_tar_part_name for image in self.images]
        if len(context_part_names) != len(set(context_part_names)):
            raise ValueError(
                "context_tar_part_name values must be unique within an application build"
            )

        function_names = [
            function_name
            for image in self.images
            for function_name in image.function_names
        ]
        if len(function_names) != len(set(function_names)):
            raise ValueError(
                "function_names must be unique across images in an application build"
            )

        return self


class ApplicationBuildImageResult(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)

    id: str
    app_version_id: str | None = None
    key: str | None = None
    name: str | None = None
    description: str | None = None
    context_sha256: str | None = None
    status: str
    error_message: str | None = None
    image_uri: str | None = None
    image_digest: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    finished_at: str | None = None
    function_names: list[str] = Field(default_factory=list)


class ApplicationBuildResult(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)

    id: str
    organization_id: str
    project_id: str
    name: str
    version: str
    status: str
    created_at: str | None = None
    updated_at: str | None = None
    finished_at: str | None = None
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

_ANSI_COLOR_CODES: dict[str, str] = {
    "red": "31",
    "green": "32",
    "yellow": "33",
    "blue": "34",
    "magenta": "35",
    "cyan": "36",
    "white": "37",
    "bright_red": "91",
    "bright_green": "92",
    "bright_yellow": "93",
    "bright_blue": "94",
    "bright_magenta": "95",
    "bright_cyan": "96",
    "bright_white": "97",
}


def _styled_text(
    message: str,
    *,
    stream,
    color: str | None = None,
    bold: bool = False,
) -> str:
    if not hasattr(stream, "isatty") or not stream.isatty():
        return message

    codes: list[str] = []
    if bold:
        codes.append("1")
    if color is not None and color in _ANSI_COLOR_CODES:
        codes.append(_ANSI_COLOR_CODES[color])
    if not codes:
        return message
    return f"\033[{';'.join(codes)}m{message}\033[0m"


def _print_message(
    message: str,
    *,
    err: bool = False,
    nl: bool = True,
    color: str | None = None,
    bold: bool = False,
) -> None:
    stream = sys.stderr if err else sys.stdout
    stream.write(
        _styled_text(message, stream=stream, color=color, bold=bold)
        + ("\n" if nl else "")
    )
    stream.flush()


@dataclass
class BuildSummary:
    total: int
    succeeded: int = 0
    failed: int = 0
    canceled: int = 0
    unknown: int = 0


class _ImageBuildReporter:
    _instance_count = 0

    def __init__(
        self,
        application_name: str,
        info: ApplicationBuildImageResult,
        *,
        disambiguate_name: bool = False,
    ):
        self._info = info
        self._last_seen_status = info.status
        self._display_name = self._build_display_name(
            application_name,
            info,
            disambiguate_name=disambiguate_name,
        )
        self._color = _IMAGE_NAME_PREFIX_COLORS[
            _ImageBuildReporter._instance_count % len(_IMAGE_NAME_PREFIX_COLORS)
        ]
        _ImageBuildReporter._instance_count += 1

    @staticmethod
    def _build_display_name(
        application_name: str,
        info: ApplicationBuildImageResult,
        *,
        disambiguate_name: bool = False,
    ) -> str:
        image_name = info.name or "image"
        display_name = f"{application_name}/{image_name}"
        if not disambiguate_name:
            return display_name

        if len(info.function_names) == 1:
            qualifier = info.function_names[0]
        elif len(info.function_names) > 1:
            qualifier = f"{info.function_names[0]}+{len(info.function_names) - 1}"
        else:
            qualifier = info.key or info.id

        return f"{display_name} [{qualifier}]"

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

    def print_log_event(self, event: BuildLogEvent) -> None:
        self._last_seen_status = event.build_status
        emit_build_log_event(
            event,
            emit_message=lambda message: self._print_message(message, err=True),
            emit_stderr_message=lambda message: self._print_message(message, err=True),
            emit_stdout_message=lambda message: self._print_message(message, nl=False),
        )

    def _print_prefix(self, err: bool) -> None:
        if _ImageBuildReporter._instance_count > 1:
            _print_message(
                f"{self._display_name}: ",
                nl=False,
                err=err,
                color=self._color,
            )

    def _print_message(
        self,
        message: str,
        *,
        err: bool = False,
        nl: bool = True,
    ) -> None:
        self._print_prefix(err)
        _print_message(message, err=err, nl=nl)


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
        request_payload = CreateApplicationBuildPayload(
            name=request.name,
            version=request.version,
            images=[
                CreateApplicationBuildImagePayload(
                    key=image.key,
                    name=image.name,
                    context_tar_part_name=image.key,
                    context_sha256=image.context_sha256,
                    function_names=image.function_names,
                )
                for image in request.images
            ],
        )
        request_json = request_payload.model_dump_json(exclude_none=True)
        image_contexts = [(image.key, image.context_tar_gz) for image in request.images]
        response_json = await asyncio.to_thread(
            self._cloud_client.create_application_build,
            self._build_service_path,
            request_json,
            image_contexts,
        )
        created_result = ApplicationBuildResult.model_validate_json(response_json)
        reporters = self._build_reporters(created_result)

        try:
            await asyncio.gather(
                *[
                    self._stream_build_logs(reporters[image_build.id])
                    for image_build in created_result.image_builds
                ]
            )
        except (asyncio.CancelledError, KeyboardInterrupt):
            await self._cancel_application_build(created_result.id)
            raise

        final_result = await self._application_build_info(created_result.id)
        self._print_summary(reporters, final_result)
        self._raise_for_failed_builds(final_result)
        return final_result

    @staticmethod
    def _build_reporters(
        result: ApplicationBuildResult,
    ) -> dict[str, _ImageBuildReporter]:
        _ImageBuildReporter._instance_count = 0
        image_name_counts: dict[str, int] = {}
        for image_build in result.image_builds:
            image_name = image_build.name or "image"
            image_name_counts[image_name] = image_name_counts.get(image_name, 0) + 1

        return {
            image_build.id: _ImageBuildReporter(
                result.name,
                image_build,
                disambiguate_name=image_name_counts.get(
                    image_build.name or "image", 0
                )
                > 1,
            )
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
        try:
            await asyncio.to_thread(
                self._cloud_client.stream_build_logs_to_stderr_prefixed,
                self._image_service_path,
                reporter.image_build_id,
                reporter.display_name,
                reporter.color,
            )
        except Exception:
            events_json: list[str] = await asyncio.to_thread(
                self._cloud_client.stream_build_logs_json,
                self._image_service_path,
                reporter.image_build_id,
            )
            for event_json in events_json:
                reporter.print_log_event(BuildLogEvent.model_validate_json(event_json))

    @staticmethod
    def _print_summary(
        reporters: dict[str, _ImageBuildReporter],
        final_result: ApplicationBuildResult,
    ) -> None:
        summary = BuildSummary(total=len(reporters))
        final_builds = {
            image_build.id: image_build for image_build in final_result.image_builds
        }

        _print_message("", err=True)
        _print_message("Image build summary:", bold=True, err=True)
        for reporter in reporters.values():
            status = reporter.print_final_result(
                final_builds.get(reporter.image_build_id)
            )
            if status == "succeeded":
                summary.succeeded += 1
            elif status == "failed":
                summary.failed += 1
            elif status in {"canceled", "canceling"}:
                summary.canceled += 1
            else:
                summary.unknown += 1

        _print_message("", err=True)
        _print_message(
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
