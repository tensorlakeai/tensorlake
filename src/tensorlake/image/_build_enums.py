"""Public enums for Image Service build responses and events."""

from typing import TypedDict, cast

from tensorlake._enums import UnknownStrEnum


class ImageBuildStatus(UnknownStrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    UNKNOWN = "unknown"


class ImageBuildPhase(UnknownStrEnum):
    AWAITING_CONTEXT = "awaiting_context"
    STARTING_BUILDER = "starting_builder"
    EXECUTING = "executing"
    SNAPSHOTTING = "snapshotting"
    PUBLISHING = "publishing"
    FINISHED = "finished"
    UNKNOWN = "unknown"


class ImageBuildKind(UnknownStrEnum):
    DOCKERFILE = "dockerfile"
    IMPORT = "import"
    DIRECT = "direct"
    UNKNOWN = "unknown"


class ImageBuildCancellationOutcome(UnknownStrEnum):
    CANCELLED = "cancelled"
    ALREADY_CANCELLED = "already_cancelled"
    ALREADY_COMPLETED = "already_completed"
    ALREADY_FAILED = "already_failed"
    NOT_ADMITTED = "not_admitted"
    UNCONFIRMED = "unconfirmed"
    UNKNOWN = "unknown"

    @property
    def confirmed(self) -> bool:
        return self in (self.CANCELLED, self.ALREADY_CANCELLED, self.ALREADY_FAILED)


class ImageBuildErrorCode(UnknownStrEnum):
    USAGE = "usage"
    AUTH = "auth"
    NOT_FOUND = "not_found"
    IMAGE_BUILD_IN_PROGRESS = "image_build_in_progress"
    CLIENT_REQUEST_ID_MISMATCH = "client_request_id_mismatch"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"
    INTERRUPTED = "interrupted"
    SERVICE = "service"
    INTERNAL = "internal"
    UNKNOWN = "unknown"


class ImageBuildEventType(UnknownStrEnum):
    STATUS = "status"
    BUILD_LOG = "build_log"
    WARNING = "warning"
    BUILD_ACCEPTED = "build_accepted"
    BUILD_PROGRESS = "build_progress"
    BUILD_TERMINAL = "build_terminal"
    INTERRUPTED = "interrupted"
    IMAGE_REGISTERED = "image_registered"
    UNKNOWN = "unknown"


class ImageBuildEvent(TypedDict, total=False):
    type: ImageBuildEventType
    message: str
    stream: str
    build_id: str
    client_request_id: str | None
    status: ImageBuildStatus
    phase: ImageBuildPhase | None
    attempt: int
    outcome: ImageBuildCancellationOutcome
    confirmed: bool


def normalize_build_event(event: dict) -> ImageBuildEvent:
    normalized = dict(event)
    for name, enum in (
        ("type", ImageBuildEventType),
        ("status", ImageBuildStatus),
        ("phase", ImageBuildPhase),
        ("outcome", ImageBuildCancellationOutcome),
    ):
        if normalized.get(name) is not None:
            normalized[name] = enum(normalized[name])
    if normalized.get("outcome") is not None and "confirmed" in normalized:
        normalized["confirmed"] = (
            normalized["confirmed"] is True and normalized["outcome"].confirmed
        )
    return cast(ImageBuildEvent, normalized)
