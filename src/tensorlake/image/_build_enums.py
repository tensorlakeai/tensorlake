"""Public enums for Image Service build responses and events."""

from typing import TypedDict, cast

from tensorlake._enums import ResponseEnum


class ImageBuildStatus(ResponseEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    UNKNOWN = "unknown"


class ImageBuildPhase(ResponseEnum):
    AWAITING_CONTEXT = "awaiting_context"
    STARTING_BUILDER = "starting_builder"
    EXECUTING = "executing"
    SNAPSHOTTING = "snapshotting"
    PUBLISHING = "publishing"
    FINISHED = "finished"
    UNKNOWN = "unknown"


class ImageBuildKind(ResponseEnum):
    DOCKERFILE = "dockerfile"
    IMPORT = "import"
    DIRECT = "direct"
    UNKNOWN = "unknown"


class ImageBuildCancellationOutcome(ResponseEnum):
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


class ImageBuildErrorCode(ResponseEnum):
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


class ImageBuildEventType(ResponseEnum):
    STATUS = "status"
    BUILD_LOG = "build_log"
    WARNING = "warning"
    BUILD_ACCEPTED = "build_accepted"
    BUILD_PROGRESS = "build_progress"
    BUILD_TERMINAL = "build_terminal"
    INTERRUPTED = "interrupted"
    IMAGE_REGISTERED = "image_registered"
    DONE = "done"
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
    raw: dict


def normalize_build_event(event: dict) -> ImageBuildEvent:
    normalized = {**event, "raw": dict(event)}
    for name, enum in (
        ("type", ImageBuildEventType),
        ("status", ImageBuildStatus),
        ("phase", ImageBuildPhase),
        ("outcome", ImageBuildCancellationOutcome),
    ):
        if name in normalized and (name == "type" or normalized[name] is not None):
            normalized[name] = enum.from_response(normalized[name])
    if "confirmed" in normalized:
        outcome = normalized.get("outcome")
        normalized["confirmed"] = (
            normalized["confirmed"] is True
            and outcome is not None
            and outcome.confirmed
        )
    return cast(ImageBuildEvent, normalized)
