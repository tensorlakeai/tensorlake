from collections.abc import Callable

from pydantic import AliasChoices, BaseModel, ConfigDict, Field


class BuildLogEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    image_build_id: str = Field(
        validation_alias=AliasChoices("image_build_id", "build_id")
    )
    timestamp: str
    stream: str
    message: str
    sequence_number: int
    build_status: str


def emit_build_log_event(
    event: BuildLogEvent,
    emit_message: Callable[[str], None],
    *,
    emit_stderr_message: Callable[[str], None],
    emit_stdout_message: Callable[[str], None],
) -> None:
    if event.build_status in {"pending", "enqueued"}:
        emit_stderr_message("Build waiting in queue...")
        return

    match event.stream:
        case "stdout":
            emit_stdout_message(event.message)
        case "stderr":
            emit_stderr_message(event.message)
        case "info":
            emit_stderr_message(f"{event.timestamp}: {event.message}")
        case _:
            emit_message(event.message)
