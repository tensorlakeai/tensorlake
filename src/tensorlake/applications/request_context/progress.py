from typing import Any

from tensorlake.applications.cloud_events import event_time, print_cloud_event


def print_progress_update(
    request_id: str,
    function_name: str,
    current: float,
    total: float,
    message: str | None,
    attributes: dict[str, str] | None,
    local_mode: bool,
) -> None:
    """Prints a progress update to stdout.

    Uses human-readable format in local mode, and Cloud Events format otherwise (remote/FE mode).
    Doesn't raise any exceptions.
    """
    current: float | int = _maybe_int(current)
    total: float | int = _maybe_int(total)
    event_message: str = (
        message
        if message is not None
        else f"{function_name}: executing step {current} of {total}"
    )

    event: dict[str, Any] = {
        "request_id": request_id,
        "function_name": function_name,
        "message": event_message,
        "step": current,
        "total": total,
        "attributes": attributes,
        "created_at": event_time(),
    }

    if local_mode:
        print(f"Progress Update: {event}", flush=True)
    else:
        try:
            print_cloud_event(
                # The shape of the object is important because these events
                # get merged with events sent by Indexify server,
                # which sets a key with the struct name with the event as value.
                {"RequestProgressUpdated": event},
                type="ai.tensorlake.progress_update",
                source="/tensorlake/applications/progress",
                message=event_message,
            )
        except Exception:
            print(f"Failed to print progress update cloud event: {event}", flush=True)


def _maybe_int(value: float) -> float | int:
    if value.is_integer():
        return int(value)
    else:
        return value
