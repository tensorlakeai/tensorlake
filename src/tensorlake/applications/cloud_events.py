import datetime
import json
import uuid
from typing import Any

from .interface.exceptions import SerializationError


def print_cloud_event(
    event: dict[str, Any],
    type: str = "ai.tensorlake.event",
    source: str = "/tensorlake/function_executor/events",
    message: str | None = None,
) -> None:
    """
    Takes a dictionary representing an event produced by the executor, wraps it in a CloudEvent and prints it to stdout.

    Raises SerializationError: If the event cannot be serialized to JSON.
    """
    print(_serialize_json(new_cloud_event(event, type, source, message)), flush=True)


def new_cloud_event(
    event: dict[str, Any],
    type: str = "ai.tensorlake.event",
    source: str = "/tensorlake/function_executor/events",
    message: str | None = None,
) -> dict[str, Any]:
    """
    Creates a new CloudEvent from the given event dictionary.

    All values in the event dictionary must be JSON serializable.
    See https://docs.python.org/3/library/json.html#json.JSONEncoder.
    """
    event_dict = {
        "specversion": "1.0",
        "id": str(uuid.uuid4()),
        "timestamp": _current_time(),
        "type": type,
        "source": source,
        "data": event,
    }
    # add custom message outside of the event dictionary to
    # avoid having to search for it in deep structures.
    if message is not None:
        event_dict["message"] = message
    return event_dict


def _current_time() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _serialize_json(obj: dict[str, Any]) -> str:
    """Convert a dictionary to a JSON string.

    Args:
        obj: The dictionary to serialize

    Returns:
        A version of the object serialized into a JSON string

    Raises:
        SerializationError: If the object cannot be serialized to JSON
    """
    try:
        return json.dumps(obj)
    except Exception as e:
        raise SerializationError(f"Failed to serialize event payload: {e}") from e
