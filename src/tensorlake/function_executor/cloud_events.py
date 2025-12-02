import datetime
import json
import uuid
from typing import Any

import pydantic

from tensorlake.applications import SerializationError


def print_cloud_event(
    event: dict[str, Any],
    type: str = "ai.tensorlake.event",
    source: str = "/tensorlake/function_executor/events",
    message: str | None = None,
) -> None:
    """
    Takes a dictionary representing an event produced by the executor, wraps it in a CloudEvent and prints it to stdout.
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
    """Recursively convert Pydantic models to dicts and verify JSON serializability.

    Args:
        obj: The object to serialize

    Returns:
        A JSON-serializable version of the object

    Raises:
        SerializationError: If the object cannot be serialized to JSON
    """
    try:
        # Recursively serializes Pydantic models to dicts
        for key, value in obj.items():
            obj[key] = _serialize_value(value)
        return json.dumps(obj)
    except Exception as e:
        raise SerializationError(f"Failed to serialize event payload: {e}") from e


def _serialize_value(obj: Any) -> Any:
    if isinstance(obj, pydantic.BaseModel):
        return obj.model_dump()
    elif isinstance(obj, dict):
        for key, value in obj.items():
            obj[key] = _serialize_value(value)
        return obj
    elif isinstance(obj, (list, tuple)):
        return [_serialize_value(item) for item in obj]
    else:
        return obj
