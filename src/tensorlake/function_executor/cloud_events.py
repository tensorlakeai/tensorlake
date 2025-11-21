import datetime
import json
import uuid
from typing import Any

from tensorlake.applications import InternalError


def print_cloud_event(
    event: dict[str, Any],
    type: str = "ai.tensorlake.event",
    source: str = "/tensorlake/function_executor/events",
) -> None:
    """
    Takes a dictionary representing an event produced by the executor, wraps it in a CloudEvent and prints it to stdout.
    """
    try:
        print(json.dumps(new_cloud_event(event)), flush=True)
    except Exception as e:
        raise InternalError("Failed to print cloud event") from e


def new_cloud_event(
    event: dict[str, Any],
    type: str = "ai.tensorlake.event",
    source: str = "/tensorlake/function_executor/events",
) -> dict[str, Any]:
    """
    Creates a new CloudEvent from the given event dictionary.
    """
    event_dict = {
        "specversion": "1.0",
        "id": str(uuid.uuid4()),
        "timestamp": current_time(),
        "type": type,
        "source": source,
        "data": event,
    }
    return event_dict


def current_time() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat() + "Z"
