import datetime
import json
import os
import uuid
from typing import Any

import httpx
from pydantic import BaseModel


def print_cloud_event(event: dict[str, Any]) -> None:
    """
    Takes a dictionary representing an event produced by the executor, wraps it in a CloudEvent and prints it to stdout.
    """
    print(json.dumps(new_cloud_event(event)), flush=True)


def new_cloud_event(
    event: dict[str, Any], source: str = "/tensorlake/function_executor/events"
) -> dict[str, Any]:
    """
    Creates a new CloudEvent from the given event dictionary.
    """
    event_dict = {
        "specversion": "1.0",
        "id": str(uuid.uuid4()),
        "timestamp": current_time(),
        "type": "ai.tensorlake.event",
        "source": source,
        "data": event,
    }
    return event_dict


def current_time() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat() + "Z"
