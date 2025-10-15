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


def render_cloud_event(_logger, _method_name, event: dict[str, Any]) -> dict[str, Any]:
    """
    Structlog processor to render executor logs as CloudEvents.
    This processor can be added to the structlog pipeline to ensure that all logs are formatted as CloudEvents.
    """

    return new_cloud_event(event)


def new_cloud_event(event: dict[str, Any]) -> dict[str, Any]:
    """
    Creates a new CloudEvent from the given event dictionary.
    """
    event_dict = {
        "specversion": "1.0",
        "id": str(uuid.uuid4()),
        "time": current_time(),
        "type": "ai.tensorlake.executor.event",
        "source": "/tensorlake/executor",
        "data": event,
    }
    return event_dict


class Resource(BaseModel):
    namespace: str
    application: str
    application_version: str
    executor_id: str
    fn_executor_id: str
    fn: str


def push_event_to_collector(
    resource: Resource,
    event: dict[str, Any],
    collector_url: str | None = None,
) -> None:
    """
    Pushes the given event to a log collector.
    This function relies on the existence of the TENSORLAKE_COLLECTOR_URL environment variable.
    If the environment variable is not set, the event is ignored.

    This function does not capture any exceptions that may occur during the HTTP request
    because it's designed to be embedded into the executor.
    The executor needs to handle HTTP errors and collect metrics.
    """
    collector_url = (
        os.environ.get("TENSORLAKE_COLLECTOR_URL")
        if collector_url is None
        else collector_url
    )
    if collector_url:
        body = resource.model_dump()
        body["event"] = new_cloud_event(event)

        response = httpx.post(collector_url, json=body)
        response.raise_for_status()


def current_time() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat() + "Z"
