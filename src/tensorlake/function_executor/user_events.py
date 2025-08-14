# This module logs important Function Executor user visible events to stdout.
# These events help users to understand the execution flow of their functions
# when they are looking at a live log stream. The events include details like
# allocation IDs which can be shared by the users with Tensorlake support to
# improve the support experience.
#
# The events have strict structured json format because they might be used for
# automatic Function Executor log stream processing in the future.
import datetime
import json
from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class InitializationEventDetails:
    namespace: str
    graph_name: str
    graph_version: str
    function_name: str


def log_user_event_initialization_started(details: InitializationEventDetails) -> None:
    # Using standardized tags, see https://github.com/tensorlakeai/indexify/blob/main/docs/tags.md.
    _log_event(
        {
            "event": "function_executor_initialization_started",
            "namespace": details.namespace,
            "graph": details.graph_name,
            "graph_version": details.graph_version,
            "fn": details.function_name,
        }
    )


def log_user_event_initialization_finished(
    details: InitializationEventDetails, success: bool
) -> None:
    # Using standardized tags, see https://github.com/tensorlakeai/indexify/blob/main/docs/tags.md.
    _log_event(
        {
            "event": "function_executor_initialization_finished",
            "success": success,
            "namespace": details.namespace,
            "graph": details.graph_name,
            "graph_version": details.graph_version,
            "fn": details.function_name,
        }
    )


@dataclass
class TaskAllocationEventDetails:
    namespace: str
    graph_name: str
    graph_version: str
    function_name: str
    graph_invocation_id: str
    task_id: str
    allocation_id: str


def log_user_event_task_allocations_started(
    details: List[TaskAllocationEventDetails],
) -> None:
    # Using standardized tags, see https://github.com/tensorlakeai/indexify/blob/main/docs/tags.md.
    _log_event(
        {
            "event": "task_allocations_started",
            "allocations": [
                {
                    "namespace": alloc_info.namespace,
                    "graph": alloc_info.graph_name,
                    "graph_version": alloc_info.graph_version,
                    "fn": alloc_info.function_name,
                    "graph_invocation_id": alloc_info.graph_invocation_id,
                    "task_id": alloc_info.task_id,
                    "allocation_id": alloc_info.allocation_id,
                }
                for alloc_info in details
            ],
        }
    )


def log_user_event_task_allocations_finished(
    details: List[TaskAllocationEventDetails],
) -> None:
    # Using standardized tags, see https://github.com/tensorlakeai/indexify/blob/main/docs/tags.md.
    _log_event(
        {
            "event": "task_allocations_finished",
            "allocations": [
                {
                    "namespace": alloc_info.namespace,
                    "graph": alloc_info.graph_name,
                    "graph_version": alloc_info.graph_version,
                    "fn": alloc_info.function_name,
                    "graph_invocation_id": alloc_info.graph_invocation_id,
                    "task_id": alloc_info.task_id,
                    "allocation_id": alloc_info.allocation_id,
                }
                for alloc_info in details
            ],
        }
    )


# Suffix used to make it clear to users that this log line is not made by them.
# This also helps to filter the events during automatic FE log stream processing.
_EVENT_SUFFIX = "tensorlake_event:"


def _log_event(event: Dict[str, Any]) -> None:
    event["timestamp"] = datetime.datetime.now(datetime.timezone.utc).isoformat() + "Z"
    # Flush stdout to make sure that the last event in FE log stream is always visible.
    print(_EVENT_SUFFIX, json.dumps(event), flush=True)
