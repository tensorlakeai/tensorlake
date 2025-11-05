# This module logs important Function Executor user visible events to stdout.
# These events help users to understand the execution flow of their functions
# when they are looking at a live log stream. The events include details like
# allocation IDs which can be shared by the users with Tensorlake support to
# improve the support experience.
#
# The events have strict structured json format because they are used for
# automatic Function Executor log stream processing in the future.
# The event attribute names are human readable because they are visible to users.
import traceback
from dataclasses import dataclass
from typing import Any

from tensorlake.function_executor.cloud_events import print_cloud_event


@dataclass
class InitializationEventDetails:
    namespace: str
    application_name: str
    application_version: str
    function_name: str


def log_user_event_initialization_started(details: InitializationEventDetails) -> None:
    # Using standardized tags, see https://github.com/tensorlakeai/indexify/blob/main/docs/tags.md.
    print_cloud_event(
        {
            "event": "function_executor_initialization_started",
            "message": "Initializing function executor",
            "namespace": details.namespace,
            "application": details.application_name,
            "application_version": details.application_version,
            "function": details.function_name,
        }
    )


def log_user_event_initialization_finished(details: InitializationEventDetails) -> None:
    # Using standardized tags, see https://github.com/tensorlakeai/indexify/blob/main/docs/tags.md.
    print_cloud_event(
        {
            "event": "function_executor_initialization_finished",
            "message": "Function executor initialization completed",
            "namespace": details.namespace,
            "application": details.application_name,
            "application_version": details.application_version,
            "function": details.function_name,
        }
    )


def log_user_event_initialization_failed(
    details: InitializationEventDetails, error: BaseException
) -> None:
    # Using standardized tags, see https://github.com/tensorlakeai/indexify/blob/main/docs/tags.md.
    print_cloud_event(
        {
            "level": "error",
            "event": "function_executor_initialization_failed",
            "message": "Function executor initialization failed",
            "namespace": details.namespace,
            "application": details.application_name,
            "application_version": details.application_version,
            "function": details.function_name,
            "error": traceback.format_exception(error),
        }
    )


@dataclass
class AllocationEventDetails:
    namespace: str
    application_name: str
    application_version: str
    function_name: str
    request_id: str
    function_call_id: str
    allocation_id: str


def log_user_event_allocations_started(
    details: list[AllocationEventDetails],
) -> None:
    # Using standardized tags, see https://github.com/tensorlakeai/indexify/blob/main/docs/tags.md.
    print_cloud_event(
        {
            "event": "allocations_started",
            "message": "Starting allocations",
            "allocations": [
                {
                    "namespace": alloc_info.namespace,
                    "application": alloc_info.application_name,
                    "application_version": alloc_info.application_version,
                    "function": alloc_info.function_name,
                    "request_id": alloc_info.request_id,
                    "function_call_id": alloc_info.function_call_id,
                    "allocation_id": alloc_info.allocation_id,
                }
                for alloc_info in details
            ],
        }
    )


def log_user_event_allocations_finished(
    details: list[AllocationEventDetails],
) -> None:
    # Using standardized tags, see https://github.com/tensorlakeai/indexify/blob/main/docs/tags.md.
    print_cloud_event(
        {
            "event": "allocations_finished",
            "message": "Allocations completed",
            "allocations": [
                {
                    "namespace": alloc_info.namespace,
                    "application": alloc_info.application_name,
                    "application_version": alloc_info.application_version,
                    "function": alloc_info.function_name,
                    "request_id": alloc_info.request_id,
                    "function_call_id": alloc_info.function_call_id,
                    "allocation_id": alloc_info.allocation_id,
                }
                for alloc_info in details
            ],
        }
    )


def log_user_event_function_call_failed(
    details: AllocationEventDetails, error: BaseException
) -> None:
    print_cloud_event(
        {
            "level": "error",
            "event": "function_call_failed",
            "message": str(error),
            "namespace": details.namespace,
            "application": details.application_name,
            "application_version": details.application_version,
            "function": details.function_name,
            "request_id": details.request_id,
            "function_call_id": details.function_call_id,
            "allocation_id": details.allocation_id,
            "error": traceback.format_exception(error),
        }
    )
