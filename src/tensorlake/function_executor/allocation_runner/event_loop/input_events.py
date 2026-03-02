from dataclasses import dataclass
from typing import Any

from tensorlake.applications.interface.exceptions import TensorlakeError


@dataclass(frozen=True)
class InputEventFunctionCallCreated:
    durable_id: str
    # Not None if creating the function call failed.
    exception: TensorlakeError | None


@dataclass(frozen=True)
class InputEventFunctionCallWatcherResult:
    """Result of a CreateFunctionCallWatcherRequest.

    Contains either the actual function call output (success) or an
    internal error (watcher installation failed or function call failed).
    AllocationRunner downloads blobs and deserializes the output before
    delivering this result.
    """

    function_call_durable_id: str
    output: Any
    # Not None if the function call failed.
    exception: TensorlakeError | None


@dataclass(frozen=True)
class InputEventEmergencyShutdown:
    """Internal event used to stop the entire AllocationRunner immediately.

    This is used when we detect an error which if raised to user code will lead it
    through a different code path than the one recorded in replay history.
    """

    pass


@dataclass(frozen=True)
class _InputEventStopInputEventProcessing:
    """Internal event used to stop input event processing thread."""

    pass


InputEventType = (
    InputEventFunctionCallCreated
    | InputEventFunctionCallWatcherResult
    | InputEventEmergencyShutdown
    | _InputEventStopInputEventProcessing
)
