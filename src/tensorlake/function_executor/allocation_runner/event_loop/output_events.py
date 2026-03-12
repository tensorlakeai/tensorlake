from dataclasses import dataclass
from typing import Any

from tensorlake.applications.metadata import SPLITTER_INPUT_MODE


@dataclass(frozen=True)
class FunctionCallRef:
    """Reference to a function call by its durable ID.

    Used in CreateFunctionCallRequest args/kwargs to reference the output of
    another function call as an input. The referenced function call can
    be previously started or from the same batch of requests.
    """

    durable_id: str


@dataclass(frozen=True)
class SpecialFunctionCallSettings:
    """Special settings for the function call, used for map-reduce operations."""

    is_map_splitter: bool = False
    is_reduce_splitter: bool = False
    splitter_function_name: str | None = None
    splitter_input_mode: SPLITTER_INPUT_MODE | None = None
    is_map_concat: bool = False


@dataclass(frozen=True)
class OutputEventCreateFunctionCall:
    """Create a function call on the server.

    AllocationRunner is responsible for serializing args, uploading blobs,
    building ExecutionPlanUpdates proto, and sending to server.
    """

    durable_id: str
    function_name: str
    args: list[Any | FunctionCallRef]
    kwargs: dict[str, Any | FunctionCallRef]
    is_tail_call: bool
    start_delay: float | None
    special_settings: SpecialFunctionCallSettings | None = None


@dataclass(frozen=True)
class OutputEventCreateFunctionCallWatcher:
    """Watch for a function call result.

    AllocationRunner is responsible for adding the watcher via
    AllocationState/gRPC, waiting for the function call to complete,
    deserializing the result, and delivering an InputEventFunctionCallWatcherCreated.
    """

    function_call_durable_id: str
    # Timestamp when this watcher times out, or None if it doesn't time out.
    deadline: float | None


@dataclass(frozen=True)
class OutputEventFinishAllocation:
    """Signals that user code has completed.

    This is the last command emitted by the event loop. No result
    is expected for this command.
    """

    value: Any = None
    # Not None if our code raised an exception (aka internal error).
    internal_exception: BaseException | None = None
    # Not None if user code raised an exception.
    user_exception: BaseException | None = None
    # Not None if tail call.
    tail_call: FunctionCallRef | None = None


OutputEventType = (
    OutputEventCreateFunctionCall
    | OutputEventCreateFunctionCallWatcher
    | OutputEventFinishAllocation
)


@dataclass(frozen=True)
class OutputEventBatch:
    """A batch of output events from a single user code operation.

    Events are ordered deterministically.
    A single event is a batch of size 1.
    """

    events: list[OutputEventType]
