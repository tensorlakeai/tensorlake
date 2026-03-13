from typing import Any

import grpc

from tensorlake.applications import (
    DeserializationError,
    FunctionError,
    InternalError,
    RequestError,
    TensorlakeError,
    TimeoutError,
)
from tensorlake.applications.blob_store import BLOBStore
from tensorlake.applications.function.user_data_serializer import (
    deserialize_value_with_metadata,
)
from tensorlake.applications.internal_logger import InternalLogger

from ..proto.function_executor_pb2 import (
    AllocationEventFunctionCallCreated,
    AllocationEventFunctionCallWatcherCreated,
    AllocationEventFunctionCallWatcherResult,
    AllocationOutcomeCode,
    FunctionCallWatcherStatus,
)
from .download import download_serialized_objects
from .event_loop import (
    AllocationEventLoop,
    InputEventFunctionCallCreated,
    InputEventFunctionCallWatcherCreated,
    InputEventFunctionCallWatcherResult,
)
from .exception_helper import deserialize_user_exception
from .value import SerializedValue


def process_function_call_created(
    event: AllocationEventFunctionCallCreated,
    event_loop: AllocationEventLoop,
    logger: InternalLogger,
) -> None:
    """Processes function call created event from the event log.
    Raises Exception on internal error.
    """
    exception: TensorlakeError | None = None
    if event.HasField("status") and event.status.code != grpc.StatusCode.OK.value[0]:
        # Check if there's pickled error metadata from a creation failure.
        if event.HasField("metadata") and len(event.metadata) > 0:
            exception = deserialize_user_exception(event.metadata)
        else:
            # We don't know why exactly creation failed, something on Server side.
            exception = InternalError("Failed to start function call")
        logger.error(
            "child future function call creation failed",
            future_fn_call_id=event.function_call_id,
            status=event.status,
            exc_info=exception,
        )
    else:
        logger.info(
            "started child function call future",
            future_fn_call_id=event.function_call_id,
        )

    event_loop.add_input_event(
        InputEventFunctionCallCreated(
            durable_id=event.function_call_id,
            exception=exception,
        )
    )


def process_function_call_watcher_created(
    event: AllocationEventFunctionCallWatcherCreated,
    event_loop: AllocationEventLoop,
    logger: InternalLogger,
) -> None:
    """Processes function call watcher created event from the event log.
    Raises Exception on internal error.
    """
    exception: TensorlakeError | None = None
    if event.HasField("status") and event.status.code != grpc.StatusCode.OK.value[0]:
        exception = InternalError("Failed to create function call watcher")
        logger.error(
            "function call watcher creation failed",
            function_call_id=event.function_call_id,
            status=event.status,
            exc_info=exception,
        )
    else:
        logger.info(
            "function call watcher created",
            function_call_id=event.function_call_id,
        )

    event_loop.add_input_event(
        InputEventFunctionCallWatcherCreated(
            durable_id=event.function_call_id,
            exception=exception,
        )
    )


def process_function_call_watcher_result(
    event: AllocationEventFunctionCallWatcherResult,
    event_loop: AllocationEventLoop,
    blob_store: BLOBStore,
    logger: InternalLogger,
) -> None:
    """Processes function call watcher result event from the event log.
    Raises Exception on internal error.
    """
    output: Any = None
    exception: TensorlakeError | None = None

    if (
        event.watcher_status
        == FunctionCallWatcherStatus.FUNCTION_CALL_WATCHER_STATUS_TIMEDOUT
    ):
        exception = TimeoutError()
    elif event.outcome_code == AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS:
        serialized_output: SerializedValue = download_serialized_objects(
            serialized_objects=[event.value_output],
            serialized_object_blobs=[event.value_blob],
            blob_store=blob_store,
            logger=logger,
        )[0]
        # Even though we fully control serialization format for child function call outputs,
        # deserialization still might fail if the output contains classes not available in the caller
        # function image. So we treat this as user code error, not internal error.
        #
        # FIXME: This error is not replayable in all cases. The fact that we failed deserializing here is
        # not written to allocation event log, so if user updates the caller function image fixing the
        # deserialization problem and then replays the request then the replay history can diverge if
        # user code previously handled this deserialization error instead of letting it bubble up and fail
        # the function call.
        try:
            output = deserialize_value_with_metadata(
                serialized_output.data, serialized_output.metadata
            )
        except DeserializationError as e:
            exception = e
    elif event.outcome_code == AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE:
        if event.HasField("request_error_output"):
            serialized_request_error: SerializedValue = download_serialized_objects(
                serialized_objects=[event.request_error_output],
                serialized_object_blobs=[event.request_error_blob],
                blob_store=blob_store,
                logger=logger,
            )[0]
            # If .decode() calls fails then this is internal error because we encoded it
            # into utf-8 ourselfs.
            exception = RequestError(
                message=serialized_request_error.data.decode("utf-8")
            )
        else:
            # We don't know the error details.
            exception = FunctionError("Function call failed")
    else:
        raise InternalError(
            f"Unexpected outcome code in function call watcher result: "
            f"{event.outcome_code}"
        )

    logger.info(
        "child future completed",
        future_fn_call_id=event.function_call_id,
        success=exception is None,
    )

    event_loop.add_input_event(
        InputEventFunctionCallWatcherResult(
            function_call_durable_id=event.function_call_id,
            output=output,
            exception=exception,
        )
    )
