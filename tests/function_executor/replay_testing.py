"""Shared helpers for replay tests.

Provides blob construction, watcher result helpers, and generic callbacks
for driving allocations through the test driver during replay tests.
"""

import hashlib
from typing import Any

import grpc
from testing import (
    AllocationTestDriver,
    create_tmp_blob,
    write_tmp_blob_bytes,
)

from tensorlake.applications.metadata import ValueMetadata, serialize_metadata
from tensorlake.applications.user_data_serializer import PickleUserDataSerializer
from tensorlake.function_executor.proto.function_executor_pb2 import (
    BLOB,
    AllocationEvent,
    AllocationEventFunctionCallCreated,
    AllocationEventFunctionCallWatcherCreated,
    AllocationEventFunctionCallWatcherResult,
    AllocationOutcomeCode,
    FunctionCallWatcherStatus,
    ReadAllocationEventLogResponse,
    SerializedObjectEncoding,
    SerializedObjectInsideBLOB,
    SerializedObjectManifest,
)
from tensorlake.function_executor.proto.status_pb2 import Status


def ok_status() -> Status:
    return Status(code=grpc.StatusCode.OK.value[0])


def make_pickle_blob(
    value: Any, type_hint: type, blob_id: str
) -> tuple[SerializedObjectInsideBLOB, BLOB]:
    """Serializes `value` as a pickle-encoded blob and writes it to a local temp file."""
    serializer: PickleUserDataSerializer = PickleUserDataSerializer()
    metadata_bytes: bytes = serialize_metadata(
        ValueMetadata(
            id="output",
            type_hint=type_hint,
            serializer_name=serializer.name,
            content_type=serializer.content_type,
        )
    )
    data_bytes: bytes = serializer.serialize(value, type_hint)
    blob_data: bytes = metadata_bytes + data_bytes

    blob: BLOB = create_tmp_blob(id=blob_id)
    write_tmp_blob_bytes(blob, blob_data)

    so: SerializedObjectInsideBLOB = SerializedObjectInsideBLOB(
        manifest=SerializedObjectManifest(
            encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE,
            encoding_version=0,
            size=len(blob_data),
            metadata_size=len(metadata_bytes),
            sha256_hash=hashlib.sha256(blob_data).hexdigest(),
        ),
        offset=0,
    )
    return so, blob


def make_watcher_result_response(
    allocation_id: str,
    function_call_id: str,
    clock: int,
    value: Any,
    type_hint: type,
    blob_id: str,
) -> ReadAllocationEventLogResponse:
    """Builds a ReadAllocationEventLogResponse with a single success WatcherResult entry."""
    so: SerializedObjectInsideBLOB
    blob: BLOB
    so, blob = make_pickle_blob(value, type_hint, blob_id)
    return ReadAllocationEventLogResponse(
        allocation_id=allocation_id,
        entries=[
            AllocationEvent(
                clock=clock,
                function_call_watcher_result=AllocationEventFunctionCallWatcherResult(
                    function_call_id=function_call_id,
                    outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                    watcher_status=FunctionCallWatcherStatus.FUNCTION_CALL_WATCHER_STATUS_COMPLETED,
                    value_output=so,
                    value_blob=blob,
                ),
            )
        ],
        last_clock=clock,
        has_more=False,
    )


def enqueue_server_recreated_watchers(
    driver: AllocationTestDriver,
    allocation_id: str,
    truncated_entries: list,
    watcher_values: dict[str, tuple[Any, type]],
    clock_box: list[int],
) -> None:
    """Simulates Server re-creating watchers whose FWCC is in the replay log but WR is not.

    Server does this before starting replay so that the live execution path
    delivers the missing WR to the event loop after replay finishes.
    """
    fwcc_ids: set[str] = set()
    wr_ids: set[str] = set()
    for entry in truncated_entries:
        if entry.HasField("function_call_watcher_created"):
            fwcc_ids.add(entry.function_call_watcher_created.function_call_id)
        elif entry.HasField("function_call_watcher_result"):
            wr_ids.add(entry.function_call_watcher_result.function_call_id)

    missing_wr_ids: set[str] = fwcc_ids - wr_ids
    for wid in sorted(missing_wr_ids):
        value: Any
        type_hint: type
        value, type_hint = watcher_values[wid]

        # Emit FWCC.
        clock_box[0] += 1
        driver.enqueue_event_log_response(
            ReadAllocationEventLogResponse(
                allocation_id=allocation_id,
                entries=[
                    AllocationEvent(
                        clock=clock_box[0],
                        function_call_watcher_created=AllocationEventFunctionCallWatcherCreated(
                            function_call_id=wid,
                            status=ok_status(),
                        ),
                    )
                ],
                last_clock=clock_box[0],
                has_more=False,
            )
        )

        # Emit WR.
        clock_box[0] += 1
        so: SerializedObjectInsideBLOB
        blob: BLOB
        so, blob = make_pickle_blob(
            value, type_hint, f"wr-blob-recreated-{allocation_id}-{clock_box[0]}"
        )
        driver.enqueue_event_log_response(
            ReadAllocationEventLogResponse(
                allocation_id=allocation_id,
                entries=[
                    AllocationEvent(
                        clock=clock_box[0],
                        function_call_watcher_result=AllocationEventFunctionCallWatcherResult(
                            function_call_id=wid,
                            outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                            watcher_status=FunctionCallWatcherStatus.FUNCTION_CALL_WATCHER_STATUS_COMPLETED,
                            value_output=so,
                            value_blob=blob,
                        ),
                    )
                ],
                last_clock=clock_box[0],
                has_more=False,
            )
        )


def respond_to_events_initial(
    events: list,
    driver: AllocationTestDriver,
    allocation_id: str,
    alloc_events_out: list[AllocationEvent],
    clock_box: list[int],
    watcher_count_box: list[int],
    watcher_values_out: dict[str, tuple[Any, type]],
    watcher_value_fn: Any,
) -> None:
    """Callback for an initial full run that captures allocation events.

    Responds to FC with FCC and watcher with typed WR (value determined by
    watcher_value_fn(watcher_index) → (value, type_hint)).
    Records every allocation event in alloc_events_out and the durable_id → value
    mapping in watcher_values_out for reuse during replays.
    """
    for event in events:
        if event.HasField("create_function_call"):
            did: str = event.create_function_call.updates.root_function_call_id
            clock_box[0] += 1
            fcc_entry: AllocationEvent = AllocationEvent(
                clock=clock_box[0],
                function_call_created=AllocationEventFunctionCallCreated(
                    function_call_id=did,
                    status=ok_status(),
                ),
            )
            alloc_events_out.append(fcc_entry)
            driver.enqueue_event_log_response(
                ReadAllocationEventLogResponse(
                    allocation_id=allocation_id,
                    entries=[fcc_entry],
                    last_clock=clock_box[0],
                    has_more=False,
                )
            )
        elif event.HasField("create_function_call_watcher"):
            wid: str = event.create_function_call_watcher.function_call_id
            value: Any
            type_hint: type
            value, type_hint = watcher_value_fn(watcher_count_box[0])
            watcher_count_box[0] += 1
            watcher_values_out[wid] = (value, type_hint)

            # Emit FWCC first.
            clock_box[0] += 1
            fwcc_entry: AllocationEvent = AllocationEvent(
                clock=clock_box[0],
                function_call_watcher_created=AllocationEventFunctionCallWatcherCreated(
                    function_call_id=wid,
                    status=ok_status(),
                ),
            )
            alloc_events_out.append(fwcc_entry)
            driver.enqueue_event_log_response(
                ReadAllocationEventLogResponse(
                    allocation_id=allocation_id,
                    entries=[fwcc_entry],
                    last_clock=clock_box[0],
                    has_more=False,
                )
            )

            # Then emit WR.
            clock_box[0] += 1
            so: SerializedObjectInsideBLOB
            blob: BLOB
            so, blob = make_pickle_blob(
                value, type_hint, f"wr-blob-{allocation_id}-{clock_box[0]}"
            )
            wr_entry: AllocationEvent = AllocationEvent(
                clock=clock_box[0],
                function_call_watcher_result=AllocationEventFunctionCallWatcherResult(
                    function_call_id=wid,
                    outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                    watcher_status=FunctionCallWatcherStatus.FUNCTION_CALL_WATCHER_STATUS_COMPLETED,
                    value_output=so,
                    value_blob=blob,
                ),
            )
            alloc_events_out.append(wr_entry)
            driver.enqueue_event_log_response(
                ReadAllocationEventLogResponse(
                    allocation_id=allocation_id,
                    entries=[wr_entry],
                    last_clock=clock_box[0],
                    has_more=False,
                )
            )


def respond_to_events_replay(
    events: list,
    driver: AllocationTestDriver,
    allocation_id: str,
    clock_box: list[int],
    watcher_values: dict[str, tuple[Any, type]],
) -> None:
    """Callback for replay runs.

    Uses the watcher_values mapping (durable_id → (value, type_hint)) built
    during the initial full run to provide identical WR values.
    """
    for event in events:
        if event.HasField("create_function_call"):
            did: str = event.create_function_call.updates.root_function_call_id
            clock_box[0] += 1
            driver.enqueue_event_log_response(
                ReadAllocationEventLogResponse(
                    allocation_id=allocation_id,
                    entries=[
                        AllocationEvent(
                            clock=clock_box[0],
                            function_call_created=AllocationEventFunctionCallCreated(
                                function_call_id=did,
                                status=ok_status(),
                            ),
                        )
                    ],
                    last_clock=clock_box[0],
                    has_more=False,
                )
            )
        elif event.HasField("create_function_call_watcher"):
            wid: str = event.create_function_call_watcher.function_call_id
            value: Any
            type_hint: type
            value, type_hint = watcher_values[wid]

            # Emit FWCC first.
            clock_box[0] += 1
            driver.enqueue_event_log_response(
                ReadAllocationEventLogResponse(
                    allocation_id=allocation_id,
                    entries=[
                        AllocationEvent(
                            clock=clock_box[0],
                            function_call_watcher_created=AllocationEventFunctionCallWatcherCreated(
                                function_call_id=wid,
                                status=ok_status(),
                            ),
                        )
                    ],
                    last_clock=clock_box[0],
                    has_more=False,
                )
            )

            # Then emit WR.
            clock_box[0] += 1
            so: SerializedObjectInsideBLOB
            blob: BLOB
            so, blob = make_pickle_blob(
                value, type_hint, f"wr-blob-{allocation_id}-{clock_box[0]}"
            )
            driver.enqueue_event_log_response(
                ReadAllocationEventLogResponse(
                    allocation_id=allocation_id,
                    entries=[
                        AllocationEvent(
                            clock=clock_box[0],
                            function_call_watcher_result=AllocationEventFunctionCallWatcherResult(
                                function_call_id=wid,
                                outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                                watcher_status=FunctionCallWatcherStatus.FUNCTION_CALL_WATCHER_STATUS_COMPLETED,
                                value_output=so,
                                value_blob=blob,
                            ),
                        )
                    ],
                    last_clock=clock_box[0],
                    has_more=False,
                )
            )
