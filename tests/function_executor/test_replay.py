import os
import unittest

from replay_testing import make_pickle_blob, make_watcher_result_response, ok_status
from testing import (
    AllocationTestDriver,
    FunctionExecutorProcessContextManager,
    application_function_inputs,
    download_and_deserialize_so,
    initialize,
    rpc_channel,
)

from tensorlake.applications import Future, application, function
from tensorlake.function_executor.proto.function_executor_pb2 import (
    ALLOCATION_FAILURE_REASON_REPLAY_EVENT_HISTORY_MISMATCH,
    BLOB,
    REPLAY_MODE_STRICT,
    Allocation,
    AllocationEvent,
    AllocationEventFunctionCallCreated,
    AllocationEventFunctionCallWatcherCreated,
    AllocationEventFunctionCallWatcherResult,
    AllocationExecutionEventFinishAllocation,
    AllocationOutcomeCode,
    CreateAllocationRequest,
    DeleteAllocationRequest,
    ReadAllocationEventLogResponse,
    SerializedObjectInsideBLOB,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)

APPLICATION_CODE_DIR_PATH = os.path.dirname(os.path.abspath(__file__))


# ---------------------------------------------------------------------------
# Functions used in replay tests.
# ---------------------------------------------------------------------------


@function()
def replay_child(x: int) -> int:
    # Never actually runs in replay tests — server provides cached result.
    return x * 2


@application()
@function()
def replay_parent(x: int) -> int:
    child_future = replay_child.future(x)
    done, _ = Future.wait([child_future])
    return done[0].result()


@function()
def replay_step(x: int) -> int:
    # Never actually runs in replay tests — server provides cached result.
    return x * 2


@application()
@function()
def replay_parent_two_calls(x: int) -> int:
    # Creates two futures in a single 2-FC batch:
    #   f1 = replay_step(x)
    #   f2 = replay_step(f1)   ← f2 depends on f1's output
    # Future.wait([f2]) starts both f1 and f2 via run_future_runtime_hook(f2),
    # which DFS-walks the tree and registers both unregistered futures at once.
    f1 = replay_step.future(x)
    f2 = replay_step.future(f1)
    done, _ = Future.wait([f2])
    return done[0].result()


@application()
@function()
def replay_parent_two_independent(x: int) -> int:
    """Creates two independent child calls, waits for both."""
    a = replay_child.future(x)
    b = replay_child.future(x + 1)
    done, _ = Future.wait([a, b])
    return sum(d.result() for d in done)


@application()
@function()
def simple_return(x: int) -> int:
    return x


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestStrictReplayMode(unittest.TestCase):
    def test_happy_path(self):
        """STRICT replay with a single child call replays correctly and returns the cached result."""
        with FunctionExecutorProcessContextManager() as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize(
                    stub,
                    app_name="replay_parent",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="replay_parent",
                )

                function_call_id: str = "test-fc-replay-happy"

                # ------------------------------------------------------------------
                # First allocation: normal run to capture the durable_id.
                # ------------------------------------------------------------------
                alloc_id_1: str = "alloc-replay-happy-1"
                stub.create_allocation(
                    CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="req-1",
                            function_call_id=function_call_id,
                            allocation_id=alloc_id_1,
                            inputs=application_function_inputs(2, int),
                        )
                    )
                )

                durable_id: str | None = None
                event_clock: int = 0

                def on_batch_1(events, driver):
                    nonlocal durable_id, event_clock
                    for event in events:
                        if event.HasField("create_function_call"):
                            durable_id = (
                                event.create_function_call.updates.root_function_call_id
                            )
                            event_clock += 1
                            driver.enqueue_event_log_response(
                                ReadAllocationEventLogResponse(
                                    allocation_id=alloc_id_1,
                                    entries=[
                                        AllocationEvent(
                                            clock=event_clock,
                                            function_call_created=AllocationEventFunctionCallCreated(
                                                function_call_id=durable_id,
                                                status=ok_status(),
                                            ),
                                        )
                                    ],
                                    last_clock=event_clock,
                                    has_more=False,
                                )
                            )
                        elif event.HasField("create_function_call_watcher"):
                            watcher_id: str = (
                                event.create_function_call_watcher.function_call_id
                            )
                            event_clock += 1
                            driver.enqueue_event_log_response(
                                ReadAllocationEventLogResponse(
                                    allocation_id=alloc_id_1,
                                    entries=[
                                        AllocationEvent(
                                            clock=event_clock,
                                            function_call_watcher_created=AllocationEventFunctionCallWatcherCreated(
                                                function_call_id=watcher_id,
                                                status=ok_status(),
                                            ),
                                        )
                                    ],
                                    last_clock=event_clock,
                                    has_more=False,
                                )
                            )
                            event_clock += 1
                            driver.enqueue_event_log_response(
                                make_watcher_result_response(
                                    allocation_id=alloc_id_1,
                                    function_call_id=watcher_id,
                                    clock=event_clock,
                                    value=4,
                                    type_hint=int,
                                    blob_id="watcher-blob-1",
                                )
                            )

                driver_1: AllocationTestDriver = AllocationTestDriver(stub, alloc_id_1)
                finish_1: AllocationExecutionEventFinishAllocation = driver_1.run(
                    on_execution_event_batch=on_batch_1
                )
                self.assertEqual(
                    finish_1.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                )
                self.assertIsNotNone(durable_id)
                stub.delete_allocation(
                    DeleteAllocationRequest(allocation_id=alloc_id_1)
                )

                # ------------------------------------------------------------------
                # Second allocation: STRICT replay using the captured durable_id.
                # Both replay events are bundled in a single pre-enqueued response.
                # ------------------------------------------------------------------
                alloc_id_2: str = "alloc-replay-happy-2"
                watcher_so: SerializedObjectInsideBLOB
                watcher_blob: BLOB
                watcher_so, watcher_blob = make_pickle_blob(
                    4, int, "watcher-blob-replay"
                )
                replay_response: ReadAllocationEventLogResponse = (
                    ReadAllocationEventLogResponse(
                        allocation_id=alloc_id_2,
                        entries=[
                            AllocationEvent(
                                clock=1,
                                function_call_created=AllocationEventFunctionCallCreated(
                                    function_call_id=durable_id,
                                    status=ok_status(),
                                ),
                            ),
                            AllocationEvent(
                                clock=2,
                                function_call_watcher_created=AllocationEventFunctionCallWatcherCreated(
                                    function_call_id=durable_id,
                                    status=ok_status(),
                                ),
                            ),
                            AllocationEvent(
                                clock=3,
                                function_call_watcher_result=AllocationEventFunctionCallWatcherResult(
                                    function_call_id=durable_id,
                                    outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                                    watcher_status=AllocationEventFunctionCallWatcherResult.DESCRIPTOR.fields_by_name[
                                        "watcher_status"
                                    ].default_value,
                                    value_output=watcher_so,
                                    value_blob=watcher_blob,
                                ),
                            ),
                        ],
                        last_clock=3,
                        has_more=False,
                    )
                )

                stub.create_allocation(
                    CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="req-2",
                            function_call_id=function_call_id,
                            allocation_id=alloc_id_2,
                            inputs=application_function_inputs(2, int),
                            replay_mode=REPLAY_MODE_STRICT,
                        )
                    )
                )

                driver_2: AllocationTestDriver = AllocationTestDriver(stub, alloc_id_2)
                driver_2.enqueue_event_log_response(replay_response)
                finish_2: AllocationExecutionEventFinishAllocation = driver_2.run()

                self.assertEqual(
                    finish_2.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                )
                self.assertTrue(finish_2.HasField("value"))
                output: int = download_and_deserialize_so(
                    self, finish_2.value, finish_2.uploaded_function_outputs_blob
                )
                self.assertEqual(output, 4)

                stub.delete_allocation(
                    DeleteAllocationRequest(allocation_id=alloc_id_2)
                )

    def test_server_reordering(self):
        """STRICT replay with 2 FCs in one batch matches FCCs positionally."""
        with FunctionExecutorProcessContextManager() as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize(
                    stub,
                    app_name="replay_parent_two_calls",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="replay_parent_two_calls",
                )

                function_call_id: str = "test-fc-replay-reorder"

                alloc_id_1: str = "alloc-reorder-1"
                stub.create_allocation(
                    CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="req-1",
                            function_call_id=function_call_id,
                            allocation_id=alloc_id_1,
                            inputs=application_function_inputs(2, int),
                        )
                    )
                )

                f1_durable_id: str | None = None
                f2_durable_id: str | None = None
                event_clock: int = 0

                def on_batch_1(events, driver):
                    nonlocal f1_durable_id, f2_durable_id, event_clock
                    fc_events = [
                        e for e in events if e.HasField("create_function_call")
                    ]
                    watcher_events = [
                        e for e in events if e.HasField("create_function_call_watcher")
                    ]

                    if len(fc_events) == 2:
                        f1_durable_id = fc_events[
                            0
                        ].create_function_call.updates.root_function_call_id
                        f2_durable_id = fc_events[
                            1
                        ].create_function_call.updates.root_function_call_id
                        event_clock = 2
                        driver.enqueue_event_log_response(
                            ReadAllocationEventLogResponse(
                                allocation_id=alloc_id_1,
                                entries=[
                                    AllocationEvent(
                                        clock=1,
                                        function_call_created=AllocationEventFunctionCallCreated(
                                            function_call_id=f1_durable_id,
                                            status=ok_status(),
                                        ),
                                    ),
                                    AllocationEvent(
                                        clock=2,
                                        function_call_created=AllocationEventFunctionCallCreated(
                                            function_call_id=f2_durable_id,
                                            status=ok_status(),
                                        ),
                                    ),
                                ],
                                last_clock=2,
                                has_more=False,
                            )
                        )

                    for we in watcher_events:
                        wid = we.create_function_call_watcher.function_call_id
                        event_clock += 1
                        driver.enqueue_event_log_response(
                            ReadAllocationEventLogResponse(
                                allocation_id=alloc_id_1,
                                entries=[
                                    AllocationEvent(
                                        clock=event_clock,
                                        function_call_watcher_created=AllocationEventFunctionCallWatcherCreated(
                                            function_call_id=wid,
                                            status=ok_status(),
                                        ),
                                    )
                                ],
                                last_clock=event_clock,
                                has_more=False,
                            )
                        )
                        event_clock += 1
                        driver.enqueue_event_log_response(
                            make_watcher_result_response(
                                allocation_id=alloc_id_1,
                                function_call_id=wid,
                                clock=event_clock,
                                value=8,
                                type_hint=int,
                                blob_id=f"watcher-blob-reorder-1-{event_clock}",
                            )
                        )

                driver_1: AllocationTestDriver = AllocationTestDriver(stub, alloc_id_1)
                finish_1 = driver_1.run(on_execution_event_batch=on_batch_1)
                self.assertEqual(
                    finish_1.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                )
                self.assertIsNotNone(f1_durable_id)
                self.assertIsNotNone(f2_durable_id)
                stub.delete_allocation(
                    DeleteAllocationRequest(allocation_id=alloc_id_1)
                )

                # STRICT replay with FCCs in positional order.
                alloc_id_2: str = "alloc-reorder-2"
                watcher_so, watcher_blob = make_pickle_blob(
                    8, int, "watcher-blob-reorder-2"
                )
                replay_response = ReadAllocationEventLogResponse(
                    allocation_id=alloc_id_2,
                    entries=[
                        AllocationEvent(
                            clock=1,
                            function_call_created=AllocationEventFunctionCallCreated(
                                function_call_id=f1_durable_id, status=ok_status()
                            ),
                        ),
                        AllocationEvent(
                            clock=2,
                            function_call_created=AllocationEventFunctionCallCreated(
                                function_call_id=f2_durable_id, status=ok_status()
                            ),
                        ),
                        AllocationEvent(
                            clock=3,
                            function_call_watcher_created=AllocationEventFunctionCallWatcherCreated(
                                function_call_id=f2_durable_id, status=ok_status()
                            ),
                        ),
                        AllocationEvent(
                            clock=4,
                            function_call_watcher_result=AllocationEventFunctionCallWatcherResult(
                                function_call_id=f2_durable_id,
                                outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                                value_output=watcher_so,
                                value_blob=watcher_blob,
                            ),
                        ),
                    ],
                    last_clock=4,
                    has_more=False,
                )

                stub.create_allocation(
                    CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="req-2",
                            function_call_id=function_call_id,
                            allocation_id=alloc_id_2,
                            inputs=application_function_inputs(2, int),
                            replay_mode=REPLAY_MODE_STRICT,
                        )
                    )
                )
                driver_2 = AllocationTestDriver(stub, alloc_id_2)
                driver_2.enqueue_event_log_response(replay_response)
                finish_2 = driver_2.run()
                self.assertEqual(
                    finish_2.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                )
                output = download_and_deserialize_so(
                    self, finish_2.value, finish_2.uploaded_function_outputs_blob
                )
                self.assertEqual(output, 8)
                stub.delete_allocation(
                    DeleteAllocationRequest(allocation_id=alloc_id_2)
                )

    def test_strict_divergence_unknown_function_call(self):
        """STRICT replay fails when FunctionCallCreated has a wrong function_call_id."""
        with FunctionExecutorProcessContextManager() as process:
            with rpc_channel(process) as channel:
                stub = FunctionExecutorStub(channel)
                initialize(
                    stub,
                    app_name="replay_parent",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="replay_parent",
                )

                alloc_id: str = "alloc-diverge-unknown"
                stub.create_allocation(
                    CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="req-1",
                            function_call_id="test-fc-diverge-unknown",
                            allocation_id=alloc_id,
                            inputs=application_function_inputs(2, int),
                            replay_mode=REPLAY_MODE_STRICT,
                        )
                    )
                )

                driver = AllocationTestDriver(stub, alloc_id)
                driver.enqueue_event_log_response(
                    ReadAllocationEventLogResponse(
                        allocation_id=alloc_id,
                        entries=[
                            AllocationEvent(
                                clock=1,
                                function_call_created=AllocationEventFunctionCallCreated(
                                    function_call_id="non-existent-durable-id",
                                    status=ok_status(),
                                ),
                            )
                        ],
                        last_clock=1,
                        has_more=False,
                    )
                )
                finish = driver.run()
                self.assertEqual(
                    finish.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
                )
                self.assertEqual(
                    finish.failure_reason,
                    ALLOCATION_FAILURE_REASON_REPLAY_EVENT_HISTORY_MISMATCH,
                )
                stub.delete_allocation(DeleteAllocationRequest(allocation_id=alloc_id))

    def test_strict_divergence_early_finish(self):
        """STRICT replay fails when user function finishes before all replay events are consumed."""
        with FunctionExecutorProcessContextManager() as process:
            with rpc_channel(process) as channel:
                stub = FunctionExecutorStub(channel)
                initialize(
                    stub,
                    app_name="simple_return",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="simple_return",
                )

                alloc_id: str = "alloc-diverge-early"
                stub.create_allocation(
                    CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="req-1",
                            function_call_id="test-fc-diverge-early",
                            allocation_id=alloc_id,
                            inputs=application_function_inputs(5, int),
                            replay_mode=REPLAY_MODE_STRICT,
                        )
                    )
                )

                driver = AllocationTestDriver(stub, alloc_id)
                driver.enqueue_event_log_response(
                    ReadAllocationEventLogResponse(
                        allocation_id=alloc_id,
                        entries=[
                            AllocationEvent(
                                clock=1,
                                function_call_created=AllocationEventFunctionCallCreated(
                                    function_call_id="some-id",
                                    status=ok_status(),
                                ),
                            )
                        ],
                        last_clock=1,
                        has_more=False,
                    )
                )
                finish = driver.run()
                self.assertEqual(
                    finish.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
                )
                self.assertEqual(
                    finish.failure_reason,
                    ALLOCATION_FAILURE_REASON_REPLAY_EVENT_HISTORY_MISMATCH,
                )
                stub.delete_allocation(DeleteAllocationRequest(allocation_id=alloc_id))

    def test_strict_divergence_reorder_function_calls(self):
        """STRICT replay fails when alloc log FunctionCallCreated events are reordered."""
        with FunctionExecutorProcessContextManager() as process:
            with rpc_channel(process) as channel:
                stub = FunctionExecutorStub(channel)
                initialize(
                    stub,
                    app_name="replay_parent_two_calls",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="replay_parent_two_calls",
                )

                function_call_id: str = "test-fc-diverge-reorder"

                # ----------------------------------------------------------
                # First allocation: normal run to capture both durable_ids.
                # ----------------------------------------------------------
                alloc_id_1: str = "alloc-diverge-reorder-1"
                stub.create_allocation(
                    CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="req-1",
                            function_call_id=function_call_id,
                            allocation_id=alloc_id_1,
                            inputs=application_function_inputs(2, int),
                        )
                    )
                )

                f1_durable_id: str | None = None
                f2_durable_id: str | None = None
                event_clock: int = 0

                def on_batch(events, driver):
                    nonlocal f1_durable_id, f2_durable_id, event_clock
                    fc_events = [
                        e for e in events if e.HasField("create_function_call")
                    ]
                    watcher_events = [
                        e for e in events if e.HasField("create_function_call_watcher")
                    ]

                    if len(fc_events) == 2:
                        f1_durable_id = fc_events[
                            0
                        ].create_function_call.updates.root_function_call_id
                        f2_durable_id = fc_events[
                            1
                        ].create_function_call.updates.root_function_call_id
                        event_clock = 2
                        driver.enqueue_event_log_response(
                            ReadAllocationEventLogResponse(
                                allocation_id=alloc_id_1,
                                entries=[
                                    AllocationEvent(
                                        clock=1,
                                        function_call_created=AllocationEventFunctionCallCreated(
                                            function_call_id=f1_durable_id,
                                            status=ok_status(),
                                        ),
                                    ),
                                    AllocationEvent(
                                        clock=2,
                                        function_call_created=AllocationEventFunctionCallCreated(
                                            function_call_id=f2_durable_id,
                                            status=ok_status(),
                                        ),
                                    ),
                                ],
                                last_clock=2,
                                has_more=False,
                            )
                        )

                    for we in watcher_events:
                        wid = we.create_function_call_watcher.function_call_id
                        event_clock += 1
                        driver.enqueue_event_log_response(
                            ReadAllocationEventLogResponse(
                                allocation_id=alloc_id_1,
                                entries=[
                                    AllocationEvent(
                                        clock=event_clock,
                                        function_call_watcher_created=AllocationEventFunctionCallWatcherCreated(
                                            function_call_id=wid,
                                            status=ok_status(),
                                        ),
                                    )
                                ],
                                last_clock=event_clock,
                                has_more=False,
                            )
                        )
                        event_clock += 1
                        driver.enqueue_event_log_response(
                            make_watcher_result_response(
                                allocation_id=alloc_id_1,
                                function_call_id=wid,
                                clock=event_clock,
                                value=8,
                                type_hint=int,
                                blob_id=f"watcher-blob-diverge-reorder-1-{event_clock}",
                            )
                        )

                driver_1 = AllocationTestDriver(stub, alloc_id_1)
                finish_1 = driver_1.run(on_execution_event_batch=on_batch)
                self.assertEqual(
                    finish_1.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                )
                self.assertIsNotNone(f1_durable_id)
                self.assertIsNotNone(f2_durable_id)
                self.assertNotEqual(f1_durable_id, f2_durable_id)
                stub.delete_allocation(
                    DeleteAllocationRequest(allocation_id=alloc_id_1)
                )

                # ----------------------------------------------------------
                # Second allocation: STRICT replay with FCCs SWAPPED.
                # The event log has f2 first and f1 second, but the function
                # creates f1 first — this positional mismatch must fail.
                # ----------------------------------------------------------
                alloc_id_2: str = "alloc-diverge-reorder-2"
                replay_response = ReadAllocationEventLogResponse(
                    allocation_id=alloc_id_2,
                    entries=[
                        AllocationEvent(
                            clock=1,
                            function_call_created=AllocationEventFunctionCallCreated(
                                function_call_id=f2_durable_id,
                                status=ok_status(),
                            ),
                        ),
                        AllocationEvent(
                            clock=2,
                            function_call_created=AllocationEventFunctionCallCreated(
                                function_call_id=f1_durable_id,
                                status=ok_status(),
                            ),
                        ),
                    ],
                    last_clock=2,
                    has_more=False,
                )

                stub.create_allocation(
                    CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="req-2",
                            function_call_id=function_call_id,
                            allocation_id=alloc_id_2,
                            inputs=application_function_inputs(2, int),
                            replay_mode=REPLAY_MODE_STRICT,
                        )
                    )
                )
                driver_2 = AllocationTestDriver(stub, alloc_id_2)
                driver_2.enqueue_event_log_response(replay_response)
                finish_2 = driver_2.run()
                self.assertEqual(
                    finish_2.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
                )
                self.assertEqual(
                    finish_2.failure_reason,
                    ALLOCATION_FAILURE_REASON_REPLAY_EVENT_HISTORY_MISMATCH,
                )
                stub.delete_allocation(
                    DeleteAllocationRequest(allocation_id=alloc_id_2)
                )

    def test_no_replay_without_strict_mode(self):
        """Without REPLAY_MODE_STRICT, replay phase is skipped and execution proceeds normally."""
        with FunctionExecutorProcessContextManager() as process:
            with rpc_channel(process) as channel:
                stub = FunctionExecutorStub(channel)
                initialize(
                    stub,
                    app_name="replay_parent",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="replay_parent",
                )

                alloc_id: str = "alloc-no-replay"
                stub.create_allocation(
                    CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="req-1",
                            function_call_id="test-fc-no-replay",
                            allocation_id=alloc_id,
                            inputs=application_function_inputs(2, int),
                        )
                    )
                )

                event_clock: int = 0

                def on_batch(events, driver):
                    nonlocal event_clock
                    for event in events:
                        if event.HasField("create_function_call"):
                            did = (
                                event.create_function_call.updates.root_function_call_id
                            )
                            event_clock += 1
                            driver.enqueue_event_log_response(
                                ReadAllocationEventLogResponse(
                                    allocation_id=alloc_id,
                                    entries=[
                                        AllocationEvent(
                                            clock=event_clock,
                                            function_call_created=AllocationEventFunctionCallCreated(
                                                function_call_id=did,
                                                status=ok_status(),
                                            ),
                                        )
                                    ],
                                    last_clock=event_clock,
                                    has_more=False,
                                )
                            )
                        elif event.HasField("create_function_call_watcher"):
                            wid = event.create_function_call_watcher.function_call_id
                            event_clock += 1
                            driver.enqueue_event_log_response(
                                ReadAllocationEventLogResponse(
                                    allocation_id=alloc_id,
                                    entries=[
                                        AllocationEvent(
                                            clock=event_clock,
                                            function_call_watcher_created=AllocationEventFunctionCallWatcherCreated(
                                                function_call_id=wid,
                                                status=ok_status(),
                                            ),
                                        )
                                    ],
                                    last_clock=event_clock,
                                    has_more=False,
                                )
                            )
                            event_clock += 1
                            driver.enqueue_event_log_response(
                                make_watcher_result_response(
                                    allocation_id=alloc_id,
                                    function_call_id=wid,
                                    clock=event_clock,
                                    value=4,
                                    type_hint=int,
                                    blob_id="watcher-blob-no-replay",
                                )
                            )

                driver = AllocationTestDriver(stub, alloc_id)
                finish = driver.run(on_execution_event_batch=on_batch)
                self.assertEqual(
                    finish.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                )
                output = download_and_deserialize_so(
                    self, finish.value, finish.uploaded_function_outputs_blob
                )
                self.assertEqual(output, 4)
                stub.delete_allocation(DeleteAllocationRequest(allocation_id=alloc_id))

    def test_watcher_result_cross_batch_reordering(self):
        """STRICT replay succeeds when WatcherResult events arrive in different order than output batches."""
        with FunctionExecutorProcessContextManager() as process:
            with rpc_channel(process) as channel:
                stub = FunctionExecutorStub(channel)
                initialize(
                    stub,
                    app_name="replay_parent_two_independent",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="replay_parent_two_independent",
                )

                function_call_id: str = "test-fc-replay-cross-batch"

                alloc_id_1: str = "alloc-cross-batch-1"
                stub.create_allocation(
                    CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="req-1",
                            function_call_id=function_call_id,
                            allocation_id=alloc_id_1,
                            inputs=application_function_inputs(2, int),
                        )
                    )
                )

                durable_ids: list[str] = []
                event_clock: int = 0

                def on_batch_cross(events, driver):
                    nonlocal event_clock
                    fc_events = [
                        e for e in events if e.HasField("create_function_call")
                    ]
                    watcher_events = [
                        e for e in events if e.HasField("create_function_call_watcher")
                    ]
                    for fc in fc_events:
                        did = fc.create_function_call.updates.root_function_call_id
                        durable_ids.append(did)
                        event_clock += 1
                        driver.enqueue_event_log_response(
                            ReadAllocationEventLogResponse(
                                allocation_id=alloc_id_1,
                                entries=[
                                    AllocationEvent(
                                        clock=event_clock,
                                        function_call_created=AllocationEventFunctionCallCreated(
                                            function_call_id=did,
                                            status=ok_status(),
                                        ),
                                    )
                                ],
                                last_clock=event_clock,
                                has_more=False,
                            )
                        )
                    for we in watcher_events:
                        wid = we.create_function_call_watcher.function_call_id
                        event_clock += 1
                        driver.enqueue_event_log_response(
                            ReadAllocationEventLogResponse(
                                allocation_id=alloc_id_1,
                                entries=[
                                    AllocationEvent(
                                        clock=event_clock,
                                        function_call_watcher_created=AllocationEventFunctionCallWatcherCreated(
                                            function_call_id=wid,
                                            status=ok_status(),
                                        ),
                                    )
                                ],
                                last_clock=event_clock,
                                has_more=False,
                            )
                        )
                        event_clock += 1
                        driver.enqueue_event_log_response(
                            make_watcher_result_response(
                                allocation_id=alloc_id_1,
                                function_call_id=wid,
                                clock=event_clock,
                                value=10,
                                type_hint=int,
                                blob_id=f"watcher-blob-cross-{event_clock}",
                            )
                        )

                driver_1 = AllocationTestDriver(stub, alloc_id_1)
                finish_1 = driver_1.run(on_execution_event_batch=on_batch_cross)
                self.assertEqual(
                    finish_1.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                )
                self.assertEqual(len(durable_ids), 2)
                stub.delete_allocation(
                    DeleteAllocationRequest(allocation_id=alloc_id_1)
                )

                # STRICT replay with WRs in REVERSED order.
                a_did, b_did = durable_ids[0], durable_ids[1]
                alloc_id_2: str = "alloc-cross-batch-2"
                a_so, a_blob = make_pickle_blob(10, int, "watcher-blob-cross-replay-a")
                b_so, b_blob = make_pickle_blob(10, int, "watcher-blob-cross-replay-b")

                replay_response = ReadAllocationEventLogResponse(
                    allocation_id=alloc_id_2,
                    entries=[
                        AllocationEvent(
                            clock=1,
                            function_call_created=AllocationEventFunctionCallCreated(
                                function_call_id=a_did, status=ok_status()
                            ),
                        ),
                        AllocationEvent(
                            clock=2,
                            function_call_created=AllocationEventFunctionCallCreated(
                                function_call_id=b_did, status=ok_status()
                            ),
                        ),
                        AllocationEvent(
                            clock=3,
                            function_call_watcher_created=AllocationEventFunctionCallWatcherCreated(
                                function_call_id=a_did, status=ok_status()
                            ),
                        ),
                        AllocationEvent(
                            clock=4,
                            function_call_watcher_created=AllocationEventFunctionCallWatcherCreated(
                                function_call_id=b_did, status=ok_status()
                            ),
                        ),
                        # WR in REVERSED order: b first, then a.
                        AllocationEvent(
                            clock=5,
                            function_call_watcher_result=AllocationEventFunctionCallWatcherResult(
                                function_call_id=b_did,
                                outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                                value_output=b_so,
                                value_blob=b_blob,
                            ),
                        ),
                        AllocationEvent(
                            clock=6,
                            function_call_watcher_result=AllocationEventFunctionCallWatcherResult(
                                function_call_id=a_did,
                                outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                                value_output=a_so,
                                value_blob=a_blob,
                            ),
                        ),
                    ],
                    last_clock=6,
                    has_more=False,
                )

                stub.create_allocation(
                    CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="req-2",
                            function_call_id=function_call_id,
                            allocation_id=alloc_id_2,
                            inputs=application_function_inputs(2, int),
                            replay_mode=REPLAY_MODE_STRICT,
                        )
                    )
                )
                driver_2 = AllocationTestDriver(stub, alloc_id_2)
                driver_2.enqueue_event_log_response(replay_response)
                finish_2 = driver_2.run()
                self.assertEqual(
                    finish_2.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                )
                output = download_and_deserialize_so(
                    self, finish_2.value, finish_2.uploaded_function_outputs_blob
                )
                self.assertEqual(output, 20)
                stub.delete_allocation(
                    DeleteAllocationRequest(allocation_id=alloc_id_2)
                )

    def test_function_call_created_interleaved_with_watcher_result(self):
        """STRICT replay succeeds when WR appears after both positionally-ordered FCCs in the log."""
        with FunctionExecutorProcessContextManager() as process:
            with rpc_channel(process) as channel:
                stub = FunctionExecutorStub(channel)
                initialize(
                    stub,
                    app_name="replay_parent_two_calls",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="replay_parent_two_calls",
                )

                function_call_id: str = "test-fc-replay-interleaved"

                alloc_id_1: str = "alloc-interleaved-1"
                stub.create_allocation(
                    CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="req-1",
                            function_call_id=function_call_id,
                            allocation_id=alloc_id_1,
                            inputs=application_function_inputs(2, int),
                        )
                    )
                )

                f1_durable_id: str | None = None
                f2_durable_id: str | None = None
                event_clock: int = 0

                def on_batch_interleaved(events, driver):
                    nonlocal f1_durable_id, f2_durable_id, event_clock
                    fc_events = [
                        e for e in events if e.HasField("create_function_call")
                    ]
                    watcher_events = [
                        e for e in events if e.HasField("create_function_call_watcher")
                    ]
                    if len(fc_events) == 2:
                        f1_durable_id = fc_events[
                            0
                        ].create_function_call.updates.root_function_call_id
                        f2_durable_id = fc_events[
                            1
                        ].create_function_call.updates.root_function_call_id
                        driver.enqueue_event_log_response(
                            ReadAllocationEventLogResponse(
                                allocation_id=alloc_id_1,
                                entries=[
                                    AllocationEvent(
                                        clock=1,
                                        function_call_created=AllocationEventFunctionCallCreated(
                                            function_call_id=f1_durable_id,
                                            status=ok_status(),
                                        ),
                                    ),
                                    AllocationEvent(
                                        clock=2,
                                        function_call_created=AllocationEventFunctionCallCreated(
                                            function_call_id=f2_durable_id,
                                            status=ok_status(),
                                        ),
                                    ),
                                ],
                                last_clock=2,
                                has_more=False,
                            )
                        )
                        event_clock = 2
                    for we in watcher_events:
                        wid = we.create_function_call_watcher.function_call_id
                        event_clock += 1
                        driver.enqueue_event_log_response(
                            ReadAllocationEventLogResponse(
                                allocation_id=alloc_id_1,
                                entries=[
                                    AllocationEvent(
                                        clock=event_clock,
                                        function_call_watcher_created=AllocationEventFunctionCallWatcherCreated(
                                            function_call_id=wid,
                                            status=ok_status(),
                                        ),
                                    )
                                ],
                                last_clock=event_clock,
                                has_more=False,
                            )
                        )
                        event_clock += 1
                        driver.enqueue_event_log_response(
                            make_watcher_result_response(
                                allocation_id=alloc_id_1,
                                function_call_id=wid,
                                clock=event_clock,
                                value=8,
                                type_hint=int,
                                blob_id=f"watcher-blob-interleaved-1-{event_clock}",
                            )
                        )

                driver_1 = AllocationTestDriver(stub, alloc_id_1)
                finish_1 = driver_1.run(on_execution_event_batch=on_batch_interleaved)
                self.assertEqual(
                    finish_1.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                )
                stub.delete_allocation(
                    DeleteAllocationRequest(allocation_id=alloc_id_1)
                )

                # STRICT replay: FCC(f1)@1, FCC(f2)@2, FWCC(f2)@3, WR(f2)@4.
                alloc_id_2: str = "alloc-interleaved-2"
                watcher_so, watcher_blob = make_pickle_blob(
                    8, int, "watcher-blob-interleaved-2"
                )
                replay_response = ReadAllocationEventLogResponse(
                    allocation_id=alloc_id_2,
                    entries=[
                        AllocationEvent(
                            clock=1,
                            function_call_created=AllocationEventFunctionCallCreated(
                                function_call_id=f1_durable_id, status=ok_status()
                            ),
                        ),
                        AllocationEvent(
                            clock=2,
                            function_call_created=AllocationEventFunctionCallCreated(
                                function_call_id=f2_durable_id, status=ok_status()
                            ),
                        ),
                        AllocationEvent(
                            clock=3,
                            function_call_watcher_created=AllocationEventFunctionCallWatcherCreated(
                                function_call_id=f2_durable_id, status=ok_status()
                            ),
                        ),
                        AllocationEvent(
                            clock=4,
                            function_call_watcher_result=AllocationEventFunctionCallWatcherResult(
                                function_call_id=f2_durable_id,
                                outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                                value_output=watcher_so,
                                value_blob=watcher_blob,
                            ),
                        ),
                    ],
                    last_clock=4,
                    has_more=False,
                )

                stub.create_allocation(
                    CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="req-2",
                            function_call_id=function_call_id,
                            allocation_id=alloc_id_2,
                            inputs=application_function_inputs(2, int),
                            replay_mode=REPLAY_MODE_STRICT,
                        )
                    )
                )
                driver_2 = AllocationTestDriver(stub, alloc_id_2)
                driver_2.enqueue_event_log_response(replay_response)
                finish_2 = driver_2.run()
                self.assertEqual(
                    finish_2.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                )
                output = download_and_deserialize_so(
                    self, finish_2.value, finish_2.uploaded_function_outputs_blob
                )
                self.assertEqual(output, 8)
                stub.delete_allocation(
                    DeleteAllocationRequest(allocation_id=alloc_id_2)
                )

    def test_strict_divergence_watcher_created_id_mismatch(self):
        """STRICT replay fails when FWCC has wrong function_call_id."""
        with FunctionExecutorProcessContextManager() as process:
            with rpc_channel(process) as channel:
                stub = FunctionExecutorStub(channel)
                initialize(
                    stub,
                    app_name="replay_parent",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="replay_parent",
                )

                function_call_id: str = "test-fc-diverge-fwcc-id"

                # ----------------------------------------------------------
                # First allocation: normal run to capture the durable_id.
                # ----------------------------------------------------------
                alloc_id_1: str = "alloc-diverge-fwcc-id-1"
                stub.create_allocation(
                    CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="req-1",
                            function_call_id=function_call_id,
                            allocation_id=alloc_id_1,
                            inputs=application_function_inputs(2, int),
                        )
                    )
                )

                durable_id: str | None = None
                event_clock: int = 0

                def on_batch_fwcc_id(events, driver):
                    nonlocal durable_id, event_clock
                    for event in events:
                        if event.HasField("create_function_call"):
                            durable_id = (
                                event.create_function_call.updates.root_function_call_id
                            )
                            event_clock += 1
                            driver.enqueue_event_log_response(
                                ReadAllocationEventLogResponse(
                                    allocation_id=alloc_id_1,
                                    entries=[
                                        AllocationEvent(
                                            clock=event_clock,
                                            function_call_created=AllocationEventFunctionCallCreated(
                                                function_call_id=durable_id,
                                                status=ok_status(),
                                            ),
                                        )
                                    ],
                                    last_clock=event_clock,
                                    has_more=False,
                                )
                            )
                        elif event.HasField("create_function_call_watcher"):
                            watcher_id = (
                                event.create_function_call_watcher.function_call_id
                            )
                            event_clock += 1
                            driver.enqueue_event_log_response(
                                ReadAllocationEventLogResponse(
                                    allocation_id=alloc_id_1,
                                    entries=[
                                        AllocationEvent(
                                            clock=event_clock,
                                            function_call_watcher_created=AllocationEventFunctionCallWatcherCreated(
                                                function_call_id=watcher_id,
                                                status=ok_status(),
                                            ),
                                        )
                                    ],
                                    last_clock=event_clock,
                                    has_more=False,
                                )
                            )
                            event_clock += 1
                            driver.enqueue_event_log_response(
                                make_watcher_result_response(
                                    allocation_id=alloc_id_1,
                                    function_call_id=watcher_id,
                                    clock=event_clock,
                                    value=4,
                                    type_hint=int,
                                    blob_id="watcher-blob-fwcc-id-1",
                                )
                            )

                driver_1 = AllocationTestDriver(stub, alloc_id_1)
                finish_1 = driver_1.run(on_execution_event_batch=on_batch_fwcc_id)
                self.assertEqual(
                    finish_1.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                )
                self.assertIsNotNone(durable_id)
                stub.delete_allocation(
                    DeleteAllocationRequest(allocation_id=alloc_id_1)
                )

                # ----------------------------------------------------------
                # Second allocation: STRICT replay with FWCC having WRONG id.
                # ----------------------------------------------------------
                alloc_id_2: str = "alloc-diverge-fwcc-id-2"
                replay_response = ReadAllocationEventLogResponse(
                    allocation_id=alloc_id_2,
                    entries=[
                        AllocationEvent(
                            clock=1,
                            function_call_created=AllocationEventFunctionCallCreated(
                                function_call_id=durable_id,
                                status=ok_status(),
                            ),
                        ),
                        AllocationEvent(
                            clock=2,
                            function_call_watcher_created=AllocationEventFunctionCallWatcherCreated(
                                function_call_id="wrong-watcher-id",
                                status=ok_status(),
                            ),
                        ),
                    ],
                    last_clock=2,
                    has_more=False,
                )

                stub.create_allocation(
                    CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="req-2",
                            function_call_id=function_call_id,
                            allocation_id=alloc_id_2,
                            inputs=application_function_inputs(2, int),
                            replay_mode=REPLAY_MODE_STRICT,
                        )
                    )
                )
                driver_2 = AllocationTestDriver(stub, alloc_id_2)
                driver_2.enqueue_event_log_response(replay_response)
                finish_2 = driver_2.run()
                self.assertEqual(
                    finish_2.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
                )
                self.assertEqual(
                    finish_2.failure_reason,
                    ALLOCATION_FAILURE_REASON_REPLAY_EVENT_HISTORY_MISMATCH,
                )
                stub.delete_allocation(
                    DeleteAllocationRequest(allocation_id=alloc_id_2)
                )

    def test_strict_divergence_watcher_created_wrong_event_type(self):
        """STRICT replay fails when FCC appears where FWCC was expected."""
        with FunctionExecutorProcessContextManager() as process:
            with rpc_channel(process) as channel:
                stub = FunctionExecutorStub(channel)
                initialize(
                    stub,
                    app_name="replay_parent",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="replay_parent",
                )

                function_call_id: str = "test-fc-diverge-fwcc-type"

                # ----------------------------------------------------------
                # First allocation: normal run to capture the durable_id.
                # ----------------------------------------------------------
                alloc_id_1: str = "alloc-diverge-fwcc-type-1"
                stub.create_allocation(
                    CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="req-1",
                            function_call_id=function_call_id,
                            allocation_id=alloc_id_1,
                            inputs=application_function_inputs(2, int),
                        )
                    )
                )

                durable_id: str | None = None
                event_clock: int = 0

                def on_batch_fwcc_type(events, driver):
                    nonlocal durable_id, event_clock
                    for event in events:
                        if event.HasField("create_function_call"):
                            durable_id = (
                                event.create_function_call.updates.root_function_call_id
                            )
                            event_clock += 1
                            driver.enqueue_event_log_response(
                                ReadAllocationEventLogResponse(
                                    allocation_id=alloc_id_1,
                                    entries=[
                                        AllocationEvent(
                                            clock=event_clock,
                                            function_call_created=AllocationEventFunctionCallCreated(
                                                function_call_id=durable_id,
                                                status=ok_status(),
                                            ),
                                        )
                                    ],
                                    last_clock=event_clock,
                                    has_more=False,
                                )
                            )
                        elif event.HasField("create_function_call_watcher"):
                            watcher_id = (
                                event.create_function_call_watcher.function_call_id
                            )
                            event_clock += 1
                            driver.enqueue_event_log_response(
                                ReadAllocationEventLogResponse(
                                    allocation_id=alloc_id_1,
                                    entries=[
                                        AllocationEvent(
                                            clock=event_clock,
                                            function_call_watcher_created=AllocationEventFunctionCallWatcherCreated(
                                                function_call_id=watcher_id,
                                                status=ok_status(),
                                            ),
                                        )
                                    ],
                                    last_clock=event_clock,
                                    has_more=False,
                                )
                            )
                            event_clock += 1
                            driver.enqueue_event_log_response(
                                make_watcher_result_response(
                                    allocation_id=alloc_id_1,
                                    function_call_id=watcher_id,
                                    clock=event_clock,
                                    value=4,
                                    type_hint=int,
                                    blob_id="watcher-blob-fwcc-type-1",
                                )
                            )

                driver_1 = AllocationTestDriver(stub, alloc_id_1)
                finish_1 = driver_1.run(on_execution_event_batch=on_batch_fwcc_type)
                self.assertEqual(
                    finish_1.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                )
                self.assertIsNotNone(durable_id)
                stub.delete_allocation(
                    DeleteAllocationRequest(allocation_id=alloc_id_1)
                )

                # ----------------------------------------------------------
                # Second allocation: STRICT replay with another FCC where
                # FWCC was expected.
                # ----------------------------------------------------------
                alloc_id_2: str = "alloc-diverge-fwcc-type-2"
                replay_response = ReadAllocationEventLogResponse(
                    allocation_id=alloc_id_2,
                    entries=[
                        AllocationEvent(
                            clock=1,
                            function_call_created=AllocationEventFunctionCallCreated(
                                function_call_id=durable_id,
                                status=ok_status(),
                            ),
                        ),
                        # Wrong event type: FCC instead of FWCC.
                        AllocationEvent(
                            clock=2,
                            function_call_created=AllocationEventFunctionCallCreated(
                                function_call_id="some-other-id",
                                status=ok_status(),
                            ),
                        ),
                    ],
                    last_clock=2,
                    has_more=False,
                )

                stub.create_allocation(
                    CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="req-2",
                            function_call_id=function_call_id,
                            allocation_id=alloc_id_2,
                            inputs=application_function_inputs(2, int),
                            replay_mode=REPLAY_MODE_STRICT,
                        )
                    )
                )
                driver_2 = AllocationTestDriver(stub, alloc_id_2)
                driver_2.enqueue_event_log_response(replay_response)
                finish_2 = driver_2.run()
                self.assertEqual(
                    finish_2.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
                )
                self.assertEqual(
                    finish_2.failure_reason,
                    ALLOCATION_FAILURE_REASON_REPLAY_EVENT_HISTORY_MISMATCH,
                )
                stub.delete_allocation(
                    DeleteAllocationRequest(allocation_id=alloc_id_2)
                )


if __name__ == "__main__":
    unittest.main()
