import os
import unittest
from typing import Any

from replay_testing import (
    enqueue_server_recreated_watchers,
    make_pickle_blob,
    make_watcher_result_response,
    ok_status,
)
from testing import (
    AllocationTestDriver,
    FunctionExecutorProcessContextManager,
    application_function_inputs,
    download_and_deserialize_so,
    initialize,
    rpc_channel,
)

from tensorlake.applications import application, function
from tensorlake.function_executor.proto.function_executor_pb2 import (
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
    FunctionCallWatcherStatus,
    ReadAllocationEventLogResponse,
    SerializedObjectInsideBLOB,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)

APPLICATION_CODE_DIR_PATH = os.path.dirname(os.path.abspath(__file__))


# ---------------------------------------------------------------------------
# 5-step sequential workflow used in replay recovery tests.
# ---------------------------------------------------------------------------


@function()
def wf_normalize(text: str) -> str:
    return text.strip().lower()


@function()
def wf_prefix(text: str) -> str:
    return "hello, " + text


@function()
def wf_exclaim(text: str) -> str:
    return text + "!"


@function()
def wf_repeat(text: str) -> str:
    return text + " " + text


@function()
def wf_wrap(text: str) -> str:
    return "[" + text + "]"


@application()
@function()
def wf_sequential(payload: str) -> str:
    normalized: str = wf_normalize(payload)
    prefixed: str = wf_prefix(normalized)
    exclaimed: str = wf_exclaim(prefixed)
    repeated: str = wf_repeat(exclaimed)
    result: str = wf_wrap(repeated)
    return result


# Intermediate values for wf_sequential("Foo") at each step.
_WF_STEP_VALUES: list[str] = [
    "foo",
    "hello, foo",
    "hello, foo!",
    "hello, foo! hello, foo!",
    "[hello, foo! hello, foo!]",
]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestStrictReplayWorkflowRecovery(unittest.TestCase):
    """Tests replay recovery for a 5-step sequential workflow.

    Each test fails a different step, erases the failure from the event log,
    then replays with STRICT mode and verifies successful completion.

    Workflow: wf_sequential("Foo") calls 5 sync steps:
      step 1 (wf_normalize) → "foo"
      step 2 (wf_prefix)    → "hello, foo"
      step 3 (wf_exclaim)   → "hello, foo!"
      step 4 (wf_repeat)    → "hello, foo! hello, foo!"
      step 5 (wf_wrap)      → "[hello, foo! hello, foo!]"
    """

    def _run_workflow_step_failure_and_replay(self, fail_at_step: int) -> None:
        """Runs the workflow with failure at step N, erases the failure, replays successfully.

        Phase 1: Normal run where step fail_at_step returns a failure WR.
                 Captures alloc event entries up to (but not including) the failure event.
        Phase 2: STRICT replay with truncated log. After replay exhausts at step N,
                 live execution provides success WRs for steps N..5.
        """
        with FunctionExecutorProcessContextManager() as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize(
                    stub,
                    app_name="wf_sequential",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="wf_sequential",
                )

                fc_id: str = f"test-fc-wf-fail-{fail_at_step}"

                # ----------------------------------------------------------
                # Phase 1: Run workflow, inject failure at step N.
                # ----------------------------------------------------------
                alloc_id_1: str = f"alloc-wf-fail-{fail_at_step}-run"
                stub.create_allocation(
                    CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="req-fail",
                            function_call_id=fc_id,
                            allocation_id=alloc_id_1,
                            inputs=application_function_inputs("Foo", str),
                        )
                    )
                )

                captured_durable_ids: list[str] = []
                captured_entries: list[AllocationEvent] = []
                watcher_values: dict[str, tuple[Any, type]] = {}
                event_clock: int = 0

                def on_batch_fail(events, driver):
                    nonlocal event_clock
                    for event in events:
                        if event.HasField("create_function_call"):
                            did: str = (
                                event.create_function_call.updates.root_function_call_id
                            )
                            captured_durable_ids.append(did)
                            event_clock += 1
                            fcc_entry: AllocationEvent = AllocationEvent(
                                clock=event_clock,
                                function_call_created=AllocationEventFunctionCallCreated(
                                    function_call_id=did,
                                    status=ok_status(),
                                ),
                            )
                            captured_entries.append(fcc_entry)
                            driver.enqueue_event_log_response(
                                ReadAllocationEventLogResponse(
                                    allocation_id=alloc_id_1,
                                    entries=[fcc_entry],
                                    last_clock=event_clock,
                                    has_more=False,
                                )
                            )
                        elif event.HasField("create_function_call_watcher"):
                            wid: str = (
                                event.create_function_call_watcher.function_call_id
                            )
                            step_idx: int = captured_durable_ids.index(wid)

                            # Emit FWCC (watcher acknowledged by server regardless of result).
                            event_clock += 1
                            fwcc_entry: AllocationEvent = AllocationEvent(
                                clock=event_clock,
                                function_call_watcher_created=AllocationEventFunctionCallWatcherCreated(
                                    function_call_id=wid,
                                    status=ok_status(),
                                ),
                            )
                            captured_entries.append(fwcc_entry)
                            driver.enqueue_event_log_response(
                                ReadAllocationEventLogResponse(
                                    allocation_id=alloc_id_1,
                                    entries=[fwcc_entry],
                                    last_clock=event_clock,
                                    has_more=False,
                                )
                            )

                            # Emit WR.
                            event_clock += 1
                            if step_idx < fail_at_step - 1:
                                # Success WR for steps before the failing one.
                                so: SerializedObjectInsideBLOB
                                blob: BLOB
                                so, blob = make_pickle_blob(
                                    _WF_STEP_VALUES[step_idx],
                                    str,
                                    f"wr-blob-{alloc_id_1}-{step_idx}",
                                )
                                watcher_values[wid] = (
                                    _WF_STEP_VALUES[step_idx],
                                    str,
                                )
                                wr_entry: AllocationEvent = AllocationEvent(
                                    clock=event_clock,
                                    function_call_watcher_result=AllocationEventFunctionCallWatcherResult(
                                        function_call_id=wid,
                                        outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                                        watcher_status=FunctionCallWatcherStatus.FUNCTION_CALL_WATCHER_STATUS_COMPLETED,
                                        value_output=so,
                                        value_blob=blob,
                                    ),
                                )
                                captured_entries.append(wr_entry)
                                driver.enqueue_event_log_response(
                                    ReadAllocationEventLogResponse(
                                        allocation_id=alloc_id_1,
                                        entries=[wr_entry],
                                        last_clock=event_clock,
                                        has_more=False,
                                    )
                                )
                            else:
                                # Failure WR at the failing step — NOT captured (erased).
                                driver.enqueue_event_log_response(
                                    ReadAllocationEventLogResponse(
                                        allocation_id=alloc_id_1,
                                        entries=[
                                            AllocationEvent(
                                                clock=event_clock,
                                                function_call_watcher_result=AllocationEventFunctionCallWatcherResult(
                                                    function_call_id=wid,
                                                    outcome_code=AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
                                                    watcher_status=FunctionCallWatcherStatus.FUNCTION_CALL_WATCHER_STATUS_COMPLETED,
                                                ),
                                            )
                                        ],
                                        last_clock=event_clock,
                                        has_more=False,
                                    )
                                )

                driver_1: AllocationTestDriver = AllocationTestDriver(stub, alloc_id_1)
                finish_1: AllocationExecutionEventFinishAllocation = driver_1.run(
                    on_execution_event_batch=on_batch_fail
                )
                self.assertEqual(
                    finish_1.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
                )
                self.assertEqual(len(captured_durable_ids), fail_at_step)
                stub.delete_allocation(
                    DeleteAllocationRequest(allocation_id=alloc_id_1)
                )

                # ----------------------------------------------------------
                # Phase 2: STRICT replay with truncated log.
                # captured_entries = FCC+WR for steps 1..N-1, FCC for step N.
                # ----------------------------------------------------------
                alloc_id_2: str = f"alloc-wf-fail-{fail_at_step}-replay"
                last_replay_clock: int = (
                    captured_entries[-1].clock if captured_entries else 0
                )
                replay_response: ReadAllocationEventLogResponse = (
                    ReadAllocationEventLogResponse(
                        allocation_id=alloc_id_2,
                        entries=list(captured_entries),
                        last_clock=last_replay_clock,
                        has_more=False,
                    )
                )

                stub.create_allocation(
                    CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="req-replay",
                            function_call_id=fc_id,
                            allocation_id=alloc_id_2,
                            inputs=application_function_inputs("Foo", str),
                            replay_mode=REPLAY_MODE_STRICT,
                        )
                    )
                )

                # Build watcher_values for steps not yet known (N..5).
                # Known durable_ids cover steps 1..N; steps N+1..5 will appear
                # during live execution and are looked up by durable_id from
                # the initial run's mapping. For live steps we register values
                # when the FC arrives (below).
                for i, did in enumerate(captured_durable_ids):
                    if did not in watcher_values:
                        watcher_values[did] = (_WF_STEP_VALUES[i], str)
                live_next_step_idx: int = fail_at_step  # 0-indexed for step N+1
                live_clock: int = 0

                def on_batch_replay(events, driver):
                    nonlocal live_next_step_idx, live_clock
                    for event in events:
                        if event.HasField("create_function_call"):
                            did: str = (
                                event.create_function_call.updates.root_function_call_id
                            )
                            if did not in watcher_values:
                                watcher_values[did] = (
                                    _WF_STEP_VALUES[live_next_step_idx],
                                    str,
                                )
                                live_next_step_idx += 1
                            live_clock += 1
                            driver.enqueue_event_log_response(
                                ReadAllocationEventLogResponse(
                                    allocation_id=alloc_id_2,
                                    entries=[
                                        AllocationEvent(
                                            clock=live_clock,
                                            function_call_created=AllocationEventFunctionCallCreated(
                                                function_call_id=did,
                                                status=ok_status(),
                                            ),
                                        )
                                    ],
                                    last_clock=live_clock,
                                    has_more=False,
                                )
                            )
                        elif event.HasField("create_function_call_watcher"):
                            wid: str = (
                                event.create_function_call_watcher.function_call_id
                            )
                            value, type_hint = watcher_values[wid]

                            # Emit FWCC.
                            live_clock += 1
                            driver.enqueue_event_log_response(
                                ReadAllocationEventLogResponse(
                                    allocation_id=alloc_id_2,
                                    entries=[
                                        AllocationEvent(
                                            clock=live_clock,
                                            function_call_watcher_created=AllocationEventFunctionCallWatcherCreated(
                                                function_call_id=wid,
                                                status=ok_status(),
                                            ),
                                        )
                                    ],
                                    last_clock=live_clock,
                                    has_more=False,
                                )
                            )

                            # Emit WR.
                            live_clock += 1
                            driver.enqueue_event_log_response(
                                make_watcher_result_response(
                                    allocation_id=alloc_id_2,
                                    function_call_id=wid,
                                    clock=live_clock,
                                    value=value,
                                    type_hint=type_hint,
                                    blob_id=f"wr-blob-{alloc_id_2}-{live_clock}",
                                )
                            )

                driver_2: AllocationTestDriver = AllocationTestDriver(stub, alloc_id_2)
                driver_2.enqueue_event_log_response(replay_response)
                # Simulate Server re-creating watchers whose WR was erased.
                server_clock: list[int] = [last_replay_clock]
                enqueue_server_recreated_watchers(
                    driver=driver_2,
                    allocation_id=alloc_id_2,
                    truncated_entries=captured_entries,
                    watcher_values=watcher_values,
                    clock_box=server_clock,
                )
                finish_2: AllocationExecutionEventFinishAllocation = driver_2.run(
                    on_execution_event_batch=on_batch_replay
                )

                self.assertEqual(
                    finish_2.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
                )
                self.assertTrue(finish_2.HasField("value"))
                output: str = download_and_deserialize_so(
                    self, finish_2.value, finish_2.uploaded_function_outputs_blob
                )
                self.assertEqual(output, "[hello, foo! hello, foo!]")

                stub.delete_allocation(
                    DeleteAllocationRequest(allocation_id=alloc_id_2)
                )

    def test_replay_after_step_1_failure(self):
        """Replay after wf_normalize fails: FCC(step1) replayed, all 5 watchers + steps 2-5 live."""
        self._run_workflow_step_failure_and_replay(fail_at_step=1)

    def test_replay_after_step_2_failure(self):
        """Replay after wf_prefix fails: steps 1 replayed, steps 2-5 live."""
        self._run_workflow_step_failure_and_replay(fail_at_step=2)

    def test_replay_after_step_3_failure(self):
        """Replay after wf_exclaim fails: steps 1-2 replayed, steps 3-5 live."""
        self._run_workflow_step_failure_and_replay(fail_at_step=3)

    def test_replay_after_step_4_failure(self):
        """Replay after wf_repeat fails: steps 1-3 replayed, steps 4-5 live."""
        self._run_workflow_step_failure_and_replay(fail_at_step=4)

    def test_replay_after_step_5_failure(self):
        """Replay after wf_wrap fails: steps 1-4 replayed, step 5 live."""
        self._run_workflow_step_failure_and_replay(fail_at_step=5)


if __name__ == "__main__":
    unittest.main()
