import os
import time
import unittest
from typing import Any

from replay_testing import (
    enqueue_server_recreated_watchers,
    respond_to_events_initial,
    respond_to_events_replay,
)
from testing import (
    AllocationTestDriver,
    FunctionExecutorProcessContextManager,
    application_function_inputs,
    download_and_deserialize_so,
    initialize,
    rpc_channel,
)

from tensorlake.applications import (
    RETURN_WHEN,
    Future,
    application,
    function,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    REPLAY_MODE_STRICT,
    Allocation,
    AllocationEvent,
    AllocationExecutionEventFinishAllocation,
    AllocationOutcomeCode,
    CreateAllocationRequest,
    DeleteAllocationRequest,
    ReadAllocationEventLogResponse,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)

APPLICATION_CODE_DIR_PATH = os.path.dirname(os.path.abspath(__file__))

# Expected reduce result: sum(2*i for i in range(10)) = 2*45 = 90.
_EXPECTED_REDUCE_VALUE: int = 90
# Number of map calls in the complex function.
_MAP_COUNT: int = 10
# Number of sleep_and_return_arg watchers (phase 1 + phase 2).
_SLEEP_WATCHER_COUNT: int = 6


# ---------------------------------------------------------------------------
# Functions under test.
# ---------------------------------------------------------------------------


@function()
def sleep_and_return_arg(arg: Any, delay: float) -> Any:
    print(f"sleep_and_return_arg: {arg}, {delay}")
    time.sleep(delay)
    return arg


@function()
def double(x: int) -> int:
    return 2 * x


@function()
def add(x: int, y: int) -> int:
    return x + y


@application()
@function()
def complex_function() -> int:
    # Phase 1: three parallel futures, wait for ALL to complete.
    Future.wait(
        futures=[
            sleep_and_return_arg.future("first", 1.0),
            sleep_and_return_arg.future("second", 2.0),
            sleep_and_return_arg.future("third", 3.0),
        ],
        return_when=RETURN_WHEN.ALL_COMPLETED,
    )

    # Phase 2: three parallel futures, wait for FIRST to complete.
    Future.wait(
        futures=[
            sleep_and_return_arg.future("fourth", 1.0),
            sleep_and_return_arg.future("fifth", 2.0),
            sleep_and_return_arg.future("sixth", 3.0),
        ],
        return_when=RETURN_WHEN.FIRST_COMPLETED,
    )

    # Phase 3: manual map calls — fan out.
    maps: list[Future] = [double.future(i) for i in range(_MAP_COUNT)]
    done, _ = Future.wait(futures=maps, return_when=RETURN_WHEN.ALL_COMPLETED)
    map_results: list[int] = [f.result() for f in done]

    # Phase 4: manual reduce — sequential chain of add futures.
    last_future: Future = add.future(map_results[0], map_results[1])
    for i in range(2, len(map_results)):
        last_future = add.future(last_future, map_results[i])
    return last_future.result()


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


def _watcher_value(watcher_index: int) -> tuple[Any, type]:
    """Returns (value, type_hint) for the given watcher index.

    Watcher ordering for complex_function:
      0-2:  phase 1 sleep_and_return_arg (ALL_COMPLETED) — discarded
      3-5:  phase 2 sleep_and_return_arg (FIRST_COMPLETED) — discarded
      6-15: phase 3 double(i) map results — consumed by reduce chain
      16:   phase 4 final add result — the function's return value
    """
    if watcher_index < _SLEEP_WATCHER_COUNT:
        return (None, type(None))
    elif watcher_index < _SLEEP_WATCHER_COUNT + _MAP_COUNT:
        i: int = watcher_index - _SLEEP_WATCHER_COUNT
        return (2 * i, int)
    else:
        return (_EXPECTED_REDUCE_VALUE, int)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestStrictReplayComplexFunction(unittest.TestCase):
    """Runs complex_function once to capture all allocation events, then replays
    with the log truncated at every possible position to verify strict replay
    handles all edge cases correctly."""

    def _assert_output(
        self, finish: AllocationExecutionEventFinishAllocation, msg: str
    ) -> None:
        self.assertEqual(
            finish.outcome_code,
            AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_SUCCESS,
            msg,
        )
        self.assertTrue(finish.HasField("value"), msg)
        output: int = download_and_deserialize_so(
            self, finish.value, finish.uploaded_function_outputs_blob
        )
        self.assertEqual(output, _EXPECTED_REDUCE_VALUE, msg)

    def test_replay_at_every_allocation_event(self):
        with FunctionExecutorProcessContextManager() as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize(
                    stub,
                    app_name="complex_function",
                    app_version="0.1",
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="complex_function",
                )

                # ----------------------------------------------------------
                # Phase 0: Full successful run to capture all alloc events.
                # ----------------------------------------------------------
                alloc_id_0: str = "alloc-complex-full"
                stub.create_allocation(
                    CreateAllocationRequest(
                        allocation=Allocation(
                            request_id="req-full",
                            function_call_id="test-fc-complex",
                            allocation_id=alloc_id_0,
                            inputs=application_function_inputs("x", str),
                        )
                    )
                )

                all_alloc_events: list[AllocationEvent] = []
                clock_box: list[int] = [0]
                watcher_count_box: list[int] = [0]
                watcher_values: dict[str, tuple[Any, type]] = {}

                def on_batch_full(events, driver):
                    respond_to_events_initial(
                        events,
                        driver,
                        alloc_id_0,
                        all_alloc_events,
                        clock_box,
                        watcher_count_box,
                        watcher_values,
                        _watcher_value,
                    )

                driver_0: AllocationTestDriver = AllocationTestDriver(stub, alloc_id_0)
                finish_0: AllocationExecutionEventFinishAllocation = driver_0.run(
                    on_execution_event_batch=on_batch_full
                )
                self._assert_output(finish_0, "Full run failed")
                total_events: int = len(all_alloc_events)
                self.assertGreater(total_events, 0, "Expected allocation events")
                stub.delete_allocation(
                    DeleteAllocationRequest(allocation_id=alloc_id_0)
                )

                # ----------------------------------------------------------
                # Phase 1: For each truncation point, replay and verify.
                # truncate_at=0 means empty log (all events erased).
                # truncate_at=N means keep first N events.
                # ----------------------------------------------------------
                for truncate_at in range(total_events):
                    with self.subTest(
                        truncate_at=truncate_at, total_events=total_events
                    ):
                        alloc_id: str = f"alloc-complex-replay-{truncate_at}"
                        truncated: list[AllocationEvent] = all_alloc_events[
                            :truncate_at
                        ]
                        last_clock: int = truncated[-1].clock if truncated else 0
                        replay_response: ReadAllocationEventLogResponse = (
                            ReadAllocationEventLogResponse(
                                allocation_id=alloc_id,
                                entries=list(truncated),
                                last_clock=last_clock,
                                has_more=False,
                            )
                        )

                        stub.create_allocation(
                            CreateAllocationRequest(
                                allocation=Allocation(
                                    request_id=f"req-replay-{truncate_at}",
                                    function_call_id="test-fc-complex",
                                    allocation_id=alloc_id,
                                    inputs=application_function_inputs("x", str),
                                    replay_mode=REPLAY_MODE_STRICT,
                                )
                            )
                        )

                        live_clock: list[int] = [0]
                        _aid: str = alloc_id
                        _wv: dict = watcher_values

                        def on_batch_replay(
                            events,
                            driver,
                            _a=_aid,
                            _lc=live_clock,
                            _wvs=_wv,
                        ):
                            respond_to_events_replay(events, driver, _a, _lc, _wvs)

                        driver: AllocationTestDriver = AllocationTestDriver(
                            stub, alloc_id
                        )
                        driver.enqueue_event_log_response(replay_response)
                        # Simulate Server re-creating watchers whose WR was truncated.
                        server_clock: list[int] = [last_clock]
                        enqueue_server_recreated_watchers(
                            driver=driver,
                            allocation_id=alloc_id,
                            truncated_entries=truncated,
                            watcher_values=watcher_values,
                            clock_box=server_clock,
                        )
                        finish: AllocationExecutionEventFinishAllocation = driver.run(
                            on_execution_event_batch=on_batch_replay
                        )

                        self._assert_output(
                            finish,
                            f"Replay failed at truncation point {truncate_at}/{total_events}",
                        )
                        stub.delete_allocation(
                            DeleteAllocationRequest(allocation_id=alloc_id)
                        )


if __name__ == "__main__":
    unittest.main()
