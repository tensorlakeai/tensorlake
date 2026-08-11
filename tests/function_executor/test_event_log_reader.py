import threading
import unittest

from tensorlake.function_executor.allocation_runner.event_log_reader import (
    EventLogReader,
    InvalidEventLogResponse,
    validate_event_log_response,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    AllocationEvent,
    AllocationEventFunctionCallCreated,
    ReadAllocationEventLogResponse,
)


def _event(clock: int = 1) -> AllocationEvent:
    return AllocationEvent(
        clock=clock,
        function_call_created=AllocationEventFunctionCallCreated(
            function_call_id="call",
        ),
    )


class TestEventLogResponseValidation(unittest.TestCase):
    def test_accepts_an_empty_terminal_page(self) -> None:
        response = ReadAllocationEventLogResponse(has_more=False)

        self.assertEqual(
            validate_event_log_response(response, requested_after_clock=7),
            7,
        )

    def test_accepts_strictly_ordered_entries(self) -> None:
        response = ReadAllocationEventLogResponse(
            entries=[_event(6), _event(8)],
            last_clock=9,
            has_more=True,
        )

        self.assertEqual(
            validate_event_log_response(response, requested_after_clock=5),
            9,
        )

    def test_rejects_malformed_pages(self) -> None:
        cases = [
            (ReadAllocationEventLogResponse(last_clock=(1 << 53)), 0),
            (ReadAllocationEventLogResponse(last_clock=4), 5),
            (ReadAllocationEventLogResponse(last_clock=5, has_more=True), 5),
            (ReadAllocationEventLogResponse(entries=[_event(5)], last_clock=5), 5),
            (
                ReadAllocationEventLogResponse(
                    entries=[AllocationEvent(clock=6)],
                    last_clock=6,
                ),
                5,
            ),
            (
                ReadAllocationEventLogResponse(
                    entries=[_event(6), _event(6)],
                    last_clock=6,
                ),
                5,
            ),
            (
                ReadAllocationEventLogResponse(
                    entries=[_event(7)],
                    last_clock=6,
                ),
                5,
            ),
        ]

        for response, requested_after_clock in cases:
            with self.subTest(
                response=response,
                requested_after_clock=requested_after_clock,
            ):
                with self.assertRaises(InvalidEventLogResponse):
                    validate_event_log_response(response, requested_after_clock)


class TestEventLogReaderTransport(unittest.TestCase):
    def test_republishes_a_pending_read_to_a_reconnected_watcher(self) -> None:
        reader = EventLogReader("allocation")
        result: list[ReadAllocationEventLogResponse] = []
        read_thread = threading.Thread(target=lambda: result.append(reader.read(7)))
        read_thread.start()

        first_watcher = reader.watch_read_requests()
        first_request = next(first_watcher)
        self.assertEqual(first_request.after_clock, 7)

        reconnected_watcher = reader.watch_read_requests()
        reconnected_request = next(reconnected_watcher)
        self.assertEqual(reconnected_request, first_request)

        response = ReadAllocationEventLogResponse(
            allocation_id="allocation",
            last_clock=7,
        )
        self.assertTrue(reader.deliver_read_response(response))
        read_thread.join(timeout=1)

        self.assertFalse(read_thread.is_alive())
        self.assertEqual(result, [response])

    def test_ignores_responses_when_no_read_is_pending(self) -> None:
        reader = EventLogReader("allocation")

        self.assertFalse(
            reader.deliver_read_response(
                ReadAllocationEventLogResponse(allocation_id="allocation")
            )
        )


if __name__ == "__main__":
    unittest.main()
