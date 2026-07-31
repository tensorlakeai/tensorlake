import unittest
from unittest.mock import patch

from tensorlake.function_executor.user_events import (
    AllocationEventDetails,
    InitializationEventDetails,
    log_user_event_allocations_finished,
    log_user_event_allocations_started,
    log_user_event_function_call_failed,
    log_user_event_initialization_failed,
    log_user_event_initialization_finished,
    log_user_event_initialization_started,
)


class UserEventsTest(unittest.TestCase):
    def test_executor_events_are_best_effort_when_stdout_is_broken(self) -> None:
        initialization = InitializationEventDetails(
            namespace="namespace",
            application_name="application",
            application_version="version",
            function_name="function",
        )
        allocation = AllocationEventDetails(
            namespace="namespace",
            application_name="application",
            application_version="version",
            function_name="function",
            request_id="request",
            function_call_id="function-call",
            allocation_id="allocation",
        )

        with patch(
            "tensorlake.function_executor.user_events.print_cloud_event",
            side_effect=OSError("stdout is closed"),
        ):
            log_user_event_initialization_started(initialization)
            log_user_event_initialization_finished(initialization)
            log_user_event_initialization_failed(initialization, RuntimeError("failed"))
            log_user_event_allocations_started([allocation])
            log_user_event_allocations_finished([allocation])
            log_user_event_function_call_failed(allocation, RuntimeError("failed"))


if __name__ == "__main__":
    unittest.main()
