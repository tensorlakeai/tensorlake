import threading
import unittest

from tensorlake.function_executor.allocation_runner.execution_log_buffer import (
    ExecutionLogBuffer,
)
from tensorlake.function_executor.proto.function_executor_pb2 import (
    AllocationExecutionEvent,
    AllocationExecutionEventFinishAllocation,
)


class ExecutionLogBufferTest(unittest.TestCase):
    def test_terminal_observation_waits_for_older_batch_to_advance(self) -> None:
        buffer = ExecutionLogBuffer()
        buffer.add_batch([AllocationExecutionEvent()])
        buffer.add_batch(
            [
                AllocationExecutionEvent(
                    finish_allocation=AllocationExecutionEventFinishAllocation()
                )
            ]
        )
        observed: list[bool] = []
        waiter = threading.Thread(
            target=lambda: observed.append(buffer.wait_for_terminal_observed(timeout=1))
        )
        waiter.start()

        self.assertEqual(buffer.get_current_batch(), [AllocationExecutionEvent()])
        self.assertEqual(observed, [])
        buffer.advance()
        terminal = buffer.get_current_batch()
        self.assertTrue(terminal[0].HasField("finish_allocation"))
        waiter.join(timeout=1)

        self.assertFalse(waiter.is_alive())
        self.assertEqual(observed, [True])


if __name__ == "__main__":
    unittest.main()
