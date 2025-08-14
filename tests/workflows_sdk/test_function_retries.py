import time
import unittest

from testing import test_graph_name

from tensorlake import (
    Graph,
    tensorlake_function,
)
from tensorlake.functions_sdk.retries import Retries


@tensorlake_function(retries=Retries(max_retries=3, max_delay=1.0))
def function_with_retry_policy(x: int) -> str:
    function_with_retry_policy.call_number += 1

    if function_with_retry_policy.call_number == 4:
        return "success"
    else:
        raise Exception("Function failed, please retry")


function_with_retry_policy.call_number = 0


class TestFunctionRetries(unittest.TestCase):
    def test_function_retries(self):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=function_with_retry_policy,
        )
        start_time = time.monotonic()
        invocation_id = graph.run(block_until_done=True, request=1)
        duration_sec = time.monotonic() - start_time

        outputs = graph.output(invocation_id, "function_with_retry_policy")
        self.assertEqual(len(outputs), 1)
        self.assertEqual(outputs[0], "success")
        self.assertLess(
            duration_sec, 10.0
        )  # 3 retries with max 1 second delay should complete in less than 10 seconds


if __name__ == "__main__":
    unittest.main()
