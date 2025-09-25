import time
import unittest

import parameterized

import tensorlake.workflows.interface as tensorlake
from tensorlake.workflows.remote.deploy import deploy

function_with_retry_policy_call_number = 0


@tensorlake.api()
@tensorlake.function(retries=tensorlake.Retries(max_retries=3))
def function_that_succeeds_on_3rd_retry(x: int) -> str:
    global function_with_retry_policy_call_number
    function_with_retry_policy_call_number += 1

    if function_with_retry_policy_call_number == 4:
        return "success"
    else:
        raise Exception("Function failed, please retry")


@tensorlake.api()
@tensorlake.function(retries=tensorlake.Retries(max_retries=3))
def function_that_always_fails(x: int) -> str:
    raise Exception("Function always fails")


class TestFunctionRetries(unittest.TestCase):
    @parameterized.parameterized.expand([(True), (False)])
    def test_function_retries(self, is_remote: bool):
        if is_remote:
            deploy(__file__)

        start_time: float = time.monotonic()
        request: tensorlake.Request = tensorlake.call_api(
            function_that_succeeds_on_3rd_retry, 1, remote=is_remote
        )
        self.assertEqual(request.output(), "success")
        duration_sec: float = time.monotonic() - start_time

        self.assertLess(
            duration_sec, 10.0
        )  # 3 retries with max 1 second delay should complete in less than 10 seconds

    @parameterized.parameterized.expand([(True), (False)])
    def test_function_fails_with_retries_exhausted(self, is_remote: bool):
        if is_remote:
            deploy(__file__)

        start_time: float = time.monotonic()
        request: tensorlake.Request = tensorlake.call_api(
            function_that_always_fails, 1, remote=is_remote
        )
        self.assertRaises(tensorlake.RequestFailureException, request.output)
        duration_sec: float = time.monotonic() - start_time

        self.assertLess(
            duration_sec, 10.0
        )  # 3 retries with max 1 second delay should complete in less than 10 seconds


if __name__ == "__main__":
    unittest.main()
