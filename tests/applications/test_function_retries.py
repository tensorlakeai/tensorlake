import time
import unittest

import parameterized

from tensorlake.applications import (
    Request,
    RequestFailureException,
    Retries,
    application,
    function,
    run_application,
)
from tensorlake.applications.remote.deploy import deploy_applications

function_with_retry_policy_call_number = 0


@application()
@function(retries=Retries(max_retries=3))
def function_that_succeeds_on_3rd_retry(x: int) -> str:
    global function_with_retry_policy_call_number
    function_with_retry_policy_call_number += 1

    if function_with_retry_policy_call_number == 4:
        return "success"
    else:
        raise Exception("Function failed, please retry")


@application()
@function(retries=Retries(max_retries=3))
def function_that_always_fails(x: int) -> str:
    raise Exception("Function always fails")


class TestFunctionRetries(unittest.TestCase):
    def setUp(self) -> None:
        global function_with_retry_policy_call_number
        function_with_retry_policy_call_number = 0

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_function_retries(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        start_time: float = time.monotonic()
        request: Request = run_application(
            function_that_succeeds_on_3rd_retry, 1, remote=is_remote
        )
        self.assertEqual(request.output(), "success")
        duration_sec: float = time.monotonic() - start_time

        self.assertLess(
            duration_sec, 10.0
        )  # 3 retries with max 1 second delay should complete in less than 10 seconds

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_function_fails_with_retries_exhausted(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        start_time: float = time.monotonic()
        request: Request = run_application(
            function_that_always_fails, 1, remote=is_remote
        )
        self.assertRaises(RequestFailureException, request.output)
        duration_sec: float = time.monotonic() - start_time

        self.assertLess(
            duration_sec, 10.0
        )  # 3 retries with max 1 second delay should complete in less than 10 seconds


if __name__ == "__main__":
    unittest.main()
