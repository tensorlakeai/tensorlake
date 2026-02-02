import unittest
from typing import List

import parameterized
import validate_all_applications

from tensorlake.applications import (
    Future,
    Request,
    RequestFailed,
    application,
    function,
)
from tensorlake.applications.applications import run_application
from tensorlake.applications.remote.deploy import deploy_applications

# Makes the test case discoverable by unittest framework.
ValidateAllApplicationsTest: unittest.TestCase = validate_all_applications.define_test()


@application()
@function()
def api_function_recursive_blocking_map(numbers: list[int]) -> list[int]:
    return to_int.map(to_string.map(numbers))


@application()
@function()
def api_function_recursive_non_blocking_map(numbers: list[int]) -> list[int]:
    future: Future = to_int.awaitable.map(to_string.awaitable.map(numbers)).run()
    return future.result()


@application()
@function()
def api_function_recursive_tail_call_map(numbers: list[int]) -> list[int]:
    return to_int.awaitable.map(to_string.awaitable.map(numbers))


@function()
def to_string(value: int) -> str:
    return str(value)


@function()
def to_int(value: str) -> int:
    return int(value)


class TestRecursiveMaps(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_blocking(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(
            api_function_recursive_blocking_map, is_remote, [1, 2, 3, 4, 5]
        )
        self.assertEqual(request.output(), [1, 2, 3, 4, 5])

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_non_blocking(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(
            api_function_recursive_non_blocking_map, is_remote, [1, 2, 3, 4, 5]
        )
        self.assertEqual(request.output(), [1, 2, 3, 4, 5])

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_tail_call(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(
            api_function_recursive_tail_call_map, is_remote, [1, 2, 3, 4, 5]
        )
        # Map tail calls are not working by design because Server can't convert individual
        # resolved items into a python list.
        self.assertRaises(RequestFailed, request.output)


if __name__ == "__main__":
    unittest.main()
