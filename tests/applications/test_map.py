import unittest

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
async def async_api_function_recursive_blocking_map(numbers: list[int]) -> list[int]:
    return await async_to_int.map(await async_to_string.map(numbers))


@application()
@function()
def api_function_recursive_non_blocking_map(numbers: list[int]) -> list[int]:
    return to_int.future.map(to_string.future.map(numbers)).run().result()


@application()
@function()
def api_function_recursive_non_blocking_map_with_future(
    numbers: list[int],
) -> list[int]:
    return to_int.future.map(to_string.future.map(numbers)).run().result()


@application()
@function()
def api_function_recursive_non_blocking_map_with_future_and_tail_call(
    numbers: list[int],
) -> list[int]:
    return to_int_tail_call.future.map(to_string.future.map(numbers)).run().result()


@application()
@function()
async def async_api_function_recursive_non_blocking_map(
    numbers: list[int],
) -> list[int]:
    return await async_to_int.map(async_to_string.map(numbers))


@application()
@function()
def api_function_recursive_tail_call_map(numbers: list[int]) -> list[int]:
    return to_int.future.map(to_string.future.map(numbers))


@application()
@function()
async def async_api_function_recursive_tail_call_map(numbers: list[int]) -> list[int]:
    return async_to_int.map(async_to_string.map(numbers))


@application()
@function()
def api_function_map_reduce_futures(numbers: list[int]) -> list[int]:
    return to_int.map(
        [
            async_concat_strings.reduce(["1", "2", "3", "4", "5"]),
            async_concat_strings.reduce(["1", "2", "3", "4", "5"]),
            async_concat_strings.reduce(["1", "2", "3", "4", "5"]),
        ]
    )


@function()
def to_string(value: int) -> str:
    return str(value)


@function()
def to_int(value: str) -> int:
    return int(value)


@function()
async def async_to_string(value: int) -> str:
    return str(value)


@function()
async def async_to_int(value: str) -> int:
    return int(value)


@function()
def to_int_tail_call(value: str) -> Future:
    return async_to_int.future(value)


@function()
async def async_concat_strings(a: str, b: str) -> str:
    return a + b


@function()
def generate_int_list(numbers: list[int]) -> list[int]:
    return numbers


@function()
async def async_generate_int_list(numbers: list[int]) -> list[int]:
    return numbers


@application()
@function()
def api_function_map_with_future_input(numbers: list[int]) -> list[str]:
    list_future = generate_int_list.future(numbers)
    return to_string.future.map(list_future).run().result()


@application()
@function()
async def async_api_function_map_with_future_input(numbers: list[int]) -> list[str]:
    list_future = async_generate_int_list.future(numbers)
    return await async_to_string.map(list_future)


@application()
@function()
def api_function_map_tail_call_with_future_input(numbers: list[int]) -> list[str]:
    list_future = generate_int_list.future(numbers)
    return to_string.future.map(list_future)


@application()
@function()
async def async_api_function_map_tail_call_with_future_input(
    numbers: list[int],
) -> list[str]:
    list_future = async_generate_int_list.future(numbers)
    return async_to_string.map(list_future)


class TestRecursiveMaps(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        deploy_applications(__file__)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_blocking(self, _: str, is_remote: bool):
        request: Request = run_application(
            api_function_recursive_blocking_map, is_remote, [1, 2, 3, 4, 5]
        )
        self.assertEqual(request.output(), [1, 2, 3, 4, 5])

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_async_blocking(self, _: str, is_remote: bool):
        request: Request = run_application(
            async_api_function_recursive_blocking_map, is_remote, [1, 2, 3, 4, 5]
        )
        self.assertEqual(request.output(), [1, 2, 3, 4, 5])

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_non_blocking(self, _: str, is_remote: bool):
        request: Request = run_application(
            api_function_recursive_non_blocking_map, is_remote, [1, 2, 3, 4, 5]
        )
        self.assertEqual(request.output(), [1, 2, 3, 4, 5])

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_non_blocking_with_future(self, _: str, is_remote: bool):
        request: Request = run_application(
            api_function_recursive_non_blocking_map_with_future,
            is_remote,
            [1, 2, 3, 4, 5],
        )
        self.assertEqual(request.output(), [1, 2, 3, 4, 5])

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_non_blocking_with_future_and_tail_call(self, _: str, is_remote: bool):
        request: Request = run_application(
            api_function_recursive_non_blocking_map_with_future_and_tail_call,
            is_remote,
            [1, 2, 3, 4, 5],
        )
        self.assertEqual(request.output(), [1, 2, 3, 4, 5])

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_async_non_blocking(self, _: str, is_remote: bool):
        request: Request = run_application(
            async_api_function_recursive_non_blocking_map, is_remote, [1, 2, 3, 4, 5]
        )
        self.assertEqual(request.output(), [1, 2, 3, 4, 5])

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_tail_call(self, _: str, is_remote: bool):
        request: Request = run_application(
            api_function_recursive_tail_call_map, is_remote, [1, 2, 3, 4, 5]
        )
        self.assertEqual(request.output(), [1, 2, 3, 4, 5])

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_async_tail_call(self, _: str, is_remote: bool):
        request: Request = run_application(
            async_api_function_recursive_tail_call_map, is_remote, [1, 2, 3, 4, 5]
        )
        self.assertEqual(request.output(), [1, 2, 3, 4, 5])

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_map_reduce_futures(self, _: str, is_remote: bool):
        request: Request = run_application(
            api_function_map_reduce_futures, is_remote, [1, 2, 3, 4, 5]
        )
        self.assertEqual(request.output(), [12345, 12345, 12345])

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_map_with_future_input(self, _: str, is_remote: bool):
        request: Request = run_application(
            api_function_map_with_future_input, is_remote, [1, 2, 3, 4, 5]
        )
        self.assertEqual(request.output(), ["1", "2", "3", "4", "5"])

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_async_map_with_future_input(self, _: str, is_remote: bool):
        request: Request = run_application(
            async_api_function_map_with_future_input, is_remote, [1, 2, 3, 4, 5]
        )
        self.assertEqual(request.output(), ["1", "2", "3", "4", "5"])

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_map_tail_call_with_future_input(self, _: str, is_remote: bool):
        request: Request = run_application(
            api_function_map_tail_call_with_future_input, is_remote, [1, 2, 3, 4, 5]
        )
        self.assertEqual(request.output(), ["1", "2", "3", "4", "5"])

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_async_map_tail_call_with_future_input(self, _: str, is_remote: bool):
        request: Request = run_application(
            async_api_function_map_tail_call_with_future_input,
            is_remote,
            [1, 2, 3, 4, 5],
        )
        self.assertEqual(request.output(), ["1", "2", "3", "4", "5"])


if __name__ == "__main__":
    unittest.main()
