import unittest

import parameterized

from tensorlake.workflows.interface import (
    Request,
    api,
    call_api,
    function,
    reduce,
)
from tensorlake.workflows.remote.deploy import deploy


# The call chain starting from this API function goes through multiple functions
# the last function in the call chain returns a value which then needs to be propagated
# back up the call chain to the API function which returns it as the API response.
# This test ensures that Server implements the output propagation correctly from the last
# function back to the API function.
# This test also verifies that the last function in the call sequence inherits its output
# serializer from the API function because its output serializer is JSON while all the
# other function in call chain use Pickle unless the caller (api function) overrides it
# because it needs to return its output in json format.
@api()
@function()
def api_function_output_propagation(payload: str) -> str:
    return foo()


@function()
def foo() -> str:
    return bar()


@function()
def bar() -> str:
    return buzz()


@function()
def buzz() -> str:
    return "buzz"


class TestFunctionOutputPropagation(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_success(self, _: str, is_remote: bool):
        if is_remote:
            deploy(__file__)
        request: Request = call_api(
            api_function_output_propagation, "test", remote=is_remote
        )
        self.assertEqual(request.output(), "buzz")


@api()
@function()
def api_reducer_value_output_propagation(payload: str) -> str:
    return foo_reducer_value()


@function()
def foo_reducer_value() -> str:
    return bar_reducer_value()


@function()
def bar_reducer_value() -> str:
    return buzz_reducer_value()


@function()
def buzz_reducer_value() -> str:
    return reduce(concat_strings_value, "buzz_reducer_value")


@function()
def concat_strings_value(a: str, b: str) -> str:
    return a + b


class TestReducerValueOutputPropagation(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_success(self, _: str, is_remote: bool):
        if is_remote:
            deploy(__file__)
        request: Request = call_api(
            api_reducer_value_output_propagation, "test", remote=is_remote
        )
        self.assertEqual(request.output(), "buzz_reducer_value")


@api()
@function()
def api_reducer_subcall_output_propagation(payload: str) -> str:
    return foo_reducer_subcall()


@function()
def foo_reducer_subcall() -> str:
    return bar_reducer_subcall()


@function()
def bar_reducer_subcall() -> str:
    return buzz_reducer_subcall()


@function()
def buzz_reducer_subcall() -> str:
    return reduce(concat_strings_subcall, "buzz_reducer_subcall")


@function()
def concat_strings_subcall(a: str, b: str) -> str:
    return concat_strings_actually(a, b)


@function()
def concat_strings_actually(a: str, b: str) -> str:
    return a + b


class TestReducerSubcallOutputPropagation(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_success(self, _: str, is_remote: bool):
        if is_remote:
            deploy(__file__)
        request: Request = call_api(
            api_reducer_subcall_output_propagation, "test", remote=is_remote
        )
        self.assertEqual(request.output(), "buzz_reducer_subcall")


if __name__ == "__main__":
    unittest.main()
