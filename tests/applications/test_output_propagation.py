import unittest

import parameterized

from tensorlake.applications import (
    Request,
    application,
    function,
    run_application,
)
from tensorlake.applications.remote.deploy import deploy_applications


# The call chain starting from this API function goes through multiple functions
# the last function in the call chain returns a value which then needs to be propagated
# back up the call chain to the API function which returns it as the API response.
# This test ensures that Server implements the output propagation correctly from the last
# function back to the API function.
# This test also verifies that the last function in the call sequence inherits its output
# serializer from the API function because its output serializer is JSON while all the
# other function in call chain use Pickle unless the caller (api function) overrides it
# because it needs to return its output in json format.
@application()
@function()
def api_function_output_propagation(payload: str) -> str:
    return foo.awaitable()


@function()
def foo() -> str:
    return bar.awaitable()


@function()
def bar() -> str:
    return buzz.awaitable()


@function()
def buzz() -> str:
    return "buzz"


class TestFunctionOutputPropagation(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_success(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(
            api_function_output_propagation, "test", remote=is_remote
        )
        self.assertEqual(request.output(), "buzz")


@application()
@function()
def api_reducer_value_output_propagation(payload: str) -> str:
    return foo_reducer_value.awaitable()


@function()
def foo_reducer_value() -> str:
    return bar_reducer_value.awaitable()


@function()
def bar_reducer_value() -> str:
    return buzz_reducer_value.awaitable()


@function()
def buzz_reducer_value() -> str:
    return concat_strings_value.awaitable.reduce("buzz_reducer_value")


@function()
def concat_strings_value(a: str, b: str) -> str:
    return a + b


class TestReducerValueOutputPropagation(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_success(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(
            api_reducer_value_output_propagation, "test", remote=is_remote
        )
        self.assertEqual(request.output(), "buzz_reducer_value")


@application()
@function()
def api_reducer_subcall_output_propagation(payload: str) -> str:
    return foo_reducer_subcall.awaitable()


@function()
def foo_reducer_subcall() -> str:
    return bar_reducer_subcall.awaitable()


@function()
def bar_reducer_subcall() -> str:
    return buzz_reducer_subcall.awaitable()


@function()
def buzz_reducer_subcall() -> str:
    return concat_strings_subcall.awaitable.reduce("buzz_reducer_subcall")


@function()
def concat_strings_subcall(a: str, b: str) -> str:
    return concat_strings_actually.awaitable(a, b)


@function()
def concat_strings_actually(a: str, b: str) -> str:
    return a + b


class TestReducerSubcallOutputPropagation(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_success(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(
            "api_reducer_subcall_output_propagation", "test", remote=is_remote
        )
        self.assertEqual(request.output(), "buzz_reducer_subcall")


if __name__ == "__main__":
    unittest.main()
