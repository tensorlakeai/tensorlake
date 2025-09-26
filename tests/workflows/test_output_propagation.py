import unittest

from tensorlake.workflows.interface import (
    Request,
    api,
    call_api,
    call_remote_api,
    function,
    reduce,
)
from tensorlake.workflows.remote.deploy import deploy


@api(output_serializer="pickle")
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


@api(output_serializer="pickle")
@function()
def api_reducer_output_propagation(payload: str) -> str:
    return foo_reducer()


@function()
def foo_reducer() -> str:
    return bar_reducer()


@function()
def bar_reducer() -> str:
    return buzz_reducer()


@function()
def buzz_reducer() -> str:
    return reduce(concat_strings, "buzz_reducer")


@function()
def concat_strings(a: str, b: str) -> str:
    return a + b


class TestOutputPropagation(unittest.TestCase):
    def test_function_output_propagation(self):
        deploy(__file__)
        request: Request = call_remote_api(api_function_output_propagation, "test")
        output: str = request.output()
        self.assertEqual(output, "buzz")

    def test_reducer_output_propagation(self):
        deploy(__file__)
        request: Request = call_remote_api(api_reducer_output_propagation, "test")
        output: str = request.output()
        self.assertEqual(output, "buzz_reducer")


if __name__ == "__main__":
    unittest.main()
