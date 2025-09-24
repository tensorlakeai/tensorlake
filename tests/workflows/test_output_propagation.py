import unittest

import tensorlake.workflows.interface as tensorlake
from tensorlake.workflows.remote.deploy import deploy


@tensorlake.api(output_serializer="pickle")
@tensorlake.function()
def api_function_output_propagation(payload: str) -> str:
    return foo()


@tensorlake.function()
def foo() -> str:
    return bar()


@tensorlake.function()
def bar() -> str:
    return buzz()


@tensorlake.function()
def buzz() -> str:
    return "buzz"


@tensorlake.api(output_serializer="pickle")
@tensorlake.function()
def api_reducer_output_propagation(payload: str) -> str:
    return foo_reducer()


@tensorlake.function()
def foo_reducer() -> str:
    return bar_reducer()


@tensorlake.function()
def bar_reducer() -> str:
    return buzz_reducer()


@tensorlake.function()
def buzz_reducer() -> str:
    return tensorlake.reduce(concat_strings, "buzz_reducer")


@tensorlake.function()
def concat_strings(a: str, b: str) -> str:
    return a + b


class TestOutputPropagation(unittest.TestCase):
    def test_function_output_propagation(self):
        deploy(__file__)
        request: tensorlake.Request = tensorlake.call_remote_api(
            api_function_output_propagation, "test"
        )
        output: str = request.output()
        self.assertEqual(output, "buzz")

    def test_reducer_output_propagation(self):
        deploy(__file__)
        request: tensorlake.Request = tensorlake.call_remote_api(
            api_reducer_output_propagation, "test"
        )
        output: str = request.output()
        self.assertEqual(output, "buzz_reducer")


if __name__ == "__main__":
    unittest.main()
