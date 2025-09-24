import unittest

import tensorlake.workflows.interface as tensorlake
from tensorlake.workflows.remote.deploy import deploy


@tensorlake.api(output_serializer="pickle")
@tensorlake.function()
def api_func(payload: str) -> str:
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


class TestFunctionOutputPropagation(unittest.TestCase):
    def test_function_output_propagation(self):
        deploy(__file__)
        request: tensorlake.Request = tensorlake.call_remote_api("api_func", "test")
        output: str = request.output()
        self.assertEqual(output, "buzz")


if __name__ == "__main__":
    unittest.main()
