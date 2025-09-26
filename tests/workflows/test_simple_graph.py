import unittest
from typing import List

from pydantic import BaseModel

# This import will be replaced by `import tensorlake` when we switch to the new SDK UX.
import tensorlake.workflows.interface as tensorlake
from tensorlake.workflows.remote.deploy import deploy


class TestGraphRequestPayload(BaseModel):
    numbers: List[str]


@tensorlake.api()
@tensorlake.function(description="test simple graph")
def test_simple_graph_api(
    ctx: tensorlake.RequestContext, payload: TestGraphRequestPayload
) -> str:
    return print_and_return_value("simple graph: " + ", ".join(payload.numbers))


@tensorlake.function()
def print_and_return_value(value: str) -> str:
    print("Printed value:", value)
    return value


class TestSimpleGraph(unittest.TestCase):
    def test_local_api_call(self):
        request = tensorlake.call_local_api(
            test_simple_graph_api,
            TestGraphRequestPayload(numbers=[str(i) for i in range(1, 6)]),
        )
        self.assertEqual(request.output(), "simple graph: 1, 2, 3, 4, 5")

    def test_remote_api_call(self):
        deploy(__file__)
        request: tensorlake.Request = tensorlake.call_remote_api(
            test_simple_graph_api,
            TestGraphRequestPayload(numbers=[str(i) for i in range(1, 6)]),
        )

        self.assertEqual(request.output(), "simple graph: 1, 2, 3, 4, 5")


if __name__ == "__main__":
    unittest.main()
