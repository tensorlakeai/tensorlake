import unittest
from typing import List

from pydantic import BaseModel

from tensorlake.applications import (
    Request,
    application,
    function,
    run_local_application,
    run_remote_application,
)
from tensorlake.applications.remote.deploy import deploy_applications


class TestGraphRequestPayload(BaseModel):
    numbers: List[str]


@application()
@function(description="test simple graph")
def test_simple_graph_api(payload: TestGraphRequestPayload) -> str:
    return print_and_return_value("simple graph: " + ", ".join(payload.numbers))


@function()
def print_and_return_value(value: str) -> str:
    print("Printed value:", value)
    return value


class TestSimpleGraph(unittest.TestCase):
    def test_local_api_call(self):
        request: Request = run_local_application(
            test_simple_graph_api,
            TestGraphRequestPayload(numbers=[str(i) for i in range(1, 6)]),
        )
        self.assertEqual(request.output(), "simple graph: 1, 2, 3, 4, 5")

    def test_remote_api_call(self):
        deploy_applications(__file__)
        request: Request = run_remote_application(
            test_simple_graph_api,
            TestGraphRequestPayload(numbers=[str(i) for i in range(1, 6)]),
        )

        self.assertEqual(request.output(), "simple graph: 1, 2, 3, 4, 5")


if __name__ == "__main__":
    unittest.main()
