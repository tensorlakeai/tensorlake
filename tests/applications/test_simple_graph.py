import unittest
from typing import List

import validate_all_applications
from pydantic import BaseModel

from tensorlake.applications import (
    Request,
    application,
    function,
    run_local_application,
    run_remote_application,
)
from tensorlake.applications.remote.deploy import deploy_applications

# Makes the test case discoverable by unittest framework.
ValidateAllApplicationsTest: unittest.TestCase = validate_all_applications.define_test()


class TestGraphRequestPayload(BaseModel):
    numbers: List[str]


@application()
@function(description="test simple graph sync")
def simple_graph_api_sync(payload: TestGraphRequestPayload) -> str:
    return print_and_return_value("simple graph: " + ", ".join(payload.numbers))


@function()
def print_and_return_value(value: str) -> str:
    print("Printed value:", value)
    return value


@application()
@function(description="test simple graph async")
async def simple_graph_api_async(payload: TestGraphRequestPayload) -> str:
    return await print_and_return_value_async(
        "simple graph: " + ", ".join(payload.numbers)
    )


@function()
async def print_and_return_value_async(value: str) -> str:
    print("Printed value:", value)
    return value


class TestSimpleGraph(unittest.TestCase):
    def test_sync_local_api_call(self):
        request: Request = run_local_application(
            simple_graph_api_sync,
            TestGraphRequestPayload(numbers=[str(i) for i in range(1, 6)]),
        )
        self.assertEqual(request.output(), "simple graph: 1, 2, 3, 4, 5")

    def test_sync_remote_api_call(self):
        deploy_applications(__file__)
        request: Request = run_remote_application(
            simple_graph_api_sync,
            TestGraphRequestPayload(numbers=[str(i) for i in range(1, 6)]),
        )

        self.assertEqual(request.output(), "simple graph: 1, 2, 3, 4, 5")

    def test_async_local_api_call(self):
        request: Request = run_local_application(
            simple_graph_api_async,
            TestGraphRequestPayload(numbers=[str(i) for i in range(1, 6)]),
        )
        self.assertEqual(request.output(), "simple graph: 1, 2, 3, 4, 5")

    def test_async_remote_api_call(self):
        deploy_applications(__file__)
        request: Request = run_remote_application(
            simple_graph_api_async,
            TestGraphRequestPayload(numbers=[str(i) for i in range(1, 6)]),
        )

        self.assertEqual(request.output(), "simple graph: 1, 2, 3, 4, 5")


if __name__ == "__main__":
    unittest.main()
