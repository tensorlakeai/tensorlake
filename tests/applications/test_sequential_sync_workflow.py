import unittest

import validate_all_applications

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


@function()
def step_normalize(text: str) -> str:
    return text.strip().lower()


@function()
def step_prefix(text: str) -> str:
    return "hello, " + text


@function()
def step_exclaim(text: str) -> str:
    return text + "!"


@function()
def step_repeat(text: str) -> str:
    return text + " " + text


@function()
def step_wrap(text: str) -> str:
    return "[" + text + "]"


@application()
@function()
def simple_workflow(payload: str) -> str:
    normalized: str = step_normalize(payload)
    prefixed: str = step_prefix(normalized)
    exclaimed: str = step_exclaim(prefixed)
    repeated: str = step_repeat(exclaimed)
    result: str = step_wrap(repeated)
    return result


class TestSequentialSyncWorkflow(unittest.TestCase):
    def test_local_api_call(self):
        request: Request = run_local_application(
            simple_workflow,
            payload="Foo",
        )
        self.assertEqual(request.output(), "[hello, foo! hello, foo!]")

    def test_remote_api_call(self):
        deploy_applications(__file__)
        request: Request = run_remote_application(
            simple_workflow,
            payload="Bar",
        )
        self.assertEqual(request.output(), "[hello, bar! hello, bar!]")


if __name__ == "__main__":
    unittest.main()
