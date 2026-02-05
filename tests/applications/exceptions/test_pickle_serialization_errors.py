import unittest

import parameterized
import validate_all_applications

from tensorlake.applications import (
    DeserializationError,
    Request,
    SerializationError,
    application,
    function,
)
from tensorlake.applications.applications import run_application
from tensorlake.applications.remote.deploy import deploy_applications

# Makes the test case discoverable by unittest framework.
ValidateAllApplicationsTest: unittest.TestCase = validate_all_applications.define_test()


class NotPicklableObject:
    def __reduce__(self):
        raise RuntimeError("This object is not picklable")


@application()
@function()
def application_call_function_passing_not_picklable_object(_: str) -> str:
    try:
        function_with_not_picklable_arg(NotPicklableObject())
    except SerializationError:
        return "success"


@function()
def function_with_not_picklable_arg(arg: NotPicklableObject) -> None:
    return None


class NotUnpicklableObject:
    def __init__(self):
        self.some_state = 42

    def __setstate__(self, state):
        raise RuntimeError("This object is not unpicklable")


@application()
@function()
def application_call_function_that_returns_unpicklable_object(_: str) -> str:
    try:
        return_unpicklable_object()
    except DeserializationError:
        return "success"


@function()
def return_unpicklable_object() -> NotUnpicklableObject:
    return NotUnpicklableObject()


class TestPickleSerializationErrors(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_call_function_passing_not_picklable_object(self, _, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            application_call_function_passing_not_picklable_object,
            is_remote,
            "whatever",
        )
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    @unittest.skip(
        "Non-local error propagation logic is not working correctly yet in both modes."
    )
    def test_call_function_that_returns_unpicklable_object(self, _, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            application_call_function_that_returns_unpicklable_object,
            is_remote,
            "whatever",
        )
        self.assertEqual(request.output(), "success")


if __name__ == "__main__":
    unittest.main()
