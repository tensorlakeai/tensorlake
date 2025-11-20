import unittest

import parameterized

from tensorlake.applications import (
    DeserializationError,
    Request,
    SerializationError,
    application,
    function,
)
from tensorlake.applications.applications import run_application
from tensorlake.applications.remote.deploy import deploy_applications


class NotSerializableObject:
    def __reduce__(self):
        raise RuntimeError("This object is not picklable")


@application()
@function()
def application_call_function_with_not_serializable_object(_: str) -> str:
    try:
        other_function(NotSerializableObject())
    except SerializationError:
        return "success"


@function()
def other_function(arg: NotSerializableObject) -> None:
    return None


class NotDeserializableObject:
    def __init__(self):
        self.some_state = 42

    def __setstate__(self, state):
        raise RuntimeError("This object is not unpicklable")


@application()
@function()
def application_call_function_that_returns_not_deserializable_object(_: str) -> str:
    try:
        return_not_deserializable_object()
    except DeserializationError:
        return "success"


@function()
def return_not_deserializable_object() -> NotDeserializableObject:
    return NotDeserializableObject()


class TestSerializationErrors(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_call_function_with_not_serializable_object(self, _, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            application_call_function_with_not_serializable_object,
            "whatever",
            remote=is_remote,
        )
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_call_function_with_not_serializable_object(self, _, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            application_call_function_with_not_serializable_object,
            "whatever",
            remote=is_remote,
        )
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    @unittest.skip(
        "Non-local error propagation logic is not working correctly yet in both modes."
    )
    def test_call_function_that_returns_not_deserializable_object(
        self, _, is_remote: bool
    ):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            application_call_function_that_returns_not_deserializable_object,
            "whatever",
            remote=is_remote,
        )
        self.assertEqual(request.output(), "success")


if __name__ == "__main__":
    unittest.main()
