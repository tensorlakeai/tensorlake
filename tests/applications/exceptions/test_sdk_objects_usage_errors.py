import unittest
from typing import Any

import parameterized
import validate_all_applications

from tensorlake.applications import (
    Awaitable,
    Future,
    Request,
    RequestFailed,
    SDKUsageError,
    SerializationError,
    application,
    function,
)
from tensorlake.applications.applications import run_application
from tensorlake.applications.remote.deploy import deploy_applications

# Makes the test case discoverable by unittest framework.
ValidateAllApplicationsTest: unittest.TestCase = validate_all_applications.define_test()


@application()
@function()
def function_call_list_as_function_argument(_: str) -> str:
    awaitable: Awaitable = other_function.awaitable(1)
    try:
        return other_function([awaitable])
    except SerializationError as e:
        assert str(e) == (
            "Failed to serialize data with pickle serializer: Attempt to pickle Tensorlake Function Call other_function(\n"
            "  1,\n"
            "). It cannot be stored inside an object which is a function argument or returned from a function."
        )
        return "success"


@function()
def other_function(arg) -> None:
    return None


# Check that returning a list of Awaitables doesn't work because
# runtime attempts to json serialize them and Awaitables are not json serializable.
@application()
@function()
def application_return_list_of_function_calls() -> str:
    awaitable1: Awaitable = other_function.awaitable(1)
    awaitable2: Awaitable = other_function.awaitable(2)
    return [awaitable1, awaitable2]


@application()
@function()
def application_call_return_list_of_function_calls() -> str:
    return return_list_of_function_calls()


# Check that returning a list of Awaitables doesn't work because
# runtime attempts to pickle them and Awaitables are not picklable.
@function()
def return_list_of_function_calls() -> list[Awaitable]:
    awaitable1: Awaitable = other_function.awaitable(1)
    awaitable2: Awaitable = other_function.awaitable(2)
    return [awaitable1, awaitable2]


@application()
@function()
def future_list_as_function_argument(_: str) -> str:
    future: Future = other_function.awaitable(1).run()
    try:
        return other_function([future])
    except SerializationError as e:
        return "success"


# Check that returning a list of Futures doesn't work because
# runtime attempts to json serialize them and Futures are not json serializable.
@application()
@function()
def application_return_list_of_futures() -> str:
    future1: Future = other_function.awaitable(1).run()
    future2: Future = other_function.awaitable(2).run()
    return [future1, future2]


@application()
@function()
def application_call_return_list_of_futures() -> str:
    return return_list_of_futures()


# Check that returning a list of Futures doesn't work because
# runtime attempts to pickle them and Futures are not picklable.
@function()
def return_list_of_futures() -> str:
    future1: Future = other_function.awaitable(1).run()
    future2: Future = other_function.awaitable(2).run()
    return [future1, future2]


@application()
@function()
def function_as_function_argument(_: str) -> str:
    try:
        return other_function(other_function)
    except SerializationError as e:
        assert str(e) == (
            "Failed to serialize data with pickle serializer: Attempt to pickle Tensorlake Function 'other_function'. "
            "It cannot be passed as a function parameter or returned from a Tensorlake Function."
        )
        return "success"


# Check that returning a Function doesn't work because
# runtime attempts to json serialize them and Functions are not json serializable.
@application()
@function()
def application_return_function() -> Any:
    return other_function


@application()
@function()
def application_call_return_function() -> Any:
    return return_function()


# Check that returning a Function doesn't work because
# runtime attempts to pickle it and Functions are not picklable.
@function()
def return_function() -> Any:
    return other_function


@application()
@function()
def future_wait_wrong_return_when(_: str) -> str:
    future: Future = other_function.awaitable(1).run()
    try:
        Future.wait([future, future], return_when="wrong_value")
    except SDKUsageError as e:
        assert str(e) == ("Not supported return_when value: 'wrong_value'")
        return "success"


@application()
@function()
def return_map_awaitable(_: str) -> str:
    return other_function.awaitable.map([1, 2, 3])


@application()
@function()
def pass_map_awaitable_as_reduced_item(_: str) -> str:
    map_awaitable: Awaitable = other_function.awaitable.map([1, 2, 3])
    try:
        other_function_reduce.reduce([map_awaitable])
    except SDKUsageError as e:
        assert str(e) == (
            "A Tensorlake Map Operation cannot be used as an input item for Tensorlake Reduce Operation of 'other_function_reduce' over [\n"
            "  Tensorlake Map Operation [\n"
            "    Tensorlake Function Call other_function(\n"
            "      1,\n"
            "    ),\n"
            "    Tensorlake Function Call other_function(\n"
            "      2,\n"
            "    ),\n"
            "    Tensorlake Function Call other_function(\n"
            "      3,\n"
            "    ),\n"
            "  ],\n"
            "]. You can work this around by creating function call awaitables using `other_function.awaitable(...)` and then passing "
            "them into `other_function_reduce.reduce(...)`."
        )
        return "success"
    else:
        return "failure"


@function()
def other_function_reduce(arg1, arg2):
    return arg1


@application()
@function()
def return_future(_: str) -> Any:
    future: Future = other_function.awaitable(1).run()
    return future


@application()
@function()
def return_running_awaitable(_: str) -> Any:
    awaitable: Awaitable = other_function.awaitable(1)
    future: Future = awaitable.run()
    return awaitable


class TestSDKObjectsUsageErrors(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_function_call_list_as_function_argument(self, _, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            function_call_list_as_function_argument,
            is_remote,
            "whatever",
        )
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_return_list_of_function_calls_from_application(self, _, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            application_return_list_of_function_calls,
            is_remote,
        )
        with self.assertRaises(RequestFailed) as context:
            request.output()

        self.assertEqual(
            str(context.exception),
            "function_error",
        )

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_return_list_of_function_calls_from_regular_function(
        self, _, is_remote: bool
    ):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            application_call_return_list_of_function_calls,
            is_remote,
        )
        with self.assertRaises(RequestFailed) as context:
            request.output()

        self.assertEqual(
            str(context.exception),
            "function_error",
        )

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_future_list_as_function_argument(self, _, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            future_list_as_function_argument,
            is_remote,
            "whatever",
        )
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_return_list_of_futures_from_application(self, _, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            application_return_list_of_futures,
            is_remote,
        )
        with self.assertRaises(RequestFailed) as context:
            request.output()

        self.assertEqual(
            str(context.exception),
            "function_error",
        )

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_return_list_of_futures_from_regular_function(self, _, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            application_call_return_list_of_futures,
            is_remote,
        )
        with self.assertRaises(RequestFailed) as context:
            request.output()

        self.assertEqual(
            str(context.exception),
            "function_error",
        )

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_function_as_function_argument(self, _, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            function_as_function_argument,
            is_remote,
            "whatever",
        )
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_return_function_from_application(self, _, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            application_return_function,
            is_remote,
            "whatever",
        )
        with self.assertRaises(RequestFailed) as context:
            request.output()

        self.assertEqual(
            str(context.exception),
            "function_error",
        )

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_return_function_from_regular_function(self, _, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            application_call_return_function,
            is_remote,
            "whatever",
        )
        with self.assertRaises(RequestFailed) as context:
            request.output()

        self.assertEqual(
            str(context.exception),
            "function_error",
        )

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_future_wait_wrong_return_when(self, _, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            future_wait_wrong_return_when,
            is_remote,
            "whatever",
        )
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_return_map_awaitable(self, _, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            return_map_awaitable,
            is_remote,
            "whatever",
        )
        with self.assertRaises(RequestFailed) as context:
            request.output()

        self.assertEqual(
            str(context.exception),
            "function_error",
        )

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_pass_map_awaitable_as_reduced_item(self, _, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            pass_map_awaitable_as_reduced_item,
            is_remote,
            "whatever",
        )
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_return_future(self, _, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            return_future,
            is_remote,
            "whatever",
        )
        with self.assertRaises(RequestFailed) as context:
            request.output()

        self.assertEqual(
            str(context.exception),
            "function_error",
        )

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_return_running_awaitable(self, _, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            return_running_awaitable,
            is_remote,
            "whatever",
        )
        with self.assertRaises(RequestFailed) as context:
            request.output()

        self.assertEqual(
            str(context.exception),
            "function_error",
        )


if __name__ == "__main__":
    unittest.main()
