import time
import unittest

import parameterized
import validate_all_applications

from tensorlake.applications import (
    FunctionError,
    Future,
    Request,
    RequestError,
    RequestFailed,
    application,
    function,
)
from tensorlake.applications.applications import run_application
from tensorlake.applications.remote.deploy import deploy_applications

# Makes the test case discoverable by unittest framework.
ValidateAllApplicationsTest: unittest.TestCase = validate_all_applications.define_test()


@function()
def fail(message: str) -> str:
    raise RuntimeError(message)


@function()
def fail_request(message: str) -> str:
    raise RequestError(message)


@function()
def append_suffix(value: str) -> str:
    return value + "-suffix"


@application()
@function()
def app_catches_blocking_call_error(_: str) -> str:
    try:
        fail("child failed")
    except FunctionError:
        return "caught"
    return "not caught"


@application()
@function()
def app_catches_future_error(_: str) -> str:
    future: Future = fail.future("child failed").run()
    try:
        future.result()
    except FunctionError:
        assert future.exception is not None
        return "caught"
    return "not caught"


@application()
@function()
def app_catches_request_error(_: str) -> str:
    try:
        fail_request("child failed the request")
    except RequestError as e:
        assert e.message == "child failed the request"
        return "caught"
    return "not caught"


@application()
@function()
def app_catches_failed_input_error(_: str) -> str:
    # append_suffix can't run because its input function call failed.
    future: Future = append_suffix.future(fail.future("child failed")).run()
    try:
        future.result()
    except FunctionError:
        return "caught"
    return "not caught"


@function()
def fail_on_two(x: int) -> int:
    if x == 2:
        raise RuntimeError("two")
    return x


@function()
def add(a: int, b: int) -> int:
    return a + b


@application()
@function()
def app_catches_map_error(_: str) -> str:
    try:
        fail_on_two.future.map([1, 2, 3]).result()
    except FunctionError:
        return "caught"
    return "not caught"


@application()
@function()
def app_catches_reduce_error(_: str) -> str:
    try:
        add.future.reduce(fail_on_two.future.map([1, 2, 3]), 0).result()
    except FunctionError:
        return "caught"
    return "not caught"


@application()
@function()
def app_continues_after_caught_error(_: str) -> str:
    try:
        fail("child failed")
    except FunctionError:
        pass
    return append_suffix("value")


@application()
@function()
def app_doesnt_catch_error(_: str) -> str:
    return fail("child failed")


@application()
@function()
def app_doesnt_catch_request_error(_: str) -> str:
    return fail_request("child failed the request")


@application()
@function()
def app_tail_calls_failing_function(_: str) -> str:
    return fail.future("child failed")


@function()
def sleep_and_fail(message: str, delay: float) -> str:
    time.sleep(delay)
    raise RuntimeError(message)


@function()
def sleep_and_fail_request(message: str, delay: float) -> str:
    time.sleep(delay)
    raise RequestError(message)


@function()
def sleep_and_catch_error(delay: float) -> str:
    time.sleep(delay)
    try:
        fail("child failed")
    except FunctionError:
        return "caught"
    return "not caught"


@function()
def start_failing_call_and_return() -> str:
    sleep_and_fail.future("child failed", delay=1).run()
    return "returned"


@application()
@function()
def app_returns_before_call_fails(_: str) -> str:
    sleep_and_fail.future("child failed", delay=1).run()
    return "returned"


@application()
@function()
def app_returns_before_call_fails_request(_: str) -> str:
    sleep_and_fail_request.future("child failed the request", delay=1).run()
    return "returned"


@application()
@function()
def app_returns_before_call_catches_error(_: str) -> str:
    sleep_and_catch_error.future(delay=1).run()
    return "returned"


@application()
@function()
def app_calls_function_that_returns_before_call_fails(_: str) -> str:
    start_failing_call_and_return()
    time.sleep(2)
    return "returned"


class TestFunctionErrorPropagation(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        deploy_applications(__file__)

    @parameterized.parameterized.expand(
        [
            (f"{mode}_{name}", is_remote, app)
            for mode, is_remote in [("remote", True), ("local", False)]
            for name, app in [
                ("blocking_call", app_catches_blocking_call_error),
                ("future", app_catches_future_error),
                ("request_error", app_catches_request_error),
                ("failed_input", app_catches_failed_input_error),
                ("map", app_catches_map_error),
                ("reduce", app_catches_reduce_error),
            ]
        ]
    )
    def test_caller_catches_error(self, _: str, is_remote: bool, app):
        request: Request = run_application(app, is_remote, "foo")
        self.assertEqual(request.output(), "caught")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_caller_continues_after_caught_error(self, _: str, is_remote: bool):
        request: Request = run_application(
            app_continues_after_caught_error, is_remote, "foo"
        )
        self.assertEqual(request.output(), "value-suffix")

    @parameterized.parameterized.expand(
        [
            ("remote_blocking_call", True, app_doesnt_catch_error),
            ("local_blocking_call", False, app_doesnt_catch_error),
            ("remote_tail_call", True, app_tail_calls_failing_function),
            ("local_tail_call", False, app_tail_calls_failing_function),
        ]
    )
    def test_uncaught_error_fails_request(self, _: str, is_remote: bool, app):
        request: Request = run_application(app, is_remote, "foo")
        with self.assertRaises(RequestFailed) as cm:
            request.output()
        self.assertEqual(str(cm.exception), "function_error")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_uncaught_request_error_fails_request(self, _: str, is_remote: bool):
        request: Request = run_application(
            app_doesnt_catch_request_error, is_remote, "foo"
        )
        with self.assertRaises(RequestError) as cm:
            request.output()
        self.assertEqual(str(cm.exception), "child failed the request")

    @parameterized.parameterized.expand(
        [
            (f"{mode}_{name}", is_remote, app)
            for mode, is_remote in [("remote", True), ("local", False)]
            for name, app in [
                ("app_returned", app_returns_before_call_fails),
                (
                    "function_returned",
                    app_calls_function_that_returns_before_call_fails,
                ),
            ]
        ]
    )
    def test_error_after_caller_returned_fails_request(
        self, _: str, is_remote: bool, app
    ):
        request: Request = run_application(app, is_remote, "foo")
        with self.assertRaises(RequestFailed) as cm:
            request.output()
        self.assertEqual(str(cm.exception), "function_error")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_request_error_after_app_returned_fails_request(
        self, _: str, is_remote: bool
    ):
        request: Request = run_application(
            app_returns_before_call_fails_request, is_remote, "foo"
        )
        with self.assertRaises(RequestError) as cm:
            request.output()
        self.assertEqual(str(cm.exception), "child failed the request")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_caught_error_after_app_returned_doesnt_fail_request(
        self, _: str, is_remote: bool
    ):
        request: Request = run_application(
            app_returns_before_call_catches_error, is_remote, "foo"
        )
        self.assertEqual(request.output(), "returned")


if __name__ == "__main__":
    unittest.main()
