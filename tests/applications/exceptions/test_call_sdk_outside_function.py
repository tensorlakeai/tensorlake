import threading
import unittest

import parameterized

from tensorlake.applications import (
    Future,
    Request,
    RequestContext,
    SDKUsageError,
    application,
    function,
)
from tensorlake.applications.applications import run_application
from tensorlake.applications.remote.deploy import deploy_applications


@application()
@function()
def application_get_context_in_child_thread(_: str) -> str:
    exception: Exception | None = None

    def thread_function():
        nonlocal exception
        try:
            RequestContext.get()
        except Exception as e:
            exception = e

    thread = threading.Thread(target=thread_function)
    thread.start()
    thread.join()

    if isinstance(exception, SDKUsageError):
        assert str(exception) == (
            "Tensorlake SDK was called outside of a Tensorlake Function thread or process."
            "Please only call Tensorlake SDK from Tensorlake Functions."
        )
        return "success"
    else:
        raise Exception("Expected SDKUsageError exception, got: " + str(exception))


@application()
@function()
def application_run_future_from_spawned_thread(_: str) -> str:
    exception: Exception | None = None

    def thread_function():
        nonlocal exception
        try:
            application_get_context_in_child_thread.awaitable("whatever").run()
        except Exception as e:
            exception = e

    thread = threading.Thread(target=thread_function)
    thread.start()
    thread.join()

    if isinstance(exception, SDKUsageError):
        assert str(exception) == (
            "Tensorlake SDK was called outside of a Tensorlake Function thread or process."
            "Please only call Tensorlake SDK from Tensorlake Functions."
        )
        return "success"
    else:
        raise Exception("Expected SDKUsageError exception, got: " + str(exception))


@application()
@function()
def application_wait_future_from_spawned_thread(_: str) -> str:
    future: Future = application_get_context_in_child_thread.awaitable("whatever").run()
    exception: Exception | None = None

    def thread_function():
        nonlocal exception
        nonlocal future
        try:
            future.result()
        except Exception as e:
            exception = e

    thread = threading.Thread(target=thread_function)
    thread.start()
    thread.join()

    if isinstance(exception, SDKUsageError):
        assert str(exception) == (
            "Tensorlake SDK was called outside of a Tensorlake Function thread or process."
            "Please only call Tensorlake SDK from Tensorlake Functions."
        )
        return "success"
    else:
        raise Exception("Expected SDKUsageError exception, got: " + str(exception))


class TestCallSDKOutsideFunction(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_error_on_get_context_from_spawned_thread(self, _, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            application_get_context_in_child_thread,
            "whatever",
            remote=is_remote,
        )
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_error_on_run_future_from_spawned_thread(self, _, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            application_run_future_from_spawned_thread,
            "whatever",
            remote=is_remote,
        )
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_error_on_wait_future_from_spawned_thread(self, _, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            application_wait_future_from_spawned_thread,
            "whatever",
            remote=is_remote,
        )
        self.assertEqual(request.output(), "success")


if __name__ == "__main__":
    unittest.main()
