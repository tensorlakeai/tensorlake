import multiprocessing as mp
import queue as mt_queue
import threading
import unittest

import parameterized
import validate_all_applications

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

# Makes the test case discoverable by unittest framework.
ValidateAllApplicationsTest: unittest.TestCase = validate_all_applications.define_test()


def get_context_worker(q) -> None:
    try:
        q.put(RequestContext.get())
    except Exception as e:
        q.put(e)


def run_future_worker(q) -> None:
    try:
        application_mt_get_context.future("whatever")
        q.put(None)
    except Exception as e:
        q.put(e)


def wait_future_worker(q, future: Future) -> None:
    try:
        future.result()
        q.put(None)
    except Exception as e:
        q.put(e)


@application()
@function()
def application_mt_get_context(_: str) -> str:
    q: mt_queue.SimpleQueue = mt_queue.SimpleQueue()
    thread = threading.Thread(target=get_context_worker, args=(q,))
    thread.start()
    thread.join()

    exception: Exception | None = q.get()
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
def application_mt_run_future(_: str) -> str:
    q: mt_queue.SimpleQueue = mt_queue.SimpleQueue()
    thread = threading.Thread(target=run_future_worker, args=(q,))
    thread.start()
    thread.join()

    exception: Exception | None = q.get()
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
def application_mt_wait_future(_: str) -> str:
    future: Future = application_mt_get_context.future("whatever")
    q: mt_queue.SimpleQueue = mt_queue.SimpleQueue()

    thread = threading.Thread(target=wait_future_worker, args=(q, future))
    thread.start()
    thread.join()

    exception: Exception | None = q.get()
    if isinstance(exception, SDKUsageError):
        assert str(exception) == (
            "Tensorlake SDK was called outside of a Tensorlake Function thread or process."
            "Please only call Tensorlake SDK from Tensorlake Functions."
        )
        return "success"
    else:
        raise Exception("Expected SDKUsageError exception, got: " + str(exception))


class TestCallSDKFromChildThread(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_error_on_get_context(self, _, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            application_mt_get_context,
            is_remote,
            "whatever",
        )
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_error_on_run_future(self, _, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            application_mt_run_future,
            is_remote,
            "whatever",
        )
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_error_on_wait_future(self, _, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            application_mt_wait_future,
            is_remote,
            "whatever",
        )
        self.assertEqual(request.output(), "success")


@application()
@function()
def application_mp_get_context(_: str) -> str:
    q: mp.Queue = mp.Queue()
    process = mp.Process(target=get_context_worker, args=(q,))
    process.start()
    process.join()

    exception: Exception | None = q.get()
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
def application_mp_run_future(_: str) -> str:
    q: mp.Queue = mp.Queue()
    process = mp.Process(target=run_future_worker, args=(q,))
    process.start()
    process.join()

    exception: Exception | None = q.get()
    if isinstance(exception, SDKUsageError):
        assert str(exception) == (
            "Tensorlake SDK is not initialized. If you are using multiprocessing, please note that "
            "only a RequestContext created in the main process can be used in child processes. "
            "Other SDK features are not available in child processes at the moment."
        )
        return "success"
    else:
        raise Exception("Expected SDKUsageError exception, got: " + str(exception))


class TestCallSDKFromChildProcess(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_error_on_get_context(self, _, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            application_mp_get_context,
            is_remote,
            "whatever",
        )
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_error_on_run_future(self, _, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            application_mp_run_future,
            is_remote,
            "whatever",
        )
        self.assertEqual(request.output(), "success")


if __name__ == "__main__":
    unittest.main()
