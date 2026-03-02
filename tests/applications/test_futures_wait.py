import time
import unittest
from typing import Any

import parameterized
import validate_all_applications

from tensorlake.applications import (
    RETURN_WHEN,
    Future,
    Request,
    RequestError,
    TimeoutError,
    application,
    function,
)
from tensorlake.applications.applications import run_application
from tensorlake.applications.remote.deploy import deploy_applications

# Makes the test case discoverable by unittest framework.
ValidateAllApplicationsTest: unittest.TestCase = validate_all_applications.define_test()


@application()
@function()
def app_return_when_all_completed(_: Any) -> str:
    futures: list[Future] = [
        sleep_and_return_arg.future(arg="foo", delay=0),
        sleep_and_return_arg.future(arg="bar", delay=2),
        sleep_and_return_arg.future(arg="buzz", delay=2),
    ]
    for f in futures:
        f.run()

    # This call should take 2 seconds to complete.
    done, not_done = Future.wait(futures, return_when=RETURN_WHEN.ALL_COMPLETED)
    assert len(done) == 3
    assert len(not_done) == 0
    assert all(future.done() for future in futures)

    assert futures[0].result() == "foo"
    assert futures[1].result() == "bar"
    assert futures[2].result() == "buzz"

    assert futures[0] == done[0]
    assert futures[1] == done[1]
    assert futures[2] == done[2]

    return "success"


@function()
def sleep_and_return_arg(arg: Any, delay: float) -> Any:
    print(f"sleep_and_return_arg: {arg}, {delay}")
    time.sleep(delay)
    return arg


@application()
@function()
def app_return_when_first_completed(_: Any) -> str:
    # FIXME: In remote mode FIRST_COMPLETED waits on futures serially,
    # so this test will fail if we put "bar" second in the futures list.
    futures: list[Future] = [
        sleep_and_return_arg.future(arg="bar", delay=0),
        sleep_and_return_arg.future(arg="foo", delay=2),
        sleep_and_return_arg.future(arg="buzz", delay=2),
    ]
    for f in futures:
        f.run()

    # This call should take 0 seconds to complete.
    done, not_done = Future.wait(futures, return_when=RETURN_WHEN.FIRST_COMPLETED)

    assert len(done) == 1
    assert len(not_done) == 2

    assert done[0].done()
    assert not not_done[0].done()
    assert not not_done[1].done()

    assert done[0].result() == "bar"

    assert futures[0] == done[0]
    assert futures[1] == not_done[0]
    assert futures[2] == not_done[1]

    return "success"


@application()
@function()
def app_wait_timeout(_: Any) -> str:
    future: Future = sleep_and_return_arg.future(arg="foo", delay=5).run()
    try:
        future.result(timeout=1.0)
    except TimeoutError:
        pass
    else:
        raise Exception("Expected FunctionTimeoutError")

    return "success"


@application()
@function()
def app_return_when_first_failure(_: Any) -> str:
    futures: list[Future] = [
        sleep_and_return_arg.future(arg="foo", delay=2),
        raise_request_error.future(message="bar", delay=0),
        sleep_and_return_arg.future(arg="buzz", delay=2),
    ]
    for f in futures:
        f.run()

    # This call should take 0 seconds to complete.
    done, not_done = Future.wait(futures, return_when=RETURN_WHEN.FIRST_FAILURE)

    assert len(done) == 1
    assert len(not_done) == 2

    assert done[0].done()
    assert not not_done[0].done()
    assert not not_done[1].done()

    try:
        done[0].result()
    except RequestError as e:
        assert e.message == "bar"

    assert futures[0] == not_done[0]
    assert futures[1] == done[0]
    assert futures[2] == not_done[1]

    return "success"


@function()
def raise_request_error(message: str, delay: float) -> Any:
    time.sleep(delay)
    raise RequestError(message)


@function()
@application()
def app_future_result_caching() -> str:
    # This call should take 2 seconds to complete.
    start_time: float = time.monotonic()
    future: Future = sleep_and_return_arg.future("foo", delay=2)
    result1: str = future.result()
    assert result1 == "foo"
    elapsed_time: float = time.monotonic() - start_time
    assert elapsed_time >= 2.0

    # This call should return immediately.
    start_time = time.monotonic()
    result2: str = future.result()
    assert result2 == "foo"
    elapsed_time = time.monotonic() - start_time
    assert elapsed_time < 1.0

    # This call should return immediately.
    start_time = time.monotonic()
    result3: str = future.result()
    assert result3 == "foo"
    elapsed_time = time.monotonic() - start_time
    assert elapsed_time < 1.0

    return "success"


@application()
@function()
def app_wait_runs_not_running_futures() -> str:
    futures: list[Future] = [
        sleep_and_return_arg.future(arg="foo", delay=0),
        sleep_and_return_arg.future(arg="bar", delay=0),
        sleep_and_return_arg.future(arg="buzz", delay=0),
    ]

    done, not_done = Future.wait(futures, return_when=RETURN_WHEN.ALL_COMPLETED)
    assert len(done) == 3
    assert len(not_done) == 0
    assert all(future.done() for future in futures)

    assert futures[0].result() == "foo"
    assert futures[1].result() == "bar"
    assert futures[2].result() == "buzz"

    return "success"


class TestFuturesWait(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        deploy_applications(__file__)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_wait_all_completed(self, _: str, is_remote: bool):
        request: Request = run_application(
            app_return_when_all_completed, is_remote, "foo"
        )
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_wait_first_completed(self, _: str, is_remote: bool):
        request: Request = run_application(
            app_return_when_first_completed, is_remote, "foo"
        )
        self.assertEqual(request.output(), "success")

    # Timeouts are not implemented in local mode.
    @parameterized.parameterized.expand([("remote", True)])
    def test_wait_timeout(self, _: str, is_remote: bool):
        request: Request = run_application(app_wait_timeout, is_remote, "foo")
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_wait_first_failure(self, _: str, is_remote: bool):
        request: Request = run_application(
            app_return_when_first_failure, is_remote, "foo"
        )
        # We're currently stopping whole request execution on a function run failure.
        # So the request error gets propagated to the request output instead of being
        # raised in the application function.
        # self.assertEqual(request.output(), "success")
        with self.assertRaises(RequestError) as cm:
            request.output()
        self.assertEqual(str(cm.exception), "bar")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_future_result_caching(self, _: str, is_remote: bool):
        request: Request = run_application(app_future_result_caching, is_remote, "foo")
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_wait_runs_not_running_futures(self, _: str, is_remote: bool):
        request: Request = run_application(
            app_wait_runs_not_running_futures,
            is_remote,
        )
        self.assertEqual(request.output(), "success")


if __name__ == "__main__":
    unittest.main()
