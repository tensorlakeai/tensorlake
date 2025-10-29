import time
import unittest
from typing import Any

import parameterized

from tensorlake.applications import (
    RETURN_WHEN,
    FunctionCallFailure,
    Future,
    Request,
    RequestError,
    application,
    function,
)
from tensorlake.applications.applications import run_application
from tensorlake.applications.remote.deploy import deploy_applications


@application()
@function()
def api_function_return_when_all_completed(_: Any) -> str:
    futures: list[Future] = [
        sleep_and_return_arg.awaitable(arg="foo", delay=0).run(),
        sleep_and_return_arg.awaitable(arg="bar", delay=2).run(),
        sleep_and_return_arg.awaitable(arg="buzz", delay=2).run(),
    ]

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
def api_function_return_when_first_completed(_: Any) -> str:
    # FIXME: In remote mode FIRST_COMPLETED waits on futures serially,
    # so this test will fail if we put "bar" second in the futures list.
    futures: list[Future] = [
        sleep_and_return_arg.awaitable(arg="bar", delay=0).run(),
        sleep_and_return_arg.awaitable(arg="foo", delay=2).run(),
        sleep_and_return_arg.awaitable(arg="buzz", delay=2).run(),
    ]

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
def api_function_return_when_first_failure(_: Any) -> str:
    futures: list[Future] = [
        sleep_and_return_arg.awaitable(arg="foo", delay=2).run(),
        raise_request_error.awaitable(message="bar", delay=0).run(),
        sleep_and_return_arg.awaitable(arg="buzz", delay=2).run(),
    ]

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


class TestFuturesWait(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_wait_all_completed(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(
            api_function_return_when_all_completed, "foo", remote=is_remote
        )
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_wait_first_completed(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(
            api_function_return_when_first_completed, "foo", remote=is_remote
        )
        self.assertEqual(request.output(), "success")

    # FIXME: Enable this test for local mode. It's currently disabled because in
    # local mode we're stopping whole request execution on a function run failure."
    # FIXME: Enable this test for remote mode. It's currently disabled for the same reason.
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    @unittest.skip(
        "We're currently stopping whole request execution on a function run failure."
    )
    def test_wait_first_failure(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(
            api_function_return_when_first_failure, "foo", remote=is_remote
        )
        self.assertEqual(request.output(), "success")


if __name__ == "__main__":
    unittest.main()
