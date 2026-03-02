import asyncio
import unittest
from typing import Any

import parameterized
import validate_all_applications

from tensorlake.applications import (
    Future,
    Request,
    RequestError,
    application,
    function,
)
from tensorlake.applications.applications import run_application
from tensorlake.applications.remote.deploy import deploy_applications

# Makes the test case discoverable by unittest framework.
ValidateAllApplicationsTest: unittest.TestCase = validate_all_applications.define_test()


# NB: asyncio.wait_for is not deterministic without deterministic event loop.
# We're not providing a similar durable API for it at the moment.
# See https://github.com/temporalio/sdk-python/issues/429.


@function()
async def sleep_and_return_arg(arg: Any, delay: float) -> Any:
    print(f"sleep_and_return_arg: {arg}, {delay}")
    await asyncio.sleep(delay)
    return arg


@function()
async def raise_request_error(message: str, delay: float) -> Any:
    await asyncio.sleep(delay)
    raise RequestError(message)


@application()
@function()
async def gather_all() -> str:
    coroutines: list[asyncio.Coroutine[Any, Any, str]] = [
        sleep_and_return_arg(arg="foo", delay=0),
        sleep_and_return_arg(arg="bar", delay=2),
        sleep_and_return_arg(arg="buzz", delay=2),
    ]

    # This call should take 2 seconds to complete.
    results: list[str | Exception] = await asyncio.gather(
        *coroutines, return_exceptions=True
    )
    assert len(results) == 3

    assert results[0] == "foo"
    assert results[1] == "bar"
    assert results[2] == "buzz"

    return "success"


@application()
@function()
async def gather_first_failure() -> str:
    coroutines: list[asyncio.Coroutine[Any, Any, str]] = [
        sleep_and_return_arg(arg="foo", delay=2),
        raise_request_error(message="bar", delay=0),
        sleep_and_return_arg(arg="buzz", delay=2),
    ]

    # This call should take 0 seconds to complete.
    # NB: It's not deterministic if multiple futures fail with return_exceptions=False.
    try:
        await asyncio.gather(*coroutines, return_exceptions=False)
    except RequestError as e:
        assert e.message == "bar"
    else:
        raise Exception("Expected RequestError, got no exception")

    return "success"


@function()
async def double(x: int) -> int:
    return x * 2


@application()
@function()
async def create_task() -> int:
    coroutine: asyncio.Coroutine[Any, Any, int] = double(x=5)
    task: asyncio.Task = asyncio.create_task(coroutine)
    return await task


@application()
@function()
async def ensure_future() -> int:
    coroutine: asyncio.Coroutine[Any, Any, int] = double(x=5)
    task: asyncio.Task = asyncio.ensure_future(coroutine)
    return await task


@application()
@function()
async def await_coroutine_twice() -> str:
    # Checks that Tensorlake coroutine behaves like a normal Python coroutine
    # and raises if awaited twice.
    coroutine: asyncio.Coroutine[Any, Any, str] = sleep_and_return_arg(
        arg="foo", delay=0
    )
    result1: str = await coroutine
    assert result1 == "foo"

    try:
        await coroutine
    except RuntimeError as e:
        assert str(e) == "cannot reuse already awaited coroutine"

    return "success"


@application()
@function()
async def await_coroutine_implicitly_started_by_sdk() -> str:
    # Checks that awaiting a coroutine that was implicitly started by the SDK fails.

    coroutine: asyncio.Coroutine[Any, Any, int] = sleep_and_return_arg(arg=2, delay=0)

    await double(coroutine)  # Implicitly runs the coroutine.

    try:
        await coroutine
    except RuntimeError as e:
        assert str(e) == "cannot reuse already awaited coroutine"

    return "success"


class TestAsyncio(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        deploy_applications(__file__)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_gather_all(self, _: str, is_remote: bool):
        request: Request = run_application(
            gather_all,
            is_remote,
        )
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_gather_first_failure(self, _: str, is_remote: bool):
        request: Request = run_application(
            gather_first_failure,
            is_remote,
        )
        # We're currently stopping whole request execution on a function run failure.
        # So the request error gets propagated to the request output instead of being
        # raised in the application function.
        # self.assertEqual(request.output(), "success")
        with self.assertRaises(RequestError) as cm:
            request.output()
        self.assertEqual(str(cm.exception), "bar")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_create_task(self, _: str, is_remote: bool):
        request: Request = run_application(create_task, is_remote)
        self.assertEqual(request.output(), 10)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_ensure_future(self, _: str, is_remote: bool):
        request: Request = run_application(ensure_future, is_remote)
        self.assertEqual(request.output(), 10)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_await_coroutine_twice(self, _: str, is_remote: bool):
        request: Request = run_application(await_coroutine_twice, is_remote)
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_await_coroutine_implicitly_started_by_sdk(self, _: str, is_remote: bool):
        request: Request = run_application(
            await_coroutine_implicitly_started_by_sdk, is_remote
        )
        self.assertEqual(request.output(), "success")


if __name__ == "__main__":
    unittest.main()
