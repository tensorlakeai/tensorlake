import asyncio
import unittest

import parameterized
import validate_all_applications

from tensorlake.applications import (
    Future,
    Request,
    application,
    function,
)
from tensorlake.applications.applications import run_application
from tensorlake.applications.remote.deploy import deploy_applications

ValidateAllApplicationsTest: unittest.TestCase = validate_all_applications.define_test()


@function()
def sync_double(x: int) -> int:
    return x * 2


@function()
def sync_add(a: int, b: int) -> int:
    return a + b


@function()
def sync_negate(x: int) -> int:
    return -x


@function()
async def async_double(x: int) -> int:
    return x * 2


@function()
async def async_add(a: int, b: int) -> int:
    return a + b


@function()
async def async_negate(x: int) -> int:
    return -x


@application()
@function()
def sync_three_futures_consume_same_future(x: int) -> int:
    doubled: Future = sync_double.future(x)
    a: int = sync_add(doubled, 1)
    b: int = sync_add(doubled, 2)
    c: int = sync_add(doubled, 3)
    return a + b + c


@application()
@function()
def sync_three_futures_consume_same_future_tail_call(x: int) -> int:
    doubled: Future = sync_double.future(x)
    a: Future = sync_add.future(doubled, 1)
    b: Future = sync_add.future(doubled, 2)
    c: Future = sync_add.future(doubled, 3)
    return sync_add.future(sync_add.future(a, b), c)


@application()
@function()
async def async_sync_three_futures_consume_same_future(x: int) -> int:
    doubled: asyncio.Coroutine = async_double(x)
    a: int = await async_add(doubled, 1)
    b: int = await async_add(doubled, 2)
    c: int = await async_add(doubled, 3)
    return a + b + c


@application()
@function()
async def async_three_futures_consume_same_future_tail_call(x: int) -> int:
    doubled: asyncio.Coroutine = async_double(x)
    a: asyncio.Coroutine = async_add(doubled, 1)
    b: asyncio.Coroutine = async_add(doubled, 2)
    c: asyncio.Coroutine = async_add(doubled, 3)
    return async_add.future(async_add(a, b), c)


# Tail call returns a coroutine (calling async function directly).
# This is distinct from async_three_futures_consume_same_future_tail_call which
# returns a Future via .future(). Here the coroutine wraps the Future and needs
# to be unwrapped by the runner.
@application()
@function()
async def async_three_futures_consume_same_future_coroutine_tail_call(x: int) -> int:
    doubled = async_double(x)
    a = async_add(doubled, 1)
    b = async_add(doubled, 2)
    c = async_add(doubled, 3)
    return async_add(async_add(a, b), c)


# Tail call returns a coroutine from sync .future().coroutine().
# Async app consumes sync function futures converted to coroutines.
@application()
@function()
async def async_sync_futures_as_coroutines_tail_call(x: int) -> int:
    doubled = sync_double.future(x).coroutine()
    a = sync_add.future(doubled, 1).coroutine()
    b = sync_add.future(doubled, 2).coroutine()
    c = sync_add.future(doubled, 3).coroutine()
    return sync_add.future(sync_add.future(a, b), c).coroutine()


# Tail call returns a coroutine from async .future().coroutine().
@application()
@function()
async def async_futures_as_coroutines_tail_call(x: int) -> int:
    doubled = async_double.future(x).coroutine()
    a = async_add.future(doubled, 1).coroutine()
    b = async_add.future(doubled, 2).coroutine()
    c = async_add.future(doubled, 3).coroutine()
    return async_add.future(async_add.future(a, b), c).coroutine()


class TestFuturesConsumeFutures(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        deploy_applications(__file__)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_sync(self, _: str, is_remote: bool):
        request: Request = run_application(
            sync_three_futures_consume_same_future,
            is_remote,
            x=5,
        )
        self.assertEqual(request.output(), 36)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_sync_tail_call(self, _: str, is_remote: bool):
        request: Request = run_application(
            sync_three_futures_consume_same_future_tail_call,
            is_remote,
            x=5,
        )
        self.assertEqual(request.output(), 36)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_async(self, _: str, is_remote: bool):
        request: Request = run_application(
            async_sync_three_futures_consume_same_future,
            is_remote,
            x=5,
        )
        self.assertEqual(request.output(), 36)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_async_tail_call(self, _: str, is_remote: bool):
        request: Request = run_application(
            async_three_futures_consume_same_future_tail_call,
            is_remote,
            x=5,
        )
        self.assertEqual(request.output(), 36)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_async_coroutine_tail_call(self, _: str, is_remote: bool):
        request: Request = run_application(
            async_three_futures_consume_same_future_coroutine_tail_call,
            is_remote,
            x=5,
        )
        self.assertEqual(request.output(), 36)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_async_sync_futures_as_coroutines_tail_call(self, _: str, is_remote: bool):
        request: Request = run_application(
            async_sync_futures_as_coroutines_tail_call,
            is_remote,
            x=5,
        )
        self.assertEqual(request.output(), 36)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_async_futures_as_coroutines_tail_call(self, _: str, is_remote: bool):
        request: Request = run_application(
            async_futures_as_coroutines_tail_call,
            is_remote,
            x=5,
        )
        self.assertEqual(request.output(), 36)


if __name__ == "__main__":
    unittest.main()
