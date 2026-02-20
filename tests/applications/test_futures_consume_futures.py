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
    return sync_add.tail_call(sync_add(a, b), c)


@application()
@function()
async def async_sync_three_futures_consume_same_future(x: int) -> int:
    doubled: Future = async_double(x)
    a: int = await async_add(doubled, 1)
    b: int = await async_add(doubled, 2)
    c: int = await async_add(doubled, 3)
    return a + b + c


@application()
@function()
async def async_three_futures_consume_same_future_tail_call(x: int) -> int:
    doubled: Future = async_double(x)
    a: Future = async_add(doubled, 1)
    b: Future = async_add(doubled, 2)
    c: Future = async_add(doubled, 3)
    return async_add.tail_call(async_add(a, b), c)


class TestFuturesConsumeFutures(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_sync(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(
            sync_three_futures_consume_same_future,
            is_remote,
            x=5,
        )
        self.assertEqual(request.output(), 36)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_sync_tail_call(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(
            sync_three_futures_consume_same_future_tail_call,
            is_remote,
            x=5,
        )
        self.assertEqual(request.output(), 36)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_async(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(
            async_sync_three_futures_consume_same_future,
            is_remote,
            x=5,
        )
        self.assertEqual(request.output(), 36)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_async_tail_call(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(
            async_three_futures_consume_same_future_tail_call,
            is_remote,
            x=5,
        )
        self.assertEqual(request.output(), 36)


if __name__ == "__main__":
    unittest.main()
