import unittest

import parameterized
import validate_all_applications

from tensorlake.applications import (
    Request,
    application,
    function,
)
from tensorlake.applications.applications import run_application
from tensorlake.applications.remote.deploy import deploy_applications

ValidateAllApplicationsTest: unittest.TestCase = validate_all_applications.define_test()


@function()
def sync_add(a: int, b: int) -> int:
    return a + b


@function()
async def async_add(a: int, b: int) -> int:
    return a + b


@function()
def sync_double(x: int) -> int:
    return x * 2


@function()
async def async_double(x: int) -> int:
    return x * 2


@application()
@function()
def sync_app_calls_sync(x: int) -> int:
    return sync_add(x, sync_double(x))


@application()
@function()
async def async_app_calls_async(x: int) -> int:
    doubled: int = await async_double(x)
    return await async_add(x, doubled)


@application()
@function()
async def async_app_calls_sync(x: int) -> int:
    return sync_add(x, sync_double(x))


@application()
@function()
def sync_app_calls_async(x: int) -> int:
    doubled: int = async_double(x).result()
    return async_add(x, doubled).result()


@application()
@function()
async def async_app_calls_mixed(x: int) -> int:
    doubled: int = sync_double(x)
    return await async_add(x, doubled)


@application()
@function()
async def async_app_calls_mixed_reversed(x: int) -> int:
    doubled: int = await async_double(x)
    return sync_add(x, doubled)


@application()
@function()
def sync_app_calls_mixed(x: int) -> int:
    doubled: int = sync_double(x)
    return async_add(x, doubled).result()


@application()
@function()
def sync_app_calls_mixed_reversed(x: int) -> int:
    doubled: int = async_double(x).result()
    return sync_add(x, doubled)


@application()
@function()
async def async_app_async_map(items: list[int]) -> list[int]:
    return await async_double.map(items)


@application()
@function()
async def async_app_sync_map(items: list[int]) -> list[int]:
    return sync_double.map(items)


@application()
@function()
def sync_app_async_map(items: list[int]) -> list[int]:
    return async_double.map(items).result()


@application()
@function()
async def async_app_async_reduce(items: list[int]) -> int:
    return async_add.tail_call.reduce(items)


@application()
@function()
async def async_app_sync_reduce(items: list[int]) -> int:
    return sync_add.tail_call.reduce(items)


@application()
@function()
def sync_app_async_reduce(items: list[int]) -> int:
    return async_add.tail_call.reduce(items)


@application()
@function()
async def async_app_async_map_then_reduce(items: list[int]) -> int:
    doubled: list[int] = await async_double.map(items)
    return async_add.tail_call.reduce(doubled)


@application()
@function()
async def async_app_sync_map_then_async_reduce(items: list[int]) -> int:
    doubled: list[int] = sync_double.map(items)
    return async_add.tail_call.reduce(doubled)


@application()
@function()
def sync_app_async_map_then_sync_reduce(items: list[int]) -> int:
    doubled: list[int] = async_double.map(items).result()
    return sync_add.tail_call.reduce(doubled)


@application()
@function()
async def async_app_returns_future(x: int) -> int:
    return async_double.tail_call(x)


@application()
@function()
def sync_app_returns_future(x: int) -> int:
    return sync_double.tail_call(x)


@application()
@function()
async def async_app_returns_sync_future(x: int) -> int:
    return sync_double.tail_call(x)


@application()
@function()
async def async_app_passes_future_to_sync(x: int) -> int:
    doubled = async_double(x)
    return sync_add.tail_call(x, doubled)


@application()
@function()
async def async_app_passes_future_to_async(x: int) -> int:
    doubled = async_double(x)
    return async_double.tail_call(doubled)


@application()
@function()
def sync_app_passes_future_to_sync(x: int) -> int:
    doubled = sync_double.future(x)
    return sync_add.tail_call(x, doubled)


@application()
@function()
async def async_app_passes_sync_future_to_async(x: int) -> int:
    doubled = sync_double.future(x)
    return async_double.tail_call(doubled)


@application()
@function()
async def async_app_chains_futures(x: int) -> int:
    a = async_double(x)
    b = async_double(x)
    return sync_add.tail_call(a, b)


@application()
@function()
def sync_app_chains_futures(x: int) -> int:
    a = sync_double.future(x)
    b = sync_double.future(x)
    return sync_add.tail_call(a, b)


class TestSyncAsyncCalls(unittest.TestCase):
    """Tests all possible calls between sync and async functions.

    Some scenarios contain not efficient code but we test it because
    these calls are valid.
    """

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_sync_app_calls_sync(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(sync_app_calls_sync, is_remote, 5)
        self.assertEqual(request.output(), 15)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_async_app_calls_async(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(async_app_calls_async, is_remote, 5)
        self.assertEqual(request.output(), 15)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_async_app_calls_sync(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(async_app_calls_sync, is_remote, 5)
        self.assertEqual(request.output(), 15)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_sync_app_calls_async(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(sync_app_calls_async, is_remote, 5)
        self.assertEqual(request.output(), 15)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_async_app_calls_mixed(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(async_app_calls_mixed, is_remote, 5)
        self.assertEqual(request.output(), 15)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_async_app_calls_mixed_reversed(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(async_app_calls_mixed_reversed, is_remote, 5)
        self.assertEqual(request.output(), 15)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_sync_app_calls_mixed(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(sync_app_calls_mixed, is_remote, 5)
        self.assertEqual(request.output(), 15)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_sync_app_calls_mixed_reversed(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(sync_app_calls_mixed_reversed, is_remote, 5)
        self.assertEqual(request.output(), 15)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_async_app_async_map(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(async_app_async_map, is_remote, [1, 2, 3])
        self.assertEqual(request.output(), [2, 4, 6])

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_async_app_sync_map(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(async_app_sync_map, is_remote, [1, 2, 3])
        self.assertEqual(request.output(), [2, 4, 6])

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_sync_app_async_map(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(sync_app_async_map, is_remote, [1, 2, 3])
        self.assertEqual(request.output(), [2, 4, 6])

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_async_app_async_reduce(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(
            async_app_async_reduce, is_remote, [1, 2, 3, 4]
        )
        self.assertEqual(request.output(), 10)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_async_app_sync_reduce(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(
            async_app_sync_reduce, is_remote, [1, 2, 3, 4]
        )
        self.assertEqual(request.output(), 10)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_sync_app_async_reduce(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(
            sync_app_async_reduce, is_remote, [1, 2, 3, 4]
        )
        self.assertEqual(request.output(), 10)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_async_app_async_map_then_reduce(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(
            async_app_async_map_then_reduce, is_remote, [1, 2, 3, 4]
        )
        self.assertEqual(request.output(), 20)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_async_app_sync_map_then_async_reduce(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(
            async_app_sync_map_then_async_reduce, is_remote, [1, 2, 3, 4]
        )
        self.assertEqual(request.output(), 20)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_sync_app_async_map_then_sync_reduce(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(
            sync_app_async_map_then_sync_reduce, is_remote, [1, 2, 3, 4]
        )
        self.assertEqual(request.output(), 20)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_async_app_returns_future(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(async_app_returns_future, is_remote, 5)
        self.assertEqual(request.output(), 10)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_sync_app_returns_future(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(sync_app_returns_future, is_remote, 5)
        self.assertEqual(request.output(), 10)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_async_app_returns_sync_future(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(async_app_returns_sync_future, is_remote, 5)
        self.assertEqual(request.output(), 10)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_async_app_passes_future_to_sync(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(
            async_app_passes_future_to_sync, is_remote, 5
        )
        self.assertEqual(request.output(), 15)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_async_app_passes_future_to_async(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(
            async_app_passes_future_to_async, is_remote, 5
        )
        self.assertEqual(request.output(), 20)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_sync_app_passes_future_to_sync(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(sync_app_passes_future_to_sync, is_remote, 5)
        self.assertEqual(request.output(), 15)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_async_app_passes_sync_future_to_async(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(
            async_app_passes_sync_future_to_async, is_remote, 5
        )
        self.assertEqual(request.output(), 20)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_async_app_chains_futures(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(async_app_chains_futures, is_remote, 5)
        self.assertEqual(request.output(), 20)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_sync_app_chains_futures(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(sync_app_chains_futures, is_remote, 5)
        self.assertEqual(request.output(), 20)


if __name__ == "__main__":
    unittest.main()
