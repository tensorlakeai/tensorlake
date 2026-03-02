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
async def async_identity(x: str) -> str:
    return x


@function()
def sync_identity(x: str) -> str:
    return x


@application()
@function()
async def api_return_coroutine(payload: str) -> str:
    return foo_coroutine(payload)


@function()
async def foo_coroutine(x: str) -> str:
    return bar_coroutine(x)


@function()
async def bar_coroutine(x: str) -> str:
    return async_identity(x)


class TestReturnCoroutine(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        deploy_applications(__file__)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_success(self, _: str, is_remote: bool):
        request: Request = run_application(api_return_coroutine, is_remote, "coroutine")
        self.assertEqual(request.output(), "coroutine")


@application()
@function()
async def api_return_future(payload: str) -> str:
    return foo_future(payload)


@function()
async def foo_future(x: str) -> str:
    return bar_future.future(x)


@function()
async def bar_future(x: str) -> str:
    return async_identity.future(x)


class TestReturnFuture(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        deploy_applications(__file__)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_success(self, _: str, is_remote: bool):
        request: Request = run_application(api_return_future, is_remote, "future")
        self.assertEqual(request.output(), "future")


@application()
@function()
async def api_return_sync_future_coroutine(payload: str) -> str:
    return foo_sync_future_coroutine(payload)


@function()
async def foo_sync_future_coroutine(x: str) -> str:
    return sync_identity.future(x).coroutine()


class TestReturnSyncFutureCoroutine(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        deploy_applications(__file__)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_success(self, _: str, is_remote: bool):
        request: Request = run_application(
            api_return_sync_future_coroutine, is_remote, "sync_future_coroutine"
        )
        self.assertEqual(request.output(), "sync_future_coroutine")


@application()
@function()
async def api_return_async_future_coroutine(payload: str) -> str:
    return foo_async_future_coroutine(payload)


@function()
async def foo_async_future_coroutine(x: str) -> str:
    return async_identity.future(x).coroutine()


class TestReturnAsyncFutureCoroutine(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        deploy_applications(__file__)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_success(self, _: str, is_remote: bool):
        request: Request = run_application(
            api_return_async_future_coroutine, is_remote, "async_future_coroutine"
        )
        self.assertEqual(request.output(), "async_future_coroutine")


@application()
@function()
async def api_mixed_chain(payload: str) -> str:
    # Tail call via coroutine (calling async function directly).
    return mixed_step_future(payload)


@function()
async def mixed_step_future(x: str) -> str:
    # Tail call via Future.
    return mixed_step_sync_future_coroutine.future(x)


@function()
async def mixed_step_sync_future_coroutine(x: str) -> str:
    # Tail call via sync .future().coroutine().
    return sync_identity.future(x).coroutine()


class TestMixedChain(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        deploy_applications(__file__)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_success(self, _: str, is_remote: bool):
        request: Request = run_application(api_mixed_chain, is_remote, "mixed")
        self.assertEqual(request.output(), "mixed")


if __name__ == "__main__":
    unittest.main()
