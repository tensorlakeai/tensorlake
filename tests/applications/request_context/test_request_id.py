import multiprocessing as mp
import queue as mt_queue
import threading
import unittest

import parameterized

from tensorlake.applications import (
    Request,
    RequestContext,
    application,
    function,
)
from tensorlake.applications.applications import run_application
from tensorlake.applications.remote.deploy import deploy_applications


def get_request_id_worker(ctx: RequestContext, q) -> None:
    try:
        request_id: str = ctx.request_id
        q.put(request_id)
    except Exception as e:
        print(f"Exception in get_request_id_worker: {e}")
        q.put(e)


@application()
@function()
def func_get_request_id(_: int) -> str:
    ctx: RequestContext = RequestContext.get()
    q: mt_queue.SimpleQueue = mt_queue.SimpleQueue()
    get_request_id_worker(ctx, q)
    return q.get()


class TestUseRequestIdFromFunction(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_get_expected_request_id(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(func_get_request_id, 11, remote=is_remote)
        self.assertEqual(request.id, request.output())


@application()
@function()
def mt_get_request_id(_: int) -> str:
    ctx: RequestContext = RequestContext.get()
    q: mt_queue.SimpleQueue = mt_queue.SimpleQueue()
    thread: threading.Thread = threading.Thread(
        target=get_request_id_worker, args=(ctx, q)
    )
    thread.start()
    thread.join()
    return q.get()


class TestUseRequestIdFromChildThread(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_get_expected_request_id(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(mt_get_request_id, 11, remote=is_remote)
        self.assertEqual(request.id, request.output())


@application()
@function()
def mp_get_request_id(_: int) -> str:
    ctx: RequestContext = RequestContext.get()
    q: mp.Queue = mp.Queue()
    process: mp.Process = mp.Process(target=get_request_id_worker, args=(ctx, q))
    process.start()
    process.join()
    return q.get()


class TestUseRequestIdFromChildProcess(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_get_expected_request_id(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(mp_get_request_id, 11, remote=is_remote)
        self.assertEqual(request.id, request.output())


if __name__ == "__main__":
    unittest.main()
