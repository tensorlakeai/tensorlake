import multiprocessing as mp
import queue as mt_queue
import threading
import unittest

import parameterized
import validate_all_applications

from tensorlake.applications import (
    Request,
    RequestContext,
    application,
    function,
)
from tensorlake.applications.applications import run_application
from tensorlake.applications.remote.deploy import deploy_applications

# Makes the test case discoverable by unittest framework.
ValidateAllApplicationsTest: unittest.TestCase = validate_all_applications.define_test()


def update_progress_worker(ctx: RequestContext, values: tuple[int, int], q) -> None:
    try:
        ctx.progress.update(current=values[0], total=values[1])
        q.put(None)
    except Exception as e:
        print(f"Exception in update_progress_worker: {e}")
        q.put(e)


@application()
@function()
def func_update_progress(values: tuple[int, int]) -> str:
    ctx: RequestContext = RequestContext.get()
    q: mt_queue.SimpleQueue = mt_queue.SimpleQueue()
    update_progress_worker(ctx, values, q)
    return "success" if q.get() is None else "failure"


class TestUseProgressFromFunction(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_update_progress(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(func_update_progress, is_remote, (10, 100))
        self.assertEqual(request.output(), "success")


@application()
@function()
def mt_update_progress(values: tuple[int, int]) -> str:
    ctx: RequestContext = RequestContext.get()
    q: mt_queue.SimpleQueue = mt_queue.SimpleQueue()
    thread: threading.Thread = threading.Thread(
        target=update_progress_worker, args=(ctx, values, q)
    )
    thread.start()
    thread.join()
    return "success" if q.get() is None else "failure"


class TestUseProgressFromChildThread(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_update_progress(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(mt_update_progress, is_remote, (10, 100))
        self.assertEqual(request.output(), "success")


@application()
@function()
def mp_update_progress(values: tuple[int, int]) -> str:
    ctx: RequestContext = RequestContext.get()
    q: mp.Queue = mp.Queue()
    process: mp.Process = mp.Process(
        target=update_progress_worker, args=(ctx, values, q)
    )
    process.start()
    process.join()
    return "success" if q.get() is None else "failure"


class TestUseProgressFromChildProcess(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_update_progress(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(mp_update_progress, is_remote, (10, 100))
        self.assertEqual("success", request.output())
        self.assertEqual(request.output(), "success")


if __name__ == "__main__":
    unittest.main()
