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


def get_state_worker(ctx: RequestContext, key: str, q) -> None:
    try:
        value = ctx.state.get(key)
        q.put(value)
    except Exception as e:
        print(f"Exception in get_state_worker: {e}")
        q.put(e)


def set_state_worker(ctx: RequestContext, key: str, value, q) -> None:
    try:
        ctx.state.set(key, value)
        q.put(None)
    except Exception as e:
        print(f"Exception in set_state_worker: {e}")
        q.put(e)


@application()
@function()
def mt_get_after_set(_: str) -> str:
    ctx: RequestContext = RequestContext.get()
    key: str = "mt_key"
    value: str = "mt_value"

    q: mt_queue.Queue = mt_queue.Queue()
    thread: threading.Thread = threading.Thread(
        target=set_state_worker, args=(ctx, key, value, q)
    )

    thread.start()
    thread.join()

    if q.get() is not None:
        return "failure"

    q: mt_queue.Queue = mt_queue.Queue()
    thread: threading.Thread = threading.Thread(
        target=get_state_worker, args=(ctx, key, q)
    )
    thread.start()
    thread.join()

    output: str = q.get()
    if output != value:
        return "failure"

    return "success"


class TestUseRequestStateFromChildThread(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_get_after_set(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(mt_get_after_set, 11, remote=is_remote)

        output: int = request.output()
        self.assertEqual(output, "success")


@application()
@function()
def mp_get_after_set(_: str) -> str:
    ctx: RequestContext = RequestContext.get()
    key: str = "mt_key"
    value: str = "mt_value"

    q: mp.Queue = mp.Queue()
    process: mp.Process = mp.Process(target=set_state_worker, args=(ctx, key, value, q))

    process.start()
    process.join()

    if q.get() is not None:
        return "failure"

    q: mp.Queue = mp.Queue()
    process: mp.Process = mp.Process(target=get_state_worker, args=(ctx, key, q))
    process.start()
    process.join()

    output: str = q.get()
    if output != value:
        return "failure"

    return "success"


class TestUseRequestStateFromChildProcess(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_get_after_set(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(mp_get_after_set, 11, remote=is_remote)

        output: int = request.output()
        self.assertEqual(output, "success")


if __name__ == "__main__":
    unittest.main()
