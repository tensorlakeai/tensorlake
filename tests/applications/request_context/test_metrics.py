import contextlib
import io
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


def emit_metrics_worker(ctx: RequestContext, q) -> None:
    try:
        ctx.metrics.timer("test_timer", 2.5)
        ctx.metrics.counter("test_counter")
        q.put(None)
    except Exception as e:
        print(f"Exception in emit_metrics_worker: {e}")
        q.put(e)


@application()
@function()
def func_emit_metrics(_: int) -> str:
    ctx: RequestContext = RequestContext.get()
    q: mt_queue.SimpleQueue = mt_queue.SimpleQueue()
    emit_metrics_worker(ctx, q)
    return "success" if q.get() is None else "failure"


class TestUseMetricsFromFunction(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_emit_metrics(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(func_emit_metrics, is_remote, 1)
        self.assertEqual(request.output(), "success")

    def test_emit_local_metrics_output(self):
        stdout: io.StringIO = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            request: Request = run_application(func_emit_metrics, False, 1)
            self.assertEqual("success", request.output())

        output: str = stdout.getvalue().strip()
        self.assertIn(
            "Counter Incremented: ",
            output,
        )
        self.assertIn(
            f"'request_id': '{request.id}'",
            output,
        )
        self.assertIn(
            "'function_name': 'func_emit_metrics'",
            output,
        )
        self.assertIn(
            "'counter_name': 'test_counter'",
            output,
        )
        self.assertIn(
            "'counter_inc': 1",
            output,
        )
        self.assertIn(
            "Timer Recorded: ",
            output,
        )
        self.assertIn(
            "'timer_name': 'test_timer'",
            output,
        )
        self.assertIn(
            "'timer_value': 2.5",
            output,
        )


@application()
@function()
def mt_emit_metrics(_: int) -> str:
    ctx: RequestContext = RequestContext.get()
    q: mt_queue.SimpleQueue = mt_queue.SimpleQueue()
    thread: threading.Thread = threading.Thread(
        target=emit_metrics_worker, args=(ctx, q)
    )
    thread.start()
    thread.join()
    return "success" if q.get() is None else "failure"


class TestUseMetricsFromChildThread(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_emit_metrics(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(mt_emit_metrics, is_remote, 1)
        self.assertEqual(request.output(), "success")

        # No verification of metrics values yet because SDK doesn't yet provide an interface
        # for reading request metrics.


@application()
@function()
def mp_emit_metrics(_: int) -> str:
    ctx: RequestContext = RequestContext.get()
    q: mp.Queue = mp.Queue()
    process: mp.Process = mp.Process(target=emit_metrics_worker, args=(ctx, q))
    process.start()
    process.join()
    return "success" if q.get() is None else "failure"


class TestUseMetricsFromChildProcess(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_emit_metrics(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(mp_emit_metrics, is_remote, 1)
        self.assertEqual(request.output(), "success")

        # No verification of metrics values yet because SDK doesn't yet provide an interface
        # for reading request metrics.


if __name__ == "__main__":
    unittest.main()
