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


@application()
@function()
def emit_metrics(x: int) -> int:
    ctx: RequestContext = RequestContext.get()
    ctx.metrics.timer("test_timer", 1.8)
    ctx.metrics.counter("test_counter", 8)
    return x + 1


class TestRequestMetrics(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_metrics_settable(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(emit_metrics, 1, remote=is_remote)
        self.assertEqual(request.output(), 2)

        # No verification of metrics values yet because SDK doesn't yet provide an interface
        # for reading request metrics.


if __name__ == "__main__":
    unittest.main()
