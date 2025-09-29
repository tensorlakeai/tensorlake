import unittest

import parameterized

from tensorlake.applications import Request, RequestContext, api, call_api, function
from tensorlake.applications.remote.deploy import deploy


@api()
@function()
def emit_metrics(ctx: RequestContext, x: int) -> int:
    ctx.metrics.timer("test_timer", 1.8)
    ctx.metrics.counter("test_counter", 8)
    return x + 1


class TestRequestMetrics(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_metrics_settable(self, _: str, is_remote: bool):
        if is_remote:
            deploy(__file__)

        request: Request = call_api(emit_metrics, 1, remote=is_remote)
        self.assertEqual(request.output(), 2)

        # No verification of metrics values yet because SDK doesn't yet provide an interface
        # for reading request metrics.


if __name__ == "__main__":
    unittest.main()
