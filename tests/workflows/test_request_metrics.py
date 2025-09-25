import unittest

import parameterized

import tensorlake.workflows.interface as tensorlake
from tensorlake.workflows.remote.deploy import deploy


@tensorlake.api()
@tensorlake.function()
def emit_metrics(ctx: tensorlake.RequestContext, x: int) -> int:
    ctx.metrics.timer("test_timer", 1.8)
    ctx.metrics.counter("test_counter", 8)
    return x + 1


class TestRequestMetrics(unittest.TestCase):
    @parameterized.parameterized.expand([(True), (False)])
    def test_metrics_settable(self, is_remote: bool):
        if is_remote:
            deploy(__file__)

        request: tensorlake.Request = tensorlake.call_api(
            emit_metrics, 1, remote=is_remote
        )
        self.assertEqual(request.output(), 2)

        # No verification of metrics values yet because SDK doesn't yet provide an interface
        # for reading request metrics.


if __name__ == "__main__":
    unittest.main()
