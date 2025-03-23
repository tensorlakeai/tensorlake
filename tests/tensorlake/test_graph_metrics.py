import unittest

from testing import test_graph_name

from tensorlake import (
    Graph,
    GraphInvocationContext,
    tensorlake_function,
)


class TestGraphMetrics(unittest.TestCase):
    def test_metrics_settable(self):
        @tensorlake_function(inject_ctx=True)
        def node_with_metrics(ctx: GraphInvocationContext, x: int) -> int:
            ctx.invocation_state.timer("test_timer", 1.8)
            ctx.invocation_state.counter("test_counter", 8)
            return x + 1

        # Only test local graph mode here because behavior of secrets in remote graph depends
        # on Executor flavor.
        graph = Graph(
            name=test_graph_name(self), description="test", start_node=node_with_metrics
        )
        invocation_id = graph.run(block_until_done=True, x=1)
        output = graph.output(invocation_id, "node_with_metrics")
        self.assertTrue(len(output) == 1)
        self.assertEqual(output[0], 2)

        self.assertEqual(
            graph._metrics[graph._local_graph_ctx.invocation_id].timers.get(
                "test_timer"
            ),
            1.8,
        )
        self.assertEqual(
            graph._metrics[graph._local_graph_ctx.invocation_id].counters.get(
                "test_counter"
            ),
            8,
        )


if __name__ == "__main__":
    unittest.main()
