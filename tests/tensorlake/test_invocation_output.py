import unittest

from testing import test_graph_name, wait_function_output

from tensorlake import Graph, RemoteGraph, tensorlake_function


@tensorlake_function()
def start_func() -> str:
    return "start_func"


@tensorlake_function()
def end_func(_: str) -> str:
    return "end_func"


class TestInvocationOutput(unittest.TestCase):
    def test_function_outputs_in_sync_invocation(self):
        g = Graph(
            name=test_graph_name(self),
            start_node=start_func,
        )
        g.add_edge(start_func, end_func)
        g = RemoteGraph.deploy(g)

        invocation_id = g.run(block_until_done=True)

        output = g.output(invocation_id, "start_func")
        self.assertEqual(len(output), 1, output)
        self.assertEqual(output[0], "start_func", output)

        output = g.output(invocation_id, "end_func")
        self.assertEqual(len(output), 1, output)
        self.assertEqual(output[0], "end_func", output)

    def test_function_outputs_in_async_invocation(self):
        g = Graph(
            name=test_graph_name(self),
            start_node=start_func,
        )
        g.add_edge(start_func, end_func)
        g = RemoteGraph.deploy(g)

        invocation_id = g.run(block_until_done=False)

        output = wait_function_output(g, invocation_id, "start_func")
        self.assertEqual(len(output), 1, output)
        self.assertEqual(output[0], "start_func", output)

        output = wait_function_output(g, invocation_id, "end_func")
        self.assertEqual(len(output), 1, output)
        self.assertEqual(output[0], "end_func", output)


if __name__ == "__main__":
    unittest.main()
