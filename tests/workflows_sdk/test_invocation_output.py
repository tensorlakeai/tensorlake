import unittest

from testing import test_graph_name, wait_function_output

from tensorlake import Graph, RemoteGraph, tensorlake_function
from tensorlake.functions_sdk.graph_serialization import graph_code_dir_path


@tensorlake_function()
def start_func(_: None) -> str:
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
        g = RemoteGraph.deploy(graph=g, code_dir_path=graph_code_dir_path(__file__))

        invocation_id = g.run(block_until_done=True, request=None)

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
        g = RemoteGraph.deploy(graph=g, code_dir_path=graph_code_dir_path(__file__))

        invocation_id = g.run(block_until_done=False, request=None)

        output = wait_function_output(g, invocation_id, "start_func")
        self.assertEqual(len(output), 1, output)
        self.assertEqual(output[0], "start_func", output)

        output = wait_function_output(g, invocation_id, "end_func")
        self.assertEqual(len(output), 1, output)
        self.assertEqual(output[0], "end_func", output)


if __name__ == "__main__":
    unittest.main()
