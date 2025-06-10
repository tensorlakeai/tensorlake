import unittest

from testing import test_graph_name

from tensorlake import (
    Graph,
    RemoteGraph,
    tensorlake_function,
)
from tensorlake.functions_sdk.graph_serialization import graph_code_dir_path


@tensorlake_function(cpu=1.1, memory=1.3, ephemeral_disk=1.0)
def function_with_custom_resources(x: int) -> str:
    return "success"


class TestFunctionResources(unittest.TestCase):
    def test_function_with_custom_resources(self):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=function_with_custom_resources,
        )
        graph = RemoteGraph.deploy(
            graph=graph, code_dir_path=graph_code_dir_path(__file__)
        )
        invocation_id = graph.run(block_until_done=True, x=1)
        outputs = graph.output(invocation_id, "function_with_custom_resources")
        self.assertEqual(len(outputs), 1)
        self.assertEqual(outputs[0], "success")


if __name__ == "__main__":
    unittest.main()
