import time
import unittest

import parameterized
import testing
from pydantic import BaseModel
from testing import test_graph_name

from tensorlake import RemoteGraph
from tensorlake.error import ApiException, GraphStillProcessing
from tensorlake.functions_sdk.functions import tensorlake_function
from tensorlake.functions_sdk.graph import Graph


class TestGraphUpdate(unittest.TestCase):
    @parameterized.parameterized.expand(
        [
            ("new_function_names", "second_graph_new_name"),
            ("existing_function_names", "second_graph_reused_function_names"),
        ]
    )
    def test_running_invocation_unaffected_by_update(
        self, test_case_name: str, second_graph_name: str
    ):
        graph_name = test_graph_name(self)

        def initial_graph():
            @tensorlake_function()
            def start_node(x: int) -> int:
                # Sleep to provide enough time for a graph update to happen
                # while this graph version is running.
                time.sleep(1)
                return x

            @tensorlake_function()
            def middle_node(x: int) -> int:
                return x + 1

            @tensorlake_function()
            def end_node(x: int) -> int:
                return x + 2

            g = Graph(name=graph_name, start_node=start_node, version="1.0")
            g.add_edge(start_node, middle_node)
            g.add_edge(middle_node, end_node)
            return g

        def second_graph_new_name():
            @tensorlake_function()
            def start_node2(x: int) -> dict:
                return {"data": dict(num=x)}

            @tensorlake_function()
            def middle_node2(data: dict) -> dict:
                return {"data": dict(num=data["num"] + 1)}

            @tensorlake_function()
            def end_node2(data: dict) -> int:
                return data["num"] + 3

            g = Graph(name=graph_name, start_node=start_node2, version="2.0")
            g.add_edge(start_node2, middle_node2)
            g.add_edge(middle_node2, end_node2)
            return g, end_node2.name

        def second_graph_reused_function_names():
            @tensorlake_function()
            def start_node(x: int) -> dict:
                return {"data": dict(num=x)}

            @tensorlake_function()
            def middle_node(data: dict) -> dict:
                return {"data": dict(num=data["num"] + 1)}

            @tensorlake_function()
            def end_node(data: dict) -> int:
                return data["num"] + 3

            g = Graph(name=graph_name, start_node=start_node, version="3.0")
            g.add_edge(start_node, middle_node)
            g.add_edge(middle_node, end_node)
            return g, end_node.name

        g = initial_graph()
        g = RemoteGraph.deploy(g, additional_modules=[testing, parameterized])
        first_invocation_id = g.run(block_until_done=False, x=0)

        if second_graph_name == "second_graph_new_name":
            g, end_node_name = second_graph_new_name()
        else:
            g, end_node_name = second_graph_reused_function_names()
        # The first invocation should not be affected by the second graph version
        # This loop waits for the first invocation to finish and checks its output.
        time.sleep(0.25)
        g = RemoteGraph.deploy(g, additional_modules=[testing, parameterized])
        g.metadata()
        invocation_id = g.run(block_until_done=True, x=0)
        output = g.output(invocation_id, fn_name=end_node_name)
        self.assertEqual(len(output), 1)
        self.assertEqual(output[0], 4)

        # The first invocation should not be affected by the second graph version
        # This loop waits for the first invocation to finish and checks its output.
        while True:
            try:
                output = g.output(first_invocation_id, fn_name="end_node")
                self.assertEqual(len(output), 1, output)
                self.assertEqual(output[0], 3)
                break
            except GraphStillProcessing:
                time.sleep(1)

    def test_graph_update_fails_without_version_update(self):
        graph_name = test_graph_name(self)

        @tensorlake_function()
        def function_a() -> str:
            return "success"

        g = Graph(
            name=graph_name,
            description="test description",
            start_node=function_a,
        )
        RemoteGraph.deploy(g)
        g.description = "updated description without version update"
        try:
            RemoteGraph.deploy(g)
            self.fail("Expected an exception to be raised")
        except ApiException as e:
            self.assertEqual(e.status_code, 400)
            self.assertIn(
                "This graph version already exists, please update the graph version",
                str(e),
            )
        except Exception as e:
            self.fail(f"Unexpected exception: {e}")


if __name__ == "__main__":
    unittest.main()
