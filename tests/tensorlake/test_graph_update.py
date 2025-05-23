import time
import unittest

from testing import test_graph_name, wait_function_output

from tensorlake import Graph, RemoteGraph, tensorlake_function
from tensorlake.error import ApiException
from tensorlake.functions_sdk.graph_serialization import graph_code_dir_path


@tensorlake_function()
def start_func_v1(sleep_sec: int) -> str:
    time.sleep(sleep_sec)
    return "start_func_v1"


@tensorlake_function()
def end_func_v1(_: str) -> str:
    return "end_func_v1"


@tensorlake_function()
def end_func_v2(_: str) -> str:
    return "end_func_v2"


class TestGraphUpdate(unittest.TestCase):
    def test_running_invocation_gets_its_graph_version_updated(self):
        g = Graph(
            name=test_graph_name(self),
            start_node=start_func_v1,
        )
        g.add_edge(start_func_v1, end_func_v1)

        g = RemoteGraph.deploy(graph=g, code_dir_path=graph_code_dir_path(__file__))

        invocation_id = g.run(block_until_done=False, sleep_sec=10)

        g = Graph(
            name=test_graph_name(self),
            start_node=start_func_v1,
            version="2.0",
        )
        g.add_edge(start_func_v1, end_func_v2)
        g = RemoteGraph.deploy(
            graph=g,
            code_dir_path=graph_code_dir_path(__file__),
            upgrade_tasks_to_latest_version=True,
        )

        output = wait_function_output(g, invocation_id, "end_func_v2")
        self.assertEqual(len(output), 1, output)
        self.assertEqual(output[0], "end_func_v2", output)

    def test_running_invocation_doesnt_get_its_graph_version_updated(self):
        g = Graph(
            name=test_graph_name(self),
            start_node=start_func_v1,
        )
        g.add_edge(start_func_v1, end_func_v1)
        g = RemoteGraph.deploy(graph=g, code_dir_path=graph_code_dir_path(__file__))

        invocation_id = g.run(block_until_done=False, sleep_sec=10)

        g = Graph(
            name=test_graph_name(self),
            start_node=start_func_v1,
            version="2.0",
        )
        g.add_edge(start_func_v1, end_func_v1)
        g.add_edge(end_func_v1, end_func_v2)
        g = RemoteGraph.deploy(
            graph=g,
            upgrade_tasks_to_latest_version=False,
            code_dir_path=graph_code_dir_path(__file__),
        )

        output = wait_function_output(g, invocation_id, "end_func_v2")
        self.assertEqual(len(output), 0, output)

    def test_graph_update_fails_without_version_update(self):
        g = Graph(
            name=test_graph_name(self),
            description="test description",
            start_node=start_func_v1,
        )
        RemoteGraph.deploy(graph=g, code_dir_path=graph_code_dir_path(__file__))
        g.description = "updated description without version update"
        try:
            RemoteGraph.deploy(graph=g, code_dir_path=graph_code_dir_path(__file__))
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
