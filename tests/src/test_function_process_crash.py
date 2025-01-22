import os
import signal
import unittest

from testing import test_graph_name

from tensorlake import Graph, RemoteGraph, tensorlake_function


@tensorlake_function()
def function(crash: bool) -> str:
    if crash:
        os.kill(os.getpid(), signal.SIGKILL)
    return "success"


class TestFunctionProcessCrash(unittest.TestCase):
    def test_function_invoke_successful_after_process_crashes(self):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=function,
        )
        graph = RemoteGraph.deploy(graph)

        print("Running a function that will crash FunctionExecutor process...")
        for i in range(2):
            crash_invocation_id = graph.run(
                block_until_done=True,
                crash=True,
            )
            crash_output = graph.output(crash_invocation_id, "function")
            self.assertEqual(crash_output, [])

        success_invocation_id = graph.run(
            block_until_done=True,
            crash=False,
        )
        success_output = graph.output(success_invocation_id, "function")
        self.assertEqual(success_output, ["success"])


if __name__ == "__main__":
    unittest.main()
