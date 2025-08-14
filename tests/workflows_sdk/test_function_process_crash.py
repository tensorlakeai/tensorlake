import os
import time
import unittest

from testing import test_graph_name

from tensorlake import Graph, RemoteGraph, tensorlake_function
from tensorlake.functions_sdk.graph_serialization import graph_code_dir_path


@tensorlake_function()
def function(crash: bool) -> str:
    if crash:
        # os.kill(getpid(), signal.SIGKILL) won't work for container init process,
        # see https://stackoverflow.com/questions/21031537/sigkill-init-process-pid-1.
        # sys.exit(1) hangs the function for some unknown reason,
        # see some ideas at https://stackoverflow.com/questions/5422831/what-does-sys-exit-do-in-python.
        os._exit(1)
    return "success"


class TestFunctionProcessCrash(unittest.TestCase):
    def test_function_invoke_successful_after_process_crashes(self):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=function,
        )
        graph = RemoteGraph.deploy(
            graph=graph, code_dir_path=graph_code_dir_path(__file__)
        )

        print("Running a function that will crash FunctionExecutor process...")
        for i in range(2):
            crash_invocation_id = graph.run(
                block_until_done=True,
                request=True,
            )
            crash_output = graph.output(crash_invocation_id, "function")
            self.assertEqual(crash_output, [])

        # FIXME: we're only doing periodic Function Executor health checks right now,
        # so we need to wait for the crash to be detected.
        time.sleep(10)
        success_invocation_id = graph.run(
            block_until_done=True,
            request=False,
        )
        success_output = graph.output(success_invocation_id, "function")
        self.assertEqual(success_output, ["success"])


if __name__ == "__main__":
    unittest.main()
