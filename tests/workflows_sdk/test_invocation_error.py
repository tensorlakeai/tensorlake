import unittest

import parameterized
from testing import remote_or_local_graph, test_graph_name

from tensorlake import Graph, RequestException, tensorlake_function


@tensorlake_function()
def start_func(cmd: str) -> str:
    if cmd == "fail_invocation":
        raise RequestException("Got command to fail the invocation")
    return f"start_func: {cmd}"


@tensorlake_function()
def end_func(_: str) -> str:
    return "end_func"


class TestInvocationError(unittest.TestCase):
    @parameterized.parameterized.expand([(False), (True)])
    def test_expected_message(self, is_remote: bool):
        g = Graph(
            name=test_graph_name(self),
            start_node=start_func,
        )
        g.add_edge(start_func, end_func)
        g = remote_or_local_graph(g, is_remote)

        invocation_id = g.run(block_until_done=True, cmd="fail_invocation")

        try:
            output = g.output(invocation_id, "start_func")
            self.fail(
                f"Expected InvocationError from start_func, but got output: {output}"
            )
        except RequestException as e:
            self.assertEqual(e.message, "Got command to fail the invocation")

        try:
            output = g.output(invocation_id, "end_func")
            self.fail(
                f"Expected InvocationError from end_func, but got output: {output}"
            )
        except RequestException as e:
            self.assertEqual(e.message, "Got command to fail the invocation")


if __name__ == "__main__":
    unittest.main()
