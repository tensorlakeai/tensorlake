import io
import sys
import unittest
from contextlib import redirect_stdout

from testing import test_graph_name

from tensorlake import RemoteGraph
from tensorlake.functions_sdk.data_objects import File
from tensorlake.functions_sdk.functions import TensorlakeCompute, tensorlake_function
from tensorlake.functions_sdk.graph import Graph


@tensorlake_function()
def extractor_a(url: str) -> File:
    print("extractor_a is writing to stdout")
    print("extractor_a is writing to stderr", file=sys.stderr)
    return File(data=b"abc", mime_type="application/pdf")


@tensorlake_function()
def extractor_b(file: File) -> str:
    print("extractor_b is writing to stdout")
    print("extractor_b is writing to stderr", file=sys.stderr)
    raise Exception("this exception was raised from extractor_b")


@tensorlake_function()
def extractor_c(s: str) -> str:
    return "this is a return from extractor_c"


class TensorlakeComputeWithFailingConstructor(TensorlakeCompute):
    name = "TensorlakeComputeWithFailingConstructor"

    def __init__(self):
        super().__init__()
        raise Exception(
            "this exception was raised by TensorlakeComputeWithFailingConstructor constructor"
        )

    def run(self) -> str:
        return "success"


class TestBrokenGraphs(unittest.TestCase):
    def test_expected_stdout_stderr_content(self):
        g = Graph(
            name=test_graph_name(self),
            start_node=extractor_a,
        )
        g.add_edge(extractor_a, extractor_b)
        g.add_edge(extractor_b, extractor_c)
        g = RemoteGraph.deploy(g)

        # We don't have a public SDK API to read a functions' stderr
        # so we rely on internal SDK behavior where it prints a failed function's
        # stderr to the current stdout.
        sdk_stdout: io.StringIO = io.StringIO()
        with redirect_stdout(sdk_stdout):
            invocation_id = g.run(
                block_until_done=True,
                url="https://www.youtube.com/watch?v=gjHv4pM8WEQ",
            )
        sdk_stdout_str: str = sdk_stdout.getvalue()

        # extractor_a output is not written by SDK because it succeeded.
        self.assertNotIn("extractor_a is writing to stdout", sdk_stdout_str)
        self.assertNotIn("extractor_a is writing to stderr", sdk_stdout_str)

        # extractor_b output is written by SDK because it failed to help user to debug.
        # TODO: Fix this test, this line is currently failing due to some race condition
        # in Function Executor function output capturing.
        # self.assertIn("extractor_b is writing to stdout", sdk_stdout_str)
        self.assertIn("extractor_b is writing to stderr", sdk_stdout_str)
        self.assertIn(
            "Exception: this exception was raised from extractor_b", sdk_stdout_str
        )

        # extractor_c should not have been executed after failed extractor_b.
        extractor_c_output = g.output(invocation_id, "extractor_c")
        self.assertEqual(len(extractor_c_output), 0)

    def test_unexpected_function_argument(self):
        g = Graph(
            name=test_graph_name(self),
            start_node=extractor_a,
        )
        g = RemoteGraph.deploy(g)

        sdk_stdout: io.StringIO = io.StringIO()
        with redirect_stdout(sdk_stdout):
            invocation_id = g.run(
                block_until_done=True,
                unexpected_argument="https://www.youtube.com/watch?v=gjHv4pM8WEQ",
            )
        sdk_stdout_str: str = sdk_stdout.getvalue()

        self.assertIn("extractor_a", sdk_stdout_str)
        self.assertIn("got an unexpected keyword argument", sdk_stdout_str)
        self.assertIn("unexpected_argument", sdk_stdout_str)

        # No output from extractor_a because it failed.
        extractor_c_output = g.output(invocation_id, "extractor_a")
        self.assertEqual(len(extractor_c_output), 0)

    def test_compute_with_failing_constructor(self):
        g = Graph(
            name=test_graph_name(self),
            start_node=TensorlakeComputeWithFailingConstructor,
        )
        g = RemoteGraph.deploy(g)

        sdk_stdout: io.StringIO = io.StringIO()
        with redirect_stdout(sdk_stdout):
            invocation_id = g.run(
                block_until_done=True,
            )
        sdk_stdout_str: str = sdk_stdout.getvalue()

        self.assertIn(
            "this exception was raised by TensorlakeComputeWithFailingConstructor",
            sdk_stdout_str,
        )
        # No output from extractor_a because it failed.
        extractor_c_output = g.output(
            invocation_id, "TensorlakeComputeWithFailingConstructor"
        )
        self.assertEqual(len(extractor_c_output), 0)


if __name__ == "__main__":
    unittest.main()
