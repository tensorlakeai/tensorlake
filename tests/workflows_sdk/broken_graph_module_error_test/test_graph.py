import io
import unittest
from contextlib import redirect_stdout

from extractors import extractor_a, extractor_c

from tensorlake import RemoteGraph
from tensorlake.functions_sdk.graph import Graph
from tensorlake.functions_sdk.graph_serialization import graph_code_dir_path


def create_broken_graph():
    g = Graph(
        "broken-graph-without-dep-registered",
        start_node=extractor_a,
    )

    # Parse the PDF which was downloaded
    g.add_edge(extractor_a, extractor_c)
    return g


class TestBrokenGraphs(unittest.TestCase):
    def test_broken_graph(self):
        g = create_broken_graph()
        g = RemoteGraph.deploy(graph=g, code_dir_path=graph_code_dir_path(__file__))

        # We don't have a public SDK API to read a function's stderr
        # so we rely on internal SDK behavior where it prints a failed function's
        # stderr to the current stdout.
        func_stdout: io.StringIO = io.StringIO()
        with redirect_stdout(func_stdout):
            g.run(
                block_until_done=True,
                request=10,
            )
        # TODO: Uncomment this line once Function Executor creation errors are reported in
        # task stdout, stderr.
        # Use regex because rich formatting characters are present in the output.
        # self.assertRegex(func_stdout.getvalue(), r"No module named.*'first_p_dep'")


if __name__ == "__main__":
    unittest.main()
