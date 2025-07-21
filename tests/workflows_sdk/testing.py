import time
import unittest
from typing import Any, Union

from tensorlake.functions_sdk.exceptions import GraphStillProcessing
from tensorlake.functions_sdk.graph import Graph
from tensorlake.functions_sdk.graph_serialization import graph_code_dir_path
from tensorlake.functions_sdk.remote_graph import RemoteGraph


def remote_or_local_graph(graph, remote=True) -> Union[RemoteGraph, Graph]:
    if remote:
        # This testing utils file is in the same directory as the tests calling it.
        return RemoteGraph.deploy(graph, code_dir_path=graph_code_dir_path(__file__))
    return graph


def test_graph_name(test_case: unittest.TestCase) -> str:
    """Converts a test case to a unique graph name.

    Example:
    >>> class TestGraphReduce(unittest.TestCase):
    ...     def test_simple(self):
    ...         g = Graph(name=graph_name(self), start_node=generate_seq)
    ...         # ...
    ...         print(g.name)
    ...         # test_graph_reduce_test_simple
    """
    return unittest.TestCase.id(test_case).replace(".", "_")


def wait_function_output(graph: RemoteGraph, invocation_id: str, func_name: str) -> Any:
    while True:
        try:
            return graph.output(invocation_id, func_name)
        except GraphStillProcessing:
            time.sleep(1)
