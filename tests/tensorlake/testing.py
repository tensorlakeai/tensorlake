import time
import unittest
from typing import Any, List, Union

from tensorlake.error import GraphStillProcessing
from tensorlake.functions_sdk.graph import Graph
from tensorlake.remote_graph import RemoteGraph


def remote_or_local_graph(
    graph, remote=True, additional_modules: List[Any] = []
) -> Union[RemoteGraph, Graph]:
    if remote:
        return RemoteGraph.deploy(graph, additional_modules=additional_modules)
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
