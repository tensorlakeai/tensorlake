import unittest
from typing import Any

from tensorlake.applications import function
from tensorlake.applications.interface.awaitables import (
    Awaitable,
    AwaitableList,
    FunctionCallAwaitable,
    ReduceOperationAwaitable,
    _AwaitableListKind,
    _AwaitableListMetadata,
)
from tensorlake.function_executor.allocation_runner.sdk_algorithms import (
    _sha256_hash_strings,
    to_durable_awaitable_tree,
)


@function()
def reduce_func(x, y):
    return x + y


@function()
def reduce_func_1(x, y):
    return x + y


@function()
def reduce_func_2(x, y):
    return x + y


@function()
def reduce_func_3(x, y):
    return x + y


class TestToDurableAwaitableTree(unittest.TestCase):
    def test_to_durable_awaitable_tree(self):
        test_cases = [
            {
                "name": "Single FunctionCallAwaitable",
                "node": FunctionCallAwaitable(
                    id="awaitable_1",
                    function_name="func_1",
                    args=[1, 2],
                    kwargs={"a": 3},
                ),
                "parent_function_call_id": "parent_function_call_id_123",
                "previous_awaitable_id": "previous_awaitable_id_456",
                "expected_node": FunctionCallAwaitable(
                    id=_sha256_hash_strings(
                        [
                            "parent_function_call_id_123",
                            "previous_awaitable_id_456",
                            "0",
                            "FunctionCall",
                            "func_1",
                        ]
                    ),
                    function_name="func_1",
                    args=[1, 2],
                    kwargs={"a": 3},
                ),
            },
            {
                "name": "Single ReduceOperationAwaitable",
                "node": ReduceOperationAwaitable(
                    id="awaitable_1",
                    function_name="reduce_func",
                    inputs=[1, 2],
                ),
                "parent_function_call_id": "parent_function_call_id_123",
                "previous_awaitable_id": "previous_awaitable_id_456",
                "expected_node": FunctionCallAwaitable(
                    id=_sha256_hash_strings(
                        [
                            "parent_function_call_id_123",
                            "previous_awaitable_id_456",
                            "0",
                            "FunctionCall",
                            "reduce_func",
                        ]
                    ),
                    function_name="reduce_func",
                    args=[1, 2],
                    kwargs={},
                ),
            },
            {
                "name": "Single Map Operation AwaitableList",
                "node": AwaitableList(
                    id="awaitable_list_1",
                    items=[
                        FunctionCallAwaitable(
                            id="item_1",
                            function_name="map_func",
                            args=[1],
                            kwargs={},
                        ),
                    ],
                    metadata=_AwaitableListMetadata(
                        kind=_AwaitableListKind.MAP_OPERATION, function_name="map_func"
                    ),
                ),
                "parent_function_call_id": "parent_function_call_id_123",
                "previous_awaitable_id": "previous_awaitable_id_456",
                "expected_node": AwaitableList(
                    id=_sha256_hash_strings(
                        [
                            "parent_function_call_id_123",
                            "previous_awaitable_id_456",
                            "0",
                            "MAP_OPERATION:map_func",
                            _sha256_hash_strings(
                                [
                                    "parent_function_call_id_123",
                                    "previous_awaitable_id_456",
                                    "1",
                                    "FunctionCall",
                                    "map_func",
                                ]
                            ),
                        ]
                    ),
                    items=[
                        FunctionCallAwaitable(
                            id=_sha256_hash_strings(
                                [
                                    "parent_function_call_id_123",
                                    "previous_awaitable_id_456",
                                    "1",
                                    "FunctionCall",
                                    "map_func",
                                ]
                            ),
                            function_name="map_func",
                            args=[1],
                            kwargs={},
                        ),
                    ],
                    metadata=_AwaitableListMetadata(
                        kind=_AwaitableListKind.MAP_OPERATION, function_name="map_func"
                    ),
                ),
            },
            {
                "name": "Nested Function Calls",
                "node": FunctionCallAwaitable(
                    id="awaitable_1",
                    function_name="func_1",
                    args=[
                        FunctionCallAwaitable(
                            id="awaitable_2",
                            function_name="func_2",
                            args=[1],
                            kwargs={},
                        ),
                        2,
                        FunctionCallAwaitable(
                            id="awaitable_3",
                            function_name="func_3",
                            args=[],
                            kwargs={
                                "arg_1": FunctionCallAwaitable(
                                    id="awaitable_4",
                                    function_name="func_4",
                                    args=[4],
                                    kwargs={
                                        "c": FunctionCallAwaitable(
                                            id="awaitable_5",
                                            function_name="func_5",
                                            args=[],
                                            kwargs={},
                                        ),
                                        "a": FunctionCallAwaitable(
                                            id="awaitable_6",
                                            function_name="func_6",
                                            args=[],
                                            kwargs={},
                                        ),
                                    },
                                )
                            },
                        ),
                    ],
                    kwargs={"a": 3},
                ),
                "parent_function_call_id": "parent_function_call_id_123",
                "previous_awaitable_id": "previous_awaitable_id_456",
                "expected_node": FunctionCallAwaitable(
                    id=_sha256_hash_strings(
                        [
                            "parent_function_call_id_123",
                            "previous_awaitable_id_456",
                            "0",
                            "FunctionCall",
                            "func_1",
                            _sha256_hash_strings(
                                [
                                    "parent_function_call_id_123",
                                    "previous_awaitable_id_456",
                                    "1",
                                    "FunctionCall",
                                    "func_2",
                                ]
                            ),
                            _sha256_hash_strings(
                                [
                                    "parent_function_call_id_123",
                                    "previous_awaitable_id_456",
                                    "2",
                                    "FunctionCall",
                                    "func_3",
                                    _sha256_hash_strings(
                                        [
                                            "parent_function_call_id_123",
                                            "previous_awaitable_id_456",
                                            "3",
                                            "FunctionCall",
                                            "func_4",
                                            # "a" kwarg key first due to alphabetical order
                                            _sha256_hash_strings(
                                                [
                                                    "parent_function_call_id_123",
                                                    "previous_awaitable_id_456",
                                                    "4",
                                                    "FunctionCall",
                                                    "func_6",
                                                ]
                                            ),
                                            _sha256_hash_strings(
                                                [
                                                    "parent_function_call_id_123",
                                                    "previous_awaitable_id_456",
                                                    "5",
                                                    "FunctionCall",
                                                    "func_5",
                                                ]
                                            ),
                                        ]
                                    ),
                                ]
                            ),
                        ]
                    ),
                    function_name="func_1",
                    args=[
                        FunctionCallAwaitable(
                            id=_sha256_hash_strings(
                                [
                                    "parent_function_call_id_123",
                                    "previous_awaitable_id_456",
                                    "1",
                                    "FunctionCall",
                                    "func_2",
                                ]
                            ),
                            function_name="func_2",
                            args=[1],
                            kwargs={},
                        ),
                        2,
                        FunctionCallAwaitable(
                            id=_sha256_hash_strings(
                                [
                                    "parent_function_call_id_123",
                                    "previous_awaitable_id_456",
                                    "2",
                                    "FunctionCall",
                                    "func_3",
                                    _sha256_hash_strings(
                                        [
                                            "parent_function_call_id_123",
                                            "previous_awaitable_id_456",
                                            "3",
                                            "FunctionCall",
                                            "func_4",
                                            # "a" kwarg key first due to alphabetical order
                                            _sha256_hash_strings(
                                                [
                                                    "parent_function_call_id_123",
                                                    "previous_awaitable_id_456",
                                                    "4",
                                                    "FunctionCall",
                                                    "func_6",
                                                ]
                                            ),
                                            _sha256_hash_strings(
                                                [
                                                    "parent_function_call_id_123",
                                                    "previous_awaitable_id_456",
                                                    "5",
                                                    "FunctionCall",
                                                    "func_5",
                                                ]
                                            ),
                                        ]
                                    ),
                                ]
                            ),
                            function_name="func_3",
                            args=[],
                            kwargs={
                                "arg_1": FunctionCallAwaitable(
                                    id=_sha256_hash_strings(
                                        [
                                            "parent_function_call_id_123",
                                            "previous_awaitable_id_456",
                                            "3",
                                            "FunctionCall",
                                            "func_4",
                                            # "a" kwarg key first due to alphabetical order
                                            _sha256_hash_strings(
                                                [
                                                    "parent_function_call_id_123",
                                                    "previous_awaitable_id_456",
                                                    "4",
                                                    "FunctionCall",
                                                    "func_6",
                                                ]
                                            ),
                                            _sha256_hash_strings(
                                                [
                                                    "parent_function_call_id_123",
                                                    "previous_awaitable_id_456",
                                                    "5",
                                                    "FunctionCall",
                                                    "func_5",
                                                ]
                                            ),
                                        ]
                                    ),
                                    function_name="func_4",
                                    args=[4],
                                    kwargs={
                                        "c": FunctionCallAwaitable(
                                            id=_sha256_hash_strings(
                                                [
                                                    "parent_function_call_id_123",
                                                    "previous_awaitable_id_456",
                                                    "5",
                                                    "FunctionCall",
                                                    "func_5",
                                                ]
                                            ),
                                            function_name="func_5",
                                            args=[],
                                            kwargs={},
                                        ),
                                        "a": FunctionCallAwaitable(
                                            id=_sha256_hash_strings(
                                                [
                                                    "parent_function_call_id_123",
                                                    "previous_awaitable_id_456",
                                                    "4",
                                                    "FunctionCall",
                                                    "func_6",
                                                ]
                                            ),
                                            function_name="func_6",
                                            args=[],
                                            kwargs={},
                                        ),
                                    },
                                )
                            },
                        ),
                    ],
                    kwargs={"a": 3},
                ),
            },
            {
                "name": "Nested Reduce Operations",
                "node": ReduceOperationAwaitable(
                    id="reduce_op_1",
                    function_name="reduce_func_1",
                    inputs=[
                        ReduceOperationAwaitable(
                            id="reduce_op_2",
                            function_name="reduce_func_2",
                            inputs=[1, 2, 3],
                        ),
                        ReduceOperationAwaitable(
                            id="reduce_op_3",
                            function_name="reduce_func_3",
                            inputs=[5, 6],
                        ),
                    ],
                ),
                "parent_function_call_id": "parent_function_call_id_327",
                "previous_awaitable_id": "previous_awaitable_id_456",
                "expected_node": FunctionCallAwaitable(
                    id=_sha256_hash_strings(
                        [
                            "parent_function_call_id_327",
                            "previous_awaitable_id_456",
                            "0",
                            "FunctionCall",
                            "reduce_func_1",
                            _sha256_hash_strings(
                                [
                                    "parent_function_call_id_327",
                                    "previous_awaitable_id_456",
                                    "1",
                                    "FunctionCall",
                                    "reduce_func_2",
                                    _sha256_hash_strings(
                                        [
                                            "parent_function_call_id_327",
                                            "previous_awaitable_id_456",
                                            "2",
                                            "FunctionCall",
                                            "reduce_func_2",
                                        ]
                                    ),
                                ],
                            ),
                            _sha256_hash_strings(
                                [
                                    "parent_function_call_id_327",
                                    "previous_awaitable_id_456",
                                    "3",
                                    "FunctionCall",
                                    "reduce_func_3",
                                ]
                            ),
                        ]
                    ),
                    function_name="reduce_func_1",
                    args=[
                        FunctionCallAwaitable(
                            id=_sha256_hash_strings(
                                [
                                    "parent_function_call_id_327",
                                    "previous_awaitable_id_456",
                                    "1",
                                    "FunctionCall",
                                    "reduce_func_2",
                                    _sha256_hash_strings(
                                        [
                                            "parent_function_call_id_327",
                                            "previous_awaitable_id_456",
                                            "2",
                                            "FunctionCall",
                                            "reduce_func_2",
                                        ]
                                    ),
                                ],
                            ),
                            function_name="reduce_func_2",
                            args=[
                                FunctionCallAwaitable(
                                    id=_sha256_hash_strings(
                                        [
                                            "parent_function_call_id_327",
                                            "previous_awaitable_id_456",
                                            "2",
                                            "FunctionCall",
                                            "reduce_func_2",
                                        ]
                                    ),
                                    function_name="reduce_func_2",
                                    args=[1, 2],
                                    kwargs={},
                                ),
                                3,
                            ],
                            kwargs={},
                        ),
                        FunctionCallAwaitable(
                            id=_sha256_hash_strings(
                                [
                                    "parent_function_call_id_327",
                                    "previous_awaitable_id_456",
                                    "3",
                                    "FunctionCall",
                                    "reduce_func_3",
                                ]
                            ),
                            function_name="reduce_func_3",
                            args=[5, 6],
                            kwargs={},
                        ),
                    ],
                    kwargs={},
                ),
            },
            {
                "name": "Mixed Awaitable Tree",
                "node": AwaitableList(
                    id="1",
                    items=[
                        ReduceOperationAwaitable(
                            id="2",
                            function_name="reduce_func_1",
                            inputs=["123", "125"],
                        ),
                        AwaitableList(
                            id="3",
                            items=[
                                ReduceOperationAwaitable(
                                    id="4",
                                    function_name="reduce_func_2",
                                    inputs=[
                                        FunctionCallAwaitable(
                                            id="5",
                                            function_name="func_map_1",
                                            args=[],
                                            kwargs={},
                                        ),
                                        100500,
                                    ],
                                ),
                            ],
                            metadata=_AwaitableListMetadata(
                                kind=_AwaitableListKind.MAP_OPERATION,
                                function_name="func_map_1",
                            ),
                        ),
                    ],
                    metadata=_AwaitableListMetadata(
                        kind=_AwaitableListKind.MAP_OPERATION, function_name="func_map2"
                    ),
                ),
                "parent_function_call_id": "parent_function_call_id_111",
                "previous_awaitable_id": "previous_awaitable_id_456",
                "expected_node": AwaitableList(
                    id=_sha256_hash_strings(
                        [
                            "parent_function_call_id_111",
                            "previous_awaitable_id_456",
                            "0",
                            "MAP_OPERATION:func_map2",
                            _sha256_hash_strings(
                                [
                                    "parent_function_call_id_111",
                                    "previous_awaitable_id_456",
                                    "1",
                                    "FunctionCall",
                                    "reduce_func_1",
                                ]
                            ),
                            _sha256_hash_strings(
                                [
                                    "parent_function_call_id_111",
                                    "previous_awaitable_id_456",
                                    "2",
                                    "MAP_OPERATION:func_map_1",
                                    _sha256_hash_strings(
                                        [
                                            "parent_function_call_id_111",
                                            "previous_awaitable_id_456",
                                            "3",
                                            "FunctionCall",
                                            "reduce_func_2",
                                            _sha256_hash_strings(
                                                [
                                                    "parent_function_call_id_111",
                                                    "previous_awaitable_id_456",
                                                    "4",
                                                    "FunctionCall",
                                                    "func_map_1",
                                                ]
                                            ),
                                        ]
                                    ),
                                ]
                            ),
                        ]
                    ),
                    items=[
                        FunctionCallAwaitable(
                            id=_sha256_hash_strings(
                                [
                                    "parent_function_call_id_111",
                                    "previous_awaitable_id_456",
                                    "1",
                                    "FunctionCall",
                                    "reduce_func_1",
                                ]
                            ),
                            function_name="reduce_func_1",
                            args=["123", "125"],
                            kwargs={},
                        ),
                        AwaitableList(
                            id=_sha256_hash_strings(
                                [
                                    "parent_function_call_id_111",
                                    "previous_awaitable_id_456",
                                    "2",
                                    "MAP_OPERATION:func_map_1",
                                    _sha256_hash_strings(
                                        [
                                            "parent_function_call_id_111",
                                            "previous_awaitable_id_456",
                                            "3",
                                            "FunctionCall",
                                            "reduce_func_2",
                                            _sha256_hash_strings(
                                                [
                                                    "parent_function_call_id_111",
                                                    "previous_awaitable_id_456",
                                                    "4",
                                                    "FunctionCall",
                                                    "func_map_1",
                                                ]
                                            ),
                                        ]
                                    ),
                                ]
                            ),
                            items=[
                                FunctionCallAwaitable(
                                    id=_sha256_hash_strings(
                                        [
                                            "parent_function_call_id_111",
                                            "previous_awaitable_id_456",
                                            "3",
                                            "FunctionCall",
                                            "reduce_func_2",
                                            _sha256_hash_strings(
                                                [
                                                    "parent_function_call_id_111",
                                                    "previous_awaitable_id_456",
                                                    "4",
                                                    "FunctionCall",
                                                    "func_map_1",
                                                ]
                                            ),
                                        ]
                                    ),
                                    function_name="reduce_func_2",
                                    args=[
                                        FunctionCallAwaitable(
                                            id=_sha256_hash_strings(
                                                [
                                                    "parent_function_call_id_111",
                                                    "previous_awaitable_id_456",
                                                    "4",
                                                    "FunctionCall",
                                                    "func_map_1",
                                                ]
                                            ),
                                            function_name="func_map_1",
                                            args=[],
                                            kwargs={},
                                        ),
                                        100500,
                                    ],
                                    kwargs={},
                                ),
                            ],
                            metadata=_AwaitableListMetadata(
                                kind=_AwaitableListKind.MAP_OPERATION,
                                function_name="func_map_1",
                            ),
                        ),
                    ],
                    metadata=_AwaitableListMetadata(
                        kind=_AwaitableListKind.MAP_OPERATION, function_name="func_map2"
                    ),
                ),
            },
        ]

        for case in test_cases:
            with self.subTest(case["name"]):
                result_node: Awaitable | Any = to_durable_awaitable_tree(
                    root=case["node"],
                    parent_function_call_id=case["parent_function_call_id"],
                    previous_awaitable_id=case["previous_awaitable_id"],
                )
                self.assertEqual(result_node, case["expected_node"])


if __name__ == "__main__":
    unittest.main()
