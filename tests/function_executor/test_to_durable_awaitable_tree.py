import unittest

from tensorlake.applications.interface.awaitables import (
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
                "awaitable_sequence_number": 1000,
                "expected_node": FunctionCallAwaitable(
                    id=_sha256_hash_strings(
                        [
                            "parent_function_call_id_123",
                            "1000",
                            "FunctionCall",
                            "func_1",
                        ]
                    ),
                    function_name="func_1",
                    args=[1, 2],
                    kwargs={"a": 3},
                ),
                "expected_awaitable_sequence_number": 1001,
            },
            {
                "name": "Single ReduceOperationAwaitable",
                "node": ReduceOperationAwaitable(
                    id="awaitable_1",
                    function_name="reduce_func",
                    inputs=[1, 2, 3],
                ),
                "parent_function_call_id": "parent_function_call_id_123",
                "awaitable_sequence_number": 199,
                "expected_node": ReduceOperationAwaitable(
                    id=_sha256_hash_strings(
                        [
                            "parent_function_call_id_123",
                            "199",
                            "ReduceOperation",
                            "reduce_func",
                        ]
                    ),
                    function_name="reduce_func",
                    inputs=[1, 2, 3],
                ),
                "expected_awaitable_sequence_number": 200,
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
                "awaitable_sequence_number": 100,
                "expected_node": AwaitableList(
                    id=_sha256_hash_strings(
                        [
                            "parent_function_call_id_123",
                            "100",
                            "MAP_OPERATION:map_func",
                            _sha256_hash_strings(
                                [
                                    "parent_function_call_id_123",
                                    "101",
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
                                    "101",
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
                "expected_awaitable_sequence_number": 102,
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
                "awaitable_sequence_number": 10,
                "expected_awaitable_sequence_number": 16,
                "expected_node": FunctionCallAwaitable(
                    id=_sha256_hash_strings(
                        [
                            "parent_function_call_id_123",
                            "10",
                            "FunctionCall",
                            "func_1",
                            _sha256_hash_strings(
                                [
                                    "parent_function_call_id_123",
                                    "11",
                                    "FunctionCall",
                                    "func_2",
                                ]
                            ),
                            _sha256_hash_strings(
                                [
                                    "parent_function_call_id_123",
                                    "12",
                                    "FunctionCall",
                                    "func_3",
                                    _sha256_hash_strings(
                                        [
                                            "parent_function_call_id_123",
                                            "13",
                                            "FunctionCall",
                                            "func_4",
                                            # "a" kwarg key first due to alphabetical order
                                            _sha256_hash_strings(
                                                [
                                                    "parent_function_call_id_123",
                                                    "14",
                                                    "FunctionCall",
                                                    "func_6",
                                                ]
                                            ),
                                            _sha256_hash_strings(
                                                [
                                                    "parent_function_call_id_123",
                                                    "15",
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
                                    "11",
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
                                    "12",
                                    "FunctionCall",
                                    "func_3",
                                    _sha256_hash_strings(
                                        [
                                            "parent_function_call_id_123",
                                            "13",
                                            "FunctionCall",
                                            "func_4",
                                            # "a" kwarg key first due to alphabetical order
                                            _sha256_hash_strings(
                                                [
                                                    "parent_function_call_id_123",
                                                    "14",
                                                    "FunctionCall",
                                                    "func_6",
                                                ]
                                            ),
                                            _sha256_hash_strings(
                                                [
                                                    "parent_function_call_id_123",
                                                    "15",
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
                                            "13",
                                            "FunctionCall",
                                            "func_4",
                                            # "a" kwarg key first due to alphabetical order
                                            _sha256_hash_strings(
                                                [
                                                    "parent_function_call_id_123",
                                                    "14",
                                                    "FunctionCall",
                                                    "func_6",
                                                ]
                                            ),
                                            _sha256_hash_strings(
                                                [
                                                    "parent_function_call_id_123",
                                                    "15",
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
                                                    "15",
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
                                                    "14",
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
                            inputs=[
                                ReduceOperationAwaitable(
                                    id="reduce_op_3",
                                    function_name="reduce_func_3",
                                    inputs=[],
                                ),
                                ReduceOperationAwaitable(
                                    id="reduce_op_4",
                                    function_name="reduce_func_4",
                                    inputs=[],
                                ),
                            ],
                        ),
                        ReduceOperationAwaitable(
                            id="reduce_op_5",
                            function_name="reduce_func_5",
                            inputs=[],
                        ),
                    ],
                ),
                "parent_function_call_id": "parent_function_call_id_327",
                "awaitable_sequence_number": 1,
                "expected_awaitable_sequence_number": 6,
                "expected_node": ReduceOperationAwaitable(
                    id=_sha256_hash_strings(
                        [
                            "parent_function_call_id_327",
                            "1",
                            "ReduceOperation",
                            "reduce_func_1",
                            _sha256_hash_strings(
                                [
                                    "parent_function_call_id_327",
                                    "2",
                                    "ReduceOperation",
                                    "reduce_func_2",
                                    _sha256_hash_strings(
                                        [
                                            "parent_function_call_id_327",
                                            "3",
                                            "ReduceOperation",
                                            "reduce_func_3",
                                        ]
                                    ),
                                    _sha256_hash_strings(
                                        [
                                            "parent_function_call_id_327",
                                            "4",
                                            "ReduceOperation",
                                            "reduce_func_4",
                                        ]
                                    ),
                                ]
                            ),
                            _sha256_hash_strings(
                                [
                                    "parent_function_call_id_327",
                                    "5",
                                    "ReduceOperation",
                                    "reduce_func_5",
                                ]
                            ),
                        ]
                    ),
                    function_name="reduce_func_1",
                    inputs=[
                        ReduceOperationAwaitable(
                            id=_sha256_hash_strings(
                                [
                                    "parent_function_call_id_327",
                                    "2",
                                    "ReduceOperation",
                                    "reduce_func_2",
                                    _sha256_hash_strings(
                                        [
                                            "parent_function_call_id_327",
                                            "3",
                                            "ReduceOperation",
                                            "reduce_func_3",
                                        ]
                                    ),
                                    _sha256_hash_strings(
                                        [
                                            "parent_function_call_id_327",
                                            "4",
                                            "ReduceOperation",
                                            "reduce_func_4",
                                        ]
                                    ),
                                ]
                            ),
                            function_name="reduce_func_2",
                            inputs=[
                                ReduceOperationAwaitable(
                                    id=_sha256_hash_strings(
                                        [
                                            "parent_function_call_id_327",
                                            "3",
                                            "ReduceOperation",
                                            "reduce_func_3",
                                        ]
                                    ),
                                    function_name="reduce_func_3",
                                    inputs=[],
                                ),
                                ReduceOperationAwaitable(
                                    id=_sha256_hash_strings(
                                        [
                                            "parent_function_call_id_327",
                                            "4",
                                            "ReduceOperation",
                                            "reduce_func_4",
                                        ]
                                    ),
                                    function_name="reduce_func_4",
                                    inputs=[],
                                ),
                            ],
                        ),
                        ReduceOperationAwaitable(
                            id=_sha256_hash_strings(
                                [
                                    "parent_function_call_id_327",
                                    "5",
                                    "ReduceOperation",
                                    "reduce_func_5",
                                ]
                            ),
                            function_name="reduce_func_5",
                            inputs=[],
                        ),
                    ],
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
                            inputs=["123", "125", "127"],
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
                "awaitable_sequence_number": 50,
                "expected_awaitable_sequence_number": 55,
                "expected_node": AwaitableList(
                    id=_sha256_hash_strings(
                        [
                            "parent_function_call_id_111",
                            "50",
                            "MAP_OPERATION:func_map2",
                            _sha256_hash_strings(
                                [
                                    "parent_function_call_id_111",
                                    "51",
                                    "ReduceOperation",
                                    "reduce_func_1",
                                ]
                            ),
                            _sha256_hash_strings(
                                [
                                    "parent_function_call_id_111",
                                    "52",
                                    "MAP_OPERATION:func_map_1",
                                    _sha256_hash_strings(
                                        [
                                            "parent_function_call_id_111",
                                            "53",
                                            "ReduceOperation",
                                            "reduce_func_2",
                                            _sha256_hash_strings(
                                                [
                                                    "parent_function_call_id_111",
                                                    "54",
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
                        ReduceOperationAwaitable(
                            id=_sha256_hash_strings(
                                [
                                    "parent_function_call_id_111",
                                    "51",
                                    "ReduceOperation",
                                    "reduce_func_1",
                                ]
                            ),
                            function_name="reduce_func_1",
                            inputs=["123", "125", "127"],
                        ),
                        AwaitableList(
                            id=_sha256_hash_strings(
                                [
                                    "parent_function_call_id_111",
                                    "52",
                                    "MAP_OPERATION:func_map_1",
                                    _sha256_hash_strings(
                                        [
                                            "parent_function_call_id_111",
                                            "53",
                                            "ReduceOperation",
                                            "reduce_func_2",
                                            _sha256_hash_strings(
                                                [
                                                    "parent_function_call_id_111",
                                                    "54",
                                                    "FunctionCall",
                                                    "func_map_1",
                                                ]
                                            ),
                                        ]
                                    ),
                                ]
                            ),
                            items=[
                                ReduceOperationAwaitable(
                                    id=_sha256_hash_strings(
                                        [
                                            "parent_function_call_id_111",
                                            "53",
                                            "ReduceOperation",
                                            "reduce_func_2",
                                            _sha256_hash_strings(
                                                [
                                                    "parent_function_call_id_111",
                                                    "54",
                                                    "FunctionCall",
                                                    "func_map_1",
                                                ]
                                            ),
                                        ]
                                    ),
                                    function_name="reduce_func_2",
                                    inputs=[
                                        FunctionCallAwaitable(
                                            id=_sha256_hash_strings(
                                                [
                                                    "parent_function_call_id_111",
                                                    "54",
                                                    "FunctionCall",
                                                    "func_map_1",
                                                ]
                                            ),
                                            function_name="func_map_1",
                                            args=[],
                                            kwargs={},
                                        ),
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
            },
        ]

        for case in test_cases:
            with self.subTest(case["name"]):
                result_node, awaitable_sequence_number = to_durable_awaitable_tree(
                    root=case["node"],
                    parent_function_call_id=case["parent_function_call_id"],
                    awaitable_sequence_number=case["awaitable_sequence_number"],
                )
                self.assertEqual(result_node, case["expected_node"])
                self.assertEqual(
                    awaitable_sequence_number,
                    case["expected_awaitable_sequence_number"],
                )


if __name__ == "__main__":
    unittest.main()
