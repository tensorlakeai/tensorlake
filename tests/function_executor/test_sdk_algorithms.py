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
    assign_sequence_numbers_to_awaitables,
    to_durable_awaitable_tree,
)


class TestAssignSequenceNumbersToAwaitables(unittest.TestCase):
    def test_assign_sequence_numbers_to_awaitables(self):
        test_cases = [
            {
                "name": "Single FunctionCallAwaitable",
                "node": FunctionCallAwaitable(
                    id="awaitable_1",
                    function_name="func_1",
                    args=[1, 2],
                    kwargs={"a": 3},
                ),
                "expected_sequence_number": 1,
                "expected_sequence_number_mapping": {"awaitable_1": 0},
            },
            {
                "name": "Single ReduceOperationAwaitable",
                "node": ReduceOperationAwaitable(
                    id="awaitable_1",
                    function_name="reduce_func",
                    inputs=[1, 2, 3],
                ),
                "expected_sequence_number": 1,
                "expected_sequence_number_mapping": {
                    "awaitable_1": 0,
                },
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
                "expected_sequence_number": 2,
                "expected_sequence_number_mapping": {
                    "awaitable_list_1": 0,
                    "item_1": 1,
                },
            },
            {
                "name": "Single value (not an Awaitable)",
                "node": 42,
                "expected_sequence_number": 0,
                "expected_sequence_number_mapping": {},
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
                "expected_sequence_number": 6,
                "expected_sequence_number_mapping": {
                    "awaitable_1": 0,
                    "awaitable_2": 1,
                    "awaitable_3": 2,
                    "awaitable_4": 3,
                    "awaitable_5": 5,  # Alphabetical order in kwargs
                    "awaitable_6": 4,  # Alphabetical order in kwargs
                },
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
                "expected_sequence_number": 5,
                "expected_sequence_number_mapping": {
                    "reduce_op_1": 0,
                    "reduce_op_2": 1,
                    "reduce_op_3": 2,
                    "reduce_op_4": 3,
                    "reduce_op_5": 4,
                },
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
                "expected_sequence_number": 5,
                "expected_sequence_number_mapping": {
                    "1": 0,
                    "2": 1,
                    "3": 2,
                    "4": 3,
                    "5": 4,
                },
            },
            {
                "name": "Deeply Nested Function Call Tree",
                "node": FunctionCallAwaitable(
                    id="func_call_1",
                    function_name="func_1",
                    args=[
                        FunctionCallAwaitable(
                            id="func_call_2",
                            function_name="func_2",
                            args=[
                                FunctionCallAwaitable(
                                    id="func_call_4",
                                    function_name="func_4",
                                    args=[],
                                    kwargs={},
                                )
                            ],
                            kwargs={
                                "d": FunctionCallAwaitable(
                                    id="func_call_5",
                                    function_name="func_5",
                                    args=[],
                                    kwargs={},
                                ),
                                "c": FunctionCallAwaitable(
                                    id="func_call_6",
                                    function_name="func_6",
                                    args=[],
                                    kwargs={},
                                ),
                            },
                        ),
                        FunctionCallAwaitable(
                            id="func_call_3",
                            function_name="func_3",
                            args=[],
                            kwargs={},
                        ),
                    ],
                    kwargs={
                        "kwarg_2": FunctionCallAwaitable(
                            id="func_call_7",
                            function_name="func_7",
                            args=[],
                            kwargs={
                                "kwarg_3": FunctionCallAwaitable(
                                    id="func_call_8",
                                    function_name="func_8",
                                    args=[],
                                    kwargs={},
                                ),
                            },
                        ),
                    },
                ),
                "expected_sequence_number": 8,
                "expected_sequence_number_mapping": {
                    "func_call_1": 0,
                    "func_call_2": 1,
                    "func_call_4": 2,
                    "func_call_6": 3,
                    "func_call_5": 4,
                    "func_call_3": 5,
                    "func_call_7": 6,
                    "func_call_8": 7,
                },
            },
        ]

        for case in test_cases:
            with self.subTest(case["name"]):
                awaitable_sequence_numbers: dict[str, int] = {}
                next_sequence_number: int = assign_sequence_numbers_to_awaitables(
                    node=case["node"],
                    current_sequence_number=0,
                    awaitable_sequence_numbers=awaitable_sequence_numbers,
                )
                self.assertEqual(next_sequence_number, case["expected_sequence_number"])
                self.assertEqual(
                    awaitable_sequence_numbers, case["expected_sequence_number_mapping"]
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
                "awaitable_sequence_numbers": {"awaitable_1": 1000},
                "expected_node": FunctionCallAwaitable(
                    id=_sha256_hash_strings(
                        [
                            "parent_function_call_id_value",
                            "1000",
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
                    inputs=[1, 2, 3],
                ),
                "awaitable_sequence_numbers": {"awaitable_1": 199},
                "expected_node": ReduceOperationAwaitable(
                    id=_sha256_hash_strings(
                        [
                            "parent_function_call_id_value",
                            "199",
                            "ReduceOperation",
                            "reduce_func",
                        ]
                    ),
                    function_name="reduce_func",
                    inputs=[1, 2, 3],
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
                "awaitable_sequence_numbers": {"awaitable_list_1": 100, "item_1": 500},
                "expected_node": AwaitableList(
                    id=_sha256_hash_strings(
                        [
                            "parent_function_call_id_value",
                            "100",
                            "MAP_OPERATION:map_func",
                            _sha256_hash_strings(
                                [
                                    "parent_function_call_id_value",
                                    "500",
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
                                    "parent_function_call_id_value",
                                    "500",
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
                "name": "Single value (not an Awaitable)",
                "node": 42,
                "awaitable_sequence_numbers": {},
                "expected_node": 42,
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
                "awaitable_sequence_numbers": {
                    "awaitable_1": 10,
                    "awaitable_2": 11,
                    "awaitable_3": 12,
                    "awaitable_4": 13,
                    "awaitable_5": 14,
                    "awaitable_6": 15,
                },
                "expected_node": FunctionCallAwaitable(
                    id=_sha256_hash_strings(
                        [
                            "parent_function_call_id_value",
                            "10",
                            "FunctionCall",
                            "func_1",
                            _sha256_hash_strings(
                                [
                                    "parent_function_call_id_value",
                                    "11",
                                    "FunctionCall",
                                    "func_2",
                                ]
                            ),
                            _sha256_hash_strings(
                                [
                                    "parent_function_call_id_value",
                                    "12",
                                    "FunctionCall",
                                    "func_3",
                                    _sha256_hash_strings(
                                        [
                                            "parent_function_call_id_value",
                                            "13",
                                            "FunctionCall",
                                            "func_4",
                                            # "a" kwarg key first due to alphabetical order
                                            _sha256_hash_strings(
                                                [
                                                    "parent_function_call_id_value",
                                                    "15",
                                                    "FunctionCall",
                                                    "func_6",
                                                ]
                                            ),
                                            _sha256_hash_strings(
                                                [
                                                    "parent_function_call_id_value",
                                                    "14",
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
                                    "parent_function_call_id_value",
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
                                    "parent_function_call_id_value",
                                    "12",
                                    "FunctionCall",
                                    "func_3",
                                    _sha256_hash_strings(
                                        [
                                            "parent_function_call_id_value",
                                            "13",
                                            "FunctionCall",
                                            "func_4",
                                            # "a" kwarg key first due to alphabetical order
                                            _sha256_hash_strings(
                                                [
                                                    "parent_function_call_id_value",
                                                    "15",
                                                    "FunctionCall",
                                                    "func_6",
                                                ]
                                            ),
                                            _sha256_hash_strings(
                                                [
                                                    "parent_function_call_id_value",
                                                    "14",
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
                                            "parent_function_call_id_value",
                                            "13",
                                            "FunctionCall",
                                            "func_4",
                                            # "a" kwarg key first due to alphabetical order
                                            _sha256_hash_strings(
                                                [
                                                    "parent_function_call_id_value",
                                                    "15",
                                                    "FunctionCall",
                                                    "func_6",
                                                ]
                                            ),
                                            _sha256_hash_strings(
                                                [
                                                    "parent_function_call_id_value",
                                                    "14",
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
                                                    "parent_function_call_id_value",
                                                    "14",
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
                                                    "parent_function_call_id_value",
                                                    "15",
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
                "awaitable_sequence_numbers": {
                    "reduce_op_1": 1,
                    "reduce_op_2": 2,
                    "reduce_op_3": 3,
                    "reduce_op_4": 4,
                    "reduce_op_5": 5,
                },
                "expected_node": ReduceOperationAwaitable(
                    id=_sha256_hash_strings(
                        [
                            "parent_function_call_id_value",
                            "1",
                            "ReduceOperation",
                            "reduce_func_1",
                            _sha256_hash_strings(
                                [
                                    "parent_function_call_id_value",
                                    "2",
                                    "ReduceOperation",
                                    "reduce_func_2",
                                    _sha256_hash_strings(
                                        [
                                            "parent_function_call_id_value",
                                            "3",
                                            "ReduceOperation",
                                            "reduce_func_3",
                                        ]
                                    ),
                                    _sha256_hash_strings(
                                        [
                                            "parent_function_call_id_value",
                                            "4",
                                            "ReduceOperation",
                                            "reduce_func_4",
                                        ]
                                    ),
                                ]
                            ),
                            _sha256_hash_strings(
                                [
                                    "parent_function_call_id_value",
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
                                    "parent_function_call_id_value",
                                    "2",
                                    "ReduceOperation",
                                    "reduce_func_2",
                                    _sha256_hash_strings(
                                        [
                                            "parent_function_call_id_value",
                                            "3",
                                            "ReduceOperation",
                                            "reduce_func_3",
                                        ]
                                    ),
                                    _sha256_hash_strings(
                                        [
                                            "parent_function_call_id_value",
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
                                            "parent_function_call_id_value",
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
                                            "parent_function_call_id_value",
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
                                    "parent_function_call_id_value",
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
                "awaitable_sequence_numbers": {
                    "1": 51,
                    "2": 52,
                    "3": 53,
                    "4": 54,
                    "5": 199,
                },
                "expected_node": AwaitableList(
                    id=_sha256_hash_strings(
                        [
                            "parent_function_call_id_value",
                            "51",
                            "MAP_OPERATION:func_map2",
                            _sha256_hash_strings(
                                [
                                    "parent_function_call_id_value",
                                    "52",
                                    "ReduceOperation",
                                    "reduce_func_1",
                                ]
                            ),
                            _sha256_hash_strings(
                                [
                                    "parent_function_call_id_value",
                                    "53",
                                    "MAP_OPERATION:func_map_1",
                                    _sha256_hash_strings(
                                        [
                                            "parent_function_call_id_value",
                                            "54",
                                            "ReduceOperation",
                                            "reduce_func_2",
                                            _sha256_hash_strings(
                                                [
                                                    "parent_function_call_id_value",
                                                    "199",
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
                                    "parent_function_call_id_value",
                                    "52",
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
                                    "parent_function_call_id_value",
                                    "53",
                                    "MAP_OPERATION:func_map_1",
                                    _sha256_hash_strings(
                                        [
                                            "parent_function_call_id_value",
                                            "54",
                                            "ReduceOperation",
                                            "reduce_func_2",
                                            _sha256_hash_strings(
                                                [
                                                    "parent_function_call_id_value",
                                                    "199",
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
                                            "parent_function_call_id_value",
                                            "54",
                                            "ReduceOperation",
                                            "reduce_func_2",
                                            _sha256_hash_strings(
                                                [
                                                    "parent_function_call_id_value",
                                                    "199",
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
                                                    "parent_function_call_id_value",
                                                    "199",
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
                result_node = to_durable_awaitable_tree(
                    node=case["node"],
                    parent_function_call_id="parent_function_call_id_value",
                    awaitable_sequence_numbers=case["awaitable_sequence_numbers"],
                )
                self.assertEqual(result_node, case["expected_node"])


if __name__ == "__main__":
    unittest.main()
