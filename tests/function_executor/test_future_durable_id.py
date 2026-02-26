import unittest

from tensorlake.applications import InternalError
from tensorlake.applications.interface.futures import (
    FunctionCallFuture,
    Future,
    ListFuture,
    ReduceOperationFuture,
    _FutureListKind,
    _FutureListMetadata,
)
from tensorlake.function_executor.allocation_runner.sdk_algorithms import (
    FutureInfo,
    _sha256_hash_strings,
    future_durable_id,
)


class TestFutureDurableId(unittest.TestCase):
    def test_function_call_future_with_plain_args_only(self):
        result = future_durable_id(
            future=FunctionCallFuture(
                id="a1",
                function_name="func_1",
                args=[1, 2],
                kwargs={"a": 3},
            ),
            parent_function_call_id="parent_function_call_id_123",
            previous_future_durable_id="previous_awaitable_id_456",
            future_infos={},
        )
        self.assertEqual(
            result,
            _sha256_hash_strings(
                [
                    "parent_function_call_id_123",
                    "previous_awaitable_id_456",
                    "FunctionCall",
                    "func_1",
                ]
            ),
        )

    def test_function_call_future_with_future_arg(self):
        child_fc = FunctionCallFuture(
            id="fc_1",
            function_name="func_2",
            args=[1],
            kwargs={},
        )
        result = future_durable_id(
            future=FunctionCallFuture(
                id="a1",
                function_name="func_1",
                args=[child_fc, 2],
                kwargs={"a": 3},
            ),
            parent_function_call_id="parent_function_call_id_123",
            previous_future_durable_id="previous_awaitable_id_456",
            future_infos={
                "fc_1": FutureInfo(
                    future=child_fc,
                    durable_id="durable_fc_1",
                    map_future_output=None,
                    reduce_future_output=None,
                ),
            },
        )
        self.assertEqual(
            result,
            _sha256_hash_strings(
                [
                    "parent_function_call_id_123",
                    "previous_awaitable_id_456",
                    "FunctionCall",
                    "func_1",
                    "durable_fc_1",
                ]
            ),
        )

    def test_function_call_future_with_future_kwargs_sorted_alphabetically(self):
        child_fc_1 = FunctionCallFuture(
            id="fc_1",
            function_name="func_2",
            args=[1],
            kwargs={},
        )
        child_fc_2 = FunctionCallFuture(
            id="fc_2",
            function_name="func_3",
            args=[],
            kwargs={},
        )
        result = future_durable_id(
            future=FunctionCallFuture(
                id="a1",
                function_name="func_1",
                args=[],
                kwargs={
                    "c": child_fc_1,
                    "a": child_fc_2,
                },
            ),
            parent_function_call_id="parent_function_call_id_123",
            previous_future_durable_id="previous_awaitable_id_456",
            future_infos={
                "fc_1": FutureInfo(
                    future=child_fc_1,
                    durable_id="durable_fc_1",
                    map_future_output=None,
                    reduce_future_output=None,
                ),
                "fc_2": FutureInfo(
                    future=child_fc_2,
                    durable_id="durable_fc_2",
                    map_future_output=None,
                    reduce_future_output=None,
                ),
            },
        )
        self.assertEqual(
            result,
            _sha256_hash_strings(
                [
                    "parent_function_call_id_123",
                    "previous_awaitable_id_456",
                    "FunctionCall",
                    "func_1",
                    # "a" sorts before "c"
                    "durable_fc_2",
                    "durable_fc_1",
                ]
            ),
        )

    def test_function_call_future_with_future_args_and_kwargs(self):
        child_fc_1 = FunctionCallFuture(
            id="fc_1",
            function_name="func_2",
            args=[1],
            kwargs={},
        )
        child_fc_2 = FunctionCallFuture(
            id="fc_2",
            function_name="func_3",
            args=[],
            kwargs={},
        )
        result = future_durable_id(
            future=FunctionCallFuture(
                id="a1",
                function_name="func_1",
                args=[child_fc_1],
                kwargs={"b": child_fc_2},
            ),
            parent_function_call_id="parent_function_call_id_123",
            previous_future_durable_id="previous_awaitable_id_456",
            future_infos={
                "fc_1": FutureInfo(
                    future=child_fc_1,
                    durable_id="durable_fc_1",
                    map_future_output=None,
                    reduce_future_output=None,
                ),
                "fc_2": FutureInfo(
                    future=child_fc_2,
                    durable_id="durable_fc_2",
                    map_future_output=None,
                    reduce_future_output=None,
                ),
            },
        )
        self.assertEqual(
            result,
            _sha256_hash_strings(
                [
                    "parent_function_call_id_123",
                    "previous_awaitable_id_456",
                    "FunctionCall",
                    "func_1",
                    "durable_fc_1",
                    "durable_fc_2",
                ]
            ),
        )

    def test_list_future_with_function_call_future_items(self):
        child_fc_1 = FunctionCallFuture(
            id="fc_1",
            function_name="func_2",
            args=[1],
            kwargs={},
        )
        child_fc_2 = FunctionCallFuture(
            id="fc_2",
            function_name="func_3",
            args=[],
            kwargs={},
        )
        result = future_durable_id(
            future=ListFuture(
                id="l1",
                items=[child_fc_1, child_fc_2],
                metadata=_FutureListMetadata(
                    kind=_FutureListKind.MAP_OPERATION,
                    function_name="map_func",
                ),
            ),
            parent_function_call_id="parent_function_call_id_123",
            previous_future_durable_id="previous_awaitable_id_456",
            future_infos={
                "fc_1": FutureInfo(
                    future=child_fc_1,
                    durable_id="durable_fc_1",
                    map_future_output=None,
                    reduce_future_output=None,
                ),
                "fc_2": FutureInfo(
                    future=child_fc_2,
                    durable_id="durable_fc_2",
                    map_future_output=None,
                    reduce_future_output=None,
                ),
            },
        )
        self.assertEqual(
            result,
            _sha256_hash_strings(
                [
                    "parent_function_call_id_123",
                    "previous_awaitable_id_456",
                    "MAP_OPERATION:map_func",
                    "durable_fc_1",
                    "durable_fc_2",
                ]
            ),
        )

    def test_list_future_with_mixed_items(self):
        child_fc_1 = FunctionCallFuture(
            id="fc_1",
            function_name="func_2",
            args=[1],
            kwargs={},
        )
        child_fc_2 = FunctionCallFuture(
            id="fc_2",
            function_name="func_3",
            args=[],
            kwargs={},
        )
        result = future_durable_id(
            future=ListFuture(
                id="l1",
                items=[child_fc_1, 42, "plain", child_fc_2],
                metadata=_FutureListMetadata(
                    kind=_FutureListKind.MAP_OPERATION,
                    function_name="map_func",
                ),
            ),
            parent_function_call_id="parent_function_call_id_123",
            previous_future_durable_id="previous_awaitable_id_456",
            future_infos={
                "fc_1": FutureInfo(
                    future=child_fc_1,
                    durable_id="durable_fc_1",
                    map_future_output=None,
                    reduce_future_output=None,
                ),
                "fc_2": FutureInfo(
                    future=child_fc_2,
                    durable_id="durable_fc_2",
                    map_future_output=None,
                    reduce_future_output=None,
                ),
            },
        )
        self.assertEqual(
            result,
            _sha256_hash_strings(
                [
                    "parent_function_call_id_123",
                    "previous_awaitable_id_456",
                    "MAP_OPERATION:map_func",
                    "durable_fc_1",
                    # 42 and "plain" are not Futures, don't contribute to hash
                    "durable_fc_2",
                ]
            ),
        )

    def test_list_future_with_list_future_items(self):
        inner_list = ListFuture(
            id="list_1",
            items=[],
            metadata=_FutureListMetadata(
                kind=_FutureListKind.MAP_OPERATION,
                function_name="map_func",
            ),
        )
        result = future_durable_id(
            future=ListFuture(
                id="l2",
                items=inner_list,
                metadata=_FutureListMetadata(
                    kind=_FutureListKind.MAP_OPERATION,
                    function_name="outer_map_func",
                ),
            ),
            parent_function_call_id="parent_function_call_id_123",
            previous_future_durable_id="previous_awaitable_id_456",
            future_infos={
                "list_1": FutureInfo(
                    future=inner_list,
                    durable_id="durable_list_1",
                    map_future_output=None,
                    reduce_future_output=None,
                ),
            },
        )
        self.assertEqual(
            result,
            _sha256_hash_strings(
                [
                    "parent_function_call_id_123",
                    "previous_awaitable_id_456",
                    "MAP_OPERATION:outer_map_func",
                    "durable_list_1",
                ]
            ),
        )

    def test_reduce_operation_future_with_plain_initial_and_plain_items(self):
        result = future_durable_id(
            future=ReduceOperationFuture(
                id="r1",
                function_name="reduce_fn",
                items=[1, 2, 3],
                initial=0,
            ),
            parent_function_call_id="parent_function_call_id_123",
            previous_future_durable_id="previous_awaitable_id_456",
            future_infos={},
        )
        self.assertEqual(
            result,
            _sha256_hash_strings(
                [
                    "parent_function_call_id_123",
                    "previous_awaitable_id_456",
                    "ReduceOperation",
                    "reduce_fn",
                    # initial=0 is not a Future, nothing added
                    # items [1,2,3] are not Futures, nothing added
                ]
            ),
        )

    def test_reduce_operation_future_with_future_initial_and_future_items(self):
        child_fc_1 = FunctionCallFuture(
            id="fc_1",
            function_name="func_2",
            args=[1],
            kwargs={},
        )
        child_fc_2 = FunctionCallFuture(
            id="fc_2",
            function_name="func_3",
            args=[],
            kwargs={},
        )
        child_fc_3 = FunctionCallFuture(
            id="fc_3",
            function_name="func_4",
            args=[4],
            kwargs={},
        )
        result = future_durable_id(
            future=ReduceOperationFuture(
                id="r1",
                function_name="reduce_fn",
                items=[child_fc_1, 42, child_fc_2],
                initial=child_fc_3,
            ),
            parent_function_call_id="parent_function_call_id_123",
            previous_future_durable_id="previous_awaitable_id_456",
            future_infos={
                "fc_1": FutureInfo(
                    future=child_fc_1,
                    durable_id="durable_fc_1",
                    map_future_output=None,
                    reduce_future_output=None,
                ),
                "fc_2": FutureInfo(
                    future=child_fc_2,
                    durable_id="durable_fc_2",
                    map_future_output=None,
                    reduce_future_output=None,
                ),
                "fc_3": FutureInfo(
                    future=child_fc_3,
                    durable_id="durable_fc_3",
                    map_future_output=None,
                    reduce_future_output=None,
                ),
            },
        )
        self.assertEqual(
            result,
            _sha256_hash_strings(
                [
                    "parent_function_call_id_123",
                    "previous_awaitable_id_456",
                    "ReduceOperation",
                    "reduce_fn",
                    "durable_fc_3",  # initial
                    "durable_fc_1",  # items[0]
                    # 42 is not a Future, nothing added
                    "durable_fc_2",  # items[2]
                ]
            ),
        )

    def test_reduce_operation_future_with_list_future_items(self):
        inner_list = ListFuture(
            id="list_1",
            items=[],
            metadata=_FutureListMetadata(
                kind=_FutureListKind.MAP_OPERATION,
                function_name="map_func",
            ),
        )
        result = future_durable_id(
            future=ReduceOperationFuture(
                id="r1",
                function_name="reduce_fn",
                items=inner_list,
                initial=0,
            ),
            parent_function_call_id="parent_function_call_id_123",
            previous_future_durable_id="previous_awaitable_id_456",
            future_infos={
                "list_1": FutureInfo(
                    future=inner_list,
                    durable_id="durable_list_1",
                    map_future_output=None,
                    reduce_future_output=None,
                ),
            },
        )
        self.assertEqual(
            result,
            _sha256_hash_strings(
                [
                    "parent_function_call_id_123",
                    "previous_awaitable_id_456",
                    "ReduceOperation",
                    "reduce_fn",
                    # initial=0 is not a Future, nothing added
                    "durable_list_1",
                ]
            ),
        )

    def test_reduce_operation_future_with_future_initial_and_list_future_items(self):
        child_fc_3 = FunctionCallFuture(
            id="fc_3",
            function_name="func_4",
            args=[4],
            kwargs={},
        )
        inner_list = ListFuture(
            id="list_1",
            items=[],
            metadata=_FutureListMetadata(
                kind=_FutureListKind.MAP_OPERATION,
                function_name="map_func",
            ),
        )
        result = future_durable_id(
            future=ReduceOperationFuture(
                id="r1",
                function_name="reduce_fn",
                items=inner_list,
                initial=child_fc_3,
            ),
            parent_function_call_id="parent_function_call_id_123",
            previous_future_durable_id="previous_awaitable_id_456",
            future_infos={
                "fc_3": FutureInfo(
                    future=child_fc_3,
                    durable_id="durable_fc_3",
                    map_future_output=None,
                    reduce_future_output=None,
                ),
                "list_1": FutureInfo(
                    future=inner_list,
                    durable_id="durable_list_1",
                    map_future_output=None,
                    reduce_future_output=None,
                ),
            },
        )
        self.assertEqual(
            result,
            _sha256_hash_strings(
                [
                    "parent_function_call_id_123",
                    "previous_awaitable_id_456",
                    "ReduceOperation",
                    "reduce_fn",
                    "durable_fc_3",  # initial
                    "durable_list_1",  # items (ListFuture)
                ]
            ),
        )

    def test_unexpected_future_type_raises_error(self):
        # A plain Future (not FunctionCallFuture/ListFuture/ReduceOperationFuture)
        # should trigger the else branch.
        with self.assertRaises(InternalError):
            future_durable_id(
                future=Future(
                    id="a1",
                ),
                parent_function_call_id="parent_id",
                previous_future_durable_id="prev_id",
                future_infos={},
            )

    def test_missing_future_info_raises_error(self):
        child_fc = FunctionCallFuture(
            id="child_1",
            function_name="func_2",
            args=[],
            kwargs={},
        )
        # future_infos is missing entry for "child_1"
        with self.assertRaises(InternalError):
            future_durable_id(
                future=FunctionCallFuture(
                    id="a1",
                    function_name="func_1",
                    args=[child_fc],
                    kwargs={},
                ),
                parent_function_call_id="parent_id",
                previous_future_durable_id="prev_id",
                future_infos={},
            )


if __name__ == "__main__":
    unittest.main()
