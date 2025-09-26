import unittest
from typing import Any, List

import parameterized
from pydantic import BaseModel

import tensorlake.workflows.interface as tensorlake
from tensorlake.workflows.remote.deploy import deploy


class AccumulatedState(BaseModel):
    sum: int = 0


@tensorlake.api()
@tensorlake.function()
def success_api_function_function_call_collection(x: int) -> AccumulatedState:
    seq = tensorlake.map(transform_int_to_accumulated_state, generate_seq(x))
    return tensorlake.reduce(accumulate_reduce, seq, AccumulatedState(sum=0))


@tensorlake.api()
@tensorlake.function()
def success_api_function_value_collection(x: int) -> AccumulatedState:
    seq = [transform_int_to_accumulated_state(i) for i in generate_seq(x)]
    return tensorlake.reduce(accumulate_reduce, seq, AccumulatedState(sum=0))


# TODO: We need to allow a future as reducer input so tensorlake functions can generate sequences.
def generate_seq(x: int) -> List[int]:
    return [i for i in range(x)]


@tensorlake.function()
def transform_int_to_accumulated_state(x: int) -> AccumulatedState:
    return AccumulatedState(sum=x)


@tensorlake.function()
def accumulate_reduce(acc: AccumulatedState, y: AccumulatedState) -> AccumulatedState:
    acc.sum += y.sum
    return acc


@tensorlake.function()
def store_result(acc: AccumulatedState) -> int:
    return acc.sum


@tensorlake.api()
@tensorlake.function()
def fail_api_function(x: int) -> AccumulatedState:
    seq = [transform_int_to_accumulated_state(i) for i in generate_seq(x)]
    return tensorlake.reduce(accumulate_reduce_fail_at_3, seq, AccumulatedState(sum=0))


@tensorlake.function()
def accumulate_reduce_fail_at_3(
    acc: AccumulatedState, y: AccumulatedState
) -> AccumulatedState:
    if y.sum == 3:
        raise ValueError("Intentional failure at 3")
    acc.sum += y.sum
    return acc


@tensorlake.api()
@tensorlake.function()
def api_reduce_no_items_no_initial(_: Any) -> AccumulatedState:
    return tensorlake.reduce(accumulate_reduce, [])


@tensorlake.api()
@tensorlake.function()
def api_reduce_no_items_with_initial(_: Any) -> AccumulatedState:
    return tensorlake.reduce(accumulate_reduce, [], AccumulatedState(sum=10))


@tensorlake.api()
@tensorlake.function()
def api_reduce_one_value_item(_: Any) -> AccumulatedState:
    return tensorlake.reduce(accumulate_reduce, [AccumulatedState(sum=10)])


@tensorlake.api()
@tensorlake.function()
def api_reduce_one_function_call_item(_: Any) -> AccumulatedState:
    return tensorlake.reduce(accumulate_reduce, [generate_single_value()])


@tensorlake.function()
def generate_single_value() -> AccumulatedState:
    return AccumulatedState(sum=7)


class TestReduce(unittest.TestCase):
    @parameterized.parameterized.expand([(True), (False)])
    def test_success_function_call_collection(self, is_remote: bool):
        if is_remote:
            deploy(__file__)

        request: tensorlake.Request = tensorlake.call_api(
            success_api_function_function_call_collection, 6, remote=is_remote
        )
        result: AccumulatedState = request.output()
        self.assertEqual(result.sum, 15)  # 0 + 1 + 2 + 3 + 4 + 5

    @parameterized.parameterized.expand([(True), (False)])
    def test_success_value_collection(self, is_remote: bool):
        if is_remote:
            deploy(__file__)

        request: tensorlake.Request = tensorlake.call_api(
            success_api_function_value_collection, 6, remote=is_remote
        )
        result: AccumulatedState = request.output()
        self.assertEqual(result.sum, 15)  # 0 + 1 + 2 + 3 + 4 + 5

    @parameterized.parameterized.expand([(True), (False)])
    def test_failure(self, is_remote: bool):
        if is_remote:
            deploy(__file__)

        request: tensorlake.Request = tensorlake.call_api(
            fail_api_function, 6, remote=is_remote
        )
        self.assertRaises(tensorlake.RequestFailureException, request.output)

    @parameterized.parameterized.expand([(True), (False)])
    def test_reduce_nothing(self, is_remote: bool):
        if is_remote:
            deploy(__file__)

        request: tensorlake.Request = tensorlake.call_api(
            api_reduce_no_items_no_initial, None, remote=is_remote
        )
        self.assertRaises(tensorlake.RequestFailureException, request.output)

    @parameterized.parameterized.expand([(True), (False)])
    def test_reduce_initial(self, is_remote: bool):
        if is_remote:
            deploy(__file__)

        request: tensorlake.Request = tensorlake.call_api(
            api_reduce_no_items_with_initial,
            None,
            remote=is_remote,
        )
        result: AccumulatedState = request.output()
        self.assertEqual(result.sum, 10)

    @parameterized.parameterized.expand([(True), (False)])
    def test_reduce_one_value_item(self, is_remote: bool):
        if is_remote:
            deploy(__file__)

        request: tensorlake.Request = tensorlake.call_api(
            api_reduce_one_value_item,
            None,
            remote=is_remote,
        )
        result: AccumulatedState = request.output()
        self.assertEqual(result.sum, 10)

    @parameterized.parameterized.expand([(True), (False)])
    def test_reduce_one_function_call_item(self, is_remote: bool):
        if is_remote:
            deploy(__file__)

        request: tensorlake.Request = tensorlake.call_api(
            api_reduce_one_function_call_item,
            None,
            remote=is_remote,
        )
        result: AccumulatedState = request.output()
        self.assertEqual(result.sum, 7)


if __name__ == "__main__":
    unittest.main()
