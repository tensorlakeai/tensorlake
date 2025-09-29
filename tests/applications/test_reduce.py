import unittest
from typing import Any, List

import parameterized
from pydantic import BaseModel

from tensorlake.applications import (
    Request,
    RequestFailureException,
    api,
    call_api,
    function,
)
from tensorlake.applications import map as tl_map
from tensorlake.applications import reduce as tl_reduce
from tensorlake.applications.remote.deploy import deploy


class AccumulatedState(BaseModel):
    sum: int = 0


@api()
@function()
def success_api_function_function_call_collection(x: int) -> AccumulatedState:
    seq = tl_map(transform_int_to_accumulated_state, generate_seq(x))
    return tl_reduce(accumulate_reduce, seq, AccumulatedState(sum=0))


@api()
@function()
def success_api_function_value_collection(x: int) -> AccumulatedState:
    seq = [transform_int_to_accumulated_state(i) for i in generate_seq(x)]
    return tl_reduce(accumulate_reduce, seq, AccumulatedState(sum=0))


# TODO: We need to allow a future as reducer input so tensorlake functions can generate sequences.
def generate_seq(x: int) -> List[int]:
    return [i for i in range(x)]


@function()
def transform_int_to_accumulated_state(x: int) -> AccumulatedState:
    return AccumulatedState(sum=x)


@function()
def accumulate_reduce(acc: AccumulatedState, y: AccumulatedState) -> AccumulatedState:
    acc.sum += y.sum
    return acc


@function()
def store_result(acc: AccumulatedState) -> int:
    return acc.sum


@api()
@function()
def fail_api_function(x: int) -> AccumulatedState:
    seq = [transform_int_to_accumulated_state(i) for i in generate_seq(x)]
    return tl_reduce(accumulate_reduce_fail_at_3, seq, AccumulatedState(sum=0))


@function()
def accumulate_reduce_fail_at_3(
    acc: AccumulatedState, y: AccumulatedState
) -> AccumulatedState:
    if y.sum == 3:
        raise ValueError("Intentional failure at 3")
    acc.sum += y.sum
    return acc


@api()
@function()
def api_reduce_no_items_no_initial(_: Any) -> AccumulatedState:
    return tl_reduce(accumulate_reduce, [])


@api()
@function()
def api_reduce_no_items_with_initial(_: Any) -> AccumulatedState:
    return tl_reduce(accumulate_reduce, [], AccumulatedState(sum=10))


@api()
@function()
def api_reduce_one_value_item(_: Any) -> AccumulatedState:
    return tl_reduce(accumulate_reduce, [AccumulatedState(sum=10)])


@api()
@function()
def api_reduce_one_function_call_item(_: Any) -> AccumulatedState:
    return tl_reduce(accumulate_reduce, [generate_single_value()])


@function()
def generate_single_value() -> AccumulatedState:
    return AccumulatedState(sum=7)


class TestReduce(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_success_function_call_collection(self, _: str, is_remote: bool):
        if is_remote:
            deploy(__file__)

        request: Request = call_api(
            success_api_function_function_call_collection, 6, remote=is_remote
        )
        result: AccumulatedState = request.output()
        self.assertEqual(result.sum, 15)  # 0 + 1 + 2 + 3 + 4 + 5

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_success_value_collection(self, _: str, is_remote: bool):
        if is_remote:
            deploy(__file__)

        request: Request = call_api(
            success_api_function_value_collection, 6, remote=is_remote
        )
        result: AccumulatedState = request.output()
        self.assertEqual(result.sum, 15)  # 0 + 1 + 2 + 3 + 4 + 5

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_failure(self, _: str, is_remote: bool):
        if is_remote:
            deploy(__file__)

        request: Request = call_api(fail_api_function, 6, remote=is_remote)
        self.assertRaises(RequestFailureException, request.output)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_reduce_nothing(self, _: str, is_remote: bool):
        if is_remote:
            deploy(__file__)

        request: Request = call_api(
            api_reduce_no_items_no_initial, None, remote=is_remote
        )
        self.assertRaises(RequestFailureException, request.output)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_reduce_initial(self, _: str, is_remote: bool):
        if is_remote:
            deploy(__file__)

        request: Request = call_api(
            api_reduce_no_items_with_initial,
            None,
            remote=is_remote,
        )
        result: AccumulatedState = request.output()
        self.assertEqual(result.sum, 10)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_reduce_one_value_item(self, _: str, is_remote: bool):
        if is_remote:
            deploy(__file__)

        request: Request = call_api(
            api_reduce_one_value_item,
            None,
            remote=is_remote,
        )
        result: AccumulatedState = request.output()
        self.assertEqual(result.sum, 10)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_reduce_one_function_call_item(self, _: str, is_remote: bool):
        if is_remote:
            deploy(__file__)

        request: Request = call_api(
            api_reduce_one_function_call_item,
            None,
            remote=is_remote,
        )
        result: AccumulatedState = request.output()
        self.assertEqual(result.sum, 7)


if __name__ == "__main__":
    unittest.main()
