import unittest
from typing import Any, List

import parameterized
from pydantic import BaseModel

from tensorlake.applications import (
    Request,
    RequestFailureException,
    application,
    function,
)
from tensorlake.applications.remote.deploy import deploy_applications
from tensorlake.applications.applications import run_application


class AccumulatedState(BaseModel):
    sum: int = 0


@application()
@function()
def success_api_function_awaitable_collection(x: int) -> AccumulatedState:
    seq = transform_int_to_accumulated_state.map(generate_seq(x))
    return accumulate_reduce.reduce(seq, AccumulatedState(sum=0))


@application()
@function()
def success_api_function_value_collection(x: int) -> AccumulatedState:
    seq = [transform_int_to_accumulated_state(i) for i in generate_seq(x)]
    return accumulate_reduce.reduce(seq, AccumulatedState(sum=0))


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


@application()
@function()
def fail_api_function(x: int) -> AccumulatedState:
    seq = [transform_int_to_accumulated_state(i) for i in generate_seq(x)]
    return accumulate_reduce_fail_at_3.awaitable.reduce(seq, AccumulatedState(sum=0))


@function()
def accumulate_reduce_fail_at_3(
    acc: AccumulatedState, y: AccumulatedState
) -> AccumulatedState:
    if y.sum == 3:
        raise ValueError("Intentional failure at 3")
    acc.sum += y.sum
    return acc


@application()
@function()
def api_reduce_no_items_no_initial(_: Any) -> AccumulatedState:
    return accumulate_reduce.awaitable.reduce([])


@application()
@function()
def api_reduce_no_items_with_initial(_: Any) -> AccumulatedState:
    return accumulate_reduce.awaitable.reduce([], AccumulatedState(sum=10))


@application()
@function()
def api_reduce_one_value_item(_: Any) -> AccumulatedState:
    return accumulate_reduce.awaitable.reduce([AccumulatedState(sum=10)])


@application()
@function()
def api_reduce_one_awaitable_item(_: Any) -> AccumulatedState:
    return accumulate_reduce.awaitable.reduce([generate_single_value.awaitable()])


@function()
def generate_single_value() -> AccumulatedState:
    return AccumulatedState(sum=7)


@application()
@function()
def api_reduce_mapped_collection_nonblocking(_: Any) -> AccumulatedState:
    mapped_collection = transform_int_to_accumulated_state.awaitable.map([1, 2, 4])
    return accumulate_reduce.awaitable.reduce(mapped_collection).run().result()


@application()
@function()
def api_reduce_mapped_collection_tailcall(_: Any) -> AccumulatedState:
    mapped_collection = transform_int_to_accumulated_state.awaitable.map([1, 2, 4])
    return accumulate_reduce.awaitable.reduce(mapped_collection)


@application()
@function()
def api_reduce_of_reduced_list(_: Any) -> str:
    reduced_str_1 = concat_strs.awaitable.reduce(["1", "2", "4"], "")
    reduced_str_2 = concat_strs.awaitable.reduce(["1", "3", "5"], "")
    reduced_str_3 = concat_strs.awaitable.reduce(["1", "4", "6"])
    list_of_reduced_strs = [
        reduced_str_1,
        reduced_str_2,
        reduced_str_3,
    ]
    return concat_strs.awaitable.reduce(list_of_reduced_strs)


@function()
def concat_strs(acc: str, y: str) -> str:
    return acc + y


@application()
@function()
def api_reduce_of_mapped_collections(_: Any) -> str:
    mapped_collection_1 = int_to_str.awaitable.map([1, 2, 4])
    mapped_collection_2 = int_to_str.awaitable.map([1, 3, 5])
    mapped_collection_3 = int_to_str.awaitable.map([1, 4, 6])
    list_of_reduced_strs = [
        concat_strs.awaitable.reduce(mapped_collection_1),
        concat_strs.awaitable.reduce(mapped_collection_2),
        concat_strs.awaitable.reduce(mapped_collection_3),
    ]
    return concat_strs.awaitable.reduce(list_of_reduced_strs)


@function()
def int_to_str(x: int) -> str:
    return str(x)


class TestReduce(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_success_function_call_collection(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            success_api_function_awaitable_collection, 6, remote=is_remote
        )
        result: AccumulatedState = request.output()
        self.assertEqual(result.sum, 15)  # 0 + 1 + 2 + 3 + 4 + 5

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_success_value_collection(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            success_api_function_value_collection, 6, remote=is_remote
        )
        result: AccumulatedState = request.output()
        self.assertEqual(result.sum, 15)  # 0 + 1 + 2 + 3 + 4 + 5

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_failure(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(fail_api_function, 6, remote=is_remote)
        self.assertRaises(RequestFailureException, request.output)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_reduce_nothing(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            api_reduce_no_items_no_initial, None, remote=is_remote
        )
        self.assertRaises(RequestFailureException, request.output)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_reduce_initial(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            api_reduce_no_items_with_initial,
            None,
            remote=is_remote,
        )
        result: AccumulatedState = request.output()
        self.assertEqual(result.sum, 10)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_reduce_one_value_item(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            api_reduce_one_value_item,
            None,
            remote=is_remote,
        )
        result: AccumulatedState = request.output()
        self.assertEqual(result.sum, 10)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_reduce_one_function_call_item(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            api_reduce_one_awaitable_item,
            None,
            remote=is_remote,
        )
        result: AccumulatedState = request.output()
        self.assertEqual(result.sum, 7)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_reduce_mapped_collection_nonblocking(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            api_reduce_mapped_collection_nonblocking,
            None,
            remote=is_remote,
        )
        result: AccumulatedState = request.output()
        self.assertEqual(result.sum, 7)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_reduce_mapped_collection_tailcall(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            api_reduce_mapped_collection_tailcall,
            None,
            remote=is_remote,
        )
        result: AccumulatedState = request.output()
        self.assertEqual(result.sum, 7)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_reduce_of_reduced_list(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            api_reduce_of_reduced_list,
            None,
            remote=is_remote,
        )
        self.assertEqual(
            request.output(),
            "124135146",
        )

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_reduce_of_mapped_collections(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            api_reduce_of_mapped_collections,
            None,
            remote=is_remote,
        )
        self.assertEqual(
            request.output(),
            "124135146",
        )


if __name__ == "__main__":
    unittest.main()
