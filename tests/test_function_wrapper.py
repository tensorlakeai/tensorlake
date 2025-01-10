import unittest
from typing import List, Union

from pydantic import BaseModel

from tensorlake.functions_sdk.invocation_state.local_invocation_state import (
    LocalInvocationState,
)
from tensorlake.functions_sdk.functions import (
    GraphInvocationContext,
    TensorlakeFunctionWrapper,
    get_ctx,
    tensorlake_function,
    tensorlake_router,
)

TEST_GRAPH_CTX = GraphInvocationContext(
    invocation_id="123",
    graph_name="test",
    graph_version="1",
    invocation_state=LocalInvocationState(),
)


class TestFunctionWrapper(unittest.TestCase):
    def test_basic_features(self):
        @tensorlake_function()
        def extractor_a(url: str) -> str:
            """
            Random description of extractor_a
            """
            return "hello"

        extractor_wrapper = TensorlakeFunctionWrapper(extractor_a, TEST_GRAPH_CTX)
        result, err = extractor_wrapper.run_fn({"url": "foo"})
        self.assertEqual(result[0], "hello")

    def test_get_output_model(self):
        @tensorlake_function()
        def extractor_b(url: str) -> str:
            """
            Random description of extractor_b
            """
            return "hello"

        extractor_wrapper = TensorlakeFunctionWrapper(extractor_b, TEST_GRAPH_CTX)
        result = extractor_wrapper.get_output_model()
        self.assertEqual(result, str)

    def test_list_output_model(self):
        @tensorlake_function()
        def extractor_b(url: str) -> List[str]:
            """
            Random description of extractor_b
            """
            return ["hello", "world"]

        extractor_wrapper = TensorlakeFunctionWrapper(extractor_b, TEST_GRAPH_CTX)
        result = extractor_wrapper.get_output_model()
        self.assertEqual(result, str)

    def test_router_fn(self):
        @tensorlake_function()
        def func_a(x: int) -> int:
            return 6

        @tensorlake_function()
        def func_b(x: int) -> int:
            return 7

        @tensorlake_router()
        def router_fn(url: str) -> List[Union[func_a, func_b]]:
            """
            Random description of router_fn
            """
            return [func_a]

        router_wrapper = TensorlakeFunctionWrapper(router_fn, TEST_GRAPH_CTX)
        result, err = router_wrapper.run_router({"url": "foo"})
        self.assertEqual(result, ["func_a"])

    def test_accumulate(self):
        class AccumulatedState(BaseModel):
            x: int

        @tensorlake_function(accumulate=AccumulatedState)
        def accumulate_fn(acc: AccumulatedState, x: int) -> AccumulatedState:
            acc.x += x
            return acc

        wrapper = TensorlakeFunctionWrapper(accumulate_fn, TEST_GRAPH_CTX)
        result, err = wrapper.run_fn(acc=AccumulatedState(x=12), input={"x": 1})
        self.assertEqual(result[0].x, 13)

    def test_get_ctx(self):
        @tensorlake_function()
        def extractor_c(url: str) -> str:
            ctx = get_ctx()  # type: ignore
            ctx.invocation_state.set("foo", "bar")
            foo_val = ctx.invocation_state.get("foo")
            return ctx.invocation_id

        extractor_wrapper = TensorlakeFunctionWrapper(extractor_c, TEST_GRAPH_CTX)
        result, _ = extractor_wrapper.run_fn({"url": "foo"})
        self.assertEqual(result[0], "123")


if __name__ == "__main__":
    unittest.main()
