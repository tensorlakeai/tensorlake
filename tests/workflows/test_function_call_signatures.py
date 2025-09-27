import unittest
from typing import Any

import parameterized

import tensorlake.workflows.interface as tensorlake
from tensorlake.workflows.remote.deploy import deploy


@tensorlake.api()
@tensorlake.function()
def test_function_with_no_args_api(_: Any) -> str:
    return test_function_with_no_args_internal()


@tensorlake.function()
def test_function_with_no_args_internal() -> str:
    return "success"


@tensorlake.api()
@tensorlake.function()
def test_function_with_only_ctx_arg_api(
    ctx: tensorlake.RequestContext, payload: Any
) -> str:
    return test_function_with_only_ctx_arg_internal(ctx)


@tensorlake.function()
def test_function_with_only_ctx_arg_internal(ctx: tensorlake.RequestContext) -> str:
    return f"ctx id: {ctx.request_id}"


@tensorlake.api()
@tensorlake.function()
def test_only_positional_args_api(args: dict[str, Any]) -> str:
    return test_only_positional_args_internal(
        args["a"], args["b"], args["c"], args["d"]
    )


@tensorlake.function()
def test_only_positional_args_internal(a: int, b: str, c: float, d: bool, /) -> str:
    return f"a={a},b={b},c={c},d={d}"


@tensorlake.api()
@tensorlake.function()
def test_only_kwargs_api(args: dict[str, Any]) -> str:
    return test_only_kwargs_internal(**args)


@tensorlake.function()
def test_only_kwargs_internal(*, a: int, b: str, c: float, d: bool) -> str:
    return f"a={a},b={b},c={c},d={d}"


@tensorlake.api()
@tensorlake.function()
def test_mixed_args_api(args: dict[str, Any]) -> str:
    return test_mixed_args_internal(args["a"], args["b"], c=args["c"], d=args["d"])


@tensorlake.function()
def test_mixed_args_internal(a: int, b: str, /, c: float, *, d: bool) -> str:
    return f"a={a},b={b},c={c},d={d}"


class TestRegularFunctionCallSignatures(unittest.TestCase):
    @parameterized.parameterized.expand([(False), (True)])
    def test_function_with_no_args(self, is_remote: bool):
        if is_remote:
            deploy(__file__)

        request: tensorlake.Request = tensorlake.call_api(
            test_function_with_no_args_api, None, remote=is_remote
        )
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([(False), (True)])
    def test_function_with_only_ctx_arg(self, is_remote: bool):
        if is_remote:
            deploy(__file__)

        request: tensorlake.Request = tensorlake.call_api(
            test_function_with_only_ctx_arg_api, None, remote=is_remote
        )
        self.assertEqual(request.output(), f"ctx id: {request.id}")

    @parameterized.parameterized.expand([(False), (True)])
    def test_only_positional_args(self, is_remote: bool):
        if is_remote:
            deploy(__file__)

        request: tensorlake.Request = tensorlake.call_api(
            test_only_positional_args_api,
            {"a": 42, "b": "hello", "c": 3.14, "d": True},
            remote=is_remote,
        )
        self.assertEqual(request.output(), "a=42,b=hello,c=3.14,d=True")

    @parameterized.parameterized.expand([(False), (True)])
    def test_only_kwargs(self, is_remote: bool):
        if is_remote:
            deploy(__file__)

        request: tensorlake.Request = tensorlake.call_api(
            test_only_kwargs_api,
            {"a": 42, "b": "hello", "c": 3.14, "d": True},
            remote=is_remote,
        )
        self.assertEqual(request.output(), "a=42,b=hello,c=3.14,d=True")

    @parameterized.parameterized.expand([(False), (True)])
    def test_mixed_args(self, is_remote: bool):
        if is_remote:
            deploy(__file__)

        request1: tensorlake.Request = tensorlake.call_api(
            test_mixed_args_api,
            {"a": 1, "b": "x", "c": 2.71, "d": False},
            remote=is_remote,
        )
        self.assertEqual(request1.output(), "a=1,b=x,c=2.71,d=False")


if __name__ == "__main__":
    unittest.main()
