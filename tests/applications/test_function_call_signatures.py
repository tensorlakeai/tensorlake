import unittest
from typing import Any

import parameterized

from tensorlake.applications import Request, application, function
from tensorlake.applications.applications import run_application
from tensorlake.applications.remote.deploy import deploy_applications
from tensorlake.applications.validation import validate_loaded_applications


@application()
@function()
def test_function_with_no_args_api(_: Any) -> str:
    return test_function_with_no_args_internal()


@function()
def test_function_with_no_args_internal() -> str:
    return "success"


@application()
@function()
def test_only_positional_args_api(args: dict[str, Any]) -> str:
    return test_only_positional_args_internal(
        args["a"], args["b"], args["c"], args["d"]
    )


@function()
def test_only_positional_args_internal(a: int, b: str, c: float, d: bool, /) -> str:
    return f"a={a},b={b},c={c},d={d}"


@application()
@function()
def test_only_kwargs_api(args: dict[str, Any]) -> str:
    return test_only_kwargs_internal(**args)


@function()
def test_only_kwargs_internal(*, a: int, b: str, c: float, d: bool) -> str:
    return f"a={a},b={b},c={c},d={d}"


@application()
@function()
def test_mixed_args_api(args: dict[str, Any]) -> str:
    return test_mixed_args_internal(args["a"], args["b"], c=args["c"], d=args["d"])


@function()
def test_mixed_args_internal(a: int, b: str, /, c: float, *, d: bool) -> str:
    return f"a={a},b={b},c={c},d={d}"


class TestRegularFunctionCallSignatures(unittest.TestCase):
    def test_applications_are_valid(self):
        self.assertEqual(validate_loaded_applications(), [])

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_function_with_no_args(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            test_function_with_no_args_api, is_remote, None
        )
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_only_positional_args(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            test_only_positional_args_api,
            is_remote,
            {"a": 42, "b": "hello", "c": 3.14, "d": True},
        )
        self.assertEqual(request.output(), "a=42,b=hello,c=3.14,d=True")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_only_kwargs(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            test_only_kwargs_api,
            is_remote,
            {"a": 42, "b": "hello", "c": 3.14, "d": True},
        )
        self.assertEqual(request.output(), "a=42,b=hello,c=3.14,d=True")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_mixed_args(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request1: Request = run_application(
            test_mixed_args_api,
            is_remote,
            {"a": 1, "b": "x", "c": 2.71, "d": False},
        )
        self.assertEqual(request1.output(), "a=1,b=x,c=2.71,d=False")


if __name__ == "__main__":
    unittest.main()
