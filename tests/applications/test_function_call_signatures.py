import unittest
from typing import Any

import parameterized
import validate_all_applications
from models import DirModel, FileModel

from tensorlake.applications import File, Request, RequestError, application, function
from tensorlake.applications.applications import run_application
from tensorlake.applications.remote.deploy import deploy_applications

# Makes the test case discoverable by unittest framework.
ValidateAllApplicationsTest: unittest.TestCase = validate_all_applications.define_test()


@application()
@function()
def test_function_with_no_args_api(_: Any) -> str:
    return test_function_with_no_args_internal()


@function()
def test_function_with_no_args_internal() -> str:
    return "success"


@application()
@function()
def test_function_returning_nothing_api(_: Any) -> str:
    return_value: Any = test_function_returning_nothing_internal()
    if return_value is not None:
        raise RequestError(f"Expected None return value, got: {return_value}")
    return "success"


@function()
def test_function_returning_nothing_internal() -> None:
    pass


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


@application()
@function()
def test_default_args_api(_: Any) -> str:
    test_default_args_internal(foo=100)
    return "success"


@function()
def test_default_args_internal(
    x: list[int] = [1, 2, 3], foo: int = 42, bar: str = "default"
) -> None:
    if x != [1, 2, 3]:
        raise RequestError(f"Expected x=[1,2,3], got: {x}")
    if foo != 100:
        raise RequestError(f"Expected foo=100, got: {foo}")
    if bar != "default":
        raise RequestError(f"Expected bar='default', got: {bar}")


@application()
@function()
def test_file_args_api(_: Any) -> str:
    # Don't return File objects directly from application function because we're
    # testing non-api (internal function calls) here. They work completely differently.
    test_file_args_internal(
        File(content=b"file_content_1", content_type="text/plain"),
        File(content=b"file_content_2", content_type="application/octet-stream"),
    )
    return "success"


@function()
def test_file_args_internal(file1: File, file2: File) -> None:
    # Don't return File objects because we're testing passing of File arguments,
    # not returning them.
    if file1.content != b"file_content_1":
        raise RequestError(f"File1 content mismatch: {file1.content}")
    if not isinstance(file1.content, bytes):
        raise RequestError(f"File1 content python type mismatch: {type(file1.content)}")
    if file1.content_type != "text/plain":
        raise RequestError(f"File1 content type mismatch: {file1.content_type}")
    if file2.content != b"file_content_2":
        raise RequestError(f"File2 content mismatch: {file2.content}")
    if not isinstance(file2.content, bytes):
        raise RequestError(f"File2 content python type mismatch: {type(file2.content)}")
    if file2.content_type != "application/octet-stream":
        raise RequestError(f"File2 content type mismatch: {file2.content_type}")


@application()
@function()
def test_file_return_value_api(_: Any) -> str:
    file: File = test_file_return_value_internal()
    if file.content != b"file_return_content":
        raise RequestError(f"File content mismatch: {file.content}")
    if not isinstance(file.content, bytes):
        raise RequestError(f"File content python type mismatch: {type(file.content)}")
    if file.content_type != "text/plain":
        raise RequestError(f"File content type mismatch: {file.content_type}")
    return "success"


@function()
def test_file_return_value_internal() -> File:
    return File(content=b"file_return_content", content_type="text/plain")


@application()
@function()
def test_pydantic_args_api(_: Any) -> str:
    test_pydantic_args_internal(
        DirModel(
            path="test_dir",
            files=[
                FileModel(path="file1.txt", size=123, is_read_only=True),
                FileModel(path="file2.txt", size=456, is_read_only=False),
            ],
        ),
        FileModel(path="single_file.txt", size=789, is_read_only=False),
    )
    return "success"


@function()
def test_pydantic_args_internal(dir: DirModel, file: FileModel) -> None:
    if dir.path != "test_dir":
        raise RequestError(f"Dir path mismatch: {dir.path}")
    if len(dir.files) != 2:
        raise RequestError(f"Dir files length mismatch: {len(dir.files)}")
    if (
        dir.files[0].path != "file1.txt"
        or dir.files[0].size != 123
        or not dir.files[0].is_read_only
    ):
        raise RequestError(f"Dir file1 mismatch: {dir.files[0]}")
    if (
        dir.files[1].path != "file2.txt"
        or dir.files[1].size != 456
        or dir.files[1].is_read_only
    ):
        raise RequestError(f"Dir file2 mismatch: {dir.files[1]}")
    if file.path != "single_file.txt" or file.size != 789 or file.is_read_only:
        raise RequestError(f"File mismatch: {file}")


@application()
@function()
def test_pydantic_return_value_api() -> str:
    dir: DirModel = test_pydantic_return_value_internal()
    if dir.path != "/returned/dir":
        raise RequestError(f"Dir path mismatch: {dir.path}")
    if len(dir.files) != 2:
        raise RequestError(f"Dir files length mismatch: {len(dir.files)}")
    if (
        dir.files[0].path != "/returned/dir/fileA.txt"
        or dir.files[0].size != 111
        or dir.files[0].is_read_only
    ):
        raise RequestError(f"Dir fileA mismatch: {dir.files[0]}")
    if (
        dir.files[1].path != "/returned/dir/fileB.txt"
        or dir.files[1].size != 222
        or not dir.files[1].is_read_only
    ):
        raise RequestError(f"Dir fileB mismatch: {dir.files[1]}")
    return "success"


@function()
def test_pydantic_return_value_internal() -> DirModel:
    dir = DirModel(
        path="/returned/dir",
        files=[
            FileModel(path="/returned/dir/fileA.txt", size=111, is_read_only=False),
            FileModel(path="/returned/dir/fileB.txt", size=222, is_read_only=True),
        ],
    )
    return dir


@function()
@application()
def test_set_arg_and_return_value_api() -> str:
    s: set[str] = test_set_arg_and_return_value_internal(
        {"apple", "banana", "cherry", "cherry"}
    )
    if not isinstance(s, set):
        raise RequestError(f"Set type mismatch: {type(s)}")
    if s != {"apple", "banana", "cherry"}:
        raise RequestError(f"Set content mismatch: {s}")
    return "success"


@function()
def test_set_arg_and_return_value_internal(arg: set[str]) -> set[str]:
    return arg


@function()
@application()
def test_tuple_arg_and_return_value_api() -> str:
    s: tuple[str, str, str, str] = test_tuple_arg_and_return_value_internal(
        ("apple", "banana", "cherry", "cherry")
    )
    if not isinstance(s, tuple):
        raise RequestError(f"Tuple type mismatch: {type(s)}")
    if s != ("apple", "banana", "cherry", "cherry"):
        raise RequestError(f"Tuple content mismatch: {s}")
    return "success"


@function()
def test_tuple_arg_and_return_value_internal(
    arg: tuple[str, str, str, str],
) -> tuple[str, str, str, str]:
    return arg


class TestRegularFunctionCallSignatures(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_function_with_no_args(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            test_function_with_no_args_api, is_remote, None
        )
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_function_returning_nothing(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            test_function_returning_nothing_api, is_remote, None
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

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_default_args(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            test_default_args_api,
            is_remote,
            None,
        )
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_file_args(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            test_file_args_api,
            is_remote,
            None,
        )
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_file_return_value(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            test_file_return_value_api,
            is_remote,
            None,
        )
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_pydantic_args(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            test_pydantic_args_api,
            is_remote,
            None,
        )
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_pydantic_return_value(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            test_pydantic_return_value_api,
            is_remote,
            None,
        )
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_set_arg_and_return_value(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            test_set_arg_and_return_value_api,
            is_remote,
        )
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_tuple_arg_and_return_value(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            test_tuple_arg_and_return_value_api,
            is_remote,
        )
        self.assertEqual(request.output(), "success")


if __name__ == "__main__":
    unittest.main()
