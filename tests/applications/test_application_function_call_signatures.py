import unittest
from typing import Any

import parameterized
import validate_all_applications
from models import DirModel, FileModel

from tensorlake.applications import File, Request, RequestError, application, function
from tensorlake.applications.applications import run_application
from tensorlake.applications.remote.deploy import deploy_applications

# Makes the test case discoverable by unittest framework.
ValidateAllApplicationsTest: unittest.TestCase = (
    validate_all_applications.define_no_validation_errors_test()
)


@application()
@function()
def function_with_no_args() -> str:
    return "success"


@application()
@function()
def function_returning_nothing() -> None:
    pass


@application()
@function()
def function_mixed_args(a: int, b: str, c: float, d: bool) -> str:
    return f"a={a},b={b},c={c},d={d}"


@function()
@application()
def function_tuple_arg_and_return_value(
    arg: tuple[str, str, str, str],
) -> tuple[str, str, str, str]:
    if not isinstance(arg, tuple):
        raise RequestError(f"Tuple type mismatch: {type(arg)}")
    if arg != ("apple", "banana", "cherry", "cherry"):
        raise RequestError(f"Tuple content mismatch: {arg}")
    return arg


@function()
@application()
def function_set_arg_and_return_value(arg: set[str]) -> set[str]:
    if not isinstance(arg, set):
        raise RequestError(f"Set type mismatch: {type(arg)}")
    if arg != {"apple", "banana", "cherry"}:
        raise RequestError(f"Set content mismatch: {arg}")
    return arg


@application()
@function()
def function_default_args(
    x: list[int] = [1, 2, 3], foo: int = 42, bar: str = "default"
) -> str:
    if x != [1, 2, 3]:
        raise RequestError(f"Expected x=[1,2,3], got: {x}")
    if foo != 100:
        raise RequestError(f"Expected foo=100, got: {foo}")
    if bar != "default":
        raise RequestError(f"Expected bar='default', got: {bar}")
    return "success"


@application()
@function()
def function_file_args(file1: File, file2: File) -> str:
    # Don't return File objects because we're testing passing of File arguments,
    # not returning them.
    if file1.content != b"file_content_1":
        raise RequestError(f"File1 content mismatch: {file1.content}")
    if file1.content_type != "text/plain":
        raise RequestError(f"File1 content type mismatch: {file1.content_type}")
    if not isinstance(file1.content, bytes):
        raise RequestError(f"File1 content python type mismatch: {type(file1.content)}")
    if file2.content != b"file_content_2":
        raise RequestError(f"File2 content mismatch: {file2.content}")
    if file2.content_type != "application/octet-stream":
        raise RequestError(f"File2 content type mismatch: {file2.content_type}")
    if not isinstance(file2.content, bytes):
        raise RequestError(f"File2 content python type mismatch: {type(file2.content)}")
    return "success"


@application()
@function()
def function_file_return_value() -> File:
    return File(content=b"file_return_content", content_type="text/plain")


@application()
@function()
def function_pydantic_args(dir: DirModel, file: FileModel) -> str:
    if dir.path != "/test/dir":
        raise RequestError(f"Dir path mismatch: {dir.path}")
    if len(dir.files) != 2:
        raise RequestError(f"Expected 2 files in dir, got: {len(dir.files)}")
    if dir.files[0].path != "/test/dir/file1.txt":
        raise RequestError(f"File1 path mismatch: {dir.files[0].path}")
    if dir.files[0].size != 1234:
        raise RequestError(f"File1 size mismatch: {dir.files[0].size}")
    if not dir.files[0].is_read_only:
        raise RequestError(f"File1 is_read_only mismatch: {dir.files[0].is_read_only}")
    if dir.files[1].path != "/test/dir/file2.txt":
        raise RequestError(f"File2 path mismatch: {dir.files[1].path}")
    if dir.files[1].size != 5678:
        raise RequestError(f"File2 size mismatch: {dir.files[1].size}")
    if dir.files[1].is_read_only:
        raise RequestError(f"File2 is_read_only mismatch: {dir.files[1].is_read_only}")
    if file.path != "/test/file.txt":
        raise RequestError(f"File path mismatch: {file.path}")
    if file.size != 4321:
        raise RequestError(f"File size mismatch: {file.size}")
    if file.is_read_only:
        raise RequestError(f"File is_read_only mismatch: {file.is_read_only}")
    return "success"


@application()
@function()
def function_pydantic_return_value() -> DirModel:
    return DirModel(
        path="/returned/dir",
        files=[
            FileModel(path="/returned/dir/fileA.txt", size=111, is_read_only=False),
            FileModel(path="/returned/dir/fileB.txt", size=222, is_read_only=True),
        ],
    )


@application()
@function()
def function_pydantic_return_value_dict() -> DirModel:
    return {
        "path": "/returned/dir",
        "files": [
            {"path": "/returned/dir/fileA.txt", "size": 111, "is_read_only": False},
            {"path": "/returned/dir/fileB.txt", "size": 222, "is_read_only": True},
        ],
    }


# With Any type hints, Pydantic falls back to standard python JSON
# serialization/deserialization logic (json.dumps and json.loads)
# See json to Python conversion table: https://docs.python.org/3/library/json.html#py-to-json-table
@application()
@function()
def function_any_arg_and_return_value(arg: Any) -> Any:
    return arg


# We fallback to Any type hint when function has no type hints.
@application()
@function()
def function_with_no_type_hints(arg):
    return arg


class TestApplicationFunctionCallSignatures(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Deploy all applications before running tests.
        deploy_applications(__file__)
        return super().setUpClass()

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_function_with_no_args(self, _: str, is_remote: bool):
        request: Request = run_application(function_with_no_args, is_remote)
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_function_with_no_args_and_extra_arg_passed(self, _: str, is_remote: bool):
        # Validate that we allow extra positional args passed to application functions and ignore them.
        request: Request = run_application(
            function_with_no_args, is_remote, "extra_arg"
        )
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_function_with_no_args_and_extra_kwarg_passed(
        self, _: str, is_remote: bool
    ):
        # Validate that we allow extra keyword args passed to application functions and ignore them.
        request: Request = run_application(
            function_with_no_args, is_remote, extra_kwarg="extra_kwarg"
        )
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_function_returning_nothing(self, _: str, is_remote: bool):
        request: Request = run_application(function_returning_nothing, is_remote)
        self.assertEqual(request.output(), None)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_call_with_only_positional_args(self, _: str, is_remote: bool):
        request: Request = run_application(
            function_mixed_args,
            is_remote,
            42,
            "hello",
            3.14,
            True,
        )
        self.assertEqual(request.output(), "a=42,b=hello,c=3.14,d=True")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_call_with_only_kwargs(self, _: str, is_remote: bool):
        request: Request = run_application(
            function_mixed_args,
            is_remote,
            a=42,
            b="hello",
            c=3.14,
            d=True,
        )
        self.assertEqual(request.output(), "a=42,b=hello,c=3.14,d=True")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_mixed_args(self, _: str, is_remote: bool):
        request1: Request = run_application(
            function_mixed_args,
            is_remote,
            1,
            "x",
            c=2.71,
            d=False,
        )
        self.assertEqual(request1.output(), "a=1,b=x,c=2.71,d=False")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_tuple_arg_and_return_value(self, _: str, is_remote: bool):
        request: Request = run_application(
            function_tuple_arg_and_return_value,
            is_remote,
            ("apple", "banana", "cherry", "cherry"),
        )
        output_tuple: tuple[str, str, str, str] = request.output()
        self.assertIsInstance(output_tuple, tuple)
        self.assertEqual(output_tuple, ("apple", "banana", "cherry", "cherry"))

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_set_arg_and_return_value(self, _: str, is_remote: bool):
        request: Request = run_application(
            function_set_arg_and_return_value,
            is_remote,
            {"apple", "banana", "cherry"},
        )
        output_set: set[str] = request.output()
        self.assertIsInstance(output_set, set)
        self.assertEqual(output_set, {"apple", "banana", "cherry"})

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_default_args(self, _: str, is_remote: bool):
        request: Request = run_application(
            function_default_args,
            is_remote,
            foo=100,
        )
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_file_args(self, _: str, is_remote: bool):
        request: Request = run_application(
            function_file_args,
            is_remote,
            File(content=b"file_content_1", content_type="text/plain"),
            File(content=b"file_content_2", content_type="application/octet-stream"),
        )
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_file_return_value(self, _: str, is_remote: bool):
        request: Request = run_application(
            function_file_return_value,
            is_remote,
        )
        output_file: File = request.output()
        self.assertIsInstance(output_file, File)
        self.assertEqual(output_file.content, b"file_return_content")
        self.assertEqual(output_file.content_type, "text/plain")
        self.assertIsInstance(output_file.content, bytes)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_pydantic_args(self, _: str, is_remote: bool):
        dir_model: DirModel = DirModel(
            path="/test/dir",
            files=[
                FileModel(path="/test/dir/file1.txt", size=1234, is_read_only=True),
                FileModel(path="/test/dir/file2.txt", size=5678, is_read_only=False),
            ],
        )
        file_model: FileModel = FileModel(
            path="/test/file.txt", size=4321, is_read_only=False
        )

        request: Request = run_application(
            function_pydantic_args,
            is_remote,
            dir=dir_model,
            file=file_model,
        )
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_pydantic_return_value(self, _: str, is_remote: bool):
        request: Request = run_application(
            function_pydantic_return_value,
            is_remote,
        )
        output_dir: DirModel = request.output()
        self.assertIsInstance(output_dir, DirModel)
        self.assertEqual(output_dir.path, "/returned/dir")
        self.assertEqual(len(output_dir.files), 2)
        self.assertEqual(output_dir.files[0].path, "/returned/dir/fileA.txt")
        self.assertEqual(output_dir.files[0].size, 111)
        self.assertFalse(output_dir.files[0].is_read_only)
        self.assertEqual(output_dir.files[1].path, "/returned/dir/fileB.txt")
        self.assertEqual(output_dir.files[1].size, 222)
        self.assertTrue(output_dir.files[1].is_read_only)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_pydantic_return_value_dict(self, _: str, is_remote: bool):
        request: Request = run_application(
            function_pydantic_return_value_dict,
            is_remote,
        )
        output_dir: DirModel = request.output()
        self.assertIsInstance(output_dir, DirModel)
        self.assertEqual(output_dir.path, "/returned/dir")
        self.assertEqual(len(output_dir.files), 2)
        self.assertEqual(output_dir.files[0].path, "/returned/dir/fileA.txt")
        self.assertEqual(output_dir.files[0].size, 111)
        self.assertFalse(output_dir.files[0].is_read_only)
        self.assertEqual(output_dir.files[1].path, "/returned/dir/fileB.txt")
        self.assertEqual(output_dir.files[1].size, 222)
        self.assertTrue(output_dir.files[1].is_read_only)

    @parameterized.parameterized.expand(
        [
            ("remote", True, "function_any_arg_and_return_value"),
            ("local", False, "function_any_arg_and_return_value"),
            ("remote_no_type_hints", True, "function_with_no_type_hints"),
            ("local_no_type_hints", False, "function_with_no_type_hints"),
        ]
    )
    def test_any_arg_and_value_str(self, _: str, is_remote: bool, function: str):
        request: Request = run_application(
            function,
            is_remote,
            "test_string",
        )
        self.assertEqual(request.output(), "test_string")

    @parameterized.parameterized.expand(
        [
            ("remote", True, "function_any_arg_and_return_value"),
            ("local", False, "function_any_arg_and_return_value"),
            ("remote_no_type_hints", True, "function_with_no_type_hints"),
            ("local_no_type_hints", False, "function_with_no_type_hints"),
        ]
    )
    def test_any_arg_and_value_int_list(self, _: str, is_remote: bool, function: str):
        request: Request = run_application(
            function,
            is_remote,
            [1, 2, 3, 4, 5],
        )
        self.assertEqual(request.output(), [1, 2, 3, 4, 5])

    @parameterized.parameterized.expand(
        [
            ("remote", True, "function_any_arg_and_return_value"),
            ("local", False, "function_any_arg_and_return_value"),
            ("remote_no_type_hints", True, "function_with_no_type_hints"),
            ("local_no_type_hints", False, "function_with_no_type_hints"),
        ]
    )
    def test_any_arg_and_value_int_set(self, _: str, is_remote: bool, function: str):
        request: Request = run_application(
            function,
            is_remote,
            {1, 2, 3, 4, 5},
        )
        # Sets are converted to lists during JSON serialization/deserialization when type hint is Any.
        self.assertEqual(request.output(), [1, 2, 3, 4, 5])

    @parameterized.parameterized.expand(
        [
            ("remote", True, "function_any_arg_and_return_value"),
            ("local", False, "function_any_arg_and_return_value"),
            ("remote_no_type_hints", True, "function_with_no_type_hints"),
            ("local_no_type_hints", False, "function_with_no_type_hints"),
        ]
    )
    def test_any_arg_and_value_list_of_pydantic_models(
        self, _: str, is_remote: bool, function: str
    ):
        request: Request = run_application(
            function,
            is_remote,
            [
                FileModel(path="/file1.txt", size=100, is_read_only=True),
                FileModel(path="/file2.txt", size=200, is_read_only=False),
            ],
        )
        # JSON objects are converted to dicts during JSON serialization/deserialization when type hint is Any.
        self.assertEqual(
            request.output(),
            [
                {"path": "/file1.txt", "size": 100, "is_read_only": True},
                {"path": "/file2.txt", "size": 200, "is_read_only": False},
            ],
        )


if __name__ == "__main__":
    unittest.main()
