import unittest

import parameterized
from models import DirModel, FileModel

from tensorlake.applications import File, Request, RequestError, application, function
from tensorlake.applications.applications import run_application
from tensorlake.applications.remote.deploy import deploy_applications
from tensorlake.applications.validation import validate_loaded_applications


@application()
@function()
def test_function_with_no_args_api() -> str:
    return "success"


@application()
@function()
def test_function_returning_nothing_api() -> None:
    pass


@application()
@function()
def test_only_positional_args_api(a: int, b: str, c: float, d: bool, /) -> str:
    return f"a={a},b={b},c={c},d={d}"


@application()
@function()
def test_only_kwargs_api(*, a: int, b: str, c: float, d: bool) -> str:
    return f"a={a},b={b},c={c},d={d}"


@application()
@function()
def test_mixed_args_api(a: int, b: str, /, c: float, *, d: bool) -> str:
    return f"a={a},b={b},c={c},d={d}"


@application()
@function()
def test_default_args_api(
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
def test_file_args_api(file1: File, file2: File) -> str:
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
def test_file_return_value_api() -> File:
    return File(content=b"file_return_content", content_type="text/plain")


@application()
@function()
def test_pydantic_args_api(dir: DirModel, file: FileModel) -> str:
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
def test_pydantic_return_value_api() -> DirModel:
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
def test_set_arg_and_return_value_api(arg: set[str]) -> set[str]:
    if not isinstance(arg, set):
        raise RequestError(f"Set type mismatch: {type(arg)}")
    if arg != {"apple", "banana", "cherry"}:
        raise RequestError(f"Set content mismatch: {arg}")
    return arg


@function()
@application()
def test_tuple_arg_and_return_value_api(
    arg: tuple[str, str, str, str],
) -> tuple[str, str, str, str]:
    if not isinstance(arg, tuple):
        raise RequestError(f"Tuple type mismatch: {type(arg)}")
    if arg != ("apple", "banana", "cherry", "cherry"):
        raise RequestError(f"Tuple content mismatch: {arg}")
    return arg


class TestApplicationFunctionCallSignatures(unittest.TestCase):
    def test_applications_are_valid(self):
        self.assertEqual(validate_loaded_applications(), [])

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_function_with_no_args(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(test_function_with_no_args_api, is_remote)
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_function_with_no_args_and_extra_arg_passed(self, _: str, is_remote: bool):
        # Validate that we allow extra positional args passed to application functions and ignore them.
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            test_function_with_no_args_api, is_remote, "extra_arg"
        )
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_function_with_no_args_and_extra_kwarg_passed(
        self, _: str, is_remote: bool
    ):
        # Validate that we allow extra keyword args passed to application functions and ignore them.
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            test_function_with_no_args_api, is_remote, extra_kwarg="extra_kwarg"
        )
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_function_returning_nothing(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            test_function_returning_nothing_api, is_remote
        )
        self.assertEqual(request.output(), None)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_only_positional_args(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            test_only_positional_args_api,
            is_remote,
            42,
            "hello",
            3.14,
            True,
        )
        self.assertEqual(request.output(), "a=42,b=hello,c=3.14,d=True")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_only_kwargs(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            test_only_kwargs_api,
            is_remote,
            a=42,
            b="hello",
            c=3.14,
            d=True,
        )
        self.assertEqual(request.output(), "a=42,b=hello,c=3.14,d=True")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_mixed_args(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request1: Request = run_application(
            test_mixed_args_api,
            is_remote,
            1,
            "x",
            c=2.71,
            d=False,
        )
        self.assertEqual(request1.output(), "a=1,b=x,c=2.71,d=False")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_default_args(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            test_default_args_api,
            is_remote,
            foo=100,
        )
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_file_args(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            test_file_args_api,
            is_remote,
            File(content=b"file_content_1", content_type="text/plain"),
            File(content=b"file_content_2", content_type="application/octet-stream"),
        )
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_file_return_value(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            test_file_return_value_api,
            is_remote,
        )
        output_file: File = request.output()
        self.assertIsInstance(output_file, File)
        self.assertEqual(output_file.content, b"file_return_content")
        self.assertEqual(output_file.content_type, "text/plain")
        self.assertIsInstance(output_file.content, bytes)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_pydantic_args(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

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
            test_pydantic_args_api,
            is_remote,
            dir=dir_model,
            file=file_model,
        )
        self.assertEqual(request.output(), "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_pydantic_return_value(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            test_pydantic_return_value_api,
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
    def test_set_arg_and_return_value(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            test_set_arg_and_return_value_api,
            is_remote,
            {"apple", "banana", "cherry"},
        )
        output_set: set[str] = request.output()
        self.assertIsInstance(output_set, set)
        self.assertEqual(output_set, {"apple", "banana", "cherry"})

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_tuple_arg_and_return_value(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            test_tuple_arg_and_return_value_api,
            is_remote,
            ("apple", "banana", "cherry", "cherry"),
        )
        output_tuple: tuple[str, str, str, str] = request.output()
        self.assertIsInstance(output_tuple, tuple)
        self.assertEqual(output_tuple, ("apple", "banana", "cherry", "cherry"))


if __name__ == "__main__":
    unittest.main()
