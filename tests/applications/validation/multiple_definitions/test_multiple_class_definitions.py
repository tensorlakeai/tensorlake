import unittest

import parameterized

from tensorlake.applications import (
    ApplicationValidationError,
    Request,
    application,
    cls,
    function,
)
from tensorlake.applications.applications import run_application
from tensorlake.applications.remote.deploy import deploy_applications


@cls()
class Class1:
    @application()
    @function()
    def method(self, _: str) -> str:
        return "Class1.method"


@cls()
class Class1:
    @application()
    @function()
    def method(self, _: str) -> str:
        return "Class1.method_redefined"


class TestMultipleClassDefinitions(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_redefine_same_class_in_the_same_file_succeeds(
        self, _: str, is_remote: bool
    ):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application("Class1.method", 1, remote=is_remote)
        self.assertEqual(request.output(), "Class1.method_redefined")

    def test_redefine_same_class_in_different_files_fails(self):
        with self.assertRaises(ApplicationValidationError):
            import class_1


if __name__ == "__main__":
    unittest.main()
