import unittest

import parameterized

from tensorlake.applications import (
    ApplicationValidateError,
    Request,
    application,
    function,
    run_application,
)
from tensorlake.applications.remote.deploy import deploy_applications


@application()
@function()
def function_1(_: str) -> str:
    return "function_1"


@application()
@function()
def function_1(_: str) -> str:
    return "function_1_redefined"


class TestMultipleFunctionDefinitions(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_redefine_same_function_in_the_same_file_succeeds(
        self, _: str, is_remote: bool
    ):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application("function_1", 1, remote=is_remote)
        self.assertEqual(request.output(), "function_1_redefined")

    def test_redefine_same_function_in_different_files_fails(self):
        with self.assertRaises(ApplicationValidateError):
            import function_1


if __name__ == "__main__":
    unittest.main()
