import unittest

import parameterized
import validate_all_applications

from tensorlake.applications import Request, application, function
from tensorlake.applications.applications import run_application
from tensorlake.applications.remote.deploy import deploy_applications

# Makes the test case discoverable by unittest framework.
ValidateAllApplicationsTest: unittest.TestCase = validate_all_applications.define_test()


@application()
@function()
def api_function(
    a: int, b: str, c: tuple[int, int, str], d: dict[str, list[int]]
) -> tuple[int, str, tuple[int, int, str], dict[str, list[int]]]:
    return regular_function(a, b, c, d)


@function()
def regular_function(
    a: int, b: str, c: tuple[int, int, str], d: dict[str, list[int]]
) -> tuple[int, str, tuple[int, int, str], dict[str, list[int]]]:
    return other_api_function(a, b, c, d)


@application()
@function()
def other_api_function(
    a: int, b: str, c: tuple[int, int, str], d: dict[str, list[int]]
) -> tuple[int, str, tuple[int, int, str], dict[str, list[int]]]:
    return a, b, c, d


class TestCallApplicationFromRegularFunction(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_success(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            api_function, is_remote, 1, "test", (2, 3, "four"), {"five": [6, 7]}
        )
        output: tuple[int, str, tuple[int, int, str], dict[str, list[int]]] = (
            request.output()
        )
        self.assertEqual(output, (1, "test", (2, 3, "four"), {"five": [6, 7]}))


if __name__ == "__main__":
    unittest.main()
