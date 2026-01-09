import unittest

import parameterized
import validate_all_applications

from tensorlake.applications import Request, application, function
from tensorlake.applications.applications import run_application
from tensorlake.applications.remote.deploy import deploy_applications

# Makes the test case discoverable by unittest framework.
ValidateAllApplicationsTest: unittest.TestCase = validate_all_applications.define_test()


@application()
@function(cpu=1.1, memory=1.3, ephemeral_disk=1.0)
def function_with_custom_resources(x: int) -> str:
    return "success"


class TestFunctionResources(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_function_with_custom_resources_succeeds(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(function_with_custom_resources, is_remote, 1)
        self.assertEqual(request.output(), "success")


if __name__ == "__main__":
    unittest.main()
