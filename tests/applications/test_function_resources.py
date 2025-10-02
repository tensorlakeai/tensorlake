import unittest

import parameterized

from tensorlake.applications import Request, application, function, run_application
from tensorlake.applications.remote.deploy import deploy_applications


@application()
@function(cpu=1.1, memory=1.3, ephemeral_disk=1.0)
def function_with_custom_resources(x: int) -> str:
    return "success"


class TestFunctionResources(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_function_with_custom_resources_succeeds(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(
            function_with_custom_resources, 1, remote=is_remote
        )
        self.assertEqual(request.output(), "success")


if __name__ == "__main__":
    unittest.main()
