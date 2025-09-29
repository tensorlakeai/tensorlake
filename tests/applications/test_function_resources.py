import unittest

import parameterized

from tensorlake.applications import Request, api, call_api, function
from tensorlake.applications.remote.deploy import deploy


@api()
@function(cpu=1.1, memory=1.3, ephemeral_disk=1.0)
def function_with_custom_resources(x: int) -> str:
    return "success"


class TestFunctionResources(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_function_with_custom_resources_succeeds(self, _: str, is_remote: bool):
        if is_remote:
            deploy(__file__)
        request: Request = call_api(function_with_custom_resources, 1, remote=is_remote)
        self.assertEqual(request.output(), "success")


if __name__ == "__main__":
    unittest.main()
