import unittest

import parameterized

from tensorlake.applications import (
    Request,
    RequestContext,
    application,
    function,
)
from tensorlake.applications.applications import run_application
from tensorlake.applications.remote.deploy import deploy_applications


@application()
@function()
def test_get_request_id(_: int) -> str:
    ctx: RequestContext = RequestContext.get()
    return ctx.request_id


class TestRequestId(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_expected_request_id(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(test_get_request_id, 11, remote=is_remote)
        self.assertEqual(request.id, request.output())


if __name__ == "__main__":
    unittest.main()
