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
def test_update_progress(values: tuple[int, int]) -> str:
    ctx: RequestContext = RequestContext.get()
    ctx.progress.update(current=values[0], total=values[1])
    return "success"


class TestProgress(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_update_progress(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            test_update_progress, (10, 100), remote=is_remote
        )
        self.assertEqual("success", request.output())


if __name__ == "__main__":
    unittest.main()
