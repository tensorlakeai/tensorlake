import os
import time
import unittest

from tensorlake.applications import (
    Request,
    RequestFailureException,
    api,
    call_remote_api,
    function,
)
from tensorlake.applications.remote.deploy import deploy


@api()
@function()
def function(crash: bool) -> str:
    if crash:
        # os.kill(getpid(), signal.SIGKILL) won't work for container init process,
        # see https://stackoverflow.com/questions/21031537/sigkill-init-process-pid-1.
        # sys.exit(1) hangs the function for some unknown reason,
        # see some ideas at https://stackoverflow.com/questions/5422831/what-does-sys-exit-do-in-python.
        os._exit(1)
    return "success"


class TestFunctionProcessCrash(unittest.TestCase):
    def test_function_invoke_successful_after_process_crashes(self):
        deploy(__file__)

        print("Running a function that will crash FunctionExecutor process...")
        for i in range(2):
            request: Request = call_remote_api(
                function,
                True,
            )
            try:
                request.output()
            except RequestFailureException as e:
                self.assertEqual(e.message, "functionerror")

        # FIXME: we're only doing periodic Function Executor health checks right now,
        # so we need to wait for the crash to be detected.
        time.sleep(10)
        success_request: Request = call_remote_api(
            function,
            False,
        )
        success_output = success_request.output()
        self.assertEqual(success_output, "success")


if __name__ == "__main__":
    unittest.main()
