import unittest

from testing import (
    FunctionExecutorProcessContextManager,
    rpc_channel,
)

from tensorlake.function_executor.proto.function_executor_pb2 import InfoRequest
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)


class TestCLIArgs(unittest.TestCase):
    # This test checks that older versions of Function Executor won't fail to start when we add new CLI arguments.
    def test_no_error_when_extra_cli_args_provided(self):
        with FunctionExecutorProcessContextManager(
            extra_args=["--unknown-argument", "test", "--another-unknown-argument=123"],
        ) as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                stub.get_info(InfoRequest())
                # The test fails if the request fails with any exception because FE didn't start up.


if __name__ == "__main__":
    unittest.main()
