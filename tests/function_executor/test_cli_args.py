import unittest

from testing import (
    DEFAULT_FUNCTION_EXECUTOR_PORT,
    FunctionExecutorProcessContextManager,
    rpc_channel,
)

from tensorlake.function_executor.proto.function_executor_pb2 import (
    HealthCheckRequest,
    HealthCheckResponse,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)


class TestCLIArgs(unittest.TestCase):
    # This test checks that older versions of Function Executor won't fail when we add new CLI arguments.
    def test_no_error_when_extra_cli_args_provided(self):
        with FunctionExecutorProcessContextManager(
            DEFAULT_FUNCTION_EXECUTOR_PORT,
            extra_args=["--unknown-argument", "test", "--another-unknown-argument=123"],
        ) as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                response: HealthCheckResponse = stub.check_health(HealthCheckRequest())
                self.assertTrue(response.healthy)


if __name__ == "__main__":
    unittest.main()
