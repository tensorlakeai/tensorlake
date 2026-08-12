import importlib.metadata
import sys
import unittest
from unittest.mock import patch

from testing import (
    FunctionExecutorProcessContextManager,
    rpc_channel,
)

from tensorlake.function_executor.info import info_response_kv_args
from tensorlake.function_executor.proto.function_executor_pb2 import (
    InfoRequest,
    InfoResponse,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)


class TestGetInfo(unittest.TestCase):
    def test_source_checkout_without_package_metadata(self):
        with patch(
            "tensorlake.function_executor.info.importlib.metadata.version",
            side_effect=importlib.metadata.PackageNotFoundError("tensorlake"),
        ):
            self.assertEqual(info_response_kv_args()["sdk_version"], "unknown")

    def test_expected_info(self):
        with FunctionExecutorProcessContextManager() as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                response: InfoResponse = stub.get_info(InfoRequest())
                self.assertEqual(response.version, "0.1.3")
                self.assertEqual(response.sdk_language, "python")
                self.assertEqual(
                    response.sdk_version, importlib.metadata.version("tensorlake")
                )
                self.assertEqual(
                    response.sdk_language_version,
                    f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
                )


if __name__ == "__main__":
    unittest.main()
