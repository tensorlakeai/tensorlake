import hashlib
import os
import unittest

from testing import (
    FunctionExecutorProcessContextManager,
    api_function_inputs,
    initialize,
    read_tmp_blob_bytes,
    rpc_channel,
    run_allocation,
)

import tensorlake.applications.interface as tensorlake
from tensorlake.function_executor.proto.function_executor_pb2 import (
    AllocationFailureReason,
    AllocationOutcomeCode,
    AllocationResult,
    InitializationOutcomeCode,
    InitializeResponse,
    SerializedObjectEncoding,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)

APPLICATION_CODE_DIR_PATH = os.path.dirname(os.path.abspath(__file__))

app: tensorlake.Application = tensorlake.define_application(name=__file__)


@tensorlake.api()
@tensorlake.function()
def raise_request_error(x: int) -> str:
    raise tensorlake.RequestError(f"The request can't succeed: {x}")


class TestRequestError(unittest.TestCase):
    def test_request_error_response(self):
        with FunctionExecutorProcessContextManager(
            capture_std_outputs=True,
        ) as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)

                initialize_response: InitializeResponse = initialize(
                    stub=stub,
                    app=app,
                    app_code_dir_path=APPLICATION_CODE_DIR_PATH,
                    function_name="raise_request_error",
                )
                self.assertEqual(
                    initialize_response.outcome_code,
                    InitializationOutcomeCode.INITIALIZATION_OUTCOME_CODE_SUCCESS,
                )

                alloc_result: AllocationResult = run_allocation(
                    stub,
                    inputs=api_function_inputs(10),
                )

                self.assertEqual(
                    alloc_result.outcome_code,
                    AllocationOutcomeCode.ALLOCATION_OUTCOME_CODE_FAILURE,
                )
                self.assertEqual(
                    alloc_result.failure_reason,
                    AllocationFailureReason.ALLOCATION_FAILURE_REASON_REQUEST_ERROR,
                )
                self.assertEqual(
                    alloc_result.request_error_output.manifest.encoding,
                    SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_TEXT,
                )
                self.assertEqual(
                    alloc_result.request_error_output.manifest.metadata_size,
                    0,
                )
                self.assertIn(
                    "The request can't succeed: 10",
                    read_tmp_blob_bytes(
                        alloc_result.uploaded_request_error_blob,
                        alloc_result.request_error_output.offset,
                        alloc_result.request_error_output.manifest.size,
                    ).decode("utf-8"),
                )
                self.assertEqual(
                    alloc_result.request_error_output.manifest.sha256_hash,
                    hashlib.sha256(
                        "The request can't succeed: 10".encode("utf-8")
                    ).hexdigest(),
                )
                self.assertFalse(
                    alloc_result.HasField("uploaded_function_outputs_blob")
                )
                self.assertFalse(alloc_result.HasField("value"))
                self.assertFalse(alloc_result.HasField("updates"))

        fe_stderr: str = process.read_stderr()
        self.assertIn("RequestError: The request can't succeed: 10", fe_stderr)


if __name__ == "__main__":
    unittest.main()
