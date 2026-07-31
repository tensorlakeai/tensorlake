import hashlib
import unittest

from tensorlake.function_executor.message_validators import validate_new_allocation
from tensorlake.function_executor.proto.function_executor_pb2 import (
    BLOB,
    Allocation,
    BLOBChunk,
    FunctionInputs,
    SerializedObjectEncoding,
    SerializedObjectInsideBLOB,
    SerializedObjectManifest,
)


def _allocation() -> Allocation:
    return Allocation(
        request_id="request",
        function_call_id="call",
        allocation_id="allocation",
        inputs=FunctionInputs(
            args=[
                SerializedObjectInsideBLOB(
                    manifest=SerializedObjectManifest(
                        encoding=SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_JSON,
                        encoding_version=0,
                        size=0,
                        metadata_size=0,
                        sha256_hash=hashlib.sha256(b"").hexdigest(),
                    ),
                    offset=0,
                )
            ],
            arg_blobs=[
                BLOB(
                    id="input",
                    chunks=[BLOBChunk(uri="file:///input", size=0)],
                )
            ],
            request_error_blob=BLOB(id="request-error"),
        ),
    )


class TestMessageValidators(unittest.TestCase):
    def test_rejects_an_explicitly_present_empty_identifier(self) -> None:
        allocation = _allocation()
        allocation.request_id = ""

        with self.assertRaisesRegex(ValueError, "must not be empty"):
            validate_new_allocation(allocation)

    def test_rejects_integers_outside_the_shared_safe_range(self) -> None:
        cases = []

        unsafe_size = _allocation()
        unsafe_size.inputs.args[0].manifest.size = 1 << 53
        cases.append(unsafe_size)

        unsafe_offset = _allocation()
        unsafe_offset.inputs.args[0].offset = 1 << 53
        cases.append(unsafe_offset)

        unsafe_chunk = _allocation()
        unsafe_chunk.inputs.arg_blobs[0].chunks[0].size = 1 << 53
        cases.append(unsafe_chunk)

        for allocation in cases:
            with self.subTest(allocation=allocation):
                with self.assertRaisesRegex(ValueError, "safe integer maximum"):
                    validate_new_allocation(allocation)


if __name__ == "__main__":
    unittest.main()
