import hashlib
import time
from typing import List

from tensorlake.workflows.ast.value_node import ValueNode

from ...blob_store.blob_store import BLOBStore
from ...logger import FunctionExecutorLogger
from ...proto.function_executor_pb2 import (
    BLOB,
    Allocation,
    SerializedObjectInsideBLOB,
    SerializedObjectManifest,
)
from .value_node_metadata import ValueNodeMetadata


def download_function_arguments(
    allocation: Allocation, blob_store: BLOBStore, logger: FunctionExecutorLogger
) -> List[ValueNode]:
    start_time = time.monotonic()
    logger = logger.bind(module=__name__)
    logger.info("downloading function arguments")

    # TODO: Do this in parallel. Keep in mind that the underlying BLOB store
    # chunks and parallelizes large downloads and performance degrades with
    # too much parallelization.
    if len(allocation.inputs.args) != len(allocation.inputs.arg_blobs):
        raise ValueError(
            "Mismatched function arguments and functions argument blobs lengths, "
            f"{len(allocation.inputs.args)} != {len(allocation.inputs.arg_blobs)}"
        )

    args: List[ValueNode] = []
    for i, arg in enumerate(allocation.inputs.args):
        arg: SerializedObjectInsideBLOB
        arg_blob: BLOB = allocation.inputs.arg_blobs[i]
        serialized_arg: bytes = _download_function_argument(
            arg_blob, arg, blob_store, logger
        )
        args.append(_deserialize_to_value_node(arg.manifest, serialized_arg))

    logger.info(
        "function arguments downloaded",
        duration_sec=time.monotonic() - start_time,
    )

    return args


def download_api_function_payload_bytes(
    allocation: Allocation, blob_store: BLOBStore, logger: FunctionExecutorLogger
) -> bytes:
    start_time = time.monotonic()
    logger = logger.bind(module=__name__)
    logger.info("downloading function arguments")

    api_payload_blob: BLOB = allocation.inputs.args[0]
    api_payload_so: SerializedObjectInsideBLOB = allocation.inputs.arg_blobs[0]

    payload: bytes = _download_function_argument(
        api_payload_blob, api_payload_so, blob_store, logger
    )

    logger.info(
        "function arguments downloaded",
        duration_sec=time.monotonic() - start_time,
    )

    return payload


def _download_function_argument(
    arg_blob: BLOB,
    arg: SerializedObjectInsideBLOB,
    blob_store: BLOBStore,
    logger: FunctionExecutorLogger,
) -> bytes:
    data: bytes = blob_store.get(
        blob=arg_blob,
        offset=arg.offset,
        size=arg.manifest.size,
        logger=logger,
    )

    data_hash: str = _sha256_hexdigest(data)
    if data_hash != arg.manifest.sha256_hash:
        logger.error(
            "function argument data hash mismatch",
            got_hash=data_hash,
            expected_hash=arg.manifest.sha256_hash,
        )
        raise ValueError(
            f"Function argument data hash {data_hash} does not match expected hash {arg.manifest.sha256_hash}."
        )


def _deserialize_to_value_node(
    manifest: SerializedObjectManifest, data: bytes
) -> ValueNode:
    """Deserialized Serialized Object created by Python SDK into its original ValueNode."""
    if not manifest.HasField("metadata_size"):
        raise ValueError("SerializedObjectManifest is missing metadata_size.")

    if not manifest.HasField("function_call_id"):
        raise ValueError("SerializedObjectManifest is missing function_call_id.")

    value_node_metadata: ValueNodeMetadata = ValueNodeMetadata.deserialize(
        memoryview(data)[: manifest.metadata_size]
    )
    serialized_value = memoryview(data)[manifest.metadata_size :]
    return ValueNode.from_serialized(
        node_id=value_node_metadata.nid,
        value=serialized_value,
        metadata=value_node_metadata.metadata,
    )


def _sha256_hexdigest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
