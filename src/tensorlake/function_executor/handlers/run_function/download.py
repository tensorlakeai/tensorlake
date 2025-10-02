import hashlib
import time
from typing import List

from tensorlake.applications.ast.value_node import ValueNode

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
        serialized_arg_metadata, serialized_arg_data = _download_serialized_object(
            arg_blob, arg, blob_store, logger
        )
        serialized_arg_metadata: bytes
        serialized_arg_data: bytes
        args.append(
            _deserialize_to_value_node(
                arg.manifest, serialized_arg_metadata, serialized_arg_data
            )
        )

    logger.info(
        "function arguments downloaded",
        duration_sec=time.monotonic() - start_time,
    )

    return args


def download_application_function_payload_bytes(
    allocation: Allocation, blob_store: BLOBStore, logger: FunctionExecutorLogger
) -> bytes:
    start_time = time.monotonic()
    logger = logger.bind(module=__name__)
    logger.info("downloading function arguments")

    if len(allocation.inputs.args) != 1 or len(allocation.inputs.arg_blobs) != 1:
        raise ValueError(
            "Application function calls must have exactly one argument and one argument blob, "
            f"got {len(allocation.inputs.args)} args and {len(allocation.inputs.arg_blobs)} arg blobs"
        )

    app_payload_blob: BLOB = allocation.inputs.arg_blobs[0]
    app_payload_so: SerializedObjectInsideBLOB = allocation.inputs.args[0]

    _, payload = _download_serialized_object(
        blob=app_payload_blob,
        so=app_payload_so,
        blob_store=blob_store,
        logger=logger,
    )

    logger.info(
        "function arguments downloaded",
        duration_sec=time.monotonic() - start_time,
    )

    return payload


def _download_serialized_object(
    blob: BLOB,
    so: SerializedObjectInsideBLOB,
    blob_store: BLOBStore,
    logger: FunctionExecutorLogger,
) -> tuple[bytes, bytes]:
    """Returns the raw bytes of the serialized object metadata and data from blob store."""
    if not so.manifest.HasField("metadata_size"):
        raise ValueError("SerializedObjectManifest is missing metadata_size.")

    # Download each part separately to avoid splitting the downloaded data and consuming extra memory.
    so_metadata: bytes = blob_store.get(
        blob=blob,
        offset=so.offset,
        size=so.manifest.metadata_size,
        logger=logger,
    )
    so_data: bytes = blob_store.get(
        blob=blob,
        offset=so.offset + so.manifest.metadata_size,
        size=so.manifest.size - so.manifest.metadata_size,
        logger=logger,
    )
    so_hash: str = _sha256_hexdigest(so_metadata, so_data)
    if so_hash != so.manifest.sha256_hash:
        logger.error(
            "serialized object data hash mismatch",
            got_hash=so_hash,
            expected_hash=so.manifest.sha256_hash,
        )
        raise ValueError(
            f"Serialized object hash {so_hash} does not match expected hash {so.manifest.sha256_hash}."
        )

    return so_metadata, so_data


def _deserialize_to_value_node(
    manifest: SerializedObjectManifest, metadata: bytes, data: bytes
) -> ValueNode:
    """Deserialized Serialized Object created by Python SDK into its original ValueNode."""
    value_node_metadata: ValueNodeMetadata = ValueNodeMetadata.deserialize(metadata)
    # The Data Payload is produced by one of the nodes in a call tree. The this data payload gets assigned as
    # a value output of the root of the call tree. The parent of the root node expect to find a ValueNode with
    # id of the root node but the Data Payload metadata contains the id of the node that produced it.
    #
    # To get the id of the root node we need to use source_function_call_id from the manifest. If it's not set
    # then it means that the Data Payload was produced in the same function call where it's consumed so the node
    # id in the metadata is correct.
    node_id: str = (
        manifest.source_function_call_id
        if manifest.HasField("source_function_call_id")
        else value_node_metadata.nid
    )
    return ValueNode.from_serialized(
        node_id=node_id,
        value=data,
        metadata=value_node_metadata.metadata,
    )


def _sha256_hexdigest(metadata: bytes, data: bytes) -> str:
    hasher = hashlib.sha256()
    hasher.update(metadata)
    hasher.update(data)
    return hasher.hexdigest()
