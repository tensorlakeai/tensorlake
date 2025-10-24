import hashlib
import time
from typing import List

from tensorlake.applications.metadata import ValueMetadata, deserialize_metadata

from ..blob_store.blob_store import BLOBStore
from ..logger import FunctionExecutorLogger
from ..proto.function_executor_pb2 import (
    BLOB,
    Allocation,
    SerializedObjectInsideBLOB,
    SerializedObjectManifest,
)
from .value import SerializedValue


def download_function_arguments(
    allocation: Allocation, blob_store: BLOBStore, logger: FunctionExecutorLogger
) -> List[SerializedValue]:
    start_time = time.monotonic()
    logger = logger.bind(module=__name__)
    logger.info("downloading function arguments")

    args: List[SerializedValue] = download_serialized_objects(
        serialized_objects=allocation.inputs.args,
        serialized_object_blobs=allocation.inputs.arg_blobs,
        blob_store=blob_store,
        logger=logger,
    )

    logger.info(
        "function arguments downloaded",
        duration_sec=time.monotonic() - start_time,
    )

    return args


def download_serialized_objects(
    serialized_objects: List[SerializedObjectInsideBLOB],
    serialized_object_blobs: List[BLOB],
    blob_store: BLOBStore,
    logger: FunctionExecutorLogger,
) -> List[SerializedValue]:
    # TODO: Do this in parallel. Keep in mind that the underlying BLOB store
    # chunks and parallelizes large downloads and performance degrades with
    # too much parallelization.
    if len(serialized_objects) != len(serialized_object_blobs):
        raise ValueError(
            "Mismatched serialized objects and serialized object blobs lengths, "
            f"{len(serialized_objects)} != {len(serialized_object_blobs)}"
        )

    return [
        _download_serialized_value(blob, so, blob_store, logger)
        for blob, so in zip(serialized_object_blobs, serialized_objects)
    ]


def _download_serialized_value(
    blob: BLOB,
    so: SerializedObjectInsideBLOB,
    blob_store: BLOBStore,
    logger: FunctionExecutorLogger,
) -> SerializedValue:
    """Returns the raw bytes of the serialized object metadata and data from blob store."""
    if not so.manifest.HasField("metadata_size"):
        raise ValueError("SerializedObjectManifest is missing metadata_size.")

    # Download each part separately to avoid splitting the downloaded data and consuming extra memory.
    serialized_metadata: bytes | None = None
    if so.manifest.metadata_size > 0:
        serialized_metadata = blob_store.get(
            blob=blob,
            offset=so.offset,
            size=so.manifest.metadata_size,
            logger=logger,
        )

    serialized_data: bytes = blob_store.get(
        blob=blob,
        offset=so.offset + so.manifest.metadata_size,
        size=so.manifest.size - so.manifest.metadata_size,
        logger=logger,
    )

    so_hash: str = _sha256_hexdigest(
        b"" if serialized_metadata is None else serialized_metadata, serialized_data
    )
    if so_hash != so.manifest.sha256_hash:
        logger.error(
            "serialized object data hash mismatch",
            got_hash=so_hash,
            expected_hash=so.manifest.sha256_hash,
        )
        raise ValueError(
            f"Serialized object hash {so_hash} does not match expected hash {so.manifest.sha256_hash}."
        )

    metadata: ValueMetadata | None = None
    if serialized_metadata is not None:
        metadata = _deserialize_value_metadata(
            manifest=so.manifest,
            serialized_metadata=serialized_metadata,
        )

    return SerializedValue(
        metadata=metadata,
        data=serialized_data,
        content_type=(
            so.manifest.content_type if so.manifest.HasField("content_type") else None
        ),
    )


def _deserialize_value_metadata(
    manifest: SerializedObjectManifest,
    serialized_metadata: bytes,
) -> ValueMetadata:
    """Deserializes Serialized Object created by Python SDK into original value with sdk metadata."""
    value_metadata: ValueMetadata = deserialize_metadata(serialized_metadata)
    if not isinstance(value_metadata, ValueMetadata):
        raise ValueError(
            "Deserialized sdk value metadata is not of type ValueMetadata."
        )

    # The Data Payload is produced by one of the nodes in a call tree. This data payload gets assigned as
    # a value output of the root of the call tree. The parent of the root node expect to find a ValueNode with
    # id of the root node but the Data Payload metadata contains the id of the node that produced it.
    #
    # To get the id of the root node we need to use source_function_call_id from the manifest. If it's not set
    # then it means that the Data Payload was produced in the same function call where it's consumed so the node
    # id in the metadata is correct.
    value_metadata.id = (
        manifest.source_function_call_id
        if manifest.HasField("source_function_call_id")
        else value_metadata.id
    )

    return value_metadata


def _sha256_hexdigest(metadata: bytes, data: bytes) -> str:
    hasher = hashlib.sha256()
    hasher.update(metadata)
    hasher.update(data)
    return hasher.hexdigest()
