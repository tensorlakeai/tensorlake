from tensorlake.applications.blob_store import BLOB, BLOBChunk

from ..proto.function_executor_pb2 import BLOB as BLOBProto
from ..proto.function_executor_pb2 import BLOBChunk as BLOBChunkProto


def blob_proto_to_blob(blob_proto: BLOBProto) -> BLOB:
    chunks: list[BLOBChunk] = [
        BLOBChunk(
            uri=chunk.uri,
            size=chunk.size,
            etag=chunk.etag if chunk.HasField("etag") else None,
        )
        for chunk in blob_proto.chunks
    ]
    return BLOB(
        id=blob_proto.id,
        chunks=chunks,
    )


def blob_to_blob_proto(blob: BLOB) -> BLOBProto:
    chunks: list[BLOBChunkProto] = [
        BLOBChunkProto(
            uri=chunk.uri,
            size=chunk.size,
            etag=None if chunk.etag is None else chunk.etag,
        )
        for chunk in blob.chunks
    ]
    return BLOBProto(
        id=blob.id,
        chunks=chunks,
    )
