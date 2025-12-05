import os
import tempfile

from tensorlake.applications.blob_store import BLOB, BLOBChunk


def create_tmp_blob(
    id: str, chunks_count: int = 5, chunk_size: int = 1 * 1024 * 1024
) -> BLOB:
    """Returns a temporary local file backed blob for writing."""
    with tempfile.NamedTemporaryFile(delete=False) as blob_file:
        # blob_file.write(b"0" * chunk_size)
        blob_file_uri: str = f"file://{os.path.abspath(blob_file.name)}"
        chunks: list[BLOBChunk] = []
        for _ in range(chunks_count):
            chunks.append(
                BLOBChunk(
                    uri=blob_file_uri,
                    size=chunk_size,
                    etag=None,
                )
            )
        return BLOB(id=id, chunks=list(chunks))


def read_tmp_blob_bytes(blob: BLOB, offset: int, size: int) -> bytes:
    """Reads a local blob and returns its content as bytes."""
    blob_file_path: str = blob.chunks[0].uri.replace("file://", "", 1)
    with open(blob_file_path, "rb") as f:
        f.seek(offset)
        return f.read(size)


def write_tmp_blob_bytes(blob: BLOB, data: bytes) -> None:
    """Writes bytes to a local blob from its very beginning."""
    blob_file_path: str = blob.chunks[0].uri.replace("file://", "", 1)
    with open(blob_file_path, "wb") as f:
        return f.write(data)
