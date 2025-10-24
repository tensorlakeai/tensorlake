from concurrent.futures import FIRST_EXCEPTION, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import List

from ..logger import FunctionExecutorLogger
from ..proto.function_executor_pb2 import BLOB, BLOBChunk
from .local_fs_blob_store import LocalFSBLOBStore
from .s3_blob_store import S3BLOBStore

# S3 multipart uploads from EC2 instances gradually speed up until we reach 10 parallel chunk uploads.
# S3 downloads from EC2 instances gradually speed up until we reach 5 parallel chunk downloads.
# Then they slow down gradually. Using a common middleground value of 7 to be able to use a single
# thread pool with a static max workers value.
_MAX_WORKER_THREADS: int = 7
# Upper limit in case a function has low CPU limit.
_IO_WORKER_THREADS_PER_AVAILABLE_CPU: int = 3


@dataclass
class _ChunkInfo:
    index: int
    offset: int


class BLOBStore:
    """Dispatches generic BLOB store calls to their real backends.

    Implements chunking."""

    def __init__(self, available_cpu_count: int, logger: FunctionExecutorLogger):
        """Creates a BLOB store that uses the supplied BLOB stores."""
        max_io_workers: int = min(
            available_cpu_count * _IO_WORKER_THREADS_PER_AVAILABLE_CPU,
            _MAX_WORKER_THREADS,
        )
        self._io_workers_pool = ThreadPoolExecutor(
            max_workers=max_io_workers, thread_name_prefix="BLOBStoreWorker"
        )
        self._local: LocalFSBLOBStore = LocalFSBLOBStore()
        self._s3: S3BLOBStore = S3BLOBStore(io_workers_count=max_io_workers)
        logger.info(
            "BLOBStore initialized",
            available_cpu_count=available_cpu_count,
            max_io_workers=max_io_workers,
        )

    def get(
        self, blob: BLOB, offset: int, size: int, logger: FunctionExecutorLogger
    ) -> bytes:
        """Returns binary data stored in BLOB with the supplied URI at the supplied offset.

        Raises Exception on error.
        """
        if offset + size > _blob_size(blob):
            raise IndexError(
                f"Offset {offset} + size {size} is out of bounds for BLOB chunks of size {_blob_size(blob)}."
            )

        # Read data from BLOB chunks in parallel until all data is read.
        # Minimize data copying by not creating any intermediate bytes/bytearray objects.
        read_chunk_futures: List[Future] = []
        destination: bytearray = bytearray(size)
        destination_view: memoryview = memoryview(destination)
        read_offset: int = offset

        first_chunk_info: _ChunkInfo = _find_chunk(blob, offset)
        chunk_ix: int = first_chunk_info.index
        offset_inside_chunk: int = offset - first_chunk_info.offset
        while read_offset != (offset + size):
            chunk: BLOBChunk = blob.chunks[chunk_ix]
            chunk_read_size: int = min(
                (offset + size) - read_offset, chunk.size - offset_inside_chunk
            )
            destination_offset: int = read_offset - offset
            chunk_in_destination: memoryview = destination_view[
                destination_offset : destination_offset + chunk_read_size
            ]
            read_chunk_futures.append(
                self._io_workers_pool.submit(
                    self._read_into,
                    # Local file chunk URI points at the beginning of the file (not the chunk).
                    # S3 chunk URI points at the beginning of the S3 object (not the chunk). This is performance optimization so we don't
                    # need to presign a ranged S3 URI per chunk. We use a single presigned S3 URI for all BLOB chunks instead.
                    blob_uri=chunk.uri,
                    blob_read_offset=read_offset,
                    destination=chunk_in_destination,
                    logger=logger,
                )
            )

            read_offset += chunk_read_size
            offset_inside_chunk = (
                0  # only read of first chunk can be not aligned at chunk boundary
            )
            chunk_ix += 1

        wait(read_chunk_futures, return_when=FIRST_EXCEPTION)
        for future in read_chunk_futures:
            if future.exception() is not None:
                raise future.exception()

        return destination

    def _read_into(
        self,
        blob_uri: str,
        blob_read_offset: int,
        destination: memoryview,
        logger: FunctionExecutorLogger,
    ) -> bytes:
        if _is_file_uri(blob_uri):
            self._local.get(
                uri=blob_uri,
                offset=blob_read_offset,
                destination=destination,
                logger=logger,
            )
        else:
            self._s3.get(
                uri=blob_uri,
                offset=blob_read_offset,
                destination=destination,
                logger=logger,
            )

    def put(
        self, blob: BLOB, data: List[bytes], logger: FunctionExecutorLogger
    ) -> BLOB:
        """Stores the supplied binary data into the supplied BLOB starting from its very beginning.

        Overwrites BLOB. Raises Exception on error.
        Data can be smaller than the BLOB size, but not larger.
        Returns the updated BLOB with chunks that were used for storing the data starting from the first chunk
        in the original BLOB. The original order of the chunks is preserved. Chunks that were not used for
        storing the data are not added to the returned BLOB. Each chunk in the returned BLOB has its size set to
        the actual size of the data that was written to it and its etag returned by the storage backend.
        """
        blob_size: int = _blob_size(blob)
        data_size: int = sum(len(chunk) for chunk in data)

        if data_size > blob_size:
            raise ValueError(f"Data size {data_size} exceeds BLOB size {blob_size}.")

        # Write data to BLOB chunks in parallel until all data is written.
        # Minimize data copying by not creating any intermediate bytes/bytearray objects.
        data_read_offset: int = 0
        write_chunk_futures: List[Future] = []
        uploaded_chunk_sizes: List[int] = []

        data_ix: int = 0
        read_offset_inside_data: int = 0
        for chunk in blob.chunks:
            chunk: BLOBChunk
            chunk_data: List[memoryview] = []
            chunk_data_size: int = 0
            chunk_offset: int = data_read_offset
            if data_ix == len(data):
                break

            # Fill the chunk with data until it is full.
            while chunk_data_size != chunk.size and data_ix != len(data):
                read_size: int = min(
                    chunk.size - chunk_data_size,
                    len(data[data_ix]) - read_offset_inside_data,
                )
                chunk_data.append(
                    memoryview(data[data_ix])[
                        read_offset_inside_data : read_offset_inside_data + read_size
                    ]
                )
                chunk_data_size += read_size
                read_offset_inside_data += read_size
                data_read_offset += read_size
                if read_offset_inside_data == len(data[data_ix]):
                    data_ix += 1
                    read_offset_inside_data = 0

            # Write the chunk (should be full except the last one).
            # Local file chunk URI points at the beginning of the file (not the chunk).
            # S3 chunk URI contains chunk's index (part number).
            write_chunk_futures.append(
                self._io_workers_pool.submit(
                    self._write_chunk,
                    chunk_uri=chunk.uri,
                    chunk_offset=chunk_offset,
                    source=chunk_data,
                    logger=logger,
                )
            )
            uploaded_chunk_sizes.append(chunk_data_size)

        wait(write_chunk_futures, return_when=FIRST_EXCEPTION)
        uploaded_blob: BLOB = BLOB(
            id=blob.id,
        )
        for ix, future in enumerate(write_chunk_futures):
            if future.exception() is not None:
                raise future.exception()
            # The futures list is ordered by the chunk index, so appending here preserves
            # the original chunks order.
            uploaded_chunk: BLOBChunk = BLOBChunk()
            uploaded_chunk.CopyFrom(blob.chunks[ix])
            uploaded_chunk.size = uploaded_chunk_sizes[ix]
            uploaded_chunk.etag = future.result()
            uploaded_blob.chunks.append(uploaded_chunk)

        return uploaded_blob

    def _write_chunk(
        self,
        chunk_uri: str,
        chunk_offset: int,
        source: List[memoryview],
        logger: FunctionExecutorLogger,
    ) -> str:
        if _is_file_uri(chunk_uri):
            return self._local.put(
                uri=chunk_uri,
                offset=chunk_offset,
                source=source,
                logger=logger,
            )
        else:
            return self._s3.put(
                uri=chunk_uri,
                source=source,
                logger=logger,
            )


def _find_chunk(blob: BLOB, offset: int) -> _ChunkInfo:
    """Returns info of the chunk where the supplied offset starts.

    Raises IndexError if the offset is outside of the BLOB."""
    current_offset: int = 0
    for ix, chunk in enumerate(blob.chunks):
        if current_offset + chunk.size > offset:
            return _ChunkInfo(index=ix, offset=current_offset)
        current_offset += chunk.size

    raise IndexError(
        f"Offset {offset} is out of bounds for BLOB chunks of size {current_offset}."
    )


def _blob_size(blob: BLOB) -> int:
    """Returns the total size of the BLOB."""
    return sum(chunk.size for chunk in blob.chunks)


def _is_file_uri(uri: str) -> bool:
    return uri.startswith("file://")
