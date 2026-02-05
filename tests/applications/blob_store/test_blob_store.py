import sys
import unittest
from typing import List

from parameterized import parameterized
from testing import create_tmp_blob, read_tmp_blob_bytes, write_tmp_blob_bytes

from tensorlake.applications import InternalError
from tensorlake.applications.blob_store import BLOB, BLOBChunk, BLOBStore
from tensorlake.applications.internal_logger import InternalLogger

TESTED_CHUNKS = [
    ("single_big_chunk", 1, 1024),
    ("multiple_small_chunks", 3, 5),
    ("one_small_chunk", 1, 5),
]


class TestBLOBStore(unittest.TestCase):
    def setUp(self):
        self.logger = InternalLogger.get_logger()
        self.blob_store = BLOBStore(available_cpu_count=1)

    def generate_data(self, size: int) -> bytes:
        """Generates a pattern of data to be used in tests."""
        charset = b"abcdefghijklmnopqrstuvwxyz"
        return (charset * (size // len(charset) + 1))[:size]

    @parameterized.expand(TESTED_CHUNKS)
    def test_get_full_blob(self, case_name: str, chunks_count: int, chunk_size: int):
        blob: BLOB = create_tmp_blob(
            id="test-blob", chunks_count=chunks_count, chunk_size=chunk_size
        )
        blob_size: int = chunks_count * chunk_size
        expect_blob_data: bytes = self.generate_data(blob_size)
        write_tmp_blob_bytes(
            blob=blob,
            data=expect_blob_data,
        )
        got_blob_data: bytearray = self.blob_store.get(
            blob=blob, offset=0, size=blob_size, logger=self.logger
        )
        self.assertEqual(got_blob_data, expect_blob_data)

    @parameterized.expand(TESTED_CHUNKS)
    def test_get_half_data_from_blob_start(
        self, case_name: str, chunks_count: int, chunk_size: int
    ):
        blob: BLOB = create_tmp_blob(
            id="test-blob", chunks_count=chunks_count, chunk_size=chunk_size
        )
        blob_size: int = chunks_count * chunk_size
        expect_blob_data: bytes = self.generate_data(blob_size)
        write_tmp_blob_bytes(
            blob=blob,
            data=expect_blob_data,
        )
        got_blob_data: bytearray = self.blob_store.get(
            blob=blob,
            offset=0,
            size=blob_size // 2,
            logger=self.logger,
        )
        self.assertEqual(got_blob_data, expect_blob_data[: blob_size // 2])

    @parameterized.expand(TESTED_CHUNKS)
    def test_get_half_data_before_blob_end(
        self, case_name: str, chunks_count: int, chunk_size: int
    ):
        blob: BLOB = create_tmp_blob(
            id="test-blob", chunks_count=chunks_count, chunk_size=chunk_size
        )
        blob_size: int = chunks_count * chunk_size
        expect_blob_data: bytes = self.generate_data(blob_size)
        write_tmp_blob_bytes(
            blob=blob,
            data=expect_blob_data,
        )
        got_blob_data: bytearray = self.blob_store.get(
            blob=blob,
            offset=blob_size // 2,
            size=blob_size // 2 + (blob_size % 2),
            logger=self.logger,
        )
        self.assertEqual(got_blob_data, expect_blob_data[blob_size // 2 :])

    @parameterized.expand(TESTED_CHUNKS)
    def test_get_half_data_from_blob_middle(
        self, case_name: str, chunks_count: int, chunk_size: int
    ):
        blob: BLOB = create_tmp_blob(
            id="test-blob", chunks_count=chunks_count, chunk_size=chunk_size
        )
        blob_size: int = chunks_count * chunk_size
        expect_blob_data: bytes = self.generate_data(blob_size)
        write_tmp_blob_bytes(
            blob=blob,
            data=expect_blob_data,
        )
        start: int = blob_size // 2 // 2
        end: int = start + blob_size // 2
        got_blob_data: bytes = self.blob_store.get(
            blob=blob,
            offset=start,
            size=end - start,
            logger=self.logger,
        )
        self.assertEqual(got_blob_data, expect_blob_data[start:end])

    @parameterized.expand(TESTED_CHUNKS)
    def test_get_with_offset_past_blob_size(
        self, case_name: str, chunks_count: int, chunk_size: int
    ):
        blob: BLOB = create_tmp_blob(
            id="test-blob", chunks_count=chunks_count, chunk_size=chunk_size
        )
        blob_size: int = chunks_count * chunk_size
        with self.assertRaises(InternalError):
            self.blob_store.get(
                blob=blob,
                offset=blob_size + 1,
                size=1,
                logger=self.logger,
            )

    @parameterized.expand(TESTED_CHUNKS)
    def test_get_with_size_past_blob_size(
        self, case_name: str, chunks_count: int, chunk_size: int
    ):
        blob: BLOB = create_tmp_blob(
            id="test-blob", chunks_count=chunks_count, chunk_size=chunk_size
        )
        blob_size: int = chunks_count * chunk_size
        with self.assertRaises(InternalError):
            self.blob_store.get(
                blob=blob,
                offset=blob_size - 1,
                size=2,
                logger=self.logger,
            )

    @parameterized.expand(TESTED_CHUNKS)
    def test_put_full_blob(self, case_name: str, chunks_count: int, chunk_size: int):
        blob: BLOB = create_tmp_blob(
            id="test-blob", chunks_count=chunks_count, chunk_size=chunk_size
        )
        blob_size: int = chunks_count * chunk_size
        expect_blob_data: List[bytes] = [
            self.generate_data(chunk_size) for _ in range(chunks_count)
        ]
        uploaded_blob: BLOB = self.blob_store.put(
            blob=blob,
            data=expect_blob_data,
            logger=self.logger,
        )
        uploaded_blob: BLOB = uploaded_blob
        self.assertEqual(len(uploaded_blob.chunks), chunks_count)
        for ix, uploaded_chunk in enumerate(uploaded_blob.chunks):
            self.assertIsNotNone(uploaded_chunk.etag)
            self.assertEqual(uploaded_chunk.size, chunk_size)
            self.assertEqual(uploaded_chunk.uri, blob.chunks[ix].uri)

        got_blob_data: bytes = read_tmp_blob_bytes(
            blob=blob,
            offset=0,
            size=blob_size,
        )
        self.assertEqual(got_blob_data, b"".join(expect_blob_data))

    @parameterized.expand(TESTED_CHUNKS)
    def test_put_full_blob_from_single_bytes(
        self, case_name: str, chunks_count: int, chunk_size: int
    ):
        blob: BLOB = create_tmp_blob(
            id="test-blob", chunks_count=chunks_count, chunk_size=chunk_size
        )
        blob_size: int = chunks_count * chunk_size
        expect_blob_data: List[bytes] = [
            self.generate_data(1) for _ in range(chunks_count * chunk_size)
        ]
        uploaded_blob: BLOB = self.blob_store.put(
            blob=blob,
            data=expect_blob_data,
            logger=self.logger,
        )
        uploaded_blob: BLOB = uploaded_blob
        self.assertEqual(len(uploaded_blob.chunks), chunks_count)
        for ix, uploaded_chunk in enumerate(uploaded_blob.chunks):
            self.assertIsNotNone(uploaded_chunk.etag)
            self.assertEqual(uploaded_chunk.size, chunk_size)
            self.assertEqual(uploaded_chunk.uri, blob.chunks[ix].uri)

        got_blob_data: bytes = read_tmp_blob_bytes(
            blob=blob,
            offset=0,
            size=blob_size,
        )
        self.assertEqual(got_blob_data, b"".join(expect_blob_data))

    @parameterized.expand(TESTED_CHUNKS)
    def test_put_first_chunk(self, case_name: str, chunks_count: int, chunk_size: int):
        blob: BLOB = create_tmp_blob(
            id="test-blob", chunks_count=chunks_count, chunk_size=chunk_size
        )
        expect_blob_data: List[bytes] = [self.generate_data(chunk_size)]
        uploaded_blob: BLOB = self.blob_store.put(
            blob=blob,
            data=expect_blob_data,
            logger=self.logger,
        )
        uploaded_blob: BLOB = uploaded_blob
        self.assertEqual(len(uploaded_blob.chunks), 1)
        uploaded_chunk: BLOBChunk = uploaded_blob.chunks[0]
        self.assertIsNotNone(uploaded_chunk.etag)
        self.assertEqual(uploaded_chunk.size, chunk_size)
        self.assertEqual(uploaded_chunk.uri, blob.chunks[0].uri)

        got_blob_data: bytes = read_tmp_blob_bytes(
            blob=blob,
            offset=0,
            size=chunk_size,
        )
        self.assertEqual(got_blob_data, b"".join(expect_blob_data))

    @parameterized.expand(TESTED_CHUNKS)
    def test_put_half_first_chunk(
        self, case_name: str, chunks_count: int, chunk_size: int
    ):
        blob: BLOB = create_tmp_blob(
            id="test-blob", chunks_count=chunks_count, chunk_size=chunk_size
        )
        expect_blob_data: List[bytes] = [self.generate_data(chunk_size // 2)]
        uploaded_blob: BLOB = self.blob_store.put(
            blob=blob,
            data=expect_blob_data,
            logger=self.logger,
        )
        uploaded_blob: BLOB = uploaded_blob
        self.assertEqual(len(uploaded_blob.chunks), 1)
        uploaded_chunk: BLOBChunk = uploaded_blob.chunks[0]
        self.assertIsNotNone(uploaded_chunk.etag)
        self.assertEqual(uploaded_chunk.size, chunk_size // 2)
        self.assertEqual(uploaded_chunk.uri, blob.chunks[0].uri)

        got_blob_data: bytes = read_tmp_blob_bytes(
            blob=blob,
            offset=0,
            size=chunk_size,
        )
        self.assertEqual(got_blob_data, b"".join(expect_blob_data))

    @parameterized.expand(TESTED_CHUNKS)
    def test_put_half_blob_data(
        self, case_name: str, chunks_count: int, chunk_size: int
    ):
        blob: BLOB = create_tmp_blob(
            id="test-blob", chunks_count=chunks_count, chunk_size=chunk_size
        )
        expect_blob_data: List[bytes] = [
            self.generate_data(chunk_size * chunks_count // 2)
        ]
        uploaded_blob: BLOB = self.blob_store.put(
            blob=blob,
            data=expect_blob_data,
            logger=self.logger,
        )
        uploaded_blob: BLOB = uploaded_blob
        self.assertEqual(
            len(uploaded_blob.chunks), (len(expect_blob_data[0]) - 1) // chunk_size + 1
        )
        for ix, uploaded_chunk in enumerate(uploaded_blob.chunks):
            self.assertIsNotNone(uploaded_chunk.etag)
            if ix == len(uploaded_blob.chunks) - 1:
                self.assertEqual(
                    uploaded_chunk.size, len(expect_blob_data[0]) % chunk_size
                )
            else:
                self.assertEqual(uploaded_chunk.size, chunk_size)
            self.assertEqual(uploaded_chunk.uri, blob.chunks[ix].uri)

        got_blob_data: bytes = read_tmp_blob_bytes(
            blob=blob,
            offset=0,
            size=sum(len(data) for data in expect_blob_data),
        )
        self.assertEqual(got_blob_data, b"".join(expect_blob_data))

    @parameterized.expand(TESTED_CHUNKS)
    def test_put_data_bigger_than_blob(
        self, case_name: str, chunks_count: int, chunk_size: int
    ):
        blob: BLOB = create_tmp_blob(
            id="test-blob", chunks_count=chunks_count, chunk_size=chunk_size
        )
        with self.assertRaises(InternalError):
            self.blob_store.put(
                blob=blob,
                data=[b"a" * (chunks_count * chunk_size), b"b"],
                logger=self.logger,
            )


if __name__ == "__main__":
    unittest.main()
