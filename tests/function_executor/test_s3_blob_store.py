import os
import sys
import unittest
from datetime import datetime
from typing import List, Optional

from tensorlake.function_executor.blob_store.s3_blob_store import S3BLOBStore
from tensorlake.function_executor.logger import FunctionExecutorLogger
from tensorlake.vendor.nanoid import generate as nanoid_generate

# Please delete the bucket after running the test.
TEST_BUCKET_NAME = (
    f"test-function-executor-s3-blob-store-{datetime.now().strftime('%Y%m%d')}"
)


class TestS3BLOBStore(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import boto3  # needs to be installed into the environment manually

        cls.s3_blob_store = S3BLOBStore(io_workers_count=5)
        cls.logger = FunctionExecutorLogger(context={}, log_file=sys.stdout)
        cls.s3 = boto3.client("s3")

        existing_buckets: List[str] = [
            bucket["Name"] for bucket in cls.s3.list_buckets()["Buckets"]
        ]
        if TEST_BUCKET_NAME not in existing_buckets:
            cls.s3.create_bucket(Bucket=TEST_BUCKET_NAME)

    @classmethod
    def generate_data(cls, size: int) -> bytes:
        return os.urandom(size)

    @classmethod
    def random_name(cls) -> str:
        """Generates a random name for the S3 object."""
        return nanoid_generate()

    @classmethod
    def presigned_uri(
        cls,
        key: str,
        operation: str,
        part_number: Optional[int] = None,
        upload_id: Optional[str] = None,
    ) -> str:
        """Generates a presigned URL for the S3 object."""
        params = {
            "Bucket": TEST_BUCKET_NAME,
            "Key": key,
        }
        if part_number is not None:
            params["PartNumber"] = part_number
        if upload_id is not None:
            params["UploadId"] = upload_id

        s3_uri: str = cls.s3.generate_presigned_url(
            ClientMethod=operation,
            Params=params,
            ExpiresIn=60,
        )
        return s3_uri.replace("https://", "s3://", 1)

    def test_get_full_object(self):
        """Tests getting a full object from S3."""
        expect_data: bytes = self.generate_data(1024)
        key: str = self.random_name()
        self.s3.put_object(Bucket=TEST_BUCKET_NAME, Key=key, Body=expect_data)
        got_data: bytearray = bytearray(1024)
        self.s3_blob_store.get(
            uri=self.presigned_uri(key=key, operation="get_object"),
            offset=0,
            destination=memoryview(got_data),
            logger=self.logger,
        )
        self.assertEqual(got_data, expect_data)

    def test_get_partial_object(self):
        """Tests getting a partial object from S3."""
        expect_data: bytes = self.generate_data(1024)
        key: str = self.random_name()
        self.s3.put_object(Bucket=TEST_BUCKET_NAME, Key=key, Body=expect_data)
        got_data: bytearray = bytearray(1024)
        self.s3_blob_store.get(
            uri=self.presigned_uri(key=key, operation="get_object"),
            offset=100,
            destination=memoryview(got_data)[100:600],
            logger=self.logger,
        )
        self.assertEqual(got_data[100:600], expect_data[100:600])

    def test_get_non_existent_object(self):
        """Tests getting a non-existent object from S3."""
        with self.assertRaises(Exception):
            got_data: bytearray = bytearray(1024)
            self.s3_blob_store.get(
                uri=self.presigned_uri(
                    key="non-existent-object", operation="get_object"
                ),
                offset=0,
                destination=memoryview(got_data),
                logger=self.logger,
            )

    def test_get_malformed_presigned_uri(self):
        """Tests getting an object with an invalid presigned URI."""
        with self.assertRaises(Exception):
            got_data: bytearray = bytearray(1024)
            self.s3_blob_store.get(
                uri="s3://malformed-uri",
                offset=0,
                destination=memoryview(got_data),
                logger=self.logger,
            )

    def test_put_object(self):
        """Tests putting an object to S3."""
        expect_data: List[memoryview] = [
            memoryview(self.generate_data(1024)),
            memoryview(self.generate_data(1024)),
            memoryview(self.generate_data(1024)),
        ]
        key: str = self.random_name()
        self.s3_blob_store.put(
            uri=self.presigned_uri(key=key, operation="put_object"),
            source=expect_data,
            logger=self.logger,
        )
        got_data: bytes = self.s3.get_object(Bucket=TEST_BUCKET_NAME, Key=key)[
            "Body"
        ].read()
        self.assertEqual(got_data, b"".join(expect_data))

    def test_overwrite_object(self):
        """Tests overwriting an existing object in S3."""
        prev_data: bytes = self.generate_data(1024)
        key: str = self.random_name()
        self.s3_blob_store.put(
            uri=self.presigned_uri(key=key, operation="put_object"),
            source=[memoryview(prev_data)],
            logger=self.logger,
        )
        new_data: bytes = self.generate_data(512)
        self.s3_blob_store.put(
            uri=self.presigned_uri(key=key, operation="put_object"),
            source=[memoryview(new_data)],
            logger=self.logger,
        )
        got_data: bytes = self.s3.get_object(Bucket=TEST_BUCKET_NAME, Key=key)[
            "Body"
        ].read()
        self.assertEqual(got_data, new_data)

    def test_put_malformed_presigned_uri(self):
        """Tests putting an object with an invalid presigned URI."""
        with self.assertRaises(Exception):
            self.s3_blob_store.put(
                uri="s3://malformed-uri",
                source=[memoryview(b"test")],
                logger=self.logger,
            )

    def test_multipart_upload(self):
        """Tests multipart upload and download of a large object."""
        key: str = self.random_name()
        chunks_count: int = 4
        chunk_size_bytes: int = (
            5 * 1024 * 1024
        )  # 5 MB, min chunk size is 5 MB, max chunk size is 5 GB
        chunks_data: List[memoryview] = [
            memoryview(self.generate_data(chunk_size_bytes))
            for _ in range(chunks_count)
        ]
        chunk_etags: List[str] = []
        multipart_upload_id: str = self.s3.create_multipart_upload(
            Bucket=TEST_BUCKET_NAME, Key=key
        )["UploadId"]
        try:
            for chunk_ix in range(chunks_count):
                chunk_etag = self.s3_blob_store.put(
                    uri=self.presigned_uri(
                        key=key,
                        operation="upload_part",
                        part_number=chunk_ix + 1,
                        upload_id=multipart_upload_id,
                    ),
                    source=[chunks_data[chunk_ix]],
                    logger=self.logger,
                )
                chunk_etags.append(chunk_etag)

            self.s3.complete_multipart_upload(
                Bucket=TEST_BUCKET_NAME,
                Key=key,
                UploadId=multipart_upload_id,
                MultipartUpload={
                    "Parts": [
                        {"ETag": chunk_etags[i], "PartNumber": i + 1}
                        for i in range(chunks_count)
                    ]
                },
            )
        except Exception:
            self.s3.abort_multipart_upload(
                Bucket=TEST_BUCKET_NAME, Key=key, UploadId=multipart_upload_id
            )
            raise

        got_data: bytes = self.s3.get_object(Bucket=TEST_BUCKET_NAME, Key=key)[
            "Body"
        ].read()
        self.assertEqual(got_data, b"".join(chunks_data))


if __name__ == "__main__" and "run" in sys.argv:
    # Run the test only if it's triggered manually by passing "run" as an argument.
    # This is because this test requires AWS credentials and leaves side effects.
    # It's not made to run in CI.
    unittest.TextTestRunner().run(
        unittest.TestLoader().loadTestsFromTestCase(TestS3BLOBStore)
    )
