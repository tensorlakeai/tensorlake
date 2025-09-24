import os
import sys
import time
from datetime import datetime
from typing import List

from tensorlake.function_executor.blob_store.blob_store import BLOBStore
from tensorlake.function_executor.logger import FunctionExecutorLogger
from tensorlake.function_executor.proto.function_executor_pb2 import (
    BLOB,
    BLOBChunk,
)
from tensorlake.vendor.nanoid import generate as nanoid_generate

# Please delete the bucket after running the benchmark.
TEST_BUCKET_NAME = (
    f"benchmark-function-executor-s3-blob-store-{datetime.now().strftime('%Y%m%d')}"
)


class Benchmark:
    def __init__(self, available_cpu_count: int):
        import boto3  # needs to be installed into the environment manually

        self.logger = FunctionExecutorLogger(context={}, log_file=sys.stdout)
        self.blob_store = BLOBStore(
            available_cpu_count=available_cpu_count, logger=self.logger
        )
        self.s3 = boto3.client("s3")

        existing_buckets: List[str] = [
            bucket["Name"] for bucket in self.s3.list_buckets()["Buckets"]
        ]
        if TEST_BUCKET_NAME not in existing_buckets:
            self.s3.create_bucket(Bucket=TEST_BUCKET_NAME)

    def generate_data(self, size: int) -> bytes:
        return os.urandom(size)

    def random_name(self) -> str:
        return nanoid_generate()

    def presigned_uri(
        self,
        key: str,
        operation: str,
        part_number: int | None = None,
        upload_id: str | None = None,
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

        s3_uri: str = self.s3.generate_presigned_url(
            ClientMethod=operation,
            Params=params,
            ExpiresIn=60,
        )
        return s3_uri.replace("https://", "s3://", 1)

    def run(
        self,
        chunks_count: int = 5,
        chunk_size_bytes: int = 10 * 1024 * 1024,
    ):
        """Runs the benchmark for uploading and downloading BLOBs using BLOBStore and boto3 S3 client."""
        data_size: int = chunks_count * chunk_size_bytes
        blob_key: str = self.random_name()
        blob_data: bytes = self.generate_data(data_size)

        multipart_upload_id: str = self.s3.create_multipart_upload(
            Bucket=TEST_BUCKET_NAME, Key=blob_key
        )["UploadId"]
        blob: BLOB = BLOB()
        for chunk_ix in range(chunks_count):
            blob.chunks.append(
                BLOBChunk(
                    uri=self.presigned_uri(
                        key=blob_key,
                        operation="upload_part",
                        part_number=chunk_ix + 1,
                        upload_id=multipart_upload_id,
                    ),
                    size=chunk_size_bytes,
                )
            )

        start_time = time.monotonic()
        uploaded_blob: BLOB = self.blob_store.put(
            blob=blob,
            data=[blob_data],
            logger=self.logger,
        )
        print(
            f"FE S3 BLOB store: uploaded {data_size / 1024 / 1024} MB in {time.monotonic() - start_time:.2f} seconds"
        )

        self.s3.complete_multipart_upload(
            Bucket=TEST_BUCKET_NAME,
            Key=blob_key,
            UploadId=multipart_upload_id,
            MultipartUpload={
                "Parts": [
                    {"ETag": uploaded_chunk.etag, "PartNumber": ix + 1}
                    for ix, uploaded_chunk in enumerate(uploaded_blob.chunks)
                ]
            },
        )

        blob: BLOB = BLOB()
        for chunk_ix in range(chunks_count):
            blob.chunks.append(
                BLOBChunk(
                    uri=self.presigned_uri(
                        key=blob_key,
                        operation="get_object",
                    ),
                    size=chunk_size_bytes,
                )
            )

        start_time = time.monotonic()
        got_blob_data: bytes = self.blob_store.get(
            blob=blob,
            offset=0,
            size=data_size,
            logger=self.logger,
        )
        print(
            f"FE S3 BLOB store: downloaded {data_size / 1024 / 1024} MB in {time.monotonic() - start_time:.2f} seconds"
        )
        assert got_blob_data == blob_data, "Data mismatch in downloaded blob"

        start_time = time.monotonic()
        self.s3.put_object(Bucket=TEST_BUCKET_NAME, Key=blob_key, Body=blob_data)
        print(
            f"boto3: uploaded {data_size / 1024 / 1024} MB in {time.monotonic() - start_time:.2f} seconds"
        )

        start_time = time.monotonic()
        got_blob_data: bytes = self.s3.get_object(
            Bucket=TEST_BUCKET_NAME, Key=blob_key
        )["Body"].read()
        print(
            f"boto3: downloaded {data_size / 1024 / 1024} MB in {time.monotonic() - start_time:.2f} seconds"
        )
        assert got_blob_data == blob_data, "Data mismatch in downloaded blob"


if __name__ == "__main__":
    # Use a high CPU limit to use optimal number of IO workers.
    Benchmark(available_cpu_count=10).run(
        # Max is 10000, all IO is parallelized at chunk level.
        chunks_count=5,
        # 100 MB, min chunk size is 5 MB, max chunk size is 5 GB
        chunk_size_bytes=100 * 1024 * 1024,
    )
