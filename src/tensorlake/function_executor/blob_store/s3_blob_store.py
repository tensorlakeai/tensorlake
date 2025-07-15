from typing import Any

# TODO: Implement fast retries for S3 operations.
_MAX_RETRIES = 3

# TODO: Implement.


class S3BLOBStore:
    def get(self, uri: str, offset: int, size: int, logger: Any) -> bytes:
        """Returns binary data stored in S3 object at the supplied URI and offset.

        The URI must be S3 URI (starts with "s3://").
        Raises Exception on error. Raises KeyError if the object doesn't exist.
        """
        try:
            bucket_name, key = _bucket_name_and_object_key_from_uri(uri)
            # response = await asyncio.to_thread(
            #     self._s3_client.get_object, Bucket=bucket_name, Key=key
            # )
            # return response["Body"].read()
        # except BotoClientError as e:
        #     logger.error("failed to get S3 object", uri=uri, exc_info=e)

        #     if e.response["Error"]["Code"] == "NoSuchKey":
        #         raise KeyError(f"Object {key} does not exist in bucket {bucket_name}")
        #     raise
        except Exception as e:
            # The URI can be presigned, it should not be logged.
            logger.error("failed to get S3 object", uri=uri, exc_info=e)
            raise

    def put(self, uri: str, offset: int, data: bytes, logger: Any) -> None:
        """Stores the supplied binary data in a S3 object at the supplied URI and offset.

        The URI must be S3 URI (starts with "s3://").
        Overwrites existing object. Raises Exception on error.
        """
        try:
            bucket_name, key = _bucket_name_and_object_key_from_uri(uri)
            # await asyncio.to_thread(
            #     self._s3_client.put_object, Bucket=bucket_name, Key=key, Body=data
            # )
        except Exception as e:
            # The URI can be presigned, it should not be logged.
            logger.error("failed to set S3 object", uri=uri, exc_info=e)
            raise


def _bucket_name_and_object_key_from_uri(uri: str) -> tuple[str, str]:
    # Example S3 object URI:
    # s3://test-indexify-server-blob-store-eugene-20250411/225b83f4-2aed-40a7-adee-b7a681f817f2
    if not uri.startswith("s3://"):
        raise ValueError(f"S3 URI '{uri}' is missing 's3://' prefix")

    parts = uri[5:].split("/", 1)
    if len(parts) != 2:
        raise ValueError(f"Failed parsing bucket name from S3 URI '{uri}'")
    return parts[0], parts[1]  # bucket_name, key
