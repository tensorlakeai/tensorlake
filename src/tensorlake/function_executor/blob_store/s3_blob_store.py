from typing import List

import httpx

from tensorlake.utils.retries import exponential_backoff

from ..logger import FunctionExecutorLogger

# Customers can upload and download large files, allow up to 1 hour per S3 operation.
_S3_OPERATION_TIMEOUT_SEC = 1 * 60 * 60  # 1 hour
# Keep established connections around for up to 1 hour to maximize download/upload throughput
# when we download function inputs and when we upload the function outputs.
_CONNECTION_KEEP_ALIVE_EXPIRY_SEC = 1 * 60 * 60  # 1 hour
# Do fast retries because we don't want to slow down functions too much due to S3 issues.
_MAX_RETRIES = 3
_INITIAL_RETRY_DELAY_SEC = 0.1
_MAX_RETRY_DELAY_SEC = 10.0


class S3BLOBStore:
    def __init__(self, io_workers_count: int):
        self._client: httpx.Client = httpx.Client(
            http2=True,
            timeout=_S3_OPERATION_TIMEOUT_SEC,
            limits=httpx.Limits(
                max_connections=io_workers_count,
                max_keepalive_connections=io_workers_count,
                keepalive_expiry=_CONNECTION_KEEP_ALIVE_EXPIRY_SEC,
            ),
        )

    def get(
        self,
        uri: str,
        offset: int,
        destination: memoryview,
        logger: FunctionExecutorLogger,
    ) -> bytes:
        """Reads binary data stored in S3 object at the supplied URI and offset into the destination memoryview.

        The URI must be S3 URI (starts with "s3://"). If the URI is not public then
        it must be presigned. Raises Exception on error.
        """

        def on_retry(
            e: Exception,
            sleep_time: float,
            retries: int,
        ) -> None:
            status_code: str = "None"
            response: str = "None"
            if isinstance(e, httpx.HTTPStatusError):
                status_code = e.response.status_code
                response = e.response.text

            # The URI can be presigned, it should not be logged as it provides access to customer data.
            # Response text doesn't contain the URI, so it can be logged.
            logger.error(
                "retrying S3 get",
                status_code=status_code,
                response=response,
                offset=offset,
                size=len(destination),
                sleep_time=sleep_time,
                retries=retries,
                exc_info=e,
            )

        @exponential_backoff(
            max_retries=_MAX_RETRIES,
            initial_delay_seconds=_INITIAL_RETRY_DELAY_SEC,
            max_delay_seconds=_MAX_RETRY_DELAY_SEC,
            retryable_exceptions=(Exception,),
            is_retryable=_is_retriable_exception,
            on_retry=on_retry,
        )
        def get_with_retries() -> bytes:
            with self._client.stream(
                "GET",
                _to_https_uri_schema(uri),
                headers={"Range": f"bytes={offset}-{offset + len(destination) - 1}"},
            ) as streaming_response:
                streaming_response.raise_for_status()
                read_size: int = 0
                for partial_data in streaming_response.iter_bytes():
                    partial_data: bytes
                    destination[read_size : read_size + len(partial_data)] = (
                        partial_data
                    )
                    read_size += len(partial_data)

        try:
            return get_with_retries()
        except httpx.HTTPStatusError as e:
            # The URI can be presigned, it should not be logged as it provides access to customer data.
            # Response text doesn't contain the URI, so it can be logged.
            logger.error(
                "failed to get S3 object",
                status_code=e.response.status_code,
                response=e.response.text,
                offset=offset,
                size=len(destination),
            )
            raise
        except (httpx.RequestError, Exception) as e:
            # The URI can be presigned, it should not be logged as it provides access to customer data.
            # RequestError message doesn't contain the URI, so it can be logged.
            # Generic Exception is not from httpx, should not contain the URI, can be logged.
            logger.error(
                "failed to get S3 object",
                exc_info=e,
                offset=offset,
                size=len(destination),
            )
            raise

    def put(
        self,
        uri: str,
        source: List[memoryview],
        logger: FunctionExecutorLogger,
    ) -> str:
        """Stores the supplied memoryviews in a S3 object at the supplied URI.

        The URI must be S3 URI (starts with "s3://").
        Overwrites existing object. Raises Exception on error.
        Returns the ETag of the stored object.
        """
        source_size: int = sum(len(data) for data in source)

        def on_retry(
            e: Exception,
            sleep_time: float,
            retries: int,
        ) -> None:
            # The URI can be presigned, it should not be logged as it provides access to customer data.
            # Response text doesn't contain the URI, so it can be logged.
            status_code: str = "None"
            response: str = "None"
            if isinstance(e, httpx.HTTPStatusError):
                status_code = e.response.status_code
                response = e.response.text
            logger.error(
                "retrying S3 put",
                status_code=status_code,
                response=response,
                size=source_size,
                sleep_time=sleep_time,
                retries=retries,
                exc_info=e,
            )

        @exponential_backoff(
            max_retries=_MAX_RETRIES,
            initial_delay_seconds=_INITIAL_RETRY_DELAY_SEC,
            max_delay_seconds=_MAX_RETRY_DELAY_SEC,
            retryable_exceptions=(Exception,),
            is_retryable=_is_retriable_exception,
            on_retry=on_retry,
        )
        def put_with_retries() -> str:
            # Don't calculate and use Content-MD5 header because the presigned URL will have to have exactly the
            # same header value on presigning and it's not known while generating the presigned URL.
            # We're calculating sha256 of each serialized object instead and very it on read.
            response = self._client.put(
                _to_https_uri_schema(uri),
                content=source,
                headers={
                    # Content-Length is required for S3 to accept the streaming request body.
                    "Content-Length": str(source_size),
                },
            )
            response.raise_for_status()
            return response.headers["ETag"]

        try:
            return put_with_retries()
        except httpx.HTTPStatusError as e:
            # The URI can be presigned, it should not be logged as it provides access to customer data.
            # Response text doesn't contain the URI, so it can be logged.
            logger.error(
                "failed to put S3 object",
                status_code=e.response.status_code,
                response=e.response.text,
                size=source_size,
            )
            raise
        except (httpx.RequestError, Exception) as e:
            # The URI can be presigned, it should not be logged as it provides access to customer data.
            # RequestError message doesn't contain the URI, so it can be logged.
            # Generic Exception is not from httpx, should not contain the URI, can be logged.
            logger.error("failed to put S3 object", exc_info=e, size=source_size)
            raise


def _is_retriable_exception(e: Exception) -> bool:
    if isinstance(e, httpx.HTTPStatusError):
        # Let's simply retry everything which is not 404 (not found) or 400 (bad request) or 403 (forbidden).
        return e.response.status_code not in [400, 403, 404]
    else:
        # Retry anything else like network errors, request timeouts, etc.
        return True


def _to_https_uri_schema(uri: str) -> str:
    # Example S3 object URI:
    # s3://test-indexify-server-blob-store-eugene-20250411/225b83f4-2aed-40a7-adee-b7a681f817f2
    if uri.startswith("s3://"):
        return "https://" + uri[5:]
    return uri
