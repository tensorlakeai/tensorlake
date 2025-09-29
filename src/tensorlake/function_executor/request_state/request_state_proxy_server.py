import hashlib
import queue
import threading
from typing import Any, Iterator

import grpc

from ..proto.function_executor_pb2 import (
    GetRequestStateRequest,
    RequestStateRequest,
    RequestStateResponse,
    SerializedObject,
    SerializedObjectEncoding,
    SerializedObjectManifest,
    SetRequestStateRequest,
)
from .response_validator import ResponseValidator


class RequestStateProxyServer:
    """A gRPC server that proxies RequestState calls to the gRPC client.

    The gRPC client is responsible for the actual implementation of the RequestState.
    We do the proxying to remove authorization logic and credentials from Function Executor.
    This improves security posture of Function Executor because it may run untrusted code.
    """

    def __init__(
        self,
        encoding: SerializedObjectEncoding,
        client_responses: Iterator[RequestStateResponse],
        logger: Any,
    ):
        self._client_responses: Iterator[RequestStateResponse] = client_responses
        self._encoding: SerializedObjectEncoding = encoding
        self._logger: Any = logger.bind(module=__name__)
        self._reciever_thread: threading.Thread = threading.Thread(
            target=self._reciever
        )
        self._request_queue: queue.SimpleQueue = queue.SimpleQueue()
        # This lock protects everything below.
        self._lock: threading.Lock = threading.Lock()
        # Python supports big integers natively so we don't need
        # to be worried about interger overflows.
        self._request_seq_num: int = 0
        # Request ID -> Client Response.
        self._response_map: dict[str, RequestStateResponse] = {}
        self._new_response: threading.Condition = threading.Condition(self._lock)

    def run(self) -> Iterator[RequestStateRequest]:
        # There's no need to implement shutdown of the server and its threads because
        # the server lives while the Function Executor process lives.
        self._reciever_thread.start()
        yield from self._sender()

    def _reciever(self) -> None:
        self._logger.info("reciever thread started")
        try:
            for response in self._client_responses:
                validator = ResponseValidator(response)
                try:
                    validator.check()
                except ValueError as e:
                    self._logger.error("invalid response from the client", exc_info=e)
                    continue

                with self._lock:
                    self._response_map[response.state_request_id] = response
                    self._new_response.notify_all()
        except grpc.RpcError:
            self._logger.info("shutting down, client disconnected")
            # This is the only shutdown path for the server.
            self._request_queue.put("shutdown")
        except Exception as e:
            self._logger.error("error in reciever thread, exiting", exc_info=e)

    def _sender(self) -> Iterator[RequestStateRequest]:
        while True:
            request: Any = self._request_queue.get()
            if request == "shutdown":
                self._logger.info("sender thread shutting down")
                return

            request: RequestStateRequest
            yield request
            with self._lock:
                # Wait until we get a response for the request.
                # This allows to ensure a serialized order of reads and writes so
                # we can avoid a read returning not previously written value.
                self._new_response.wait()

    def set(self, allocation_id: str, key: str, data: bytes) -> None:
        with self._lock:
            state_request_id: str = str(self._request_seq_num)
            self._request_seq_num += 1

            request = RequestStateRequest(
                state_request_id=state_request_id,
                allocation_id=allocation_id,
                set=SetRequestStateRequest(
                    key=key,
                    value=SerializedObject(
                        manifest=SerializedObjectManifest(
                            encoding=self._encoding,
                            encoding_version=0,
                            size=len(data),
                            sha256_hash=hashlib.sha256(data).hexdigest(),
                        ),
                        data=data,
                    ),
                ),
            )
            self._request_queue.put(request)
            while state_request_id not in self._response_map:
                self._new_response.wait()

            response: RequestStateResponse = self._response_map.pop(state_request_id)
            if response.state_request_id != state_request_id:
                self._logger.error(
                    "response state_request_id doesn't match actual request_id",
                    state_request_id=state_request_id,
                    response=response,
                )
                raise RuntimeError(
                    "response state_request_id doesn't match actual request_id"
                )
            if not response.HasField("set"):
                self._logger.error(
                    "set response is missing in the client response",
                    state_request_id=state_request_id,
                    response=response,
                )
                raise RuntimeError("set response is missing in the client response")
            if not response.success:
                self._logger.error(
                    "failed to set the request state for key",
                    key=key,
                )
                raise RuntimeError("failed to set the request state for key")

    def get(self, allocation_id: str, key: str) -> bytes | None:
        with self._lock:
            state_request_id: str = str(self._request_seq_num)
            self._request_seq_num += 1

            request = RequestStateRequest(
                state_request_id=state_request_id,
                allocation_id=allocation_id,
                get=GetRequestStateRequest(
                    key=key,
                ),
            )
            self._request_queue.put(request)
            while state_request_id not in self._response_map:
                self._new_response.wait()

            response: RequestStateResponse = self._response_map.pop(state_request_id)
            if response.state_request_id != state_request_id:
                self._logger.error(
                    "response state_request_id doesn't match actual state_request_id",
                    state_request_id=state_request_id,
                    response=response,
                )
                raise RuntimeError(
                    "response state_request_id doesn't match actual state_request_id"
                )
            if not response.HasField("get"):
                self._logger.error(
                    "get response is missing in the client response",
                    state_request_id=state_request_id,
                    response=response,
                )
                raise RuntimeError("get response is missing in the client response")
            if not response.success:
                self._logger.error(
                    "failed to get the request state for key",
                    key=key,
                )
                raise RuntimeError("failed to get the request state for key")
            if not response.get.HasField("value"):
                return None

            so_value: SerializedObject = response.get.value
            if so_value.manifest.encoding != self._encoding:
                self._logger.error(
                    "unexpected encoding of the request state value",
                    key=key,
                    encoding=SerializedObjectEncoding.Name(so_value.manifest.encoding),
                    expected_encoding=SerializedObjectEncoding.Name(self._encoding),
                )
                raise RuntimeError("unexpected encoding of the request state value")

            return so_value.data
