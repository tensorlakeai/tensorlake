import io
import queue
import threading
import time
from contextlib import redirect_stderr, redirect_stdout
from enum import Enum
from typing import Any, Dict, Generator, Iterator, List, Optional, Union

import grpc

from tensorlake.functions_sdk.functions import (
    FunctionCallResult,
    GraphInvocationContext,
    TensorlakeFunctionWrapper,
)
from tensorlake.functions_sdk.graph_definition import ComputeGraphMetadata
from tensorlake.functions_sdk.invocation_state.invocation_state import InvocationState

from ..handlers.run_function.response_helper import ResponseHelper
from ..proto.function_executor_pb2 import (
    GetInvocationStateResponse,
    LeaveSessionRequest,
    LeaveSessionResponse,
    RunTaskAllocationsRequest,
    RunTaskAllocationsResponse,
    RunTaskAllocationsSessionClientMessage,
    RunTaskAllocationsSessionServerMessage,
    SerializedObjectChunk,
    SerializedObjectID,
    SerializedObjectManifest,
    SetInvocationStateResponse,
    TaskAllocationInput,
    TaskAllocationOutput,
    UploadSerializedObjectRequest,
    UploadSerializedObjectResponse,
)
from ..proto.google.rpc.code_pb2 import Code
from ..proto.google.rpc.status_pb2 import Status
from ..std_outputs_capture import flush_logs, read_till_the_end
from .chunked_serialized_object import ChunkedSerializedObject
from .message_validators import validate_client_session_message


# The following exceptions are used for control flow in session streams implemented
# as generators. This is similar to StopIteration exception.
class CloseSession(Exception):
    """Exception raised when a session is closed."""


class LeaveSession(Exception):
    """Exception raised when client left the session."""


class _INTERNAL_SESSION_COMMAND(Enum):
    CLOSE_SESSION = 1
    LEAVE_SESSION = 2


class RunTaskAllocationsSession:
    def __init__(
        self,
        id: str,
        function_wrapper: TensorlakeFunctionWrapper,
        function_stdout: io.StringIO,
        function_stderr: io.StringIO,
        graph_metadata: ComputeGraphMetadata,
        logger: Any,
    ):
        self._id = id
        self._function_wrapper = function_wrapper
        self._function_stdout = function_stdout
        self._function_stderr = function_stderr
        self._graph_metadata = graph_metadata
        self._logger = logger.bind(module=__name__, session_id=id)
        # Session state, preserved until the session is closed.
        self._server_messages: queue.SimpleQueue = queue.SimpleQueue()
        # Serialized object ID -> ChunkedSerializedObject
        self._serialized_objects: Dict[str, ChunkedSerializedObject] = {}

        # Joined state, fields are initialized when the session is joined.
        self._process_client_messages_thread: Optional[threading.Thread] = None

        self._logger.info("created session")

    def is_joined(self) -> bool:
        """Returns True if the session is joined, False otherwise."""
        return self._process_client_messages_thread is not None

    def join(
        self, client_stream: Iterator[RunTaskAllocationsSessionClientMessage]
    ) -> Generator[RunTaskAllocationsSessionServerMessage, None, None]:
        """Starts processing the client messages in the scope of the session

        Raises CloseSession when the session is fully closed."""
        if self.is_joined():
            raise RuntimeError("Session is already joined")
        self._logger.info("joined session")

        self._process_client_messages_thread = threading.Thread(
            target=self._process_client_messages,
            name=f"run_task_allocations_session_{self._id}_client_message_processor_thread",
            args=(client_stream,),
            daemon=True,
        )
        self._process_client_messages_thread.start()

        while True:
            server_message: Union[
                RunTaskAllocationsSessionServerMessage, _INTERNAL_SESSION_COMMAND
            ] = self._server_messages.get()
            if isinstance(server_message, _INTERNAL_SESSION_COMMAND):
                if server_message == _INTERNAL_SESSION_COMMAND.CLOSE_SESSION:
                    yield from self._close_session()
                if server_message == _INTERNAL_SESSION_COMMAND.LEAVE_SESSION:
                    yield from self._leave_session()
            else:
                yield server_message

    def _leave_session(
        self,
    ) -> Generator[RunTaskAllocationsSessionServerMessage, None, None]:
        self._logger.info("client is leaving session")
        self._process_client_messages_thread.join()
        self._process_client_messages_thread = None
        yield RunTaskAllocationsSessionServerMessage(
            leave_session_response=LeaveSessionResponse(
                status=Status(
                    code=Code.OK,
                    message="Session left successfully",
                )
            )
        )
        raise LeaveSession()

    def _close_session(
        self,
    ) -> Generator[RunTaskAllocationsSessionServerMessage, None, None]:
        """Closes the session."""
        self._logger.info("closing session")
        try:
            if not self._server_messages.empty():
                self._logger.warning(
                    "closing session with pending messages from server",
                    num_messages=self._server_messages.qsize(),
                )
            yield RunTaskAllocationsSessionServerMessage(
                leave_session_response=LeaveSessionResponse(
                    status=Status(
                        code=Code.OK,
                        message="Session closed successfully",
                    )
                )
            )
        except Exception as e:
            self._logger.error("error while closing session", exc_info=e)
            yield RunTaskAllocationsSessionServerMessage(
                leave_session_response=LeaveSessionResponse(
                    status=Status(
                        code=Code.INTERNAL,
                        message="Error while closing session",
                    )
                )
            )
        finally:
            # All the session resources are freed when the service removes its
            # reference to the session object.
            raise CloseSession()

    def _process_client_messages(
        self, client_stream: Iterator[RunTaskAllocationsSessionClientMessage]
    ) -> None:
        """Processes client messages in the session."""
        try:
            for message in client_stream:
                message: RunTaskAllocationsSessionClientMessage
                self._handle_client_message(message)
        except grpc.RpcError as e:
            # The stream is closed, it's usually due to a network error or client disconnect.
            self._server_messages.put(_INTERNAL_SESSION_COMMAND.LEAVE_SESSION)
        except CloseSession:
            # Client requested to close the session.
            self._server_messages.put(_INTERNAL_SESSION_COMMAND.CLOSE_SESSION)
        except LeaveSession:
            # Client requested to leave the session.
            self._server_messages.put(_INTERNAL_SESSION_COMMAND.LEAVE_SESSION)
        except Exception as e:
            self._logger.error(
                "unexpected exception during client message stream processing",
                exc_info=e,
            )
            # The session is inconsistent state now, forcibly close it.
            self._server_messages.put(_INTERNAL_SESSION_COMMAND.CLOSE_SESSION)

    def _handle_client_message(
        self, message: RunTaskAllocationsSessionClientMessage
    ) -> None:
        """Handles a client message in the session.

        Doesn't raise any exceptions.
        """
        try:
            validate_client_session_message(message)
        except ValueError as e:
            self._logger.error(
                "invalid client message, ignoring",
                message=message,
                exc_info=e,
            )
            return

        if message.HasField("upload_serialized_object_request"):
            self._handle_upload_serialized_object_request(
                message.upload_serialized_object_request
            )
        elif message.HasField("upload_serialized_object_response"):
            self._handle_upload_serialized_object_response(
                message.upload_serialized_object_response
            )
        elif message.HasField("run_task_allocations_request"):
            self._handle_run_task_allocations_request(
                message.run_task_allocations_request
            )
        elif message.HasField("set_invocation_state_response"):
            self._handle_set_invocation_state_response(
                message.set_invocation_state_response
            )
        elif message.HasField("get_invocation_state_response"):
            self._handle_get_invocation_state_response(
                message.get_invocation_state_response
            )
        elif message.HasField("leave_session_request"):
            self._handle_leave_session_request(message.leave_session_request)
        else:
            self._logger.error(
                "unknown client message type",
                message=message,
            )

    def _handle_upload_serialized_object_request(
        self, request: UploadSerializedObjectRequest
    ) -> None:
        """Handles an upload serialized object request.

        Doesn't raise any exceptions.
        """
        if request.HasField("manifest"):
            return self._handle_upload_serialized_object_manifest(request.manifest)
        elif request.HasField("chunk"):
            return self._handle_upload_serialized_object_chunk(request.chunk)

    def _handle_upload_serialized_object_manifest(
        self, manifest: SerializedObjectManifest
    ) -> None:
        """Handles an upload serialized object manifest.

        Doesn't raise any exceptions.
        """
        obj_id: str = manifest.id.value
        if obj_id in self._serialized_objects:
            return self._server_messages.put(
                _upload_serialized_object_response(
                    object_id=obj_id,
                    code=Code.ALREADY_EXISTS,
                    message=f"Serialized object with ID '{obj_id}' already exists",
                )
            )

        self._serialized_objects[obj_id] = ChunkedSerializedObject(manifest)
        self._server_messages.put(
            _upload_serialized_object_response(
                object_id=obj_id,
                code=Code.OK,
                message="Serialized object manifest accepted",
            )
        )

    def _handle_upload_serialized_object_chunk(
        self, chunk: SerializedObjectChunk
    ) -> None:
        """Handles an upload serialized object chunk.

        Doesn't raise any exceptions.
        """
        obj_id: str = chunk.id.value
        obj: Optional[ChunkedSerializedObject] = self._serialized_objects.get(obj_id)
        if obj is None:
            return self._server_messages.put(
                _upload_serialized_object_response(
                    object_id=obj_id,
                    code=Code.NOT_FOUND,
                    message=f"Serialized object with ID '{obj_id}' not found",
                )
            )

        obj.add_chunk(chunk.data)
        self._server_messages.put(
            _upload_serialized_object_response(
                object_id=obj_id,
                code=Code.OK,
                message="Serialized object chunk accepted",
            )
        )

    def _handle_upload_serialized_object_response(
        self, response: UploadSerializedObjectResponse
    ) -> None:
        """Handles an upload serialized object response.

        Doesn't raise any exceptions.
        """
        pass

    def _handle_run_task_allocations_request(
        self, request: RunTaskAllocationsRequest
    ) -> None:
        """Handles a run task allocations request.

        Doesn't raise any exceptions.
        """
        if len(request.allocations) != 1:
            return self._server_messages.put(
                _run_task_allocations_response(
                    code=Code.INVALID_ARGUMENT,
                    message="Only one task allocation is supported per request",
                )
            )

        allocation: TaskAllocationInput = request.allocations[0]

    def _handle_set_invocation_state_response(
        self, response: SetInvocationStateResponse
    ) -> None:
        """Handles a set invocation state response.

        Doesn't raise any exceptions.
        """
        pass

    def _handle_get_invocation_state_response(
        self, response: GetInvocationStateResponse
    ) -> None:
        """Handles a get invocation state response.

        Doesn't raise any exceptions.
        """
        pass

    def _handle_leave_session_request(self, request: LeaveSessionRequest) -> None:
        """Handles a leave session request.

        Doesn't raise any exceptions.
        """
        if request.close:
            raise CloseSession()
        else:
            raise LeaveSession()


def _upload_serialized_object_response(
    object_id: str, code: Code, message: str = ""
) -> RunTaskAllocationsSessionServerMessage:
    """Creates an upload serialized object response message."""
    return RunTaskAllocationsSessionServerMessage(
        upload_serialized_object_response=UploadSerializedObjectResponse(
            id=SerializedObjectID(value=object_id),
            status=Status(
                code=code,
                message=message,
            ),
        )
    )


def _run_task_allocations_response(
    code: Code, message: str = "", outputs: List[TaskAllocationOutput] = []
):
    """Creates a run task allocations response message."""
    return RunTaskAllocationsSessionServerMessage(
        run_task_allocations_response=RunTaskAllocationsResponse(
            status=Status(
                code=code,
                message=message,
            ),
            outputs=outputs,
        )
    )
