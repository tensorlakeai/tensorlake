import io
import queue
import threading
import time
from contextlib import redirect_stderr, redirect_stdout
from enum import Enum
from typing import Any, Dict, Generator, Iterator, List, Optional, Union

import grpc

from tensorlake.functions_sdk.data_objects import TensorlakeData
from tensorlake.functions_sdk.functions import (
    FunctionCallResult,
    GraphInvocationContext,
    TensorlakeFunctionWrapper,
)
from tensorlake.functions_sdk.graph_definition import ComputeGraphMetadata
from tensorlake.functions_sdk.invocation_state.invocation_state import InvocationState

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
from .function_input import FunctionInput
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


# If the timeout expired we assume that the thread is not healthy and the session is in an inconsistent state.
_CLIENT_MESSAGE_PROCESSOR_THREAD_NORMAL_EXIT_TIMEOUT_SEC = 5


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
        self._client_messages: queue.SimpleQueue = queue.SimpleQueue()
        # Serialized object ID -> ChunkedSerializedObject
        self._serialized_objects: Dict[str, ChunkedSerializedObject] = {}
        self._client_messages_processor_thread: threading.Thread = threading.Thread(
            target=self._process_client_messages,
            name=f"run_task_allocations_session_{self._id}_client_message_processor_thread",
            daemon=True,
        )
        self._client_messages_processor_thread.start()

        # Joined state, fields are initialized when the session is joined.
        self._read_client_messages_thread: Optional[threading.Thread] = None

        self._logger.info("created session")

    def is_joined(self) -> bool:
        """Returns True if the session is joined, False otherwise."""
        return self._read_client_messages_thread is not None

    def join(
        self, client_stream: Iterator[RunTaskAllocationsSessionClientMessage]
    ) -> Generator[RunTaskAllocationsSessionServerMessage, None, None]:
        """Starts processing the client messages in the scope of the session

        Raises CloseSession when the session is fully closed."""
        self._logger.info("joined session")

        self._read_client_messages_thread = threading.Thread(
            target=self._read_client_messages,
            name=f"run_task_allocations_session_{self._id}_read_client_messages_thread",
            args=(client_stream,),
            daemon=True,
        )
        self._read_client_messages_thread.start()

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
        self._read_client_messages_thread.join()
        self._read_client_messages_thread = None
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
            if not self._client_messages.empty():
                self._logger.warning(
                    "closing session with pending messages from client",
                    num_messages=self._client_messages.qsize(),
                )
            if (
                self._client_messages_processor_thread.join(
                    timeout=_CLIENT_MESSAGE_PROCESSOR_THREAD_NORMAL_EXIT_TIMEOUT_SEC
                )
                and self._read_client_messages_thread.is_alive()
            ):
                # This typically happens when customer code hangs so the thread can't exit.
                yield RunTaskAllocationsSessionServerMessage(
                    leave_session_response=LeaveSessionResponse(
                        status=Status(
                            code=Code.INTERNAL,
                            message="Client messages processor thread is still running, cannot close session",
                        )
                    )
                )
            else:
                yield RunTaskAllocationsSessionServerMessage(
                    leave_session_response=LeaveSessionResponse(
                        status=Status(
                            code=Code.OK,
                            message="Session closed successfully",
                        )
                    )
                )
                # All the session resources are freed when the service removes its
                # reference to the session object.
                raise CloseSession()
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

    def _read_client_messages(
        self, client_stream: Iterator[RunTaskAllocationsSessionClientMessage]
    ) -> None:
        """Processes client messages in the session."""
        try:
            for message in client_stream:
                message: RunTaskAllocationsSessionClientMessage
                self._client_messages.put(message)
        except grpc.RpcError as e:
            # The stream is closed, it's usually due to a network error or client disconnect.
            # This implies leaving the session.
            self._server_messages.put(_INTERNAL_SESSION_COMMAND.LEAVE_SESSION)

    def _process_client_messages(self) -> None:
        """Processes client messages in the session."""
        while True:
            message: RunTaskAllocationsSessionClientMessage = (
                self._client_messages.get()
            )
            try:
                self._handle_client_message(message)
            except CloseSession:
                self._server_messages.put(_INTERNAL_SESSION_COMMAND.CLOSE_SESSION)
                return
            except Exception as e:
                self._logger.error(
                    "unexpected exception during handling of client message",
                    exc_info=e,
                )
                # The session is inconsistent state now, forcibly close it.
                self._server_messages.put(_INTERNAL_SESSION_COMMAND.CLOSE_SESSION)
                return

    def _handle_client_message(
        self, message: RunTaskAllocationsSessionClientMessage
    ) -> None:
        """Handles a client message in the session.

        The handlers can read any number of client messages from the client queue
        and add any number of server messages to the server queue.
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
        if len(request.inputs) != 1:
            return self._server_messages.put(
                _run_task_allocations_response(
                    code=Code.INVALID_ARGUMENT,
                    message=f"One task allocation is required per request, got {len(request.inputs)}",
                )
            )

        function_inputs: List[FunctionInput] = []

        for allocation_input in request.inputs:
            allocation_input: TaskAllocationInput
            if allocation_input.function_input_id.value not in self._serialized_objects:
                return self._server_messages.put(
                    _run_task_allocations_response(
                        code=Code.NOT_FOUND,
                        message=f"Serialized object with ID '{allocation_input.function_input_id.value}' not found",
                    )
                )
            obj: ChunkedSerializedObject = self._serialized_objects[
                allocation_input.function_input_id.value
            ]
            try:
                obj.validate()
                func_input: TensorlakeData = obj.to_tensorlake_data()
            except ValueError as e:
                return self._server_messages.put(
                    _run_task_allocations_response(
                        code=Code.INVALID_ARGUMENT,
                        message=f"Serialized object with ID '{allocation_input.function_input_id.value}' is invalid: {str(e)}",
                    )
                )

            func_init_value: Optional[TensorlakeData] = None
            if allocation_input.HasField("function_init_value_id"):
                if (
                    allocation_input.function_init_value_id.value
                    not in self._serialized_objects
                ):
                    return self._server_messages.put(
                        _run_task_allocations_response(
                            code=Code.NOT_FOUND,
                            message=f"Serialized object with ID '{allocation_input.function_init_value_id.value}' not found",
                        )
                    )
                obj: ChunkedSerializedObject = self._serialized_objects[
                    allocation_input.function_init_value_id.value
                ]
                try:
                    obj.validate()
                    func_init_value = obj.to_tensorlake_data()
                except ValueError as e:
                    return self._server_messages.put(
                        _run_task_allocations_response(
                            code=Code.INVALID_ARGUMENT,
                            message=f"Serialized object with ID '{allocation_input.function_init_value_id.value}' is invalid: {str(e)}",
                        )
                    )

            function_inputs.append(
                FunctionInput(
                    task_allocation_input=allocation_input,
                    input=func_input,
                    init_value=func_init_value,
                )
            )

        self._run_function(function_inputs)

    def _run_function(self, inputs: List[FunctionInput]) -> None:
        """Runs the function with the supplied inputs in the session.

        Function stdout and stderr are captured so they don't get into Function Executor process stdout
        and stderr. Exceptions in customer function are passed in the responses. Only exceptions in our
        own code are handled locally. Doesn't raise any exceptions.
        """
        # [0] only, no batching yet.
        input: FunctionInput = inputs[0]
        logger = self._logger.bind(
            invocation_id=input.task_allocation_input.graph_invocation_id,
            task_id=input.task_allocation_input.task_id,
            allocation_id=input.task_allocation_input.allocation_id,
        )
        self._logger.info("running function")
        start_time = time.monotonic()
        # response: RunTaskResponse = self._run_task(inputs)
        # TODO: run the function and use self._server_messages to send
        # the response and self._client_messages to get requests.
        logger.info(
            "function finished",
            duration_sec=f"{time.monotonic() - start_time:.3f}",
        )

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
            # Signals client message processor thread to close the session.
            raise CloseSession()
        else:
            self._server_messages.put(_INTERNAL_SESSION_COMMAND.LEAVE_SESSION)


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
