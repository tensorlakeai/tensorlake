from collections.abc import Iterable as _Iterable
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar
from typing import Optional as _Optional
from typing import Union as _Union

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper

from tensorlake.function_executor.proto.google.rpc import status_pb2 as _status_pb2

DESCRIPTOR: _descriptor.FileDescriptor

class SerializedObjectEncoding(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SERIALIZED_OBJECT_ENCODING_UNKNOWN: _ClassVar[SerializedObjectEncoding]
    SERIALIZED_OBJECT_ENCODING_UTF8_JSON: _ClassVar[SerializedObjectEncoding]
    SERIALIZED_OBJECT_ENCODING_UTF8_TEXT: _ClassVar[SerializedObjectEncoding]
    SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE: _ClassVar[SerializedObjectEncoding]
    SERIALIZED_OBJECT_ENCODING_BINARY_ZIP: _ClassVar[SerializedObjectEncoding]

class InitializationOutcomeCode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    INITIALIZE_OUTCOME_CODE_UNKNOWN: _ClassVar[InitializationOutcomeCode]
    INITIALIZE_OUTCOME_CODE_SUCCESS: _ClassVar[InitializationOutcomeCode]
    INITIALIZE_OUTCOME_CODE_FAILURE: _ClassVar[InitializationOutcomeCode]

class InitializationFailureReason(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    INITIALIZATION_FAILURE_REASON_UNKNOWN: _ClassVar[InitializationFailureReason]
    INITIALIZATION_FAILURE_REASON_INTERNAL_ERROR: _ClassVar[InitializationFailureReason]
    INITIALIZATION_FAILURE_REASON_FUNCTION_ERROR: _ClassVar[InitializationFailureReason]

class TaskOutcomeCode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TASK_OUTCOME_CODE_UNKNOWN: _ClassVar[TaskOutcomeCode]
    TASK_OUTCOME_CODE_SUCCESS: _ClassVar[TaskOutcomeCode]
    TASK_OUTCOME_CODE_FAILURE: _ClassVar[TaskOutcomeCode]

class TaskFailureReason(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TASK_FAILURE_REASON_UNKNOWN: _ClassVar[TaskFailureReason]
    TASK_FAILURE_REASON_INTERNAL_ERROR: _ClassVar[TaskFailureReason]
    TASK_FAILURE_REASON_FUNCTION_ERROR: _ClassVar[TaskFailureReason]
    TASK_FAILURE_REASON_INVOCATION_ERROR: _ClassVar[TaskFailureReason]

SERIALIZED_OBJECT_ENCODING_UNKNOWN: SerializedObjectEncoding
SERIALIZED_OBJECT_ENCODING_UTF8_JSON: SerializedObjectEncoding
SERIALIZED_OBJECT_ENCODING_UTF8_TEXT: SerializedObjectEncoding
SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE: SerializedObjectEncoding
SERIALIZED_OBJECT_ENCODING_BINARY_ZIP: SerializedObjectEncoding
INITIALIZE_OUTCOME_CODE_UNKNOWN: InitializationOutcomeCode
INITIALIZE_OUTCOME_CODE_SUCCESS: InitializationOutcomeCode
INITIALIZE_OUTCOME_CODE_FAILURE: InitializationOutcomeCode
INITIALIZATION_FAILURE_REASON_UNKNOWN: InitializationFailureReason
INITIALIZATION_FAILURE_REASON_INTERNAL_ERROR: InitializationFailureReason
INITIALIZATION_FAILURE_REASON_FUNCTION_ERROR: InitializationFailureReason
TASK_OUTCOME_CODE_UNKNOWN: TaskOutcomeCode
TASK_OUTCOME_CODE_SUCCESS: TaskOutcomeCode
TASK_OUTCOME_CODE_FAILURE: TaskOutcomeCode
TASK_FAILURE_REASON_UNKNOWN: TaskFailureReason
TASK_FAILURE_REASON_INTERNAL_ERROR: TaskFailureReason
TASK_FAILURE_REASON_FUNCTION_ERROR: TaskFailureReason
TASK_FAILURE_REASON_INVOCATION_ERROR: TaskFailureReason

class SerializedObject(_message.Message):
    __slots__ = ("data", "encoding", "encoding_version")
    DATA_FIELD_NUMBER: _ClassVar[int]
    ENCODING_FIELD_NUMBER: _ClassVar[int]
    ENCODING_VERSION_FIELD_NUMBER: _ClassVar[int]
    data: bytes
    encoding: SerializedObjectEncoding
    encoding_version: int
    def __init__(
        self,
        data: _Optional[bytes] = ...,
        encoding: _Optional[_Union[SerializedObjectEncoding, str]] = ...,
        encoding_version: _Optional[int] = ...,
    ) -> None: ...

class InitializeRequest(_message.Message):
    __slots__ = ("namespace", "graph_name", "graph_version", "function_name", "graph")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    GRAPH_NAME_FIELD_NUMBER: _ClassVar[int]
    GRAPH_VERSION_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_NAME_FIELD_NUMBER: _ClassVar[int]
    GRAPH_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    graph_name: str
    graph_version: str
    function_name: str
    graph: SerializedObject
    def __init__(
        self,
        namespace: _Optional[str] = ...,
        graph_name: _Optional[str] = ...,
        graph_version: _Optional[str] = ...,
        function_name: _Optional[str] = ...,
        graph: _Optional[_Union[SerializedObject, _Mapping]] = ...,
    ) -> None: ...

class InitializeResponse(_message.Message):
    __slots__ = ("outcome_code", "failure_reason", "stdout", "stderr")
    OUTCOME_CODE_FIELD_NUMBER: _ClassVar[int]
    FAILURE_REASON_FIELD_NUMBER: _ClassVar[int]
    STDOUT_FIELD_NUMBER: _ClassVar[int]
    STDERR_FIELD_NUMBER: _ClassVar[int]
    outcome_code: InitializationOutcomeCode
    failure_reason: InitializationFailureReason
    stdout: str
    stderr: str
    def __init__(
        self,
        outcome_code: _Optional[_Union[InitializationOutcomeCode, str]] = ...,
        failure_reason: _Optional[_Union[InitializationFailureReason, str]] = ...,
        stdout: _Optional[str] = ...,
        stderr: _Optional[str] = ...,
    ) -> None: ...

class SetInvocationStateRequest(_message.Message):
    __slots__ = ("key", "value")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: SerializedObject
    def __init__(
        self,
        key: _Optional[str] = ...,
        value: _Optional[_Union[SerializedObject, _Mapping]] = ...,
    ) -> None: ...

class SetInvocationStateResponse(_message.Message):
    __slots__ = ("status",)
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: _status_pb2.Status
    def __init__(
        self, status: _Optional[_Union[_status_pb2.Status, _Mapping]] = ...
    ) -> None: ...

class GetInvocationStateRequest(_message.Message):
    __slots__ = ("key",)
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: str
    def __init__(self, key: _Optional[str] = ...) -> None: ...

class GetInvocationStateResponse(_message.Message):
    __slots__ = ("key", "value", "status")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: SerializedObject
    status: _status_pb2.Status
    def __init__(
        self,
        key: _Optional[str] = ...,
        value: _Optional[_Union[SerializedObject, _Mapping]] = ...,
        status: _Optional[_Union[_status_pb2.Status, _Mapping]] = ...,
    ) -> None: ...

class InvocationStateRequest(_message.Message):
    __slots__ = ("request_id", "task_id", "set", "get")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    SET_FIELD_NUMBER: _ClassVar[int]
    GET_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    task_id: str
    set: SetInvocationStateRequest
    get: GetInvocationStateRequest
    def __init__(
        self,
        request_id: _Optional[str] = ...,
        task_id: _Optional[str] = ...,
        set: _Optional[_Union[SetInvocationStateRequest, _Mapping]] = ...,
        get: _Optional[_Union[GetInvocationStateRequest, _Mapping]] = ...,
    ) -> None: ...

class InvocationStateResponse(_message.Message):
    __slots__ = ("request_id", "success", "set", "get")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    SET_FIELD_NUMBER: _ClassVar[int]
    GET_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    success: bool
    set: SetInvocationStateResponse
    get: GetInvocationStateResponse
    def __init__(
        self,
        request_id: _Optional[str] = ...,
        success: bool = ...,
        set: _Optional[_Union[SetInvocationStateResponse, _Mapping]] = ...,
        get: _Optional[_Union[GetInvocationStateResponse, _Mapping]] = ...,
    ) -> None: ...

class RunTaskRequest(_message.Message):
    __slots__ = (
        "namespace",
        "graph_name",
        "graph_version",
        "function_name",
        "graph_invocation_id",
        "task_id",
        "allocation_id",
        "function_input",
        "function_init_value",
    )
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    GRAPH_NAME_FIELD_NUMBER: _ClassVar[int]
    GRAPH_VERSION_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_NAME_FIELD_NUMBER: _ClassVar[int]
    GRAPH_INVOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_INPUT_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_INIT_VALUE_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    graph_name: str
    graph_version: str
    function_name: str
    graph_invocation_id: str
    task_id: str
    allocation_id: str
    function_input: SerializedObject
    function_init_value: SerializedObject
    def __init__(
        self,
        namespace: _Optional[str] = ...,
        graph_name: _Optional[str] = ...,
        graph_version: _Optional[str] = ...,
        function_name: _Optional[str] = ...,
        graph_invocation_id: _Optional[str] = ...,
        task_id: _Optional[str] = ...,
        allocation_id: _Optional[str] = ...,
        function_input: _Optional[_Union[SerializedObject, _Mapping]] = ...,
        function_init_value: _Optional[_Union[SerializedObject, _Mapping]] = ...,
    ) -> None: ...

class Metrics(_message.Message):
    __slots__ = ("timers", "counters")

    class TimersEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: float
        def __init__(
            self, key: _Optional[str] = ..., value: _Optional[float] = ...
        ) -> None: ...

    class CountersEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: int
        def __init__(
            self, key: _Optional[str] = ..., value: _Optional[int] = ...
        ) -> None: ...

    TIMERS_FIELD_NUMBER: _ClassVar[int]
    COUNTERS_FIELD_NUMBER: _ClassVar[int]
    timers: _containers.ScalarMap[str, float]
    counters: _containers.ScalarMap[str, int]
    def __init__(
        self,
        timers: _Optional[_Mapping[str, float]] = ...,
        counters: _Optional[_Mapping[str, int]] = ...,
    ) -> None: ...

class RunTaskResponse(_message.Message):
    __slots__ = (
        "task_id",
        "function_outputs",
        "next_functions",
        "stdout",
        "stderr",
        "is_reducer",
        "metrics",
        "outcome_code",
        "failure_reason",
        "invocation_error_output",
    )
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_OUTPUTS_FIELD_NUMBER: _ClassVar[int]
    NEXT_FUNCTIONS_FIELD_NUMBER: _ClassVar[int]
    STDOUT_FIELD_NUMBER: _ClassVar[int]
    STDERR_FIELD_NUMBER: _ClassVar[int]
    IS_REDUCER_FIELD_NUMBER: _ClassVar[int]
    METRICS_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_CODE_FIELD_NUMBER: _ClassVar[int]
    FAILURE_REASON_FIELD_NUMBER: _ClassVar[int]
    INVOCATION_ERROR_OUTPUT_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    function_outputs: _containers.RepeatedCompositeFieldContainer[SerializedObject]
    next_functions: _containers.RepeatedScalarFieldContainer[str]
    stdout: str
    stderr: str
    is_reducer: bool
    metrics: Metrics
    outcome_code: TaskOutcomeCode
    failure_reason: TaskFailureReason
    invocation_error_output: SerializedObject
    def __init__(
        self,
        task_id: _Optional[str] = ...,
        function_outputs: _Optional[
            _Iterable[_Union[SerializedObject, _Mapping]]
        ] = ...,
        next_functions: _Optional[_Iterable[str]] = ...,
        stdout: _Optional[str] = ...,
        stderr: _Optional[str] = ...,
        is_reducer: bool = ...,
        metrics: _Optional[_Union[Metrics, _Mapping]] = ...,
        outcome_code: _Optional[_Union[TaskOutcomeCode, str]] = ...,
        failure_reason: _Optional[_Union[TaskFailureReason, str]] = ...,
        invocation_error_output: _Optional[_Union[SerializedObject, _Mapping]] = ...,
    ) -> None: ...

class HealthCheckRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class HealthCheckResponse(_message.Message):
    __slots__ = ("healthy", "status_message")
    HEALTHY_FIELD_NUMBER: _ClassVar[int]
    STATUS_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    healthy: bool
    status_message: str
    def __init__(
        self, healthy: bool = ..., status_message: _Optional[str] = ...
    ) -> None: ...

class InfoRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class InfoResponse(_message.Message):
    __slots__ = ("version", "sdk_version", "sdk_language", "sdk_language_version")
    VERSION_FIELD_NUMBER: _ClassVar[int]
    SDK_VERSION_FIELD_NUMBER: _ClassVar[int]
    SDK_LANGUAGE_FIELD_NUMBER: _ClassVar[int]
    SDK_LANGUAGE_VERSION_FIELD_NUMBER: _ClassVar[int]
    version: str
    sdk_version: str
    sdk_language: str
    sdk_language_version: str
    def __init__(
        self,
        version: _Optional[str] = ...,
        sdk_version: _Optional[str] = ...,
        sdk_language: _Optional[str] = ...,
        sdk_language_version: _Optional[str] = ...,
    ) -> None: ...

class SerializedObjectID(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: str
    def __init__(self, value: _Optional[str] = ...) -> None: ...

class SerializedObjectManifest(_message.Message):
    __slots__ = ("id", "encoding", "encoding_version", "size", "sha256_hash")
    ID_FIELD_NUMBER: _ClassVar[int]
    ENCODING_FIELD_NUMBER: _ClassVar[int]
    ENCODING_VERSION_FIELD_NUMBER: _ClassVar[int]
    SIZE_FIELD_NUMBER: _ClassVar[int]
    SHA256_HASH_FIELD_NUMBER: _ClassVar[int]
    id: SerializedObjectID
    encoding: SerializedObjectEncoding
    encoding_version: int
    size: int
    sha256_hash: str
    def __init__(
        self,
        id: _Optional[_Union[SerializedObjectID, _Mapping]] = ...,
        encoding: _Optional[_Union[SerializedObjectEncoding, str]] = ...,
        encoding_version: _Optional[int] = ...,
        size: _Optional[int] = ...,
        sha256_hash: _Optional[str] = ...,
    ) -> None: ...

class SerializedObjectChunk(_message.Message):
    __slots__ = ("id", "data")
    ID_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    id: SerializedObjectID
    data: bytes
    def __init__(
        self,
        id: _Optional[_Union[SerializedObjectID, _Mapping]] = ...,
        data: _Optional[bytes] = ...,
    ) -> None: ...

class UploadSerializedObjectRequest(_message.Message):
    __slots__ = ("manifest", "chunk")
    MANIFEST_FIELD_NUMBER: _ClassVar[int]
    CHUNK_FIELD_NUMBER: _ClassVar[int]
    manifest: SerializedObjectManifest
    chunk: SerializedObjectChunk
    def __init__(
        self,
        manifest: _Optional[_Union[SerializedObjectManifest, _Mapping]] = ...,
        chunk: _Optional[_Union[SerializedObjectChunk, _Mapping]] = ...,
    ) -> None: ...

class UploadSerializedObjectResponse(_message.Message):
    __slots__ = ("status", "id")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    status: _status_pb2.Status
    id: SerializedObjectID
    def __init__(
        self,
        status: _Optional[_Union[_status_pb2.Status, _Mapping]] = ...,
        id: _Optional[_Union[SerializedObjectID, _Mapping]] = ...,
    ) -> None: ...

class OpenSessionRequest(_message.Message):
    __slots__ = ("session_id",)
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    def __init__(self, session_id: _Optional[str] = ...) -> None: ...

class LeaveSessionRequest(_message.Message):
    __slots__ = ("close",)
    CLOSE_FIELD_NUMBER: _ClassVar[int]
    close: bool
    def __init__(self, close: bool = ...) -> None: ...

class TaskAllocationInput(_message.Message):
    __slots__ = (
        "graph_invocation_id",
        "task_id",
        "allocation_id",
        "function_input_id",
        "function_init_value_id",
    )
    GRAPH_INVOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_INPUT_ID_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_INIT_VALUE_ID_FIELD_NUMBER: _ClassVar[int]
    graph_invocation_id: str
    task_id: str
    allocation_id: str
    function_input_id: SerializedObjectID
    function_init_value_id: SerializedObjectID
    def __init__(
        self,
        graph_invocation_id: _Optional[str] = ...,
        task_id: _Optional[str] = ...,
        allocation_id: _Optional[str] = ...,
        function_input_id: _Optional[_Union[SerializedObjectID, _Mapping]] = ...,
        function_init_value_id: _Optional[_Union[SerializedObjectID, _Mapping]] = ...,
    ) -> None: ...

class RunTaskAllocationsRequest(_message.Message):
    __slots__ = ("inputs",)
    INPUTS_FIELD_NUMBER: _ClassVar[int]
    inputs: _containers.RepeatedCompositeFieldContainer[TaskAllocationInput]
    def __init__(
        self, inputs: _Optional[_Iterable[_Union[TaskAllocationInput, _Mapping]]] = ...
    ) -> None: ...

class RunTaskAllocationsSessionClientMessage(_message.Message):
    __slots__ = (
        "open_session_request",
        "upload_serialized_object_request",
        "upload_serialized_object_response",
        "run_task_allocations_request",
        "set_invocation_state_response",
        "get_invocation_state_response",
        "leave_session_request",
    )
    OPEN_SESSION_REQUEST_FIELD_NUMBER: _ClassVar[int]
    UPLOAD_SERIALIZED_OBJECT_REQUEST_FIELD_NUMBER: _ClassVar[int]
    UPLOAD_SERIALIZED_OBJECT_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    RUN_TASK_ALLOCATIONS_REQUEST_FIELD_NUMBER: _ClassVar[int]
    SET_INVOCATION_STATE_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    GET_INVOCATION_STATE_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    LEAVE_SESSION_REQUEST_FIELD_NUMBER: _ClassVar[int]
    open_session_request: OpenSessionRequest
    upload_serialized_object_request: UploadSerializedObjectRequest
    upload_serialized_object_response: UploadSerializedObjectResponse
    run_task_allocations_request: RunTaskAllocationsRequest
    set_invocation_state_response: SetInvocationStateResponse
    get_invocation_state_response: GetInvocationStateResponse
    leave_session_request: LeaveSessionRequest
    def __init__(
        self,
        open_session_request: _Optional[_Union[OpenSessionRequest, _Mapping]] = ...,
        upload_serialized_object_request: _Optional[
            _Union[UploadSerializedObjectRequest, _Mapping]
        ] = ...,
        upload_serialized_object_response: _Optional[
            _Union[UploadSerializedObjectResponse, _Mapping]
        ] = ...,
        run_task_allocations_request: _Optional[
            _Union[RunTaskAllocationsRequest, _Mapping]
        ] = ...,
        set_invocation_state_response: _Optional[
            _Union[SetInvocationStateResponse, _Mapping]
        ] = ...,
        get_invocation_state_response: _Optional[
            _Union[GetInvocationStateResponse, _Mapping]
        ] = ...,
        leave_session_request: _Optional[_Union[LeaveSessionRequest, _Mapping]] = ...,
    ) -> None: ...

class OpenSessionResponse(_message.Message):
    __slots__ = ("status", "is_new")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    IS_NEW_FIELD_NUMBER: _ClassVar[int]
    status: _status_pb2.Status
    is_new: bool
    def __init__(
        self,
        status: _Optional[_Union[_status_pb2.Status, _Mapping]] = ...,
        is_new: bool = ...,
    ) -> None: ...

class LeaveSessionResponse(_message.Message):
    __slots__ = ("status",)
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: _status_pb2.Status
    def __init__(
        self, status: _Optional[_Union[_status_pb2.Status, _Mapping]] = ...
    ) -> None: ...

class TaskAllocationOutput(_message.Message):
    __slots__ = (
        "graph_invocation_id",
        "task_id",
        "allocation_id",
        "outcome_code",
        "failure_reason",
        "function_outputs",
        "invocation_error_output",
        "stdout",
        "stderr",
        "next_functions",
        "metrics",
    )
    GRAPH_INVOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_CODE_FIELD_NUMBER: _ClassVar[int]
    FAILURE_REASON_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_OUTPUTS_FIELD_NUMBER: _ClassVar[int]
    INVOCATION_ERROR_OUTPUT_FIELD_NUMBER: _ClassVar[int]
    STDOUT_FIELD_NUMBER: _ClassVar[int]
    STDERR_FIELD_NUMBER: _ClassVar[int]
    NEXT_FUNCTIONS_FIELD_NUMBER: _ClassVar[int]
    METRICS_FIELD_NUMBER: _ClassVar[int]
    graph_invocation_id: str
    task_id: str
    allocation_id: str
    outcome_code: TaskOutcomeCode
    failure_reason: TaskFailureReason
    function_outputs: _containers.RepeatedCompositeFieldContainer[SerializedObjectID]
    invocation_error_output: SerializedObjectID
    stdout: SerializedObjectID
    stderr: SerializedObjectID
    next_functions: _containers.RepeatedScalarFieldContainer[str]
    metrics: Metrics
    def __init__(
        self,
        graph_invocation_id: _Optional[str] = ...,
        task_id: _Optional[str] = ...,
        allocation_id: _Optional[str] = ...,
        outcome_code: _Optional[_Union[TaskOutcomeCode, str]] = ...,
        failure_reason: _Optional[_Union[TaskFailureReason, str]] = ...,
        function_outputs: _Optional[
            _Iterable[_Union[SerializedObjectID, _Mapping]]
        ] = ...,
        invocation_error_output: _Optional[_Union[SerializedObjectID, _Mapping]] = ...,
        stdout: _Optional[_Union[SerializedObjectID, _Mapping]] = ...,
        stderr: _Optional[_Union[SerializedObjectID, _Mapping]] = ...,
        next_functions: _Optional[_Iterable[str]] = ...,
        metrics: _Optional[_Union[Metrics, _Mapping]] = ...,
    ) -> None: ...

class RunTaskAllocationsResponse(_message.Message):
    __slots__ = ("status", "outputs")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    OUTPUTS_FIELD_NUMBER: _ClassVar[int]
    status: _status_pb2.Status
    outputs: _containers.RepeatedCompositeFieldContainer[TaskAllocationOutput]
    def __init__(
        self,
        status: _Optional[_Union[_status_pb2.Status, _Mapping]] = ...,
        outputs: _Optional[_Iterable[_Union[TaskAllocationOutput, _Mapping]]] = ...,
    ) -> None: ...

class RunTaskAllocationsSessionServerMessage(_message.Message):
    __slots__ = (
        "open_session_response",
        "upload_serialized_object_request",
        "upload_serialized_object_response",
        "run_task_allocations_response",
        "set_invocation_state_request",
        "get_invocation_state_request",
        "leave_session_response",
    )
    OPEN_SESSION_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    UPLOAD_SERIALIZED_OBJECT_REQUEST_FIELD_NUMBER: _ClassVar[int]
    UPLOAD_SERIALIZED_OBJECT_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    RUN_TASK_ALLOCATIONS_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    SET_INVOCATION_STATE_REQUEST_FIELD_NUMBER: _ClassVar[int]
    GET_INVOCATION_STATE_REQUEST_FIELD_NUMBER: _ClassVar[int]
    LEAVE_SESSION_RESPONSE_FIELD_NUMBER: _ClassVar[int]
    open_session_response: OpenSessionResponse
    upload_serialized_object_request: UploadSerializedObjectRequest
    upload_serialized_object_response: UploadSerializedObjectResponse
    run_task_allocations_response: RunTaskAllocationsResponse
    set_invocation_state_request: SetInvocationStateRequest
    get_invocation_state_request: GetInvocationStateRequest
    leave_session_response: LeaveSessionResponse
    def __init__(
        self,
        open_session_response: _Optional[_Union[OpenSessionResponse, _Mapping]] = ...,
        upload_serialized_object_request: _Optional[
            _Union[UploadSerializedObjectRequest, _Mapping]
        ] = ...,
        upload_serialized_object_response: _Optional[
            _Union[UploadSerializedObjectResponse, _Mapping]
        ] = ...,
        run_task_allocations_response: _Optional[
            _Union[RunTaskAllocationsResponse, _Mapping]
        ] = ...,
        set_invocation_state_request: _Optional[
            _Union[SetInvocationStateRequest, _Mapping]
        ] = ...,
        get_invocation_state_request: _Optional[
            _Union[GetInvocationStateRequest, _Mapping]
        ] = ...,
        leave_session_response: _Optional[_Union[LeaveSessionResponse, _Mapping]] = ...,
    ) -> None: ...
