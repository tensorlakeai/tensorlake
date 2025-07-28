from collections.abc import Iterable as _Iterable
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar
from typing import Optional as _Optional
from typing import Union as _Union

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper

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
    INITIALIZATION_OUTCOME_CODE_UNKNOWN: _ClassVar[InitializationOutcomeCode]
    INITIALIZATION_OUTCOME_CODE_SUCCESS: _ClassVar[InitializationOutcomeCode]
    INITIALIZATION_OUTCOME_CODE_FAILURE: _ClassVar[InitializationOutcomeCode]

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
INITIALIZATION_OUTCOME_CODE_UNKNOWN: InitializationOutcomeCode
INITIALIZATION_OUTCOME_CODE_SUCCESS: InitializationOutcomeCode
INITIALIZATION_OUTCOME_CODE_FAILURE: InitializationOutcomeCode
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

class Empty(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class SerializedObjectManifest(_message.Message):
    __slots__ = ("encoding", "encoding_version", "size", "sha256_hash")
    ENCODING_FIELD_NUMBER: _ClassVar[int]
    ENCODING_VERSION_FIELD_NUMBER: _ClassVar[int]
    SIZE_FIELD_NUMBER: _ClassVar[int]
    SHA256_HASH_FIELD_NUMBER: _ClassVar[int]
    encoding: SerializedObjectEncoding
    encoding_version: int
    size: int
    sha256_hash: str
    def __init__(
        self,
        encoding: _Optional[_Union[SerializedObjectEncoding, str]] = ...,
        encoding_version: _Optional[int] = ...,
        size: _Optional[int] = ...,
        sha256_hash: _Optional[str] = ...,
    ) -> None: ...

class SerializedObject(_message.Message):
    __slots__ = ("manifest", "data")
    MANIFEST_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    manifest: SerializedObjectManifest
    data: bytes
    def __init__(
        self,
        manifest: _Optional[_Union[SerializedObjectManifest, _Mapping]] = ...,
        data: _Optional[bytes] = ...,
    ) -> None: ...

class BLOBChunk(_message.Message):
    __slots__ = ("uri", "size", "etag")
    URI_FIELD_NUMBER: _ClassVar[int]
    SIZE_FIELD_NUMBER: _ClassVar[int]
    ETAG_FIELD_NUMBER: _ClassVar[int]
    uri: str
    size: int
    etag: str
    def __init__(
        self,
        uri: _Optional[str] = ...,
        size: _Optional[int] = ...,
        etag: _Optional[str] = ...,
    ) -> None: ...

class BLOB(_message.Message):
    __slots__ = ("chunks",)
    CHUNKS_FIELD_NUMBER: _ClassVar[int]
    chunks: _containers.RepeatedCompositeFieldContainer[BLOBChunk]
    def __init__(
        self, chunks: _Optional[_Iterable[_Union[BLOBChunk, _Mapping]]] = ...
    ) -> None: ...

class SerializedObjectInsideBLOB(_message.Message):
    __slots__ = ("manifest", "offset")
    MANIFEST_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    manifest: SerializedObjectManifest
    offset: int
    def __init__(
        self,
        manifest: _Optional[_Union[SerializedObjectManifest, _Mapping]] = ...,
        offset: _Optional[int] = ...,
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

class InitializeDiagnostics(_message.Message):
    __slots__ = ("function_executor_log",)
    FUNCTION_EXECUTOR_LOG_FIELD_NUMBER: _ClassVar[int]
    function_executor_log: str
    def __init__(self, function_executor_log: _Optional[str] = ...) -> None: ...

class InitializeResponse(_message.Message):
    __slots__ = ("outcome_code", "failure_reason", "diagnostics")
    OUTCOME_CODE_FIELD_NUMBER: _ClassVar[int]
    FAILURE_REASON_FIELD_NUMBER: _ClassVar[int]
    DIAGNOSTICS_FIELD_NUMBER: _ClassVar[int]
    outcome_code: InitializationOutcomeCode
    failure_reason: InitializationFailureReason
    diagnostics: InitializeDiagnostics
    def __init__(
        self,
        outcome_code: _Optional[_Union[InitializationOutcomeCode, str]] = ...,
        failure_reason: _Optional[_Union[InitializationFailureReason, str]] = ...,
        diagnostics: _Optional[_Union[InitializeDiagnostics, _Mapping]] = ...,
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
    __slots__ = ()
    def __init__(self) -> None: ...

class GetInvocationStateRequest(_message.Message):
    __slots__ = ("key",)
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: str
    def __init__(self, key: _Optional[str] = ...) -> None: ...

class GetInvocationStateResponse(_message.Message):
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

class ListTasksRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListTasksResponse(_message.Message):
    __slots__ = ("tasks",)
    TASKS_FIELD_NUMBER: _ClassVar[int]
    tasks: _containers.RepeatedCompositeFieldContainer[Task]
    def __init__(
        self, tasks: _Optional[_Iterable[_Union[Task, _Mapping]]] = ...
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

class ProgressUpdate(_message.Message):
    __slots__ = ("current", "total")
    CURRENT_FIELD_NUMBER: _ClassVar[int]
    TOTAL_FIELD_NUMBER: _ClassVar[int]
    current: float
    total: float
    def __init__(
        self, current: _Optional[float] = ..., total: _Optional[float] = ...
    ) -> None: ...

class AwaitTaskProgress(_message.Message):
    __slots__ = ("progress", "task_result")
    PROGRESS_FIELD_NUMBER: _ClassVar[int]
    TASK_RESULT_FIELD_NUMBER: _ClassVar[int]
    progress: ProgressUpdate
    task_result: TaskResult
    def __init__(
        self,
        progress: _Optional[_Union[ProgressUpdate, _Mapping]] = ...,
        task_result: _Optional[_Union[TaskResult, _Mapping]] = ...,
    ) -> None: ...

class FunctionInputs(_message.Message):
    __slots__ = (
        "function_input_blob",
        "function_input",
        "function_init_value_blob",
        "function_init_value",
        "function_outputs_blob",
        "invocation_error_blob",
    )
    FUNCTION_INPUT_BLOB_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_INPUT_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_INIT_VALUE_BLOB_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_INIT_VALUE_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_OUTPUTS_BLOB_FIELD_NUMBER: _ClassVar[int]
    INVOCATION_ERROR_BLOB_FIELD_NUMBER: _ClassVar[int]
    function_input_blob: BLOB
    function_input: SerializedObjectInsideBLOB
    function_init_value_blob: BLOB
    function_init_value: SerializedObjectInsideBLOB
    function_outputs_blob: BLOB
    invocation_error_blob: BLOB
    def __init__(
        self,
        function_input_blob: _Optional[_Union[BLOB, _Mapping]] = ...,
        function_input: _Optional[_Union[SerializedObjectInsideBLOB, _Mapping]] = ...,
        function_init_value_blob: _Optional[_Union[BLOB, _Mapping]] = ...,
        function_init_value: _Optional[
            _Union[SerializedObjectInsideBLOB, _Mapping]
        ] = ...,
        function_outputs_blob: _Optional[_Union[BLOB, _Mapping]] = ...,
        invocation_error_blob: _Optional[_Union[BLOB, _Mapping]] = ...,
    ) -> None: ...

class TaskDiagnostics(_message.Message):
    __slots__ = ("function_executor_log",)
    FUNCTION_EXECUTOR_LOG_FIELD_NUMBER: _ClassVar[int]
    function_executor_log: str
    def __init__(self, function_executor_log: _Optional[str] = ...) -> None: ...

class TaskResult(_message.Message):
    __slots__ = (
        "outcome_code",
        "failure_reason",
        "function_outputs",
        "uploaded_function_outputs_blob",
        "invocation_error_output",
        "uploaded_invocation_error_blob",
        "next_functions",
        "metrics",
        "diagnostics",
    )
    OUTCOME_CODE_FIELD_NUMBER: _ClassVar[int]
    FAILURE_REASON_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_OUTPUTS_FIELD_NUMBER: _ClassVar[int]
    UPLOADED_FUNCTION_OUTPUTS_BLOB_FIELD_NUMBER: _ClassVar[int]
    INVOCATION_ERROR_OUTPUT_FIELD_NUMBER: _ClassVar[int]
    UPLOADED_INVOCATION_ERROR_BLOB_FIELD_NUMBER: _ClassVar[int]
    NEXT_FUNCTIONS_FIELD_NUMBER: _ClassVar[int]
    METRICS_FIELD_NUMBER: _ClassVar[int]
    DIAGNOSTICS_FIELD_NUMBER: _ClassVar[int]
    outcome_code: TaskOutcomeCode
    failure_reason: TaskFailureReason
    function_outputs: _containers.RepeatedCompositeFieldContainer[
        SerializedObjectInsideBLOB
    ]
    uploaded_function_outputs_blob: BLOB
    invocation_error_output: SerializedObjectInsideBLOB
    uploaded_invocation_error_blob: BLOB
    next_functions: _containers.RepeatedScalarFieldContainer[str]
    metrics: Metrics
    diagnostics: TaskDiagnostics
    def __init__(
        self,
        outcome_code: _Optional[_Union[TaskOutcomeCode, str]] = ...,
        failure_reason: _Optional[_Union[TaskFailureReason, str]] = ...,
        function_outputs: _Optional[
            _Iterable[_Union[SerializedObjectInsideBLOB, _Mapping]]
        ] = ...,
        uploaded_function_outputs_blob: _Optional[_Union[BLOB, _Mapping]] = ...,
        invocation_error_output: _Optional[
            _Union[SerializedObjectInsideBLOB, _Mapping]
        ] = ...,
        uploaded_invocation_error_blob: _Optional[_Union[BLOB, _Mapping]] = ...,
        next_functions: _Optional[_Iterable[str]] = ...,
        metrics: _Optional[_Union[Metrics, _Mapping]] = ...,
        diagnostics: _Optional[_Union[TaskDiagnostics, _Mapping]] = ...,
    ) -> None: ...

class Task(_message.Message):
    __slots__ = (
        "namespace",
        "graph_name",
        "graph_version",
        "function_name",
        "graph_invocation_id",
        "task_id",
        "allocation_id",
        "request",
        "result",
    )
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    GRAPH_NAME_FIELD_NUMBER: _ClassVar[int]
    GRAPH_VERSION_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_NAME_FIELD_NUMBER: _ClassVar[int]
    GRAPH_INVOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    REQUEST_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    graph_name: str
    graph_version: str
    function_name: str
    graph_invocation_id: str
    task_id: str
    allocation_id: str
    request: FunctionInputs
    result: TaskResult
    def __init__(
        self,
        namespace: _Optional[str] = ...,
        graph_name: _Optional[str] = ...,
        graph_version: _Optional[str] = ...,
        function_name: _Optional[str] = ...,
        graph_invocation_id: _Optional[str] = ...,
        task_id: _Optional[str] = ...,
        allocation_id: _Optional[str] = ...,
        request: _Optional[_Union[FunctionInputs, _Mapping]] = ...,
        result: _Optional[_Union[TaskResult, _Mapping]] = ...,
    ) -> None: ...

class CreateTaskRequest(_message.Message):
    __slots__ = ("task",)
    TASK_FIELD_NUMBER: _ClassVar[int]
    task: Task
    def __init__(self, task: _Optional[_Union[Task, _Mapping]] = ...) -> None: ...

class AwaitTaskRequest(_message.Message):
    __slots__ = ("task_id",)
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    def __init__(self, task_id: _Optional[str] = ...) -> None: ...

class DeleteTaskRequest(_message.Message):
    __slots__ = ("task_id",)
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    def __init__(self, task_id: _Optional[str] = ...) -> None: ...

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
