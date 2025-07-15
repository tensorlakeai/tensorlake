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

class SerializedObjectManifest(_message.Message):
    __slots__ = ("encoding", "encoding_version", "size")
    ENCODING_FIELD_NUMBER: _ClassVar[int]
    ENCODING_VERSION_FIELD_NUMBER: _ClassVar[int]
    SIZE_FIELD_NUMBER: _ClassVar[int]
    encoding: SerializedObjectEncoding
    encoding_version: int
    size: int
    def __init__(
        self,
        encoding: _Optional[_Union[SerializedObjectEncoding, str]] = ...,
        encoding_version: _Optional[int] = ...,
        size: _Optional[int] = ...,
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

class BLOB(_message.Message):
    __slots__ = ("uri",)
    URI_FIELD_NUMBER: _ClassVar[int]
    uri: str
    def __init__(self, uri: _Optional[str] = ...) -> None: ...

class SerializedObjectInsideBLOB(_message.Message):
    __slots__ = ("manifest", "blob", "offset")
    MANIFEST_FIELD_NUMBER: _ClassVar[int]
    BLOB_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    manifest: SerializedObjectManifest
    blob: BLOB
    offset: int
    def __init__(
        self,
        manifest: _Optional[_Union[SerializedObjectManifest, _Mapping]] = ...,
        blob: _Optional[_Union[BLOB, _Mapping]] = ...,
        offset: _Optional[int] = ...,
    ) -> None: ...

class InitializeRequest(_message.Message):
    __slots__ = (
        "namespace",
        "graph_name",
        "graph_version",
        "function_name",
        "graph",
        "stdout",
        "stderr",
    )
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    GRAPH_NAME_FIELD_NUMBER: _ClassVar[int]
    GRAPH_VERSION_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_NAME_FIELD_NUMBER: _ClassVar[int]
    GRAPH_FIELD_NUMBER: _ClassVar[int]
    STDOUT_FIELD_NUMBER: _ClassVar[int]
    STDERR_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    graph_name: str
    graph_version: str
    function_name: str
    graph: SerializedObject
    stdout: BLOB
    stderr: BLOB
    def __init__(
        self,
        namespace: _Optional[str] = ...,
        graph_name: _Optional[str] = ...,
        graph_version: _Optional[str] = ...,
        function_name: _Optional[str] = ...,
        graph: _Optional[_Union[SerializedObject, _Mapping]] = ...,
        stdout: _Optional[_Union[BLOB, _Mapping]] = ...,
        stderr: _Optional[_Union[BLOB, _Mapping]] = ...,
    ) -> None: ...

class InitializeResponse(_message.Message):
    __slots__ = ("outcome_code", "failure_reason")
    OUTCOME_CODE_FIELD_NUMBER: _ClassVar[int]
    FAILURE_REASON_FIELD_NUMBER: _ClassVar[int]
    outcome_code: InitializationOutcomeCode
    failure_reason: InitializationFailureReason
    def __init__(
        self,
        outcome_code: _Optional[_Union[InitializationOutcomeCode, str]] = ...,
        failure_reason: _Optional[_Union[InitializationFailureReason, str]] = ...,
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
        "stdout",
        "stderr",
        "function_outputs",
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
    STDOUT_FIELD_NUMBER: _ClassVar[int]
    STDERR_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_OUTPUTS_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    graph_name: str
    graph_version: str
    function_name: str
    graph_invocation_id: str
    task_id: str
    allocation_id: str
    function_input: SerializedObjectInsideBLOB
    function_init_value: SerializedObjectInsideBLOB
    stdout: BLOB
    stderr: BLOB
    function_outputs: BLOB
    def __init__(
        self,
        namespace: _Optional[str] = ...,
        graph_name: _Optional[str] = ...,
        graph_version: _Optional[str] = ...,
        function_name: _Optional[str] = ...,
        graph_invocation_id: _Optional[str] = ...,
        task_id: _Optional[str] = ...,
        allocation_id: _Optional[str] = ...,
        function_input: _Optional[_Union[SerializedObjectInsideBLOB, _Mapping]] = ...,
        function_init_value: _Optional[
            _Union[SerializedObjectInsideBLOB, _Mapping]
        ] = ...,
        stdout: _Optional[_Union[BLOB, _Mapping]] = ...,
        stderr: _Optional[_Union[BLOB, _Mapping]] = ...,
        function_outputs: _Optional[_Union[BLOB, _Mapping]] = ...,
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
        "is_reducer",
        "metrics",
        "outcome_code",
        "failure_reason",
        "invocation_error_output",
    )
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_OUTPUTS_FIELD_NUMBER: _ClassVar[int]
    NEXT_FUNCTIONS_FIELD_NUMBER: _ClassVar[int]
    IS_REDUCER_FIELD_NUMBER: _ClassVar[int]
    METRICS_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_CODE_FIELD_NUMBER: _ClassVar[int]
    FAILURE_REASON_FIELD_NUMBER: _ClassVar[int]
    INVOCATION_ERROR_OUTPUT_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    function_outputs: _containers.RepeatedCompositeFieldContainer[
        SerializedObjectInsideBLOB
    ]
    next_functions: _containers.RepeatedScalarFieldContainer[str]
    is_reducer: bool
    metrics: Metrics
    outcome_code: TaskOutcomeCode
    failure_reason: TaskFailureReason
    invocation_error_output: SerializedObjectInsideBLOB
    def __init__(
        self,
        task_id: _Optional[str] = ...,
        function_outputs: _Optional[
            _Iterable[_Union[SerializedObjectInsideBLOB, _Mapping]]
        ] = ...,
        next_functions: _Optional[_Iterable[str]] = ...,
        is_reducer: bool = ...,
        metrics: _Optional[_Union[Metrics, _Mapping]] = ...,
        outcome_code: _Optional[_Union[TaskOutcomeCode, str]] = ...,
        failure_reason: _Optional[_Union[TaskFailureReason, str]] = ...,
        invocation_error_output: _Optional[
            _Union[SerializedObjectInsideBLOB, _Mapping]
        ] = ...,
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
