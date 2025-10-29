import datetime
from collections.abc import Iterable as _Iterable
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar
from typing import Optional as _Optional
from typing import Union as _Union

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import timestamp_pb2 as _timestamp_pb2
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
    SERIALIZED_OBJECT_ENCODING_RAW: _ClassVar[SerializedObjectEncoding]

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

class AllocationOutcomeCode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ALLOCATION_OUTCOME_CODE_UNKNOWN: _ClassVar[AllocationOutcomeCode]
    ALLOCATION_OUTCOME_CODE_SUCCESS: _ClassVar[AllocationOutcomeCode]
    ALLOCATION_OUTCOME_CODE_FAILURE: _ClassVar[AllocationOutcomeCode]

class AllocationFailureReason(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ALLOCATION_FAILURE_REASON_UNKNOWN: _ClassVar[AllocationFailureReason]
    ALLOCATION_FAILURE_REASON_INTERNAL_ERROR: _ClassVar[AllocationFailureReason]
    ALLOCATION_FAILURE_REASON_FUNCTION_ERROR: _ClassVar[AllocationFailureReason]
    ALLOCATION_FAILURE_REASON_REQUEST_ERROR: _ClassVar[AllocationFailureReason]

SERIALIZED_OBJECT_ENCODING_UNKNOWN: SerializedObjectEncoding
SERIALIZED_OBJECT_ENCODING_UTF8_JSON: SerializedObjectEncoding
SERIALIZED_OBJECT_ENCODING_UTF8_TEXT: SerializedObjectEncoding
SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE: SerializedObjectEncoding
SERIALIZED_OBJECT_ENCODING_BINARY_ZIP: SerializedObjectEncoding
SERIALIZED_OBJECT_ENCODING_RAW: SerializedObjectEncoding
INITIALIZATION_OUTCOME_CODE_UNKNOWN: InitializationOutcomeCode
INITIALIZATION_OUTCOME_CODE_SUCCESS: InitializationOutcomeCode
INITIALIZATION_OUTCOME_CODE_FAILURE: InitializationOutcomeCode
INITIALIZATION_FAILURE_REASON_UNKNOWN: InitializationFailureReason
INITIALIZATION_FAILURE_REASON_INTERNAL_ERROR: InitializationFailureReason
INITIALIZATION_FAILURE_REASON_FUNCTION_ERROR: InitializationFailureReason
ALLOCATION_OUTCOME_CODE_UNKNOWN: AllocationOutcomeCode
ALLOCATION_OUTCOME_CODE_SUCCESS: AllocationOutcomeCode
ALLOCATION_OUTCOME_CODE_FAILURE: AllocationOutcomeCode
ALLOCATION_FAILURE_REASON_UNKNOWN: AllocationFailureReason
ALLOCATION_FAILURE_REASON_INTERNAL_ERROR: AllocationFailureReason
ALLOCATION_FAILURE_REASON_FUNCTION_ERROR: AllocationFailureReason
ALLOCATION_FAILURE_REASON_REQUEST_ERROR: AllocationFailureReason

class Empty(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class SerializedObjectManifest(_message.Message):
    __slots__ = (
        "encoding",
        "encoding_version",
        "size",
        "metadata_size",
        "sha256_hash",
        "content_type",
        "source_function_call_id",
    )
    ENCODING_FIELD_NUMBER: _ClassVar[int]
    ENCODING_VERSION_FIELD_NUMBER: _ClassVar[int]
    SIZE_FIELD_NUMBER: _ClassVar[int]
    METADATA_SIZE_FIELD_NUMBER: _ClassVar[int]
    SHA256_HASH_FIELD_NUMBER: _ClassVar[int]
    CONTENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FUNCTION_CALL_ID_FIELD_NUMBER: _ClassVar[int]
    encoding: SerializedObjectEncoding
    encoding_version: int
    size: int
    metadata_size: int
    sha256_hash: str
    content_type: str
    source_function_call_id: str
    def __init__(
        self,
        encoding: _Optional[_Union[SerializedObjectEncoding, str]] = ...,
        encoding_version: _Optional[int] = ...,
        size: _Optional[int] = ...,
        metadata_size: _Optional[int] = ...,
        sha256_hash: _Optional[str] = ...,
        content_type: _Optional[str] = ...,
        source_function_call_id: _Optional[str] = ...,
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
    __slots__ = ("id", "chunks")
    ID_FIELD_NUMBER: _ClassVar[int]
    CHUNKS_FIELD_NUMBER: _ClassVar[int]
    id: str
    chunks: _containers.RepeatedCompositeFieldContainer[BLOBChunk]
    def __init__(
        self,
        id: _Optional[str] = ...,
        chunks: _Optional[_Iterable[_Union[BLOBChunk, _Mapping]]] = ...,
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

class FunctionRef(_message.Message):
    __slots__ = (
        "namespace",
        "application_name",
        "function_name",
        "application_version",
    )
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    APPLICATION_NAME_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_NAME_FIELD_NUMBER: _ClassVar[int]
    APPLICATION_VERSION_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    application_name: str
    function_name: str
    application_version: str
    def __init__(
        self,
        namespace: _Optional[str] = ...,
        application_name: _Optional[str] = ...,
        function_name: _Optional[str] = ...,
        application_version: _Optional[str] = ...,
    ) -> None: ...

class InitializeRequest(_message.Message):
    __slots__ = ("function", "application_code")
    FUNCTION_FIELD_NUMBER: _ClassVar[int]
    APPLICATION_CODE_FIELD_NUMBER: _ClassVar[int]
    function: FunctionRef
    application_code: SerializedObject
    def __init__(
        self,
        function: _Optional[_Union[FunctionRef, _Mapping]] = ...,
        application_code: _Optional[_Union[SerializedObject, _Mapping]] = ...,
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

class SetRequestStateRequest(_message.Message):
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

class SetRequestStateResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetRequestStateRequest(_message.Message):
    __slots__ = ("key",)
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: str
    def __init__(self, key: _Optional[str] = ...) -> None: ...

class GetRequestStateResponse(_message.Message):
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

class RequestStateRequest(_message.Message):
    __slots__ = ("state_request_id", "allocation_id", "set", "get")
    STATE_REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    SET_FIELD_NUMBER: _ClassVar[int]
    GET_FIELD_NUMBER: _ClassVar[int]
    state_request_id: str
    allocation_id: str
    set: SetRequestStateRequest
    get: GetRequestStateRequest
    def __init__(
        self,
        state_request_id: _Optional[str] = ...,
        allocation_id: _Optional[str] = ...,
        set: _Optional[_Union[SetRequestStateRequest, _Mapping]] = ...,
        get: _Optional[_Union[GetRequestStateRequest, _Mapping]] = ...,
    ) -> None: ...

class RequestStateResponse(_message.Message):
    __slots__ = ("state_request_id", "success", "set", "get")
    STATE_REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    SET_FIELD_NUMBER: _ClassVar[int]
    GET_FIELD_NUMBER: _ClassVar[int]
    state_request_id: str
    success: bool
    set: SetRequestStateResponse
    get: GetRequestStateResponse
    def __init__(
        self,
        state_request_id: _Optional[str] = ...,
        success: bool = ...,
        set: _Optional[_Union[SetRequestStateResponse, _Mapping]] = ...,
        get: _Optional[_Union[GetRequestStateResponse, _Mapping]] = ...,
    ) -> None: ...

class ListAllocationsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListAllocationsResponse(_message.Message):
    __slots__ = ("allocations",)
    ALLOCATIONS_FIELD_NUMBER: _ClassVar[int]
    allocations: _containers.RepeatedCompositeFieldContainer[Allocation]
    def __init__(
        self, allocations: _Optional[_Iterable[_Union[Allocation, _Mapping]]] = ...
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

class AllocationProgress(_message.Message):
    __slots__ = ("current", "total")
    CURRENT_FIELD_NUMBER: _ClassVar[int]
    TOTAL_FIELD_NUMBER: _ClassVar[int]
    current: float
    total: float
    def __init__(
        self, current: _Optional[float] = ..., total: _Optional[float] = ...
    ) -> None: ...

class AllocationOutputBLOBRequest(_message.Message):
    __slots__ = ("id", "size")
    ID_FIELD_NUMBER: _ClassVar[int]
    SIZE_FIELD_NUMBER: _ClassVar[int]
    id: str
    size: int
    def __init__(
        self, id: _Optional[str] = ..., size: _Optional[int] = ...
    ) -> None: ...

class AllocationFunctionCall(_message.Message):
    __slots__ = ("updates", "args_blob")
    UPDATES_FIELD_NUMBER: _ClassVar[int]
    ARGS_BLOB_FIELD_NUMBER: _ClassVar[int]
    updates: ExecutionPlanUpdates
    args_blob: BLOB
    def __init__(
        self,
        updates: _Optional[_Union[ExecutionPlanUpdates, _Mapping]] = ...,
        args_blob: _Optional[_Union[BLOB, _Mapping]] = ...,
    ) -> None: ...

class AllocationFunctionCallWatcher(_message.Message):
    __slots__ = ("watcher_id", "function_call_id")
    WATCHER_ID_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_CALL_ID_FIELD_NUMBER: _ClassVar[int]
    watcher_id: str
    function_call_id: str
    def __init__(
        self, watcher_id: _Optional[str] = ..., function_call_id: _Optional[str] = ...
    ) -> None: ...

class AllocationState(_message.Message):
    __slots__ = (
        "progress",
        "output_blob_requests",
        "function_calls",
        "function_call_watchers",
        "result",
        "sha256_hash",
    )
    PROGRESS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_BLOB_REQUESTS_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_CALLS_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_CALL_WATCHERS_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    SHA256_HASH_FIELD_NUMBER: _ClassVar[int]
    progress: AllocationProgress
    output_blob_requests: _containers.RepeatedCompositeFieldContainer[
        AllocationOutputBLOBRequest
    ]
    function_calls: _containers.RepeatedCompositeFieldContainer[AllocationFunctionCall]
    function_call_watchers: _containers.RepeatedCompositeFieldContainer[
        AllocationFunctionCallWatcher
    ]
    result: AllocationResult
    sha256_hash: str
    def __init__(
        self,
        progress: _Optional[_Union[AllocationProgress, _Mapping]] = ...,
        output_blob_requests: _Optional[
            _Iterable[_Union[AllocationOutputBLOBRequest, _Mapping]]
        ] = ...,
        function_calls: _Optional[
            _Iterable[_Union[AllocationFunctionCall, _Mapping]]
        ] = ...,
        function_call_watchers: _Optional[
            _Iterable[_Union[AllocationFunctionCallWatcher, _Mapping]]
        ] = ...,
        result: _Optional[_Union[AllocationResult, _Mapping]] = ...,
        sha256_hash: _Optional[str] = ...,
    ) -> None: ...

class FunctionInputs(_message.Message):
    __slots__ = ("args", "arg_blobs", "request_error_blob", "function_call_metadata")
    ARGS_FIELD_NUMBER: _ClassVar[int]
    ARG_BLOBS_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ERROR_BLOB_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_CALL_METADATA_FIELD_NUMBER: _ClassVar[int]
    args: _containers.RepeatedCompositeFieldContainer[SerializedObjectInsideBLOB]
    arg_blobs: _containers.RepeatedCompositeFieldContainer[BLOB]
    request_error_blob: BLOB
    function_call_metadata: bytes
    def __init__(
        self,
        args: _Optional[_Iterable[_Union[SerializedObjectInsideBLOB, _Mapping]]] = ...,
        arg_blobs: _Optional[_Iterable[_Union[BLOB, _Mapping]]] = ...,
        request_error_blob: _Optional[_Union[BLOB, _Mapping]] = ...,
        function_call_metadata: _Optional[bytes] = ...,
    ) -> None: ...

class FunctionArg(_message.Message):
    __slots__ = ("function_call_id", "value")
    FUNCTION_CALL_ID_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    function_call_id: str
    value: SerializedObjectInsideBLOB
    def __init__(
        self,
        function_call_id: _Optional[str] = ...,
        value: _Optional[_Union[SerializedObjectInsideBLOB, _Mapping]] = ...,
    ) -> None: ...

class FunctionCall(_message.Message):
    __slots__ = ("id", "target", "args", "call_metadata")
    ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    ARGS_FIELD_NUMBER: _ClassVar[int]
    CALL_METADATA_FIELD_NUMBER: _ClassVar[int]
    id: str
    target: FunctionRef
    args: _containers.RepeatedCompositeFieldContainer[FunctionArg]
    call_metadata: bytes
    def __init__(
        self,
        id: _Optional[str] = ...,
        target: _Optional[_Union[FunctionRef, _Mapping]] = ...,
        args: _Optional[_Iterable[_Union[FunctionArg, _Mapping]]] = ...,
        call_metadata: _Optional[bytes] = ...,
    ) -> None: ...

class ReduceOp(_message.Message):
    __slots__ = ("id", "collection", "reducer", "call_metadata")
    ID_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    REDUCER_FIELD_NUMBER: _ClassVar[int]
    CALL_METADATA_FIELD_NUMBER: _ClassVar[int]
    id: str
    collection: _containers.RepeatedCompositeFieldContainer[FunctionArg]
    reducer: FunctionRef
    call_metadata: bytes
    def __init__(
        self,
        id: _Optional[str] = ...,
        collection: _Optional[_Iterable[_Union[FunctionArg, _Mapping]]] = ...,
        reducer: _Optional[_Union[FunctionRef, _Mapping]] = ...,
        call_metadata: _Optional[bytes] = ...,
    ) -> None: ...

class ExecutionPlanUpdate(_message.Message):
    __slots__ = ("function_call", "reduce")
    FUNCTION_CALL_FIELD_NUMBER: _ClassVar[int]
    REDUCE_FIELD_NUMBER: _ClassVar[int]
    function_call: FunctionCall
    reduce: ReduceOp
    def __init__(
        self,
        function_call: _Optional[_Union[FunctionCall, _Mapping]] = ...,
        reduce: _Optional[_Union[ReduceOp, _Mapping]] = ...,
    ) -> None: ...

class ExecutionPlanUpdates(_message.Message):
    __slots__ = ("updates", "root_function_call_id", "start_at")
    UPDATES_FIELD_NUMBER: _ClassVar[int]
    ROOT_FUNCTION_CALL_ID_FIELD_NUMBER: _ClassVar[int]
    START_AT_FIELD_NUMBER: _ClassVar[int]
    updates: _containers.RepeatedCompositeFieldContainer[ExecutionPlanUpdate]
    root_function_call_id: str
    start_at: _timestamp_pb2.Timestamp
    def __init__(
        self,
        updates: _Optional[_Iterable[_Union[ExecutionPlanUpdate, _Mapping]]] = ...,
        root_function_call_id: _Optional[str] = ...,
        start_at: _Optional[
            _Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]
        ] = ...,
    ) -> None: ...

class AllocationResult(_message.Message):
    __slots__ = (
        "outcome_code",
        "failure_reason",
        "value",
        "updates",
        "uploaded_function_outputs_blob",
        "request_error_output",
        "uploaded_request_error_blob",
        "metrics",
    )
    OUTCOME_CODE_FIELD_NUMBER: _ClassVar[int]
    FAILURE_REASON_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    UPDATES_FIELD_NUMBER: _ClassVar[int]
    UPLOADED_FUNCTION_OUTPUTS_BLOB_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ERROR_OUTPUT_FIELD_NUMBER: _ClassVar[int]
    UPLOADED_REQUEST_ERROR_BLOB_FIELD_NUMBER: _ClassVar[int]
    METRICS_FIELD_NUMBER: _ClassVar[int]
    outcome_code: AllocationOutcomeCode
    failure_reason: AllocationFailureReason
    value: SerializedObjectInsideBLOB
    updates: ExecutionPlanUpdates
    uploaded_function_outputs_blob: BLOB
    request_error_output: SerializedObjectInsideBLOB
    uploaded_request_error_blob: BLOB
    metrics: Metrics
    def __init__(
        self,
        outcome_code: _Optional[_Union[AllocationOutcomeCode, str]] = ...,
        failure_reason: _Optional[_Union[AllocationFailureReason, str]] = ...,
        value: _Optional[_Union[SerializedObjectInsideBLOB, _Mapping]] = ...,
        updates: _Optional[_Union[ExecutionPlanUpdates, _Mapping]] = ...,
        uploaded_function_outputs_blob: _Optional[_Union[BLOB, _Mapping]] = ...,
        request_error_output: _Optional[
            _Union[SerializedObjectInsideBLOB, _Mapping]
        ] = ...,
        uploaded_request_error_blob: _Optional[_Union[BLOB, _Mapping]] = ...,
        metrics: _Optional[_Union[Metrics, _Mapping]] = ...,
    ) -> None: ...

class Allocation(_message.Message):
    __slots__ = ("request_id", "function_call_id", "allocation_id", "inputs", "result")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_CALL_ID_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    INPUTS_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    function_call_id: str
    allocation_id: str
    inputs: FunctionInputs
    result: AllocationResult
    def __init__(
        self,
        request_id: _Optional[str] = ...,
        function_call_id: _Optional[str] = ...,
        allocation_id: _Optional[str] = ...,
        inputs: _Optional[_Union[FunctionInputs, _Mapping]] = ...,
        result: _Optional[_Union[AllocationResult, _Mapping]] = ...,
    ) -> None: ...

class CreateAllocationRequest(_message.Message):
    __slots__ = ("allocation",)
    ALLOCATION_FIELD_NUMBER: _ClassVar[int]
    allocation: Allocation
    def __init__(
        self, allocation: _Optional[_Union[Allocation, _Mapping]] = ...
    ) -> None: ...

class WatchAllocationStateRequest(_message.Message):
    __slots__ = ("allocation_id",)
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    allocation_id: str
    def __init__(self, allocation_id: _Optional[str] = ...) -> None: ...

class DeleteAllocationRequest(_message.Message):
    __slots__ = ("allocation_id",)
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    allocation_id: str
    def __init__(self, allocation_id: _Optional[str] = ...) -> None: ...

class AllocationFunctionCallResult(_message.Message):
    __slots__ = (
        "function_call_id",
        "outcome_code",
        "value_output",
        "value_blob",
        "request_error_output",
        "request_error_blob",
    )
    FUNCTION_CALL_ID_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_CODE_FIELD_NUMBER: _ClassVar[int]
    VALUE_OUTPUT_FIELD_NUMBER: _ClassVar[int]
    VALUE_BLOB_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ERROR_OUTPUT_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ERROR_BLOB_FIELD_NUMBER: _ClassVar[int]
    function_call_id: str
    outcome_code: AllocationOutcomeCode
    value_output: SerializedObjectInsideBLOB
    value_blob: BLOB
    request_error_output: SerializedObjectInsideBLOB
    request_error_blob: BLOB
    def __init__(
        self,
        function_call_id: _Optional[str] = ...,
        outcome_code: _Optional[_Union[AllocationOutcomeCode, str]] = ...,
        value_output: _Optional[_Union[SerializedObjectInsideBLOB, _Mapping]] = ...,
        value_blob: _Optional[_Union[BLOB, _Mapping]] = ...,
        request_error_output: _Optional[
            _Union[SerializedObjectInsideBLOB, _Mapping]
        ] = ...,
        request_error_blob: _Optional[_Union[BLOB, _Mapping]] = ...,
    ) -> None: ...

class AllocationUpdate(_message.Message):
    __slots__ = ("allocation_id", "function_call_result", "output_blob")
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_CALL_RESULT_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_BLOB_FIELD_NUMBER: _ClassVar[int]
    allocation_id: str
    function_call_result: AllocationFunctionCallResult
    output_blob: BLOB
    def __init__(
        self,
        allocation_id: _Optional[str] = ...,
        function_call_result: _Optional[
            _Union[AllocationFunctionCallResult, _Mapping]
        ] = ...,
        output_blob: _Optional[_Union[BLOB, _Mapping]] = ...,
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
