from typing import ClassVar as _ClassVar
from typing import Iterable as _Iterable
from typing import Mapping as _Mapping
from typing import Optional as _Optional
from typing import Union as _Union

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf.internal import containers as _containers

DESCRIPTOR: _descriptor.FileDescriptor

class SerializedObject(_message.Message):
    __slots__ = ("bytes", "string", "content_type")
    BYTES_FIELD_NUMBER: _ClassVar[int]
    STRING_FIELD_NUMBER: _ClassVar[int]
    CONTENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    bytes: bytes
    string: str
    content_type: str
    def __init__(
        self,
        bytes: _Optional[bytes] = ...,
        string: _Optional[str] = ...,
        content_type: _Optional[str] = ...,
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
    __slots__ = ("success", "customer_error")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    CUSTOMER_ERROR_FIELD_NUMBER: _ClassVar[int]
    success: bool
    customer_error: str
    def __init__(
        self, success: bool = ..., customer_error: _Optional[str] = ...
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

class FunctionOutput(_message.Message):
    __slots__ = ("outputs", "output_encoding")
    OUTPUTS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_ENCODING_FIELD_NUMBER: _ClassVar[int]
    outputs: _containers.RepeatedCompositeFieldContainer[SerializedObject]
    output_encoding: str
    def __init__(
        self,
        outputs: _Optional[_Iterable[_Union[SerializedObject, _Mapping]]] = ...,
        output_encoding: _Optional[str] = ...,
    ) -> None: ...

class RouterOutput(_message.Message):
    __slots__ = ("edges",)
    EDGES_FIELD_NUMBER: _ClassVar[int]
    edges: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, edges: _Optional[_Iterable[str]] = ...) -> None: ...

class RunTaskRequest(_message.Message):
    __slots__ = (
        "namespace",
        "graph_name",
        "graph_version",
        "function_name",
        "graph_invocation_id",
        "task_id",
        "function_input",
        "function_init_value",
    )
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    GRAPH_NAME_FIELD_NUMBER: _ClassVar[int]
    GRAPH_VERSION_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_NAME_FIELD_NUMBER: _ClassVar[int]
    GRAPH_INVOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_INPUT_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_INIT_VALUE_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    graph_name: str
    graph_version: str
    function_name: str
    graph_invocation_id: str
    task_id: str
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
        "function_output",
        "router_output",
        "stdout",
        "stderr",
        "is_reducer",
        "success",
        "metrics",
    )
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_OUTPUT_FIELD_NUMBER: _ClassVar[int]
    ROUTER_OUTPUT_FIELD_NUMBER: _ClassVar[int]
    STDOUT_FIELD_NUMBER: _ClassVar[int]
    STDERR_FIELD_NUMBER: _ClassVar[int]
    IS_REDUCER_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    METRICS_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    function_output: FunctionOutput
    router_output: RouterOutput
    stdout: str
    stderr: str
    is_reducer: bool
    success: bool
    metrics: Metrics
    def __init__(
        self,
        task_id: _Optional[str] = ...,
        function_output: _Optional[_Union[FunctionOutput, _Mapping]] = ...,
        router_output: _Optional[_Union[RouterOutput, _Mapping]] = ...,
        stdout: _Optional[str] = ...,
        stderr: _Optional[str] = ...,
        is_reducer: bool = ...,
        success: bool = ...,
        metrics: _Optional[_Union[Metrics, _Mapping]] = ...,
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
