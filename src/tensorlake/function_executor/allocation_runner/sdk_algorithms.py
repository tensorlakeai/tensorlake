from typing import Any, Dict, List

from tensorlake.applications import (
    Function,
    InternalError,
)
from tensorlake.applications.function.application_call import (
    SerializedApplicationArgument,
    deserialize_application_function_call_arguments,
)
from tensorlake.applications.function.function_call import (
    set_self_arg,
)
from tensorlake.applications.function.user_data_serializer import (
    deserialize_value_with_metadata,
    function_input_serializer,
    serialize_value,
)
from tensorlake.applications.interface.futures import (
    _request_scoped_id,
)
from tensorlake.applications.metadata import (
    CollectionItemMetadata,
    CollectionMetadata,
    FunctionCallArgumentMetadata,
    FunctionCallMetadata,
    ValueMetadata,
    deserialize_metadata,
    serialize_metadata,
)
from tensorlake.applications.registry import get_function
from tensorlake.applications.user_data_serializer import (
    UserDataSerializer,
)

from ...applications.internal_logger import InternalLogger
from ..proto.function_executor_pb2 import (
    ExecutionPlanUpdate,
    ExecutionPlanUpdates,
    FunctionArg,
    FunctionCall,
    FunctionRef,
    SerializedObjectInsideBLOB,
)
from .event_loop.output_events import (
    FunctionCallCollectionRef,
    FunctionCallRef,
    OutputEventCreateFunctionCall,
)
from .http_request_parse import (
    parse_application_function_call_arg_from_single_payload,
    parse_application_function_call_args_from_http_request,
    parse_application_function_call_args_from_multipart_form_data,
)
from .value import SerializedValue, Value


def serialize_user_value(
    value: Any, serializer: UserDataSerializer, type_hint: Any
) -> SerializedValue:
    """Serializes a user value into SerializedValue."""
    data: bytes
    metadata: ValueMetadata
    data, metadata = serialize_value(
        value=value,
        serializer=serializer,
        value_id=_request_scoped_id(),
        type_hint=type_hint,
    )
    return SerializedValue(
        metadata=metadata,
        data=data,
        content_type=metadata.content_type,
    )


def deserialize_application_function_call_args(
    function: Function,
    payload: SerializedValue,
    function_instance_arg: Any | None,
) -> tuple[List[Any], Dict[str, Any]]:
    """Returns a mapping from application function positional argument index or keyword to its deserialized Value.

    Raises DeserializationError if deserialization of any argument fails.
    Raises InternalError on other errors.
    """
    input_serializer: UserDataSerializer = function_input_serializer(
        function, app_call=True
    )
    serialized_args: list[SerializedApplicationArgument]
    serialized_kwargs: dict[str, SerializedApplicationArgument]

    if payload.content_type == "message/http":
        # Future mode for application function calls where the HTTP request is forwarded from Server.
        # Server will start doing this only once all users migrated to FE version 1.2+.
        serialized_args, serialized_kwargs = (
            parse_application_function_call_args_from_http_request(payload.data)
        )
    elif payload.content_type is not None and payload.content_type.startswith(
        "multipart/form-data"
    ):
        # Current mode for multi-parameter application function calls (>1 parameter).
        # Legacy mode for multi-parameter application function calls (>1 parameter).
        serialized_args, serialized_kwargs = (
            parse_application_function_call_args_from_multipart_form_data(
                body_buffer=payload.data,
                body_offset=0,
                content_type=payload.content_type,
            )
        )
    else:
        # Current mode for application function calls with a single argument.
        content_type: str = (
            input_serializer.content_type
            if payload.content_type is None
            else payload.content_type
        )
        serialized_arg: SerializedApplicationArgument = (
            parse_application_function_call_arg_from_single_payload(
                body_buffer=payload.data,
                body_offset=0,
                body_end_offset=len(payload.data),
                content_type=content_type,
            )
        )
        # Single payload is always mapped to the first positional application function argument.
        serialized_args = [serialized_arg]
        serialized_kwargs = {}

    args, kwargs = deserialize_application_function_call_arguments(
        application=function,
        serialized_args=serialized_args,
        serialized_kwargs=serialized_kwargs,
    )

    if function_instance_arg is not None:
        set_self_arg(args, function_instance_arg)

    return args, kwargs


def validate_and_deserialize_function_call_metadata(
    serialized_function_call_metadata: bytes,
    serialized_args: List[SerializedValue],
    function: Function,
    logger: InternalLogger,
) -> FunctionCallMetadata | None:
    if len(serialized_function_call_metadata) > 0:
        # Function call created by SDK.
        for serialized_arg in serialized_args:
            if serialized_arg.metadata is None:
                logger.error(
                    "function argument is missing metadata",
                )
                raise InternalError("Function argument is missing metadata.")

        function_call_metadata = deserialize_metadata(serialized_function_call_metadata)
        if not isinstance(function_call_metadata, FunctionCallMetadata):
            logger.error(
                "unexpected function call metadata type",
                metadata_type=type(function_call_metadata),
            )
            raise InternalError(
                f"Unexpected function call metadata type: {type(function_call_metadata)}"
            )

        return function_call_metadata
    else:
        # Application function call created by Server.
        if len(serialized_args) != 1:
            logger.error(
                "expected exactly one function argument for server-created application function call",
                num_args=len(serialized_args),
            )
            raise InternalError(
                f"Expected exactly one function argument for server-created application "
                f"function call, got {len(serialized_args)}."
            )

        if function._application_config is None:
            raise InternalError(
                "Non-application function was called without SDK metadata"
            )

        return None


def deserialize_sdk_function_call_args(
    serialized_args: List[SerializedValue],
) -> Dict[str, Value]:
    """Returns a mapping from serialized argument IDs to their deserialized Values.

    Raises TensorlakeError on error.
    """
    args: Dict[str, Value] = {}
    for ix, serialized_arg in enumerate(serialized_args):
        if serialized_arg.metadata is None:
            raise InternalError("SDK function call arguments must have metadata.")

        args[serialized_arg.metadata.id] = Value(
            metadata=serialized_arg.metadata,
            object=deserialize_value_with_metadata(
                serialized_value=serialized_arg.data,
                metadata=serialized_arg.metadata,
            ),
            input_ix=ix,
        )

    return args


def reconstruct_sdk_function_call_args(
    function_call_metadata: FunctionCallMetadata,
    arg_values: Dict[str, Value],
    function_instance_arg: Any | None,
) -> tuple[List[Any], Dict[str, Any]]:
    """Returns function call args and kwargs reconstructed from arg_values."""
    args, kwargs = _reconstruct_sdk_function_call_args(
        function_call_metadata=function_call_metadata,
        arg_values=arg_values,
    )

    if function_instance_arg is not None:
        set_self_arg(args, function_instance_arg)

    return args, kwargs


def _reconstruct_sdk_function_call_args(
    function_call_metadata: FunctionCallMetadata,
    arg_values: Dict[str, Value],
) -> tuple[List[Any], Dict[str, Any]]:
    args: List[Any] = []
    kwargs: Dict[str, Any] = {}

    for arg_metadata in function_call_metadata.args:
        args.append(_reconstruct_function_arg_value(arg_metadata, arg_values))
    for kwarg_key, kwarg_metadata in function_call_metadata.kwargs.items():
        kwargs[kwarg_key] = _reconstruct_function_arg_value(kwarg_metadata, arg_values)
    return args, kwargs


def _reconstruct_function_arg_value(
    arg_metadata: FunctionCallArgumentMetadata, arg_values: Dict[str, Value]
) -> Any:
    """Reconstructs the original value from function arg metadata."""
    # NB: This code needs to be in sync with LocalRunner where it's doing a similar thing.
    if arg_metadata.collection is None:
        return arg_values[arg_metadata.value_id].object
    else:
        return _reconstruct_collection_value(arg_metadata.collection, arg_values)


def _reconstruct_collection_value(
    collection_metadata: CollectionMetadata, arg_values: Dict[str, Value]
) -> List[Any]:
    """Reconstructs the original values from the supplied collection metadata."""
    # NB: This code needs to be in sync with LocalRunner where it's doing a similar thing.
    result: list[Any] = []
    stack: list[tuple[CollectionMetadata, list[Any]]] = [(collection_metadata, result)]
    while len(stack) > 0:
        collection_metadata, collection_values = stack.pop()

        for item in collection_metadata.items:
            if item.collection is None:
                collection_values.append(arg_values[item.value_id].object)
            else:
                item_collection: list[Any] = []
                stack.append((item.collection, item_collection))
                collection_values.append(item_collection)

    return result


_OutputEventArgValueType = Any | FunctionCallRef | FunctionCallCollectionRef
_OutputEventArgSerializedValueType = (
    SerializedValue | FunctionCallRef | FunctionCallCollectionRef
)


def serialize_output_event_args(
    args: List[_OutputEventArgValueType],
    kwargs: Dict[str, _OutputEventArgValueType],
    function_name: str,
) -> tuple[
    List[_OutputEventArgSerializedValueType],
    Dict[str, _OutputEventArgSerializedValueType],
    Dict[str, SerializedValue],
]:
    """Serializes raw values in output event args, passes refs through unchanged.

    Returns (serialized_args, serialized_kwargs, serialized_values_for_blob_upload).

    Raises SerializationError if serialization of any value fails.
    """
    serialized_values: Dict[str, SerializedValue] = {}
    args_serializer: UserDataSerializer = function_input_serializer(
        get_function(function_name), app_call=False
    )

    def serialize_arg(
        arg: Any | FunctionCallRef | FunctionCallCollectionRef,
    ) -> SerializedValue | FunctionCallRef | FunctionCallCollectionRef:
        if isinstance(arg, (FunctionCallRef, FunctionCallCollectionRef)):
            return arg
        serialized_value: SerializedValue = serialize_user_value(
            value=arg, serializer=args_serializer, type_hint=type(arg)
        )
        serialized_values[serialized_value.metadata.id] = serialized_value
        return serialized_value

    serialized_args = [serialize_arg(a) for a in args]
    serialized_kwargs = {k: serialize_arg(v) for k, v in kwargs.items()}
    return serialized_args, serialized_kwargs, serialized_values


def output_event_to_execution_plan_updates(
    output_event: OutputEventCreateFunctionCall,
    serialized_args: List[
        SerializedValue | FunctionCallRef | FunctionCallCollectionRef
    ],
    serialized_kwargs: Dict[
        str, SerializedValue | FunctionCallRef | FunctionCallCollectionRef
    ],
    uploaded_serialized_objects: Dict[str, SerializedObjectInsideBLOB],
    output_serializer_name_override: str | None,
    has_output_type_hint_override: bool,
    output_type_hint_override: Any,
    function_ref: FunctionRef,
) -> ExecutionPlanUpdates:
    """Constructs ExecutionPlanUpdates proto from an OutputEventCreateFunctionCall and serialized args.

    Raises TensorlakeError on error.
    """
    metadata: FunctionCallMetadata = FunctionCallMetadata(
        id=output_event.durable_id,
        output_serializer_name_override=output_serializer_name_override,
        output_type_hint_override=output_type_hint_override,
        has_output_type_hint_override=has_output_type_hint_override,
        args=[],
        kwargs={},
    )
    function_pb_args: List[FunctionArg] = []

    def process_arg(
        arg: SerializedValue | FunctionCallRef | FunctionCallCollectionRef,
    ) -> FunctionCallArgumentMetadata:
        if isinstance(arg, SerializedValue):
            function_pb_args.append(
                FunctionArg(value=uploaded_serialized_objects[arg.metadata.id])
            )
            return FunctionCallArgumentMetadata(
                value_id=arg.metadata.id,
                collection=None,
            )
        elif isinstance(arg, FunctionCallRef):
            function_pb_args.append(FunctionArg(function_call_id=arg.durable_id))
            return FunctionCallArgumentMetadata(
                value_id=arg.durable_id,
                collection=None,
            )
        elif isinstance(arg, FunctionCallCollectionRef):
            for durable_id in arg.durable_ids:
                function_pb_args.append(FunctionArg(function_call_id=durable_id))
            return FunctionCallArgumentMetadata(
                value_id=None,
                collection=CollectionMetadata(
                    items=[
                        CollectionItemMetadata(
                            value_id=durable_id,
                            collection=None,
                        )
                        for durable_id in arg.durable_ids
                    ]
                ),
            )
        else:
            raise InternalError(
                f"Unexpected type of serialized output event argument: {type(arg)}"
            )

    for arg in serialized_args:
        metadata.args.append(process_arg(arg))
    for kwarg_name, kwarg_value in serialized_kwargs.items():
        metadata.kwargs[kwarg_name] = process_arg(kwarg_value)

    return ExecutionPlanUpdates(
        updates=[
            ExecutionPlanUpdate(
                function_call=FunctionCall(
                    id=output_event.durable_id,
                    target=FunctionRef(
                        namespace=function_ref.namespace,
                        application_name=function_ref.application_name,
                        function_name=output_event.function_name,
                        application_version=function_ref.application_version,
                    ),
                    args=function_pb_args,
                    call_metadata=serialize_metadata(metadata),
                )
            )
        ],
        root_function_call_id=output_event.durable_id,
    )
