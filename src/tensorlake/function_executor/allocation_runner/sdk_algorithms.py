import hashlib
from dataclasses import dataclass
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
    FunctionCallFuture,
    Future,
    ListFuture,
    ReduceOperationFuture,
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
from .http_request_parse import (
    parse_application_function_call_arg_from_single_payload,
    parse_application_function_call_args_from_http_request,
    parse_application_function_call_args_from_multipart_form_data,
)
from .value import SerializedValue, Value


@dataclass
class FutureInfo:
    # Original Future created by user code or internal Future created by SDK i.e.
    # a future per map function call, or a future per reduce operation step.
    future: Future
    # The future's durable ID.
    # ReduceOp and ListFuture are not visible to Server but we still
    # compute durable IDs because this allows to detect changes not visible
    # to Server and also avoids us using a recursive durable ID compute algorithm.
    durable_id: str
    # Set if this future is ListFuture.
    map_future_output: List[FunctionCallFuture] | None
    # Set if this is reduce operation future. None can be a valid output.
    reduce_future_output: Future | Any | None


def future_durable_id(
    future: Future,
    parent_function_call_id: str,
    previous_future_durable_id: str,
    future_infos: dict[str, FutureInfo],
) -> str:
    """Return durable ID for the supplied Future.

    parent_function_call_id is durable ID of the function call that created the Future.
    previous_durable_id is durable ID of the previous Future created by the parent function call.
    future_infos is a mapping from Future IDs to their FutureInfo.

    Durable Future IDs are the same across different executions (allocations) of the same parent function call
    if the parent function call is deterministic, i.e. it creates the same Futures in the same order each
    time it's executed. If this is not the case then the durable Future IDs will differ between executions, which may
    lead to re-execution of some function calls even if their inputs are the same as in a previous execution.

    To produce a durable Future ID, we compute it as a hash of:
    - parent_function_call_id, this scopes each durable ID to its parent function call and allows to generate them locally while running
      the parent function call.
    - previous_durable_id, this ties each Future durable ID to the previous Future created by the parent function call.
      If while replaying the parent function call it follows a different execution path (i.e. running a different function call) then this new
      function call and all next function calls won't be replayed because their durable IDs will be different due to different previous_durable_id
      in their durable ID hash. This ensures that any drift in the execution path gets detected and gets handled according to the replay mode used.
    - Future-specific metadata. This ensures that we detect changes inside each Future, i.e. a change of called function name.
    - Deterministically ordered durable IDs of all immediate child Futures.
      This ensures that changes in the structure of the Future tree leads to different durable IDs of its nodes
      starting from root so it's easy to detect a drift on Server side just by comparing durable ID of root.

    We're deliberately not hashing entire user values (i.e. function call args) to produce their durable IDs. This is because hashing entire user values
    is an expensive operation (i.e. hashing gigabytes of arbitrary user supplied objects which are function call parameters).
    This also results in better UX, i.e. this allows:
    - Seamless Schema Evolution: Users may want to change the schema of function parameters (e.g. add a new field with a default value
      to a pydantic model).
    - Use of non-deterministic functions: Users may want to use non-deterministic functions (e.g. functions that return current time or random values)
      inside otherwise deterministic function call trees.
    - To avoid "Serialization Flakiness": Strict equality checks on serialized data are fragile and can lead to false positive
      re-executions due to minor, non-semantic changes in serialization (e.g. different field ordering in protobufs, or insertion order in dicts).
    - To decouple Logic from Data. We adhere to a philosophy of being Strict on Control Flow but Lenient on Data. "The History is the Source of Truth."

    Raises TensorlakeError on error.
    """
    # Warning: any change of ordering of operations in this function may lead to different durable IDs being generated
    # which may lead to re-execution of function calls on Server side even if nothing changed in the future tree.
    durable_id_attrs: list[str] = [
        parent_function_call_id,
        previous_future_durable_id,
    ]

    if isinstance(future, FunctionCallFuture):
        # Future specific metadata, part of durable ID.
        durable_id_attrs.extend(["FunctionCall", future._function_name])
        for arg in future._args:
            _add_future_durable_id(
                value=arg,
                future_infos=future_infos,
                durable_id_attrs=durable_id_attrs,
            )

        # Iterate over sorted dict keys to ensure deterministic hash key order.
        sorted_kwarg_keys: list[str] = sorted(future._kwargs.keys())
        for kwarg_name in sorted_kwarg_keys:
            kwarg_value: Future | Any = future._kwargs[kwarg_name]
            _add_future_durable_id(
                value=kwarg_value,
                future_infos=future_infos,
                durable_id_attrs=durable_id_attrs,
            )
    elif isinstance(future, ListFuture):
        # Future specific metadata, part of durable ID.
        durable_id_attrs.append(future._metadata.durability_key)
        if isinstance(future._items, ListFuture):
            _add_future_durable_id(
                value=future._items,
                future_infos=future_infos,
                durable_id_attrs=durable_id_attrs,
            )
        else:
            for item in future._items:
                _add_future_durable_id(
                    value=item,
                    future_infos=future_infos,
                    durable_id_attrs=durable_id_attrs,
                )

    elif isinstance(future, ReduceOperationFuture):
        # Future specific metadata, part of durable ID.
        durable_id_attrs.extend(["ReduceOperation", future._function_name])

        _add_future_durable_id(
            value=future._initial,
            future_infos=future_infos,
            durable_id_attrs=durable_id_attrs,
        )

        if isinstance(future._items, ListFuture):
            _add_future_durable_id(
                value=future._items,
                future_infos=future_infos,
                durable_id_attrs=durable_id_attrs,
            )
        else:
            for item in future._items:
                _add_future_durable_id(
                    value=item,
                    future_infos=future_infos,
                    durable_id_attrs=durable_id_attrs,
                )
    else:
        raise InternalError(f"Unexpected Future type: {type(future)}")

    return _sha256_hash_strings(durable_id_attrs)


def _add_future_durable_id(
    value: Future | Any,
    future_infos: dict[str, FutureInfo],
    durable_id_attrs: list[str],
) -> None:
    """Adds durable ID of the given Future to durable_attrs if the value is a Future. Does nothing otherwise.

    Raises InternalError if the value is a Future but its durable ID is not found in future_durable_ids.
    """
    # We don't hash user provided values. Only hash Futures to verify tree structure.
    if isinstance(value, Future):
        value_future_info: FutureInfo | None = future_infos.get(value._id, None)
        if value_future_info is None:
            raise InternalError(
                f"FutureInfo for Future with id {value._id} not found in future_infos."
            )
        durable_id_attrs.append(value_future_info.durable_id)


def _sha256_hash_strings(strings: list[str]) -> str:
    """Returns sha256 hash of the concatenation of strings in the given list.

    If the strings are sha256 hashes, the result is also a high quality sha256 hash
    of the original hashed values. See https://en.wikipedia.org/wiki/Merkle_tree.
    """
    sha256 = hashlib.sha256()
    for s in strings:
        sha256.update(s.encode("utf-8"))
        sha256.update(b"|")  # Separator to avoid collisions of neighbouring strings.
    return sha256.hexdigest()


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


def replace_user_values_with_serialized_values(
    future: FunctionCallFuture,
    future_infos: dict[str, FutureInfo],
) -> Dict[str, SerializedValue]:
    """Replaces user values in the given FunctionCallFuture with their SerializedValues.

    The provided future is modified in-place with each user supplied value being
    SerializedValue instead of the original user object.

    Returns a mapping from value ID to SerializedValue for all serialized user values in the tree.

    Raises SerializationError if serialization of any value fails.
    Raises TensorlakeError for other errors.
    """
    serialized_values: Dict[str, SerializedValue] = {}

    def to_serialized_value(
        future_or_value: Future | Any, value_serializer: UserDataSerializer
    ) -> Any | SerializedValue:
        if isinstance(future_or_value, ReduceOperationFuture):
            future_info: FutureInfo = future_infos[future_or_value._id]
            if not isinstance(future_info.reduce_future_output, Future):
                # Replace the ReduceOperationFuture with its output value and upload.
                # This simplifies execution plan update generation.
                future_or_value = future_info.reduce_future_output
            else:
                return future_or_value
        elif isinstance(future_or_value, Future):
            return future_or_value

        # This is user supplied value now, need to serialize it.
        future_or_value: Any
        serialized_value: SerializedValue = serialize_user_value(
            value=future_or_value,
            serializer=value_serializer,
            type_hint=type(future_or_value),
        )
        serialized_values[serialized_value.metadata.id] = serialized_value
        return serialized_value

    args_serializer: UserDataSerializer = function_input_serializer(
        get_function(future._function_name), app_call=False
    )
    # Iterating over list copy to allow modifying the original list.
    for index, arg in enumerate(list(future._args)):
        future._args[index] = to_serialized_value(
            future_or_value=arg, value_serializer=args_serializer
        )
    # Iterating over dict copy to allow modifying the original list.
    for kwarg_name, kwarg_value in dict(future._kwargs).items():
        future._kwargs[kwarg_name] = to_serialized_value(
            future_or_value=kwarg_value, value_serializer=args_serializer
        )

    return serialized_values


def to_execution_plan_updates(
    future: FunctionCallFuture,
    uploaded_serialized_objects: Dict[str, SerializedObjectInsideBLOB],
    output_serializer_name_override: str | None,
    has_output_type_hint_override: bool,
    output_type_hint_override: Any,
    function_ref: FunctionRef,
    future_infos: dict[str, FutureInfo],
) -> ExecutionPlanUpdates:
    """Constructs ExecutionPlanUpdates proto for the supplied function call future.

    Each user supplied value in the args must be a SerializedValue present in uploaded_serialized_objects.
    function_call_ids is a mapping from FunctionCallFuture IDs to their durable IDs for all FunctionCallFutures
    in the tree rooted at function_call_future.

    Raises TensorlakeError on error.
    """
    updates: List[ExecutionPlanUpdate] = []

    updates.append(
        _function_call_execution_plan_update(
            function_call_future=future,
            uploaded_serialized_objects=uploaded_serialized_objects,
            output_serializer_name_override=output_serializer_name_override,
            has_output_type_hint_override=has_output_type_hint_override,
            output_type_hint_override=output_type_hint_override,
            function_ref=function_ref,
            future_infos=future_infos,
        )
    )

    return ExecutionPlanUpdates(
        updates=updates,
        root_function_call_id=future._id,
    )


def _function_call_execution_plan_update(
    function_call_future: FunctionCallFuture,
    uploaded_serialized_objects: Dict[str, SerializedObjectInsideBLOB],
    output_serializer_name_override: str | None,
    has_output_type_hint_override: bool,
    output_type_hint_override: Any,
    function_ref: FunctionRef,
    future_infos: dict[str, FutureInfo],
) -> ExecutionPlanUpdate:
    metadata: FunctionCallMetadata = FunctionCallMetadata(
        id=function_call_future._id,
        output_serializer_name_override=output_serializer_name_override,
        output_type_hint_override=output_type_hint_override,
        has_output_type_hint_override=has_output_type_hint_override,
        args=[],
        kwargs={},
    )
    function_pb_args: List[FunctionArg] = []

    def process_function_call_argument(
        arg: SerializedValue | Future,
    ) -> FunctionCallArgumentMetadata:
        if isinstance(arg, SerializedValue):
            function_pb_args.append(
                FunctionArg(
                    value=uploaded_serialized_objects[arg.metadata.id],
                )
            )
            return FunctionCallArgumentMetadata(
                value_id=arg.metadata.id,
                collection=None,
            )
        elif isinstance(arg, ListFuture):
            _embed_collection_into_function_pb_args(
                future=arg,
                function_pb_args=function_pb_args,
                future_infos=future_infos,
            )
            return FunctionCallArgumentMetadata(
                value_id=None,
                collection=_to_collection_metadata(arg, future_infos),
            )
        elif isinstance(arg, FunctionCallFuture):
            arg_durable_id: str = future_infos[arg._id].durable_id
            function_pb_args.append(
                FunctionArg(
                    function_call_id=arg_durable_id,
                )
            )
            return FunctionCallArgumentMetadata(
                value_id=arg_durable_id,
                collection=None,
            )
        elif isinstance(arg, ReduceOperationFuture):
            future_info: FutureInfo = future_infos[arg._id]
            if isinstance(future_info.reduce_future_output, Future):
                # FIXME: recursion, should be an iterative algorithm.
                return process_function_call_argument(future_info.reduce_future_output)
            else:
                raise InternalError(
                    "ReduceOperationFuture with value output should have been replaced by its output value by now."
                )
        else:
            raise InternalError(
                f"Unexpected type of function call argument: {type(arg)}"
            )

    for arg in function_call_future._args:
        metadata.args.append(process_function_call_argument(arg))

    for kwarg_name, kwarg_value in function_call_future._kwargs.items():
        metadata.kwargs[kwarg_name] = process_function_call_argument(kwarg_value)

    return ExecutionPlanUpdate(
        function_call=FunctionCall(
            id=function_call_future._id,
            target=FunctionRef(
                namespace=function_ref.namespace,
                application_name=function_ref.application_name,
                function_name=function_call_future._function_name,
                application_version=function_ref.application_version,
            ),
            args=function_pb_args,
            call_metadata=serialize_metadata(metadata),
        )
    )


def _to_collection_metadata(
    future: ListFuture, future_infos: dict[str, FutureInfo]
) -> CollectionMetadata:
    collection_metadata: CollectionMetadata = CollectionMetadata(items=[])
    future_info: FutureInfo = future_infos[future._id]

    for mapped_item in future_info.map_future_output:
        mapped_item: FunctionCallFuture
        collection_metadata.items.append(
            CollectionItemMetadata(
                value_id=future_infos[mapped_item._id].durable_id,
                collection=None,
            )
        )

    return collection_metadata


def _embed_collection_into_function_pb_args(
    future: ListFuture,
    function_pb_args: List[FunctionArg],
    future_infos: dict[str, FutureInfo],
) -> None:
    for output_item in future_infos[future._id].map_future_output:
        output_item: FunctionCallFuture
        function_pb_args.append(
            FunctionArg(
                function_call_id=future_infos[output_item._id].durable_id,
            )
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
