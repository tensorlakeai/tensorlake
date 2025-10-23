from typing import Any, Dict, List, Set

from tensorlake.applications import (
    ApplicationValidationError,
    Function,
)
from tensorlake.applications.function.application_call import (
    deserialize_application_function_call_payload,
)
from tensorlake.applications.function.function_call import (
    set_self_arg,
)
from tensorlake.applications.function.user_data_serializer import (
    deserialize_value,
    serialize_value,
)
from tensorlake.applications.interface.awaitables import (
    Awaitable,
    AwaitableList,
    FunctionCallAwaitable,
    Future,
    ReduceOperationAwaitable,
)
from tensorlake.applications.metadata import (
    CollectionItemMetadata,
    CollectionMetadata,
    FunctionCallArgumentMetadata,
    FunctionCallMetadata,
    ReduceOperationMetadata,
    deserialize_metadata,
    serialize_metadata,
)
from tensorlake.applications.user_data_serializer import (
    UserDataSerializer,
)

from ..logger import FunctionExecutorLogger
from ..proto.function_executor_pb2 import (
    ExecutionPlanUpdate,
    ExecutionPlanUpdates,
    FunctionArg,
    FunctionCall,
    FunctionRef,
    ReduceOp,
    SerializedObjectInsideBLOB,
)
from .value import SerializedValue, Value


def process_function_output(
    output: Any,
    serializer: UserDataSerializer,
    function_call_ids: Set[str],
    serialized_values: Dict[str, SerializedValue],
) -> SerializedValue | Awaitable:
    """Validates the function output and replaces each value with a SerializedValue.

    This results in the original Awaitable tree being returned with each value being
    SerializedValue instead of the original user object. Updates serialized_values with
    each SerializedValue created from concrete values. serialized_values is mapping from
    value ID to SerializedValue.
    """
    # NB: This code needs to be in sync with LocalRunner where it's doing a similar thing.
    if not isinstance(output, (Awaitable, Future)):
        data, metadata = serialize_value(output, serializer=serializer)
        serialized_values[metadata.id] = SerializedValue(
            metadata=metadata,
            data=data,
            content_type=metadata.content_type,
        )
        return serialized_values[metadata.id]

    if isinstance(output, Future):
        raise ApplicationValidationError(
            f"Invalid argument: cannot run Future {repr(output)}, "
            "please pass an Awaitable or a concrete value."
        )

    awaitable: Awaitable
    if awaitable.id in function_call_ids:
        raise ApplicationValidationError(
            f"Invalid argument: {repr(awaitable)} is an Awaitable with already running Future, "
            "only not running Awaitable can be passed as function argument or returned from a function."
        )

    if isinstance(output, AwaitableList):
        awaitable: AwaitableList
        for index, item in enumerate(list(awaitable.items)):
            # Iterating over list copy to allow modifying the original list.
            awaitable.items[index] = process_function_output(
                output=item,
                serializer=serializer,
                function_call_ids=function_call_ids,
            )
        return awaitable
    elif isinstance(awaitable, ReduceOperationAwaitable):
        awaitable: ReduceOperationAwaitable
        for index, item in enumerate(list(awaitable.inputs)):
            # Iterating over list copy to allow modifying the original list.
            awaitable.inputs[index] = process_function_output(
                output=item,
                serializer=serializer,
                function_call_ids=function_call_ids,
            )
        return awaitable
    elif isinstance(awaitable, FunctionCallAwaitable):
        awaitable: FunctionCallAwaitable
        for index, arg in enumerate(list(awaitable.args)):
            # Iterating over list copy to allow modifying the original list.
            awaitable.args[index] = process_function_output(
                output=arg,
                serializer=serializer,
                function_call_ids=function_call_ids,
            )
        for kwarg_name, kwarg_value in dict(awaitable.kwargs).items():
            # Iterating over dict copy to allow modifying the original list.
            awaitable.kwargs[kwarg_name] = process_function_output(
                output=kwarg_value,
                serializer=serializer,
                function_call_ids=function_call_ids,
            )
        return awaitable
    else:
        raise ApplicationValidationError(
            f"Unexpected type of awaitable returned from function: {type(awaitable)}"
        )


def awaitable_to_execution_plan_updates(
    awaitable: Awaitable,
    uploaded_serialized_objects: Dict[str, SerializedObjectInsideBLOB],
    output_serializer_name_override: str,
    function_ref: FunctionRef,
    logger: FunctionExecutorLogger,
) -> ExecutionPlanUpdates:
    """Traverses the awaitable tree and constructs ExecutionPlanUpdates proto.

    The awaitable must be validated already. The root awaitable must not be an AwaitableList.
    Caller must call this function for each item in the AwaitableList separately instead.
    Each value in the awaitable tree must be a SerializedValue present in uploaded_serialized_objects.
    """
    updates: List[ExecutionPlanUpdate] = []
    _fill_execution_plan_updates(
        awaitable=awaitable,
        uploaded_serialized_objects=uploaded_serialized_objects,
        output_serializer_name_override=output_serializer_name_override,
        destination=updates,
        function_ref=function_ref,
        logger=logger,
    )
    return ExecutionPlanUpdates(
        updates=updates,
        root_function_call_id=awaitable.id,
    )


def _fill_execution_plan_updates(
    awaitable: Awaitable,
    uploaded_serialized_objects: Dict[str, SerializedObjectInsideBLOB],
    output_serializer_name_override: str | None,
    destination: List[ExecutionPlanUpdate],
    function_ref: FunctionRef,
    logger: FunctionExecutorLogger,
) -> None:
    if isinstance(awaitable, FunctionCallAwaitable):
        metadata: FunctionCallMetadata = FunctionCallMetadata(
            id=awaitable.id,
            output_serializer_name_override=output_serializer_name_override,
            args=[],
            kwargs={},
        )
        function_pb_args: List[FunctionArg] = []

        def process_function_call_argument(arg: Any) -> FunctionCallArgumentMetadata:
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
            elif isinstance(arg, AwaitableList):
                _embed_collection_into_function_pb_args(
                    awaitable=arg,
                    uploaded_serialized_objects=uploaded_serialized_objects,
                    function_pb_args=function_pb_args,
                    logger=logger,
                )
                # Collection is fully embedded now into function call args but its function
                # calls are not in the execution plan yet.
                for item in arg.items:
                    if isinstance(
                        item, (FunctionCallAwaitable, ReduceOperationAwaitable)
                    ):
                        _fill_execution_plan_updates(
                            awaitable=item,
                            uploaded_serialized_objects=uploaded_serialized_objects,
                            output_serializer_name_override=None,  # Only override at root function call.
                            destination=destination,
                            function_ref=function_ref,
                            logger=logger,
                        )
                return FunctionCallArgumentMetadata(
                    value_id=None,
                    collection=_to_collection_metadata(arg, logger),
                )
            elif isinstance(arg, (FunctionCallAwaitable, ReduceOperationAwaitable)):
                _fill_execution_plan_updates(
                    awaitable=arg,
                    uploaded_serialized_objects=uploaded_serialized_objects,
                    output_serializer_name_override=None,  # Only override at root function call.
                    destination=destination,
                    function_ref=function_ref,
                    logger=logger,
                )
                function_pb_args.append(
                    FunctionArg(
                        function_call_id=arg.id,
                    )
                )
                return FunctionCallArgumentMetadata(
                    value_id=arg.id,
                    collection=None,
                )
            else:
                raise ApplicationValidationError(
                    f"Unexpected type of function call argument: {type(arg)}"
                )

        for arg in awaitable.args:
            metadata.args.append(process_function_call_argument(arg))

        for kwarg_name, kwarg_value in awaitable.kwargs.items():
            metadata.kwargs[kwarg_name] = process_function_call_argument(kwarg_value)

        update = ExecutionPlanUpdate(
            function_call=FunctionCall(
                id=awaitable.id,
                target=FunctionRef(
                    namespace=function_ref.namespace,
                    application_name=function_ref.application_name,
                    function_name=awaitable.function_name,
                    application_version=function_ref.application_version,
                ),
                args=function_pb_args,
                call_metadata=serialize_metadata(metadata),
            )
        )
        destination.append(update)

    elif isinstance(awaitable, ReduceOperationAwaitable):
        metadata: ReduceOperationMetadata = ReduceOperationMetadata(
            id=awaitable.id,
            output_serializer_name_override=output_serializer_name_override,
        )
        collection: List[FunctionArg] = []

        for item in awaitable.inputs:
            if isinstance(item, SerializedValue):
                collection.append(
                    FunctionArg(
                        value=uploaded_serialized_objects[item.metadata.id],
                    )
                )
            elif isinstance(item, AwaitableList):
                raise ApplicationValidationError(
                    "AwaitableList cannot be used as an input item for ReduceOperationAwaitable, "
                    "please use individual Awaitable items instead."
                )
            elif isinstance(item, (FunctionCallAwaitable, ReduceOperationAwaitable)):
                _fill_execution_plan_updates(
                    awaitable=item,
                    uploaded_serialized_objects=uploaded_serialized_objects,
                    output_serializer_name_override=None,  # Only override at root function call.
                    destination=destination,
                    function_ref=function_ref,
                    logger=logger,
                )
                collection.append(
                    FunctionArg(
                        function_call_id=item.id,
                    )
                )
            else:
                raise ApplicationValidationError(
                    f"Unexpected type of reduce operation input item: {type(item)}"
                )

        update = ExecutionPlanUpdate(
            reduce=ReduceOp(
                id=awaitable.id,
                reducer=FunctionRef(
                    namespace=function_ref.namespace,
                    application_name=function_ref.application_name,
                    function_name=awaitable.function_name,
                    application_version=function_ref.application_version,
                ),
                collection=collection,
                call_metadata=serialize_metadata(metadata),
            )
        )
        destination.append(update)
    else:
        raise ApplicationValidationError(
            f"Unexpected type of awaitable: {type(awaitable)}"
        )


def _to_collection_metadata(
    awaitable: AwaitableList, logger: FunctionExecutorLogger
) -> CollectionMetadata:
    collection_metadata: CollectionMetadata = CollectionMetadata(
        items=[],
    )
    for item in awaitable.items:
        if isinstance(item, SerializedValue):
            collection_metadata.items.append(
                CollectionItemMetadata(
                    value_id=item.metadata.id,
                    collection=None,
                )
            )
        elif isinstance(item, AwaitableList):
            collection_metadata.items.append(
                CollectionItemMetadata(
                    value_id=None,
                    collection=_to_collection_metadata(item, logger),
                )
            )
        elif isinstance(item, (FunctionCallAwaitable, ReduceOperationAwaitable)):
            collection_metadata.items.append(
                CollectionItemMetadata(
                    value_id=item.id,
                    collection=None,
                )
            )
        else:
            raise ApplicationValidationError(
                f"Unexpected type of awaitable list item: {type(item)}"
            )
    return collection_metadata


def _embed_collection_into_function_pb_args(
    awaitable: AwaitableList,
    uploaded_serialized_objects: Dict[str, SerializedObjectInsideBLOB],
    function_pb_args: List[FunctionArg],
    logger: FunctionExecutorLogger,
) -> None:
    for item in awaitable.items:
        if isinstance(item, SerializedValue):
            function_pb_args.append(
                FunctionArg(
                    value=uploaded_serialized_objects[item.metadata.id],
                )
            )
        elif isinstance(item, AwaitableList):
            _embed_collection_into_function_pb_args(
                item, uploaded_serialized_objects, function_pb_args, logger
            )
        elif isinstance(item, (FunctionCallAwaitable, ReduceOperationAwaitable)):
            function_pb_args.append(
                FunctionArg(
                    function_call_id=item.id,
                )
            )
        else:
            raise ApplicationValidationError(
                f"Unexpected type of AwaitableList item: {type(item)}"
            )


def deserialize_function_arguments(
    function: Function, serialized_args: List[SerializedValue]
) -> Dict[str, Value]:
    args: Dict[str, Value] = {}
    for ix, serialized_arg in enumerate(serialized_args):
        if serialized_arg.metadata is None:
            # Application payload argument. It's allready validated to be only one argument.
            args["application_payload"] = Value(
                metadata=None,
                object=deserialize_application_function_call_payload(
                    application=function,
                    payload=serialized_arg.data,
                    payload_content_type=serialized_arg.content_type,
                ),
                input_ix=ix,
            )
        else:
            args[serialized_arg.metadata.id] = Value(
                metadata=serialized_arg.metadata,
                object=deserialize_value(
                    serialized_value=serialized_arg.data,
                    metadata=serialized_arg.metadata,
                ),
                input_ix=ix,
            )

    return args


def validate_and_deserialize_function_call_metadata(
    serialized_function_call_metadata: bytes,
    serialized_args: List[SerializedValue],
    function: Function,
    logger: FunctionExecutorLogger,
) -> FunctionCallMetadata | ReduceOperationMetadata | None:
    if len(serialized_function_call_metadata) > 0:
        # Function call created by SDK.
        for serialized_arg in serialized_args:
            if serialized_arg.metadata is None:
                logger.error(
                    "function argument is missing metadata",
                )
                raise ValueError("Function argument is missing metadata.")

        function_call_metadata = deserialize_metadata(serialized_function_call_metadata)
        if not isinstance(
            function_call_metadata, (FunctionCallMetadata, ReduceOperationMetadata)
        ):
            logger.error(
                "unsupported function call metadata type",
                metadata_type=type(function_call_metadata).__name__,
            )
            raise ValueError(
                f"Unsupported function call metadata type: {type(function_call_metadata).__name__}"
            )

        if (
            isinstance(function_call_metadata, ReduceOperationMetadata)
            and len(serialized_args) != 2
        ):
            raise ValueError(
                f"Expected 2 arguments for reducer function call, got {len(serialized_args)}"
            )
    else:
        # Application function call created by Server.
        if len(serialized_args) != 1:
            logger.error(
                "expected exactly one function argument for server-created application function call",
                num_args=len(serialized_args),
            )
            raise ValueError(
                f"Expected exactly one function argument for server-created application "
                f"function call, got {len(serialized_args)}."
            )

        if function._application_config is None:
            raise ValueError("Non-application function was called without SDK metadata")


def reconstruct_function_call_args(
    function_call_metadata: FunctionCallMetadata | ReduceOperationMetadata | None,
    arg_values: Dict[str, Value],
    function_instance_arg: Any | None,
) -> tuple[List[Any], Dict[str, Any]]:
    """Returns function call args and kwargs reconstructed from arg_values."""
    if function_call_metadata is None:
        # Application function call created by Server.
        args: List[Any] = [arg_values["application_payload"]]
        kwargs: Dict[str, Any] = {}
    else:
        # SDK-created function call.
        args, kwargs = _reconstruct_sdk_function_call_args(
            function_call_metadata=function_call_metadata,
            arg_values=arg_values,
        )

    if function_instance_arg is not None:
        set_self_arg(args, function_instance_arg)

    return args, kwargs


def _reconstruct_sdk_function_call_args(
    function_call_metadata: FunctionCallMetadata | ReduceOperationMetadata,
    arg_values: Dict[str, Value],
) -> tuple[List[Any], Dict[str, Any]]:
    if isinstance(function_call_metadata, FunctionCallMetadata):
        args: List[Any] = []
        kwargs: Dict[str, Any] = {}

        for arg_metadata in function_call_metadata.args:
            args.append(_reconstruct_function_arg_value(arg_metadata, arg_values))
        for kwarg_key, kwarg_metadata in function_call_metadata.kwargs.items():
            kwargs[kwarg_key] = _reconstruct_function_arg_value(
                kwarg_metadata, arg_values
            )
        return args, kwargs
    elif isinstance(function_call_metadata, ReduceOperationMetadata):
        args: List[Value] = list(arg_values.values())
        # Server provides accumulator first, item second
        args.sort(key=lambda arg: arg.input_ix)
        return args, {}


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
    values: List[Any] = []
    for item in collection_metadata.items:
        if item.collection is None:
            values.append(arg_values[item.value_id].object)
        else:
            values.append(_reconstruct_collection_value(item.collection, arg_values))
    return values
