import hashlib
from typing import Any, Dict, List

from tensorlake.applications import (
    Function,
    InternalError,
)
from tensorlake.applications.function.application_call import (
    deserialize_application_function_call_payload,
)
from tensorlake.applications.function.function_call import (
    set_self_arg,
)
from tensorlake.applications.function.user_data_serializer import (
    deserialize_value,
    function_input_serializer,
    serialize_value,
)
from tensorlake.applications.interface.awaitables import (
    Awaitable,
    AwaitableList,
    FunctionCallAwaitable,
    ReduceOperationAwaitable,
    _request_scoped_id,
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
    ReduceOp,
    SerializedObjectInsideBLOB,
)
from .value import SerializedValue, Value


def to_durable_awaitable_tree(
    root: Awaitable | Any,
    parent_function_call_id: str,
    awaitable_sequence_number: int,
) -> tuple[Awaitable | Any, int]:
    """Returns a shallow copy of the supplied Awaitable tree with Awaitable IDs supporting durable execution.

    The shallow copy has the same structure as the original tree, but each Awaitable in the tree
    has its ID replaced with a durable ID. User provided values are kept in the returned tree as is.

    parent_function_call_id is ID of the function call that created the awaitable tree.
    awaitable_sequence_number is the last unused sequential number of awaitables created by the parent function call.

    Durable Awaitable IDs are the same across different executions (allocations) of the same parent function call
    if the parent function call is deterministic, i.e. it creates the same awaitable trees in the same order each
    time it's executed. If this is not the case then the Awaitable IDs will differ between executions, which may
    lead to re-execution of some function calls even if their inputs are the same as in a previous execution.

    To produce a durable Awaitable ID, we compute it as a hash of:
    - parent_function_call_id in the tree
    - awaitable_sequence_number (unique per Awaitable in the tree). This ensures that any two Awaitables created by
      the same parent function call get different durable IDs even if the Awaitables are otherwise identical
      (i.e. two calls of the same function with same parameters). This also ensures that each durable ID is unique
      per each execution of parent function call.
    - Awaitable-specific metadata. This ensures that we detect changes in Awaitable tree nodes, i.e. change of called function name.
    - Deterministically ordered durable IDs of all immediate child Awaitables.
      This ensures that changes in the structure of the awaitable tree leads to different durable IDs of its nodes
      starting from root so it's easy to detect a drift on Server side just by comparing durable ID of root.

    We're deliberately not hashing entire awaitables to produce their durable IDs. This is because hashing entire awaitables
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
    # NB: Any change to ordering of operations in this function results in change of durable IDs for all durable Awaitables
    # which would lead to previously computed IDs not being replayable anymore.
    if not isinstance(root, Awaitable):
        return root, awaitable_sequence_number  # Return user-provided value as is.

    awaitable: Awaitable = root
    durable_id_attrs: list[str] = [
        parent_function_call_id,
        str(awaitable_sequence_number),
    ]
    awaitable_sequence_number += 1

    if isinstance(awaitable, AwaitableList):
        awaitable: AwaitableList
        # Awaitable specific metadata, part of durable ID.
        durable_id_attrs.append(awaitable.metadata.durability_key)
        durable_items: list[Awaitable | Any] = []
        for item in awaitable.items:
            durable_item, awaitable_sequence_number = to_durable_awaitable_tree(
                root=item,
                parent_function_call_id=parent_function_call_id,
                awaitable_sequence_number=awaitable_sequence_number,
            )
            durable_items.append(durable_item)
            _add_durable_id_attr(durable_items[-1], durable_id_attrs)

        return (
            AwaitableList(
                id=_sha256_hash_strings(durable_id_attrs),
                items=durable_items,
                metadata=awaitable.metadata,
            ),
            awaitable_sequence_number,
        )

    elif isinstance(awaitable, ReduceOperationAwaitable):
        awaitable: ReduceOperationAwaitable
        # Awaitable specific metadata, part of durable ID.
        durable_id_attrs.extend(["ReduceOperation", awaitable.function_name])
        durable_inputs: list[Awaitable | Any] = []
        for input in awaitable.inputs:
            durable_input, awaitable_sequence_number = to_durable_awaitable_tree(
                root=input,
                parent_function_call_id=parent_function_call_id,
                awaitable_sequence_number=awaitable_sequence_number,
            )
            durable_inputs.append(durable_input)
            _add_durable_id_attr(durable_inputs[-1], durable_id_attrs)

        return (
            ReduceOperationAwaitable(
                id=_sha256_hash_strings(durable_id_attrs),
                function_name=awaitable.function_name,
                inputs=durable_inputs,
            ),
            awaitable_sequence_number,
        )

    elif isinstance(awaitable, FunctionCallAwaitable):
        awaitable: FunctionCallAwaitable
        # Awaitable specific metadata, part of durable ID.
        durable_id_attrs.extend(["FunctionCall", awaitable.function_name])
        durable_args: list[Awaitable | Any] = []
        for arg in awaitable.args:
            durable_arg, awaitable_sequence_number = to_durable_awaitable_tree(
                root=arg,
                parent_function_call_id=parent_function_call_id,
                awaitable_sequence_number=awaitable_sequence_number,
            )
            durable_args.append(durable_arg)
            _add_durable_id_attr(durable_args[-1], durable_id_attrs)

        durable_kwargs: dict[str, Awaitable | Any] = {}
        # Iterate over sorted dict keys to ensure deterministic hash key order.
        sorted_kwarg_keys: list[str] = sorted(awaitable.kwargs.keys())
        for kwarg_name in sorted_kwarg_keys:
            kwarg_value: Awaitable | Any = awaitable.kwargs[kwarg_name]
            durable_kwarg, awaitable_sequence_number = to_durable_awaitable_tree(
                root=kwarg_value,
                parent_function_call_id=parent_function_call_id,
                awaitable_sequence_number=awaitable_sequence_number,
            )
            durable_kwargs[kwarg_name] = durable_kwarg
            _add_durable_id_attr(durable_kwargs[kwarg_name], durable_id_attrs)

        return (
            FunctionCallAwaitable(
                id=_sha256_hash_strings(durable_id_attrs),
                function_name=awaitable.function_name,
                args=durable_args,
                kwargs=durable_kwargs,
            ),
            awaitable_sequence_number,
        )
    else:
        raise InternalError(f"Unexpected Awaitable subclass: {type(awaitable)}")


def serialize_values_in_awaitable_tree(
    root: Awaitable | Any,
    value_serializer: UserDataSerializer,
    serialized_values: Dict[str, SerializedValue],
) -> SerializedValue | Awaitable:
    """Converts values in the given Awaitable tree into SerializedValues.

    The provided Awaitable tree is modified in-place with each user supplied value being
    SerializedValue instead of the original user object. Updates serialized_values with
    each SerializedValue created from concrete values. serialized_values is mapping from
    value ID to SerializedValue.

    Raises SerializationError if serialization of any value fails.
    Raises TensorlakeError for other errors.
    """
    # NB: This code needs to be in sync with LocalRunner where it's doing a similar thing.
    if not isinstance(root, Awaitable):
        data, metadata = serialize_value(
            value=root,
            serializer=value_serializer,
            value_id=_request_scoped_id(),
        )
        serialized_values[metadata.id] = SerializedValue(
            metadata=metadata,
            data=data,
            content_type=metadata.content_type,
        )
        return serialized_values[metadata.id]

    awaitable: Awaitable = root
    if isinstance(awaitable, AwaitableList):
        awaitable: AwaitableList
        for index, item in enumerate(list(awaitable.items)):
            # Iterating over list copy to allow modifying the original list.
            awaitable.items[index] = serialize_values_in_awaitable_tree(
                root=item,
                value_serializer=value_serializer,
                serialized_values=serialized_values,
            )
        return awaitable
    elif isinstance(awaitable, ReduceOperationAwaitable):
        awaitable: ReduceOperationAwaitable
        args_serializer: UserDataSerializer = function_input_serializer(
            get_function(awaitable.function_name)
        )
        for index, item in enumerate(list(awaitable.inputs)):
            # Iterating over list copy to allow modifying the original list.
            awaitable.inputs[index] = serialize_values_in_awaitable_tree(
                root=item,
                value_serializer=args_serializer,
                serialized_values=serialized_values,
            )
        return awaitable
    elif isinstance(awaitable, FunctionCallAwaitable):
        awaitable: FunctionCallAwaitable
        args_serializer: UserDataSerializer = function_input_serializer(
            get_function(awaitable.function_name)
        )
        for index, arg in enumerate(list(awaitable.args)):
            # Iterating over list copy to allow modifying the original list.
            awaitable.args[index] = serialize_values_in_awaitable_tree(
                root=arg,
                value_serializer=args_serializer,
                serialized_values=serialized_values,
            )
        for kwarg_name, kwarg_value in dict(awaitable.kwargs).items():
            # Iterating over dict copy to allow modifying the original list.
            awaitable.kwargs[kwarg_name] = serialize_values_in_awaitable_tree(
                root=kwarg_value,
                value_serializer=args_serializer,
                serialized_values=serialized_values,
            )
        return awaitable
    else:
        raise InternalError(f"Unexpected Awaitable subclass: {type(awaitable)}")


def to_execution_plan_updates(
    awaitable: Awaitable,
    uploaded_serialized_objects: Dict[str, SerializedObjectInsideBLOB],
    output_serializer_name_override: str,
    function_ref: FunctionRef,
    logger: InternalLogger,
) -> ExecutionPlanUpdates:
    """Traverses the awaitable tree and constructs ExecutionPlanUpdates proto.

    The awaitable must be validated already. The root awaitable must not be an AwaitableList.
    Caller must call this function for each item in the AwaitableList separately instead.
    Each value in the awaitable tree must be a SerializedValue present in uploaded_serialized_objects.

    Raises TensorlakeError on error.
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
    logger: InternalLogger,
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
                raise InternalError(
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
            # AwaitableList inside ReduceOperation inputs is not supported. We checked for this during user object validation.
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
                raise InternalError(
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
        raise InternalError(f"Unexpected Awaitable subclass: {type(awaitable)}")


def _to_collection_metadata(
    awaitable: AwaitableList, logger: InternalLogger
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
            raise InternalError(f"Unexpected type of AwaitableList item: {type(item)}")
    return collection_metadata


def _embed_collection_into_function_pb_args(
    awaitable: AwaitableList,
    uploaded_serialized_objects: Dict[str, SerializedObjectInsideBLOB],
    function_pb_args: List[FunctionArg],
    logger: InternalLogger,
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
            raise InternalError(f"Unexpected type of AwaitableList item: {type(item)}")


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
    logger: InternalLogger,
) -> FunctionCallMetadata | ReduceOperationMetadata | None:
    if len(serialized_function_call_metadata) > 0:
        # Function call created by SDK.
        for serialized_arg in serialized_args:
            if serialized_arg.metadata is None:
                logger.error(
                    "function argument is missing metadata",
                )
                raise InternalError("Function argument is missing metadata.")

        function_call_metadata = deserialize_metadata(serialized_function_call_metadata)
        if not isinstance(
            function_call_metadata, (FunctionCallMetadata, ReduceOperationMetadata)
        ):
            logger.error(
                "unexpected function call metadata type",
                metadata_type=type(function_call_metadata),
            )
            raise InternalError(
                f"Unexpected function call metadata type: {type(function_call_metadata)}"
            )

        if (
            isinstance(function_call_metadata, ReduceOperationMetadata)
            and len(serialized_args) != 2
        ):
            raise InternalError(
                f"Expected 2 arguments for reducer function call, got {len(serialized_args)}"
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


def reconstruct_function_call_args(
    function_call_metadata: FunctionCallMetadata | ReduceOperationMetadata | None,
    arg_values: Dict[str, Value],
    function_instance_arg: Any | None,
) -> tuple[List[Any], Dict[str, Any]]:
    """Returns function call args and kwargs reconstructed from arg_values."""
    if function_call_metadata is None:
        # Application function call created by Server.
        payload_arg: Value = arg_values["application_payload"]
        args: List[Any] = [payload_arg.object]
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
        if len(arg_values) != 2:
            raise InternalError(
                f"Expected exactly 2 argument values for reducer function call, got {len(arg_values)}"
            )
        args: List[Value] = list(arg_values.values())
        # Server provides accumulator first, item second
        args.sort(key=lambda arg: arg.input_ix)
        return [arg.object for arg in args], {}


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


def _add_durable_id_attr(node: Awaitable | Any, durable_attrs: list[str]) -> None:
    # We don't hash user provided values. Only hash Awaitables to verify tree structure.
    if isinstance(node, Awaitable):
        durable_attrs.append(node.id)


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
