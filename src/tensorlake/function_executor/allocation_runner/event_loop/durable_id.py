import hashlib
from typing import Any

from tensorlake.applications import InternalError
from tensorlake.applications.interface.futures import (
    FunctionCallFuture,
    Future,
    ListFuture,
    ReduceOperationFuture,
    _TensorlakeFutureWrapper,
    _unwrap_future,
)

from .future_info import FutureInfo


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
            kwarg_value: _TensorlakeFutureWrapper[Future] | Any = future._kwargs[
                kwarg_name
            ]
            _add_future_durable_id(
                value=kwarg_value,
                future_infos=future_infos,
                durable_id_attrs=durable_id_attrs,
            )
    elif isinstance(future, ListFuture):
        # Future specific metadata, part of durable ID.
        durable_id_attrs.append(future._metadata.durability_key)
        items: ListFuture | list[_TensorlakeFutureWrapper[Future] | Any] = (
            _unwrap_future(future._items)
        )
        if isinstance(items, ListFuture):
            _add_future_durable_id(
                value=items,
                future_infos=future_infos,
                durable_id_attrs=durable_id_attrs,
            )
        else:
            for item in items:
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

        items: ListFuture | list[_TensorlakeFutureWrapper[Future] | Any] = (
            _unwrap_future(future._items)
        )
        if isinstance(items, ListFuture):
            _add_future_durable_id(
                value=items,
                future_infos=future_infos,
                durable_id_attrs=durable_id_attrs,
            )
        else:
            for item in items:
                _add_future_durable_id(
                    value=item,
                    future_infos=future_infos,
                    durable_id_attrs=durable_id_attrs,
                )
    else:
        raise InternalError(f"Unexpected Future type: {type(future)}")

    return _sha256_hash_strings(durable_id_attrs)


def _add_future_durable_id(
    value: _TensorlakeFutureWrapper[Future] | Any,
    future_infos: dict[str, FutureInfo],
    durable_id_attrs: list[str],
) -> None:
    """Adds durable ID of the given Future to durable_attrs if the value is a Future. Does nothing otherwise.

    Raises InternalError if the value is a Future but its durable ID is not found in future_durable_ids.
    """
    # We don't hash user provided values. Only hash Futures to verify tree structure.
    value: Future | Any = _unwrap_future(value)
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
