from typing import Any

from tensorlake.applications import (
    Function,
    Future,
    InternalError,
    SDKUsageError,
)
from tensorlake.applications.internal_logger import InternalLogger
from tensorlake.applications.metadata import SPLITTER_INPUT_MODE
from tensorlake.applications.registry import get_function

from .output_events import SpecialFunctionCallSettings


def special_function_call(
    settings: SpecialFunctionCallSettings,
    function: Function,
    args: list[Any],
    kwargs: dict[str, Any],
    logger: InternalLogger,
) -> Any | Future:
    if settings.is_map_concat:
        return _map_concat(args, logger)
    elif settings.is_map_splitter:
        return _map_splitter(settings, function, args, logger)
    elif settings.is_reduce_splitter:
        return _reduce_splitter(settings, args, kwargs, logger)
    else:
        raise InternalError(
            f"Special function call settings {settings} don't specify any special function call"
        )


def _map_splitter(
    settings: SpecialFunctionCallSettings,
    function: Function,
    args: list[Any],
    logger: InternalLogger,
) -> Future:
    logger.info("running map splitter special function call")

    map_function: Function = get_function(settings.splitter_function_name)
    map_inputs: list[Any]
    if settings.splitter_input_mode == SPLITTER_INPUT_MODE.ITEMS_IN_ONE_ARG:
        # User code passed a Future as map operation input.
        if not isinstance(args[0], list):
            raise SDKUsageError(
                f"Map operation input must be a list, got {type(args[0])}"
            )
        map_inputs = args[0]
    else:
        # User code passed a list as map operation input.
        map_inputs = args

    # Important: use tail calls to optimize.
    map_futures: list[Future] = [
        map_function.future(map_input) for map_input in map_inputs
    ]
    return function.future(*map_futures)


def _map_concat(
    args: list[Any],
    logger: InternalLogger,
) -> list[Any]:
    logger.info("running map concat special function call")
    return args


def _reduce_splitter(
    settings: SpecialFunctionCallSettings,
    args: list[Any],
    kwargs: dict[str, Any],
    logger: InternalLogger,
) -> Any | Future:
    logger.info("running reduce splitter special function call")

    reduce_function: Function = get_function(settings.splitter_function_name)
    reduce_inputs: list[Any] = []
    if "initial" in kwargs:
        reduce_inputs.append(kwargs["initial"])

    if settings.splitter_input_mode == SPLITTER_INPUT_MODE.ITEMS_IN_ONE_ARG:
        # User code passed a Future as reduce operation input.
        if not isinstance(args[0], list):
            raise SDKUsageError(
                f"Reduce operation input must be a list, got {type(args[0])}"
            )
        reduce_inputs.extend(args[0])
    else:
        # User code passed a list as reduce operation input.
        reduce_inputs.extend(args)

    if len(reduce_inputs) == 0:
        raise SDKUsageError("reduce of empty iterable with no initial value")

    if len(reduce_inputs) == 1:
        return reduce_inputs[0]

    # Create a chain of function calls to reduce all args one by one.
    # Ordering of calls is important here. We should reduce ["a", "b", "c", "d"]
    # using string concat function into "abcd".

    # reduce_inputs now contain at least two items.
    last_future: Future = reduce_function.future(reduce_inputs[0], reduce_inputs[1])
    for item in reduce_inputs[2:]:
        last_future = reduce_function.future(last_future, item)

    # Important: use tail calls to optimize.
    return last_future
