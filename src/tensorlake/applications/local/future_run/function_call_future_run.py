import asyncio
import inspect
from concurrent.futures import ThreadPoolExecutor
from queue import SimpleQueue
from typing import Any

from ...function.function_call import (
    create_function_error,
    set_self_arg,
)
from ...interface.exceptions import InternalError, RequestError, SDKUsageError
from ...interface.function import Function
from ...interface.futures import (
    FunctionCallFuture,
    Future,
    _TensorlakeFutureWrapper,
    _unwrap_future,
)
from ...interface.request_context import RequestContext
from ...interface.retries import Retries
from ...metadata import SPLITTER_INPUT_MODE, FunctionCallMetadata
from ...registry import get_function
from ...request_context.contextvar import set_current_request_context
from ..future import LocalFunctionCallFuture
from .future_run import (
    LocalFutureRun,
    LocalFutureRunResult,
    StopLocalFutureRun,
)


class FunctionCallFutureRun(LocalFutureRun):
    """LocalFutureRun that runs a function call and returns its result."""

    def __init__(
        self,
        local_future: LocalFunctionCallFuture,
        result_queue: SimpleQueue,
        thread_pool: ThreadPoolExecutor,
        application: Function,
        function: Function,
        class_instance: Any | None,
        request_context: RequestContext,
    ):
        super().__init__(
            local_future=local_future,
            result_queue=result_queue,
            thread_pool=thread_pool,
        )

        self._application: Function = application
        self._function: Function = function
        self._class_instance: Any | None = class_instance
        self._request_context: RequestContext = request_context
        self._arg_values: list[Any] | None = None
        self._kwarg_values: dict[str, Any] | None = None

    def start(self, arg_values: list[Any], kwarg_values: dict[str, Any]) -> None:
        """Starts the function call future run with resolved argument values.

        The argument values must be fully resolved (no unresolved futures among them).
        """
        self._arg_values = arg_values
        self._kwarg_values = kwarg_values
        super().start()

    def _run_future(self) -> LocalFutureRunResult:
        """Runs the function call and returns its result.

        The function call must have all its arguments resolved (no futures among them).
        If self._class_instance is not None, it is set as the self argument of the function call.

        Must be run in contextvars.Context of the Tensorlake Function call.

        Doesn't raise any exceptions, instead returns them in LocalFutureRunResult.exception.
        """
        set_current_request_context(self._request_context)

        future: FunctionCallFuture = self._local_future.future
        metadata: FunctionCallMetadata = self._local_future.future_metadata
        is_special_function_call: bool = (
            metadata.is_map_splitter
            or metadata.is_map_concat
            or metadata.is_reduce_splitter
        )

        if not is_special_function_call and self._class_instance is not None:
            set_self_arg(args=self._arg_values, self_instance=self._class_instance)

        # Application retries are used if function retries are not set.
        retries: Retries = (
            self._application._application_config.retries
            if self._function._function_config.retries is None
            else self._function._function_config.retries
        )
        runs_left: int = 1 + retries.max_retries
        while True:
            try:
                if is_special_function_call:
                    result: Any | _TensorlakeFutureWrapper[Future] = (
                        self._run_special_function_call(metadata)
                    )
                elif inspect.iscoroutinefunction(self._function):
                    result: Any | _TensorlakeFutureWrapper[Future] = asyncio.run(
                        self._function._original_function(
                            *self._arg_values, **self._kwarg_values
                        )
                    )
                else:
                    result: Any | _TensorlakeFutureWrapper[Future] = (
                        self._function._original_function(
                            *self._arg_values, **self._kwarg_values
                        )
                    )
                return LocalFutureRunResult(
                    id=future._id, output=_unwrap_future(result), error=None
                )
            except RequestError as e:
                # Never retry on RequestError.
                return LocalFutureRunResult(id=future._id, output=None, error=e)
            except StopLocalFutureRun:
                return LocalFutureRunResult(
                    id=future._id,
                    output=None,
                    error=create_function_error(future, cause="stopped"),
                )
            except BaseException as e:
                runs_left -= 1
                if runs_left == 0:
                    return LocalFutureRunResult(
                        id=future._id,
                        output=None,
                        error=create_function_error(future, cause=e),
                    )

    def _run_special_function_call(
        self, metadata: FunctionCallMetadata
    ) -> Any | _TensorlakeFutureWrapper[Future]:
        """Dispatches to the appropriate special function call handler."""
        if metadata.is_map_splitter:
            return self._special_function_call_map_splitter(metadata)
        elif metadata.is_map_concat:
            return self._special_function_call_map_concat()
        elif metadata.is_reduce_splitter:
            return self._special_function_call_reduce_splitter(metadata)
        else:
            raise InternalError(
                f"Special function call metadata doesn't specify any special function call"
            )

    def _special_function_call_map_splitter(
        self, metadata: FunctionCallMetadata
    ) -> Future:
        """Splits map inputs into individual function calls."""
        map_function: Function = get_function(metadata.splitter_function_name)
        map_inputs: list[Any]
        if metadata.splitter_input_mode == SPLITTER_INPUT_MODE.ITEMS_IN_ONE_ARG:
            # User code passed a Future as map operation input.
            if not isinstance(self._arg_values[0], list):
                raise SDKUsageError(
                    f"Map operation input must be a list, got {type(self._arg_values[0])}"
                )
            map_inputs = self._arg_values[0]
        else:
            # User code passed a list as map operation input.
            map_inputs = self._arg_values

        # Important: use tail calls to optimize.
        map_futures: list[Future] = [
            map_function.future(map_input) for map_input in map_inputs
        ]
        # Create concat future as tail call — collects all map results into a list.
        # LocalRunner handles this special tail call.
        return self._function.future(*map_futures)

    def _special_function_call_map_concat(self) -> list[Any]:
        """Concatenates resolved map function call results into a list."""
        return self._arg_values

    def _special_function_call_reduce_splitter(
        self, metadata: FunctionCallMetadata
    ) -> Future:
        """Splits reduce inputs into a chain of function calls."""
        reduce_function: Function = get_function(metadata.splitter_function_name)
        reduce_inputs: list[Any] = []
        if "initial" in self._kwarg_values:
            reduce_inputs.append(self._kwarg_values["initial"])

        if metadata.splitter_input_mode == SPLITTER_INPUT_MODE.ITEMS_IN_ONE_ARG:
            # User code passed a Future as reduce operation input.
            if not isinstance(self._arg_values[0], list):
                raise SDKUsageError(
                    f"Reduce operation input must be a list, got {type(self._arg_values[0])}"
                )
            reduce_inputs.extend(self._arg_values[0])
        else:
            # User code passed a list as reduce operation input.
            reduce_inputs.extend(self._arg_values)

        if len(reduce_inputs) == 0:
            raise SDKUsageError("reduce of empty iterable with no initial value")

        if len(reduce_inputs) == 1:
            return reduce_inputs[0]

        # Create a chain of function calls to reduce all args one by one.
        # Ordering of calls is important here. We should reduce ["a", "b", "c", "d"]
        # using string concat function into "abcd".

        # reduce_inputs now contain at least two items.
        last_future: Future = reduce_function.future(reduce_inputs[0], reduce_inputs[1])
        for input in reduce_inputs[2:]:
            last_future = reduce_function.future(last_future, input)

        # Important: use tail calls to optimize.
        return last_future
