from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List

from .function_call import ReducerFunctionCall, RegularFunctionCall
from .future import Future, FutureList
from .image import Image
from .retries import Retries


@dataclass
class _FunctionConfiguration:
    # None for non-method functions, only available after all modules are loaded
    # because class objects are created after their methods.
    class_name: str | None
    class_method_name: str | None
    class_init_timeout: int | None
    function_name: str
    description: str
    image: Image
    secrets: List[str]
    retries: Retries | None  # Uses application retry policy if None
    timeout: int
    cpu: float
    memory: float
    ephemeral_disk: float
    gpu: None | str | List[str]
    region: str | None
    cacheable: bool
    max_concurrency: int


@dataclass
class _ApplicationConfiguration:
    tags: Dict[str, str]
    retries: Retries
    region: str | None
    input_serializer: str
    output_serializer: str
    version: str


__runtime_hook_start_function_calls: Callable[
    [List[RegularFunctionCall | ReducerFunctionCall]], None
] = None
__runtime_hook_start_and_wait_function_calls: Callable[
    [List[RegularFunctionCall | ReducerFunctionCall]], List[Any]
] = None


class _InitialMissingType:
    pass


_InitialMissing = _InitialMissingType()


class Function:
    """Class that represents a Tensorlake Function configured by user.

    No validation is done at object creation time because Function objects
    are created at Python script loading time where it's not possible to provide
    a good UX. This is why all the validation is done separately."""

    def __init__(self, original_function: Callable):
        self._original_function: Callable = original_function
        self._function_config: _FunctionConfiguration | None = None
        self._application_config: _ApplicationConfiguration | None = None

    def __call__(self, *args, **kwargs) -> Any:
        """Does a blocking function call and returns its result."""
        # Called when the Function is called using () operator.
        global __runtime_hook_start_and_wait_function_calls
        function_call: RegularFunctionCall = RegularFunctionCall(
            function_name=self._function_config.function_name,
            args=list(args),
            kwargs=dict(kwargs),
            start_delay=None,
        )
        return __runtime_hook_start_and_wait_function_calls([function_call])[0]

    def map(self, iterable: Iterable) -> List[Any]:
        """Returns a list with every item transformed using the function.

        Blocks until the result is ready.
        Similar to https://docs.python.org/3/library/functions.html#map except all transformations
        are done in parallel.
        """
        global __runtime_hook_start_and_wait_function_calls
        map_calls: List[RegularFunctionCall] = self._make_map_calls(iterable)
        return __runtime_hook_start_and_wait_function_calls(map_calls)

    def reduce(
        self,
        iterable: Iterable,
        initial: Any | _InitialMissingType = _InitialMissing,
        /,
    ) -> Any:
        """Calls the function as a reducer of the supplied iterable.

        Blocks until the result is ready.
        Similar to https://docs.python.org/3/library/functools.html#functools.reduce.
        """
        global __runtime_hook_start_and_wait_function_calls
        reducer_call: ReducerFunctionCall = self._make_reducer_call(iterable, initial)
        return __runtime_hook_start_and_wait_function_calls([reducer_call])[0]

    def future(self, *args, **kwargs) -> Future:
        """Runs a non-blocking function call and returns its Future."""
        global __runtime_hook_start_function_calls
        function_call: RegularFunctionCall = RegularFunctionCall(
            function_name=self._function_config.function_name,
            args=list(args),
            kwargs=dict(kwargs),
            start_delay=None,
        )
        __runtime_hook_start_function_calls([function_call])
        return function_call.to_future()

    def later_future(self, start_delay: float, *args, **kwargs) -> Future:
        """Runs a non-blocking function call after start_delay seconds and returns its Future."""
        if start_delay < 0:
            raise ValueError("start_delay must be non-negative")
        global __runtime_hook_start_function_calls
        function_call: RegularFunctionCall = RegularFunctionCall(
            function_name=self._function_config.function_name,
            args=list(args),
            kwargs=dict(kwargs),
            start_delay=start_delay,
        )
        __runtime_hook_start_function_calls([function_call])
        return function_call.to_future()

    def map_future(self, iterable: Iterable) -> Future:
        """Returns a future that resolves into a list with every item transformed using the function.

        Similar to https://docs.python.org/3/library/functions.html#map except all transformations
        are done in parallel.
        """
        global __runtime_hook_start_function_calls
        map_calls: List[RegularFunctionCall] = self._make_map_calls(iterable)
        __runtime_hook_start_function_calls(map_calls)
        return FutureList([call.to_future() for call in map_calls])

    def reduce_future(
        self,
        iterable: Iterable,
        initial: Any | _InitialMissingType = _InitialMissing,
        /,
    ) -> Future:
        """Calls the function as a reducer of the supplied iterable and returns a Future with the result.

        Similar to https://docs.python.org/3/library/functools.html#functools.reduce.
        """
        global __runtime_hook_start_function_calls
        reducer_call: ReducerFunctionCall = self._make_reducer_call(iterable, initial)
        __runtime_hook_start_function_calls([reducer_call])
        return reducer_call.to_future()

    def __repr__(self) -> str:
        return (
            f"<Tensorlake Function(\n"
            f"  original_function={self._original_function!r},\n"
            f"  function_config={self._function_config!r},\n"
            f"  application_config={self._application_config!r}\n"
            f")>"
        )

    def _make_map_calls(self, iterable: Iterable) -> List[RegularFunctionCall]:
        map_calls: List[RegularFunctionCall] = []
        for item in iterable:
            map_calls.append(
                RegularFunctionCall(
                    function_name=self._function_config.function_name,
                    args=[item],
                    kwargs={},
                    start_delay=None,
                )
            )
        return map_calls

    def _make_reducer_call(
        self, iterable: Iterable, initial: Any | _InitialMissingType
    ) -> ReducerFunctionCall:
        inputs: List[Any] = list(iterable)
        if len(inputs) == 0 and initial is _InitialMissing:
            raise TypeError("reduce() of empty iterable with no initial value")

        if initial is not _InitialMissing:
            inputs.insert(0, initial)

        return ReducerFunctionCall(
            reducer_function_name=self._function_config.function_name,
            inputs=inputs,
            start_delay=None,
        )

    def __get__(self, instance: Any | None, cls: Any) -> "Function":
        # Called when the Function is called as an `instance` method of class `cls`.
        # We don't need to bind the Function object to the provided instance because
        # all the instances are created using an empty constructor, they are mutually replaceable.
        #
        # TODO: Fail with RequestError if cls.__tensorlake_name__ is not set.
        # This means the @tensorlake.cls decorator wasn't called on the class.
        return self

    def __reduce__(self):
        # This helps users to see that they made a coding mistake and returned a Tensorlake Function
        # object from their function without calling it.
        raise TypeError(
            f"Attempt to pickle a Tensorlake Function. "
            "Please return a single Tensorlake Function Call from your Tensorlake Function. "
            "A Tensorlake Function Call cannot be a part of another returned object, i.e. a list."
        )
