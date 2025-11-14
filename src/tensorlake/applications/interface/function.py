from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List

from .awaitables import (
    Awaitable,
    AwaitableList,
    FunctionCallAwaitable,
    _InitialMissing,
    _InitialMissingType,
    make_map_operation_awaitable,
    make_reduce_operation_awaitable,
    request_scoped_id,
)
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


def _function_name(original_function: Callable) -> str:
    """Returns function name of the given original (user) function."""
    # "{class}.{method}" for methods, otherwise just function name. Doesn't include module name.
    # All functions and classes in the application share a single namespace.
    # NB: this might not be a class if user passed something else to @cls decorator.
    return getattr(original_function, "__qualname__", "<unknown>")


@dataclass
class _ApplicationConfiguration:
    tags: Dict[str, str]
    retries: Retries
    region: str | None
    input_deserializer: str
    output_serializer: str
    version: str


class Function:
    """Class that represents a Tensorlake Function configured by user.

    No validation is done at object creation time because Function objects
    are created at Python script loading time where it's not possible to provide
    a good UX. This is why all the validation is done separately."""

    def __init__(self, original_function: Callable):
        self._original_function: Callable = original_function
        self._function_config: _FunctionConfiguration | None = None
        self._application_config: _ApplicationConfiguration | None = None
        self._awaitables_factory: FunctionAwaitablesFactory = FunctionAwaitablesFactory(
            self
        )

    def __call__(self, *args, **kwargs) -> Any:
        """Does a blocking function call and returns its result."""
        # Called when the Function is called using () operator.
        return (
            FunctionCallAwaitable(
                id=request_scoped_id(),
                function_name=self._function_config.function_name,
                args=list(args),
                kwargs=dict(kwargs),
            )
            .run()
            .result()
        )

    def map(self, items: Iterable[Any | Awaitable] | AwaitableList) -> List[Any]:
        """Returns a list with every item transformed using the function.

        Blocks until the result is ready.
        Similar to https://docs.python.org/3/library/functions.html#map except all transformations
        are done in parallel.
        """
        return (
            make_map_operation_awaitable(
                function_name=self._function_config.function_name,
                items=items,
            )
            .run()
            .result()
        )

    def reduce(
        self,
        items: Iterable[Any | Awaitable] | AwaitableList,
        initial: Any | _InitialMissingType = _InitialMissing,
        /,
    ) -> Any:
        """Calls the function as a reducer of the supplied iterable.

        Blocks until the result is ready.
        Similar to https://docs.python.org/3/library/functools.html#functools.reduce.
        """
        return (
            make_reduce_operation_awaitable(
                function_name=self._function_config.function_name,
                items=items,
                initial=initial,
            )
            .run()
            .result()
        )

    @property
    def awaitable(self) -> "FunctionAwaitablesFactory":
        """Returns function factory for creating awaitables."""
        return self._awaitables_factory

    def __repr__(self) -> str:
        return (
            f"<Tensorlake Function(\n"
            f"  original_function={self._original_function!r},\n"
            f"  function_config={self._function_config!r},\n"
            f"  application_config={self._application_config!r}\n"
            f")>"
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


class FunctionAwaitablesFactory:
    """Factory for creating awaitables for a specific Tensorlake Function.

    This class is returned by Function.awaitable property.
    """

    def __init__(self, function: Function):
        self._function: Function = function

    def __call__(self, *args, **kwargs) -> Awaitable:
        """Returns an awaitable that represents a call of the function."""
        return FunctionCallAwaitable(
            id=request_scoped_id(),
            function_name=self._function._function_config.function_name,
            args=list(args),
            kwargs=dict(kwargs),
        )

    def map(self, iterable: Iterable[Any | Awaitable]) -> Awaitable:
        """Returns an awaitable that represents mapping the function over the iterable."""
        return make_map_operation_awaitable(
            function_name=self._function._function_config.function_name,
            items=iterable,
        )

    def reduce(
        self,
        iterable: Iterable[Any | Awaitable],
        initial: Any | _InitialMissingType = _InitialMissing,
        /,
    ) -> Awaitable:
        """Returns an awaitable that represents reducing the iterable using the function."""
        return make_reduce_operation_awaitable(
            function_name=self._function._function_config.function_name,
            items=iterable,
            initial=initial,
        )
