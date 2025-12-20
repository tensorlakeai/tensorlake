import inspect
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List

from .awaitables import (
    Awaitable,
    AwaitableList,
    FunctionCallAwaitable,
    _InitialMissing,
    _InitialMissingType,
    _request_scoped_id,
    make_map_operation_awaitable,
    make_reduce_operation_awaitable,
)
from .exceptions import SDKUsageError
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
    min_containers: int | None
    max_containers: int | None


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
    a good UX. This is why all the validation is done separately i.e. on deployment."""

    def __init__(self, original_function: Callable):
        self._original_function: Callable = original_function
        self._function_config: _FunctionConfiguration | None = None
        self._application_config: _ApplicationConfiguration | None = None
        self._awaitables_factory: FunctionAwaitablesFactory = FunctionAwaitablesFactory(
            self
        )
        # Mimic original function if it's a regular user defined function.
        if inspect.isfunction(self._original_function):
            # Copy original function metadata into this Function object so function inspection
            # tools like these used by Agentic frameworks to generate tool descriptions work.
            # See the attributes at "function" at
            # https://docs.python.org/3/library/inspect.html#types-and-members.
            for mimic_attr in [
                "__doc__",
                "__name__",
                "__qualname__",
                "__code__",
                "__defaults__",
                "__kwdefaults__",
                "__globals__",
                "__builtins__",
                "__annotations__",
                "__type_params__",
                "__module__",
            ]:
                if hasattr(self._original_function, mimic_attr):
                    setattr(
                        self, mimic_attr, getattr(self._original_function, mimic_attr)
                    )

    def __call__(self, *args, **kwargs) -> Any:
        """Does a blocking function call and returns its result.

        Raises RequestError if the function raised RequestError.
        Raises FunctionError if the function failed.
        Raises TensorlakeError on other errors.
        """
        # Called when the Function is called using () operator.
        return (
            FunctionCallAwaitable(
                id=_request_scoped_id(),
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

        Raises RequestError if the function raised RequestError.
        Raises FunctionError if the function failed.
        Raises TensorlakeError on other errors.
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

        Raises RequestError if the function raised RequestError.
        Raises FunctionError if the function failed.
        Raises TensorlakeError on other errors.
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
        # Shows exact structure of the Function. Used for debug logging.
        return (
            f"<Tensorlake Function(\n"
            f"  original_function={self._original_function!r},\n"
            f"  function_config={self._function_config!r},\n"
            f"  application_config={self._application_config!r}\n"
            f")>"
        )

    def __str__(self) -> str:
        # Shows a simple human readable representation of the Function. Used in error messages.
        function_name: str = (
            getattr(self._original_function, "__qualname__", "<unknown>")
            if self._function_config is None
            else self._function_config.function_name
        )
        return f"Tensorlake Function '{function_name}'"

    def __get__(self, instance: Any | None, cls: Any) -> "Function":
        # Called when the Function is called as an `instance` method of class `cls`.
        # We don't need to bind the Function object to the provided instance because
        # all the instances are created using an empty constructor, they are mutually replaceable.
        return self

    def __reduce__(self):
        # This helps users to see that they made a coding mistake and returned a Tensorlake Function
        # object from their function without calling it or passed it as a function call parameter (directly
        # or inside another object).
        # Note: this exception will be converted into SerializationError when pickling is attempted.
        raise SDKUsageError(
            f"Attempt to pickle {self}. It cannot be passed as a function parameter or returned from a Tensorlake Function."
        )


class FunctionAwaitablesFactory:
    """Factory for creating awaitables for a specific Tensorlake Function.

    This class is returned by Function.awaitable property.
    """

    def __init__(self, function: Function):
        self._function: Function = function

    def __call__(self, *args, **kwargs) -> Awaitable:
        """Returns an awaitable that represents a call of the function.

        Raises TensorlakeError on error.
        """
        return FunctionCallAwaitable(
            id=_request_scoped_id(),
            function_name=self._function._function_config.function_name,
            args=list(args),
            kwargs=dict(kwargs),
        )

    def map(self, iterable: Iterable[Any | Awaitable]) -> Awaitable:
        """Returns an awaitable that represents mapping the function over the iterable.

        Raises TensorlakeError on error.
        """
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
        """Returns an awaitable that represents reducing the iterable using the function.

        Raises TensorlakeError on error.
        """
        return make_reduce_operation_awaitable(
            function_name=self._function._function_config.function_name,
            items=iterable,
            initial=initial,
        )

    def __repr__(self) -> str:
        # Shows exact structure of the Awaitable Factory. Used for debug logging.
        return f"<FunctionAwaitablesFactory function={self._function!r}>"

    def __str__(self) -> str:
        # Shows a simple human readable representation of the Awaitable Factory. Used in error messages.
        return f"Tensorlake Function.awaitable factory for {self._function}"

    def __reduce__(self):
        # This helps users to see that they made a coding mistake and returned a Tensorlake Function .awaitable
        # factory object from their function without calling it or passed it as a function call parameter (directly
        # or inside another object).
        # Note: this exception will be converted into SerializationError when pickling is attempted.
        raise SDKUsageError(
            f"Attempt to pickle a Tensorlake Function.awaitable factory for {self._function}. It cannot "
            "be passed as a function parameter or returned from a Tensorlake Function."
        )


# Non-public helper functions placed here for now to avoid circular imports.


def _function_name(original_function: Callable) -> str:
    """Returns function name of the given original (user) function."""
    # "{class}.{method}" for methods, otherwise just function name. Doesn't include module name.
    # All functions and classes in the application share a single namespace.
    # NB: this might not be a class if user passed something else to @cls decorator.
    return getattr(original_function, "__qualname__", "<unknown>")


def _is_application_function(fn: Function) -> bool:
    """Returns True if the function defines an application.

    Doesn't raise any exceptions. Works for invalid functions that fail validation.
    """
    return fn._application_config is not None
