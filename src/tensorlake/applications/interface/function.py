import inspect
import types
from collections.abc import Coroutine
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List

from .exceptions import SDKUsageError
from .futures import (
    FunctionCallFuture,
    Future,
    ListFuture,
    ReduceOperationFuture,
    _InitialMissing,
    _InitialMissingType,
    _make_map_operation_future,
    _make_reduce_operation_future,
    _request_scoped_id,
    _wrap_future_into_coroutine,
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
    warm_containers: int | None
    min_containers: int | None
    max_containers: int | None


@dataclass
class _ApplicationConfiguration:
    tags: Dict[str, str]
    retries: Retries
    region: str | None
    version: str


class Function:
    """Class that represents a Tensorlake Function configured by user.

    No validation is done at object creation time because Function objects
    are created at Python script loading time where it's not possible to provide
    a good UX. This is why all the validation is done separately i.e. on deployment."""

    @property
    def __class__(self):
        # Make isinstance(self, types.FunctionType) return True so that
        # inspect.isfunction() and inspect.iscoroutinefunction() work on Function
        # instances. This is needed for other frameworks (e.g. agentic frameworks)
        # to detect async Tensorlake Functions via inspect.iscoroutinefunction().
        # Python's isinstance() checks type(obj) first (still Function), then falls
        # back to obj.__class__. So isinstance(self, Function) continues to work.
        return types.FunctionType

    def __init__(self, original_function: Callable):
        self._original_function: Callable = original_function
        self._function_config: _FunctionConfiguration | None = None
        self._application_config: _ApplicationConfiguration | None = None
        self._future_factory: FunctionFutureFactory = FunctionFutureFactory(self)
        # Mimic original function if it's a regular user defined function.
        if inspect.isfunction(self._original_function):
            # Copy original function metadata into this Function object so function inspection
            # tools like these used by Agentic frameworks to generate tool descriptions work.
            # See the attributes at "function" at
            # https://docs.python.org/3/library/inspect.html#types-and-members and
            # https://docs.python.org/3/reference/datamodel.html#special-read-only-attributes.
            for mimic_attr in [
                "__annotate__",
                "__doc__",
                "__name__",
                "__qualname__",
                "__code__",
                "__closure__",
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

    def __call__(self, *args, **kwargs) -> Any | Coroutine[Any, Any, Any]:
        """Calls the function and returns its result if sync function. Returns a coroutine otherwise.

        If the function is async function then returns its coroutine and doesn't block.
        The coroutine will only be executed if awaited or if started by user using i.e. asyncio.create_task().
        Otherwise, blocks until the result is ready and returns it.

        Raises RequestError if the function raised RequestError.
        Raises FunctionError if the function failed.
        Raises TensorlakeError on other errors.
        """
        future: FunctionCallFuture = self.future(*args, **kwargs)
        if inspect.iscoroutinefunction(self):
            return _wrap_future_into_coroutine(future)
        else:
            future.run()
            return future.result()

    def map(
        self, items: Iterable[Any | Future] | ListFuture
    ) -> Iterable[Any] | Coroutine[Any, Any, Any]:
        """Returns an iterable with every item transformed using the function if sync function. Returns a coroutine otherwise.

        If the function is async function then returns its coroutine and doesn't block.
        The coroutine will only be executed if awaited or if started by user using i.e. asyncio.create_task().
        Otherwise, blocks until the result is ready and returns an iterable of results.
        Similar to https://docs.python.org/3/library/functions.html#map except all transformations
        are done in parallel.

        Raises RequestError if the function raised RequestError.
        Raises FunctionError if the function failed.
        Raises TensorlakeError on other errors.
        """
        future: ListFuture = self.future.map(items)
        if inspect.iscoroutinefunction(self):
            return _wrap_future_into_coroutine(future)
        else:
            future.run()
            return future.result()

    def reduce(
        self,
        items: Iterable[Any | Future] | ListFuture,
        initial: Any | Future | _InitialMissingType = _InitialMissing,
        /,
    ) -> Any | Coroutine[Any, Any, Any]:
        """Performs a reduce operation using the supplied function over the supplied items and returns result if sync function. Returns a coroutine otherwise.

        If the function is async function then returns its coroutine and doesn't block.
        The coroutine will only be executed if awaited or if started by user using i.e. asyncio.create_task().
        Otherwise, blocks until the result is ready.
        Similar to https://docs.python.org/3/library/functools.html#functools.reduce.

        Raises RequestError if the function raised RequestError.
        Raises FunctionError if the function failed.
        Raises TensorlakeError on other errors.
        """
        future: ReduceOperationFuture = self.future.reduce(items, initial)
        if inspect.iscoroutinefunction(self):
            return _wrap_future_into_coroutine(future)
        else:
            future.run()
            return future.result()

    @property
    def future(self) -> "FunctionFutureFactory":
        """Returns function factory for creating futures."""
        return self._future_factory

    def _make_function_call_future(
        self, args: list[Any | Future], kwargs: dict[str, Any | Future]
    ) -> FunctionCallFuture:
        return FunctionCallFuture(
            id=_request_scoped_id(),
            function_name=self._function_config.function_name,
            args=args,
            kwargs=kwargs,
        )

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
        # Works for invalid/inconsistent functions that fail validation.
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

    @property
    def _name(self) -> str:
        """Returns function name of the Tensorlake Function in its Tensorlake Application.

        This is a private getter not exposed in SDK interface to users.
        """
        if self._function_config is not None:
            return self._function_config.function_name
        else:
            return _function_name(self._original_function)


class FunctionFutureFactory:
    """Factory for creating futures for a specific Tensorlake Function.

    This class is returned by Function.future property.
    """

    def __init__(self, function: Function):
        self._function: Function = function

    def __call__(self, *args, **kwargs) -> Future:
        """Returns a Future that represents a call of the function.

        Raises TensorlakeError on error.
        """
        return self._function._make_function_call_future(list(args), dict(kwargs))

    def map(self, items: Iterable[Any | Future] | ListFuture) -> Future:
        """Returns a Future that represents mapping of the function over the iterable.

        Raises TensorlakeError on error.
        """
        return _make_map_operation_future(
            function_name=self._function._function_config.function_name,
            items=items,
        )

    def reduce(
        self,
        items: Iterable[Any | Future] | ListFuture,
        initial: Any | Future | _InitialMissingType = _InitialMissing,
        /,
    ) -> Future:
        """Returns a Future that represents reducing the iterable using the function.

        Raises TensorlakeError on error.
        """
        return _make_reduce_operation_future(
            function_name=self._function._function_config.function_name,
            items=items,
            initial=initial,
        )

    def __repr__(self) -> str:
        # Shows exact structure of the Future Factory. Used for debug logging.
        return f"<FunctionFutureFactory function={self._function!r}>"

    def __str__(self) -> str:
        # Shows a simple human readable representation of the Future Factory. Used in error messages.
        return f"Tensorlake Function.future factory for {self._function}"

    def __reduce__(self):
        # This helps users to see that they made a coding mistake and returned a Tensorlake Function .future
        # factory object from their function without calling it or passed it as a function call parameter (directly
        # or inside another object).
        # Note: this exception will be converted into SerializationError when pickling is attempted.
        raise SDKUsageError(
            f"Attempt to pickle a Tensorlake Function.future factory for {self._function}. It cannot "
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
