from dataclasses import dataclass
from typing import Any, Callable, Dict, List

from .callable import TensorlakeCallable
from .exceptions import TensorlakeException
from .function_call import RegularFunctionCall
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


@dataclass
class _AsyncCallConfig:
    start_delay: int | None


_IMMEDIATE_ASYNC_CALL_CONFIG = _AsyncCallConfig(start_delay=None)

# (Function, _AsyncCallConfig, List args, Dict kwargs) -> Future
__runtime_hook_async_function_call = None
# (Function, List args, Dict kwargs) -> Any
__runtime_hook_sync_function_call = None


class Function(TensorlakeCallable):
    """Class that represents a Tensorlake Function configured by user.

    No validation is done at object creation time because Function objects
    are created at Python script loading time where it's not possible to provide
    a good UX. This is why all the validation is done separately."""

    def __init__(self, original_function: Callable):
        self._original_function: Callable = original_function
        self._function_config: _FunctionConfiguration | None = None
        self._application_config: _ApplicationConfiguration | None = None

    def run(self, *args, **kwargs) -> RegularFunctionCall:
        """Creates a function call that will be executed asynchronously as soon as possible."""
        return self._async_call(_IMMEDIATE_ASYNC_CALL_CONFIG, list(args), dict(kwargs))

    def run_later(self, start_delay: float, *args, **kwargs) -> RegularFunctionCall:
        """Creates a function call that will be executed asynchronously after at least start_delay seconds."""
        if start_delay < 0:
            raise ValueError("start_delay must be non-negative")
        return self._async_call(
            _AsyncCallConfig(start_delay=start_delay), list(args), dict(kwargs)
        )

    def __call__(self, *args, **kwargs) -> Any | RegularFunctionCall:
        # Called when the Function is called using () operator.
        #
        # TODO: implement the current_tensorlake_function_is_async() check
        # to decide whether to call _async_call or _sync_call.
        # This is only needed when we support async Tensorlake Functions.
        # if current_tensorlake_function_is_async():
        #     return self._async_call(_IMMEDIATE_ASYNC_CALL_CONFIG, list(args), dict(kwargs))
        # else:
        return self._sync_call(list(args), dict(kwargs))

    def __repr__(self) -> str:
        return (
            f"<Tensorlake Function(\n"
            f"  original_function={self._original_function!r},\n"
            f"  _function_config={self._function_config!r},\n"
            f"  _application_config={self._application_config!r}\n"
            f")>"
        )

    def _async_call(
        self,
        config: _AsyncCallConfig,
        args: List[Any],
        kwargs: Dict[str, Any],
    ) -> RegularFunctionCall:
        """Returns function call for the function."""
        if __runtime_hook_async_function_call is None:
            raise TensorlakeException(
                "Internal Error: No Tensorlake runtime hook is set for async function calls"
            )
        return __runtime_hook_async_function_call(self, config, args, kwargs)

    def _sync_call(self, args: List[Any], kwargs: Dict[str, Any]) -> Any:
        """Call the function synchronously."""
        if __runtime_hook_sync_function_call is None:
            raise TensorlakeException(
                "Internal Error: No Tensorlake runtime hook is set for sync function calls"
            )
        return __runtime_hook_sync_function_call(self, args, kwargs)

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
