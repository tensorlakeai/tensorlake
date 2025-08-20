from dataclasses import dataclass
from typing import Any, Callable, Dict, List

from .function_call import FunctionCall
from .image import Image
from .request_context import RequestContext, RequestContextPlaceholder
from .retries import Retries


@dataclass
class _FunctionConfiguration:
    # None for non-method functions, only available after all modules are loaded
    # because class objects are created after their methods.
    class_name: str | None
    class_method_name: str | None
    function_name: str
    description: str
    image: Image
    secrets: List[str]
    input_serializer: str
    output_serializer: str
    retries: Retries | None  # Uses API retry policy if None
    timeout: int
    cpu: float
    memory: float
    ephemeral_disk: float
    gpu: None | str | List[str]
    region: str | None
    cacheable: bool
    max_concurrency: int


@dataclass
class _ReducerConfiguration:
    pass  # Nothing here so far.


@dataclass
class _APIConfiguration:
    pass  # Nothing here so far.


class Function:
    """Class that represents a Tensorlake Function configured by user.

    No validation is done at object creation time because Function objects
    are created at Python script loading time where it's not possible to provide
    a good UX. This is why all the validation is done separately."""

    def __init__(self, original_function: Callable):
        self._original_function: Callable = original_function
        self._function_config: _FunctionConfiguration | None = None
        self._reducer_config: _ReducerConfiguration | None = None
        self._api_config: _APIConfiguration | None = None

    @property
    def original_function(self) -> Callable:
        return self._original_function

    @property
    def function_config(self) -> _FunctionConfiguration | None:
        return self._function_config

    @property
    def reducer_config(self) -> _ReducerConfiguration | None:
        return self._reducer_config

    @property
    def api_config(self) -> _APIConfiguration | None:
        return self._api_config

    def __repr__(self) -> str:
        return (
            f"<Tensorlake Function(\n"
            f"  original_function={self._original_function!r},\n"
            f"  _function_config={self._function_config!r},\n"
            f"  _reducer_config={self._reducer_config!r},\n"
            f"  _api_config={self._api_config!r}\n"
            f")>"
        )

    def _call(
        self,
        instance: Any | None,
        cls: Any | None,
        args: List[Any],
        kwargs: Dict[str, Any],
    ) -> FunctionCall:
        """Return function call for the function.

        instance and cls are not None if the function was called as instance.method where cls is the class of instance.
        """
        for key, value in kwargs.items():
            if isinstance(value, RequestContext) or isinstance(
                value, RequestContextPlaceholder
            ):
                kwargs[key] = RequestContextPlaceholder()
        for i, arg in enumerate(args):
            if isinstance(arg, RequestContext) or isinstance(
                arg, RequestContextPlaceholder
            ):
                args[i] = RequestContextPlaceholder()

        # TODO: Fail with RequestError if cls.__tensorlake_name__ is not set.
        # This means the @tensorlake.cls decorator wasn't called on the class.

        return FunctionCall(
            class_name=cls.__tensorlake_name__ if cls is not None else None,
            function_name=self._function_config.function_name,
            args=args,
            kwargs=kwargs,
        )

    def __get__(self, instance: Any | None, cls: Any) -> Callable:
        # Called when the Function is called as an `instance` method of class `cls`.
        def bound_call(*args, **kwargs) -> FunctionCall:
            return self._call(instance, cls, list(args), dict(kwargs))

        return bound_call

    def __call__(self, *args, **kwargs) -> FunctionCall:
        # Called when the Function is called as a regular function.
        return self._call(None, None, list(args), dict(kwargs))
