from inspect import Parameter, Signature, signature
from typing import Callable, List, Literal, TypeVar

from tensorlake.functions_sdk.image import Image
from tensorlake.functions_sdk.retries import Retries

from ..registry import register_class, register_function
from .function import (
    Function,
    _APIConfiguration,
    _FunctionConfiguration,
    _ReducerConfiguration,
)


def api(
    description: str = "",
) -> Callable:
    def decorator(fn: Callable | Function) -> Function:
        if not isinstance(fn, Function):
            fn = Function(fn)

        fn: Function
        inject_ctx: bool = False
        if isinstance(fn._original_function, Callable):
            sig: Signature = signature(fn._original_function)
            parameters: List[Parameter] = list(sig.parameters.values())
            if len(parameters) > 0:
                first_parameter: Parameter = parameters[0]
                if first_parameter.name == "ctx":
                    inject_ctx = True

        fn._api_config = _APIConfiguration(
            description=description,
            inject_ctx=inject_ctx,
        )

        return fn

    return decorator


_DEFAULT_TIMEOUT_SEC: int = 300  # 5 minutes
# We need full CPU core to start Function Executor and load customer function quickly.
# Otherwise, starting up Function Executor without loading customer functions takes more
# than 5 seconds with 0.125 CPU.
_DEFAULT_CPU: float = 1.0
# nvidia-smi health checks consume up to 100 MB of memory.
# Function Executor itself currently consumes up to 75 MB of memory.
# So we need a large enough minimal memory limit to ensure stability while running customer
# functions that consume memory too.
_DEFAULT_MEMORY_GB: float = 1.0
_DEFAULT_EPHEMERAL_DISK_GB: float = 2.0  # 2 GB
_DEFAULT_GPU = None  # No GPU by default
_DEFAULT_MAX_CONCURRENCY = 1  # No concurrent threads running the function by default


def function(
    cpu: float = _DEFAULT_CPU,
    memory: float = _DEFAULT_MEMORY_GB,
    ephemeral_disk: float = _DEFAULT_EPHEMERAL_DISK_GB,
    gpu: None | str | List[str] = _DEFAULT_GPU,
    timeout: int = _DEFAULT_TIMEOUT_SEC,
    image: Image = Image(),
    secrets: List[str] = [],
    input_serializer: Literal["json", "cloudpickle"] = "json",
    output_serializer: Literal["json", "cloudpickle"] = "json",
    retries: Retries | None = None,
    region: str | None = None,
    cacheable: bool = False,
    max_concurrency: int = _DEFAULT_MAX_CONCURRENCY,
) -> Callable:
    """Decorator to register a function with the Tensorlake framework.

    When a function calls another function the called function input serializer is used.
    When a function produces a value its output serializer is used.
    """

    def decorator(fn: Callable | Function) -> Function:
        # If this function is a class method then the class does not exist yet.
        if not isinstance(fn, Function):
            fn = Function(fn)

        fn: Function
        fn._function_config = _FunctionConfiguration(
            # "{class}.{method}" for methods, otherwise just function name. Doesn't include module name.
            function_name=fn._original_function.__qualname__,
            image=image,
            secrets=secrets,
            input_serializer=input_serializer,
            output_serializer=output_serializer,
            retries=retries,
            timeout=timeout,
            cpu=cpu,
            memory=memory,
            ephemeral_disk=ephemeral_disk,
            gpu=gpu,
            region=region,
            cacheable=cacheable,
            max_concurrency=max_concurrency,
        )

        register_function(fn._function_config.function_name, fn)

        return fn

    return decorator


def reducer() -> Callable:
    def decorator(fn: Callable | Function) -> Function:
        if not isinstance(fn, Function):
            fn = Function(fn)

        fn: Function
        # Not non reducer
        fn._reducer_config = _ReducerConfiguration()

        return fn

    return decorator


def cls() -> Callable:
    CLASS = TypeVar("CLASS")

    def decorator(original_class: CLASS) -> CLASS:
        original_class.__tensorlake_original_init__ = original_class.__init__
        # Doesn't include module name. This is good because all Tensorlake functions and classes share a single namespace.
        original_class.__tensorlake_name__ = original_class.__qualname__

        def __tensorlake_empty_init__(self):
            # Don't do anything in this constructor when the class methods are called CLASS().method(...).
            pass

        original_class.__init__ = __tensorlake_empty_init__

        register_class(original_class.__tensorlake_name__, original_class)

        return original_class

    return decorator
