from typing import Callable, Dict, List, Literal, TypeVar

from tensorlake.vendor.nanoid import generate as nanoid

from ..registry import register_class, register_function
from .function import (
    Function,
    _ApplicationConfiguration,
    _FunctionConfiguration,
)
from .image import Image
from .retries import Retries


def application(
    tags: Dict[str, str] = {},
    retries: Retries = Retries(),
    region: str | None = None,
    input_serializer: Literal["json", "pickle"] = "json",
    output_serializer: Literal["json", "pickle"] = "json",
) -> Callable:
    def decorator(fn: Callable | Function) -> Function:
        if not isinstance(fn, Function):
            fn = Function(fn)

        fn: Function
        fn._application_config = _ApplicationConfiguration(
            tags=tags,
            retries=retries,
            region=region,
            input_serializer=input_serializer,
            output_serializer=output_serializer,
            # Use a unique random version. We don't provide user controlled versioning at the moment.
            version=nanoid(),
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
    description: str = "",
    cpu: float = _DEFAULT_CPU,
    memory: float = _DEFAULT_MEMORY_GB,
    ephemeral_disk: float = _DEFAULT_EPHEMERAL_DISK_GB,
    gpu: None | str | List[str] = _DEFAULT_GPU,
    timeout: int = _DEFAULT_TIMEOUT_SEC,
    image: Image = Image(),
    secrets: List[str] = [],
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
            # set by cls() decorator if this is a class method
            class_name=None,
            class_method_name=None,
            class_init_timeout=None,
            # "{class}.{method}" for methods, otherwise just function name. Doesn't include module name.
            # All functions and classes in the application share a single namespace.
            function_name=fn._original_function.__qualname__,
            description=description,
            image=image,
            secrets=secrets,
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


def cls(
    init_timeout: int | None = None,
) -> Callable:
    CLASS = TypeVar("CLASS")

    def decorator(original_class: CLASS) -> CLASS:
        # Doesn't include module name. This is good because all Tensorlake functions and classes share a single namespace.
        class_name: str = original_class.__qualname__
        original_class.__tensorlake_original_init__ = original_class.__init__
        original_class.__tensorlake_name__ = class_name

        def __tensorlake_empty_init__(self):
            # Don't do anything in this constructor when the class methods are called CLASS().method(...).
            pass

        original_class.__init__ = __tensorlake_empty_init__

        register_class(class_name, original_class)

        for attr_name in dir(original_class):
            attr = getattr(original_class, attr_name)
            if isinstance(attr, Function):
                attr.function_config.class_name = class_name
                attr.function_config.class_method_name = attr_name
                attr.function_config.class_init_timeout = init_timeout

        return original_class

    return decorator
