import inspect
import os.path
from typing import Callable, Dict, List, Literal, TypeVar

from tensorlake.vendor.nanoid import generate as nanoid

from ..registry import (
    get_class,
    get_function,
    has_class,
    has_function,
    register_class,
    register_function,
)
from .exceptions import ApplicationValidationError
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
    region: Literal["us-east-1", "eu-west-1"] | None = None,
    input_deserializer: Literal["json", "pickle"] = "json",
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
            input_deserializer=input_deserializer,
            output_serializer=output_serializer,
            # Use a unique random version. We don't provide user controlled versioning at the moment.
            # Use only alphanumeric characters so app version can be used as container tags.
            version=nanoid(
                alphabet="0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
            ),
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
    region: Literal["us-east-1", "eu-west-1"] | None = None,
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
            # Hidden from users because not implemented in Server yet.
            cacheable=False,
            # Hidden from users because not implemented in Telemetry yet.
            max_concurrency=_DEFAULT_MAX_CONCURRENCY,
        )

        if has_function(fn._function_config.function_name):
            existing_fn: Function = get_function(fn._function_config.function_name)
            existing_fn_file_path: str | None = inspect.getsourcefile(
                existing_fn._original_function
            )
            fn_file_path: str | None = inspect.getsourcefile(fn._original_function)
            if existing_fn_file_path is not None:
                existing_fn_file_path = os.path.abspath(existing_fn_file_path)
            if fn_file_path is not None:
                fn_file_path = os.path.abspath(fn_file_path)
            # Allow re-registering the same function from the same file.
            # This is needed because pickle.loads imports __main__ module
            # second time but with its real name (i.e. real_name.py) when unpickling
            # classes stored in function call and application entrypoint metadata.
            # So two modules exist for real_name.py in sys.modules:
            # * __main__
            # * real_name
            # Another legitimate use case if when user redefines the function
            # in the same file. This is a valid Python code.
            if (
                existing_fn_file_path != fn_file_path
                or existing_fn_file_path is None
                or fn_file_path is None
            ):
                raise ApplicationValidationError(
                    f"Function '{fn._function_config.function_name}' already exists. "
                    f"First defined in {existing_fn_file_path}, "
                    f"redefined in {fn_file_path}. "
                    "Please rename one of the functions."
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

        # TODO: Update the existing class in case new definition is loaded.
        if has_class(class_name):
            existing_cls: CLASS = get_class(class_name)
            existing_cls_file_path: str | None = inspect.getsourcefile(existing_cls)
            cls_file_path: str | None = inspect.getsourcefile(original_class)
            if existing_cls_file_path is not None:
                existing_cls_file_path = os.path.abspath(existing_cls_file_path)
            if cls_file_path is not None:
                cls_file_path = os.path.abspath(cls_file_path)
            # Allow re-registering the same class from the same file.
            # This is needed because pickle.loads imports __main__ module
            # second time but with its real name (i.e. real_name.py) when unpickling
            # classes stored in function call and application entrypoint metadata.
            # So two modules exist for real_name.py in sys.modules:
            # * __main__
            # * real_name
            # Another legitimate use case if when user redefines the class
            # in the same file. This is a valid Python code.
            if (
                existing_cls_file_path != cls_file_path
                or existing_cls_file_path is None
                or cls_file_path is None
            ):
                raise ApplicationValidationError(
                    f"Class '{class_name}' already exists. "
                    f"First defined in {existing_cls_file_path}, "
                    f"redefined in {cls_file_path}. "
                    "Please rename one of the classes."
                )

        def __tensorlake_empty_init__(self):
            # Don't do anything in this constructor when the class methods are called CLASS().method(...).
            pass

        original_class.__tensorlake_original_init__ = original_class.__init__
        original_class.__tensorlake_name__ = class_name
        original_class.__init__ = __tensorlake_empty_init__

        register_class(class_name, original_class)

        for attr_name in dir(original_class):
            attr = getattr(original_class, attr_name)
            if isinstance(attr, Function):
                attr._function_config.class_name = class_name
                attr._function_config.class_method_name = attr_name
                attr._function_config.class_init_timeout = init_timeout

        return original_class

    return decorator
