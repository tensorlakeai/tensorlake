from typing import Any, Callable, Dict, List, Literal, TypeVar

from tensorlake.vendor.nanoid import generate as nanoid

from ..registry import (
    register_class,
    register_decorator,
    register_function,
)
from .function import (
    Function,
    _ApplicationConfiguration,
    _function_name,
    _FunctionConfiguration,
)
from .image import Image
from .retries import Retries


class _Decorator:
    def __init__(self):
        self._is_called: bool = False
        # None for class decorator.
        self._function: Function | _Decorator | None = None
        register_decorator(self)

    @property
    def is_called(self) -> bool:
        return self._is_called

    @property
    def function(self) -> "Function | _Decorator | None":
        return self._function

    def create_function(
        self, fn: "_Decorator" | Callable | Function
    ) -> "Function | _Decorator":
        self._is_called = True

        if isinstance(fn, _Decorator):
            # User code didn't add () to @decorator.
            # Return the decorator itself so we can fail validation later.
            self._function = fn
        elif isinstance(fn, Function):
            self._function = fn
        else:
            fn = Function(fn)
            register_function(_function_name(fn._original_function), fn)
            self._function = fn

        return self._function


class _ApplicationDecorator(_Decorator):
    def __init__(
        self,
        tags: Dict[str, str],
        retries: Retries,
        region: str | None,
        input_deserializer: str,
        output_serializer: str,
    ):
        super().__init__()
        self._tags: Dict[str, str] = tags
        self._retries: Retries = retries
        self._region: str | None = region
        self._input_deserializer: str = input_deserializer
        self._output_serializer: str = output_serializer

    def __call__(self, fn: _Decorator | Callable | Function) -> Function | _Decorator:
        fn: Function | _Decorator = self.create_function(fn)
        if isinstance(fn, _Decorator):
            # No () added to @decorator call, shortcut here, this simplifies validation code.
            return fn

        fn._application_config = _ApplicationConfiguration(
            tags=self._tags,
            retries=self._retries,
            region=self._region,
            input_deserializer=self._input_deserializer,
            output_serializer=self._output_serializer,
            # Use a unique random version. We don't provide user controlled versioning at the moment.
            # Use only alphanumeric characters so app version can be used as container tags.
            version=nanoid(
                alphabet="0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
            ),
        )

        return fn


def application(
    tags: Dict[str, str] = {},
    retries: Retries = Retries(),
    region: Literal["us-east-1", "eu-west-1"] | None = None,
    input_deserializer: Literal["json", "pickle"] = "json",
    output_serializer: Literal["json", "pickle"] = "json",
) -> _ApplicationDecorator:
    return _ApplicationDecorator(
        # NB: the first argument here is used during pre-deployment validation.
        tags=tags,
        retries=retries,
        region=region,
        input_deserializer=input_deserializer,
        output_serializer=output_serializer,
    )


class _FunctionDecorator(_Decorator):
    def __init__(
        self,
        description: str,
        cpu: float,
        memory: float,
        ephemeral_disk: float,
        gpu: None | str | List[str],
        timeout: int,
        image: Image,
        secrets: List[str],
        retries: Retries | None,
        region: str | None,
    ):
        super().__init__()
        self._description: str = description
        self._cpu: float = cpu
        self._memory: float = memory
        self._ephemeral_disk: float = ephemeral_disk
        self._gpu: None | str | List[str] = gpu
        self._timeout: int = timeout
        self._image: Image = image
        self._secrets: List[str] = secrets
        self._retries: Retries | None = retries
        self._region: str | None = region

    def __call__(self, fn: _Decorator | Callable | Function) -> Function | _Decorator:
        fn: Function | _Decorator = self.create_function(fn)
        if isinstance(fn, _Decorator):
            # No () added to @decorator call, shortcut here, this simplifies validation code.
            return fn

        fn._function_config = _FunctionConfiguration(
            # set by cls() decorator if this is a class method
            class_name=None,
            class_method_name=None,
            class_init_timeout=None,
            function_name=_function_name(fn._original_function),
            description=self._description,
            image=self._image,
            secrets=self._secrets,
            retries=self._retries,
            timeout=self._timeout,
            cpu=self._cpu,
            memory=self._memory,
            ephemeral_disk=self._ephemeral_disk,
            gpu=self._gpu,
            region=self._region,
            # Hidden from users because not implemented in Server yet.
            cacheable=False,
            # Hidden from users because not implemented in Telemetry yet.
            max_concurrency=_DEFAULT_MAX_CONCURRENCY,
        )

        return fn


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
) -> _FunctionDecorator:
    """Decorator to register a function with the Tensorlake framework.

    When a function calls another function the called function input serializer is used.
    When a function produces a value its output serializer is used.
    """
    return _FunctionDecorator(
        # NB: the first argument here is used during pre-deployment validation.
        description=description,
        cpu=cpu,
        memory=memory,
        ephemeral_disk=ephemeral_disk,
        gpu=gpu,
        timeout=timeout,
        image=image,
        secrets=secrets,
        retries=retries,
        region=region,
    )


def _class_name(cls: Any) -> str:
    # Doesn't include module name. This is good because all Tensorlake functions and classes share a single namespace.
    # NB: this might not be a class if user passed something else to @cls decorator.
    return getattr(cls, "__qualname__", "<unknown>")


def __tensorlake_empty_class_instance_init__(self):
    # Don't do anything in this constructor when the class methods are called using CLASS().method(...).
    pass


class _ClassDecorator(_Decorator):
    CLASS = TypeVar("CLASS")

    def __init__(
        self,
        init_timeout: int | None,
    ):
        super().__init__()
        self._init_timeout: int | None = init_timeout
        self._original_class: Any | None = None

    @property
    def original_class(self) -> Any | None:
        return self._original_class

    def __call__(self, original_class: CLASS) -> CLASS:
        self._is_called = True
        self._original_class = original_class

        original_class.__tensorlake_original_init__ = original_class.__init__
        original_class.__tensorlake_name__ = _class_name(original_class)
        original_class.__init__ = __tensorlake_empty_class_instance_init__

        register_class(_class_name(original_class), original_class)

        for attr_name in dir(original_class):
            attr = getattr(original_class, attr_name)
            if isinstance(attr, Function):
                attr._function_config.class_name = _class_name(original_class)
                attr._function_config.class_method_name = attr_name
                attr._function_config.class_init_timeout = self._init_timeout

        return original_class


def cls(
    init_timeout: int | None = None,
) -> _ClassDecorator:
    return _ClassDecorator(
        # NB: the first argument here is used during pre-deployment validation.
        init_timeout=init_timeout,
    )
