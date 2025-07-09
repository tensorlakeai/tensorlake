import inspect
from dataclasses import dataclass
from inspect import Parameter
from typing import (
    Any,
    Dict,
    Generic,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    get_args,
    get_origin,
)

from pydantic import BaseModel
from typing_extensions import get_type_hints

from .data_objects import Metrics, TensorlakeData
from .image import Image
from .invocation_state.invocation_state import InvocationState
from .object_serializer import get_serializer
from .retries import Retries


class GraphInvocationContext:
    def __init__(
        self,
        invocation_id: str,
        graph_name: str,
        graph_version: str,
        invocation_state: InvocationState,
    ):
        self.invocation_id = invocation_id
        self.graph_name = graph_name
        self.graph_version = graph_version
        self.invocation_state = invocation_state


def is_pydantic_model_from_annotation(type_annotation):
    # If it's a string representation
    if isinstance(type_annotation, str):
        # Extract the class name from the string
        class_name = type_annotation.split("'")[-2].split(".")[-1]
        # This part is tricky and might require additional context or imports
        # You might need to import the actual class or module where it's defined
        # For example:
        # from indexify.functions_sdk.data_objects import File
        # return issubclass(eval(class_name), BaseModel)
        return False  # Default to False if we can't evaluate

    # If it's a Type object
    origin = get_origin(type_annotation)
    if origin is not None:
        # Handle generic types like List[File], Optional[File], etc.
        args = get_args(type_annotation)
        if args:
            return is_pydantic_model_from_annotation(args[0])

    # If it's a direct class reference
    if isinstance(type_annotation, type):
        return issubclass(type_annotation, BaseModel)

    return False


_DEFAULT_TIMEOUT: int = 300  # 5 minutes
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


class TensorlakeCompute:
    """Base class for functions.

    Args:
        cpu (float): The number of CPUs available to the function. 1.0 means 1 CPU, 0.5 means half a CPU, 2 means 2 CPUs.
        memory (float): The amount of memory available to the function in GB. 0.5 means 512 MB, 1.0 means 1 GB, etc.
        ephemeral_disk (float): The amount of ephemeral disk space available to the function in GB.
        gpu (Optional[str]): GPU(s) available to the function. No GPU is allocated by default.
                             The value should be a string "GPU_MODEL:COUNT" representing the GPU model and the number of GPUs.
                             See supported GPU models and counts in Tensorlake Cloud documentation.
        cacheable (bool): Declares that applications of this function are cacheable.
                          A function should only be marked cacheable if its outputs are a
                          pure function of its inputs.
    """

    name: str = ""
    description: str = ""
    image: Image = Image()
    secrets: Optional[List[str]] = None
    accumulate: Optional[Type[Any]] = None
    input_encoder: Optional[str] = "cloudpickle"
    output_encoder: Optional[str] = "cloudpickle"
    inject_ctx = False
    retries: Optional[Retries] = None  # Use graph retry policy if not set
    timeout: int = _DEFAULT_TIMEOUT
    cpu: float = _DEFAULT_CPU
    memory: float = _DEFAULT_MEMORY_GB
    ephemeral_disk: float = _DEFAULT_EPHEMERAL_DISK_GB
    gpu: Optional[Union[str, List[str]]] = _DEFAULT_GPU
    next: Optional[Union["TensorlakeCompute", List["TensorlakeCompute"]]] = None
    cacheable: bool = False

    def run(self, *args, **kwargs) -> Union[List[Any], Any]:
        pass

    _created_by_decorator: bool = (
        False  # True if class was created using @tensorlake_function
    )

    def _call_run(self, *args, **kwargs) -> Union[List[Any], Any]:
        # Process dictionary argument mapping it to args or to kwargs.
        if self.accumulate and len(args) == 2 and isinstance(args[1], dict):
            sig = inspect.signature(self.run)
            new_args = [args[0]]  # Keep the accumulate argument
            dict_arg = args[1]
            new_args_from_dict, new_kwargs = _process_dict_arg(dict_arg, sig)
            new_args.extend(new_args_from_dict)
            return self.run(*new_args, **new_kwargs)
        elif len(args) == 1 and isinstance(args[0], dict):
            sig = inspect.signature(self.run)
            dict_arg = args[0]
            new_args, new_kwargs = _process_dict_arg(dict_arg, sig)
            return self.run(*new_args, **new_kwargs)

        return self.run(*args, **kwargs)

    @classmethod
    def deserialize_output(cls, output: TensorlakeData) -> Any:
        serializer = get_serializer(cls.output_encoder)
        return serializer.deserialize(output.payload)


def _process_dict_arg(dict_arg: dict, sig: inspect.Signature) -> Tuple[list, dict]:
    new_args = []
    new_kwargs = {}
    remaining_kwargs = dict_arg.copy()

    # Match dictionary keys to function parameters
    for param_name, param in sig.parameters.items():
        if param_name in dict_arg:
            new_args.append(dict_arg[param_name])
            remaining_kwargs.pop(param_name, None)

    if any(v.kind == Parameter.VAR_KEYWORD for v in sig.parameters.values()):
        # Combine remaining dict items with additional kwargs
        new_kwargs.update(remaining_kwargs)
    elif len(remaining_kwargs) > 0:
        # If there are remaining kwargs, add them as a single dict argument
        new_args.append(remaining_kwargs)

    return new_args, new_kwargs


def tensorlake_function(
    name: Optional[str] = None,
    description: Optional[str] = "",
    image: Image = Image(),
    accumulate: Optional[Type[BaseModel]] = None,
    input_encoder: Optional[str] = "cloudpickle",
    output_encoder: Optional[str] = "cloudpickle",
    secrets: Optional[List[str]] = None,
    inject_ctx: Optional[bool] = False,
    retries: Optional[Retries] = None,  # Use graph retry policy if not set
    timeout: int = _DEFAULT_TIMEOUT,
    cpu: float = _DEFAULT_CPU,
    memory: float = _DEFAULT_MEMORY_GB,
    ephemeral_disk: float = _DEFAULT_EPHEMERAL_DISK_GB,
    gpu: Optional[Union[str, List[str]]] = _DEFAULT_GPU,
    next: Optional[Union["TensorlakeCompute", List["TensorlakeCompute"]]] = None,
    cacheable: bool = False,
):
    def construct(fn):
        attrs = {
            "_created_by_decorator": True,
            "name": name if name else fn.__name__,
            "description": (
                description
                if description
                else (fn.__doc__ or "").strip().replace("\n", "")
            ),
            "image": image,
            "accumulate": accumulate,
            "input_encoder": input_encoder,
            "output_encoder": output_encoder,
            "secrets": secrets,
            "inject_ctx": inject_ctx,
            "retries": retries,
            "timeout": timeout,
            "cpu": cpu,
            "memory": memory,
            "ephemeral_disk": ephemeral_disk,
            "gpu": gpu,
            "cacheable": cacheable,
            "run": staticmethod(fn),
            "next": next,
        }

        return type("TensorlakeCompute", (TensorlakeCompute,), attrs)

    return construct


@dataclass
class FunctionCallResult:
    ser_outputs: List[TensorlakeData]
    metrics: Metrics
    edges: Optional[List[str]]  # None means use graph routing
    exception: Optional[Exception]


V = TypeVar("V")
N = TypeVar("N", bound=TensorlakeCompute)


@dataclass
class RouteTo(Generic[V, N]):
    """Describes a routing of data values to downstream functions.

    RouteTo is returned by compute functions that require non-trivial
    output value routing.  It's constructed with a value (as
    ordinarily returned by a compute function), together with a list
    of the downsteam functions that should receive that value.

    NB: Each downstream function supplied to a RouteTo must be listed
    in the compute function's @tensorlake_function decorator's "next"
    argument.

    For example:

        @tensorlake_function()
        def handle_even(x: int) -> int:
            # Do something with even values of x
            return x

        @tensorlake_function()
        def handle_odd(x: int) -> int:
            # Do something with odd values of x
            return x

        @tensorlake_function(next=[handle_even, handle_odd])
        def pass_value_to_some_function(x: int) -> RouteTo[
            int, Union[handle_even, handle_odd]
        ]:
            if x % 2 == 0:
                return RouteTo(x, handle_even)
            return RouteTo(x, handle_odd)
    """

    value: V
    edges: List[N]


class TensorlakeFunctionWrapper:
    def __init__(
        self,
        indexify_function: TensorlakeCompute,
    ):
        self.indexify_function: TensorlakeCompute = indexify_function()

    def get_output_model(self) -> Any:
        if not isinstance(self.indexify_function, TensorlakeCompute):
            raise TypeError("Input must be an instance of TensorlakeCompute")

        extract_method = self.indexify_function.run
        type_hints = get_type_hints(extract_method)
        return_type = type_hints.get("return", Any)
        if get_origin(return_type) is RouteTo:
            return_type = get_args(return_type)[0]
        if get_origin(return_type) is list:
            return_type = get_args(return_type)[0]
        elif get_origin(return_type) is Union:
            inner_types = get_args(return_type)
            if len(inner_types) == 2 and type(None) in inner_types:
                return_type = (
                    inner_types[0] if inner_types[1] is type(None) else inner_types[1]
                )
        return return_type

    def get_input_types(self) -> Dict[str, Any]:
        if not isinstance(self.indexify_function, TensorlakeCompute):
            raise TypeError("Input must be an instance of TensorlakeCompute")

        extract_method = self.indexify_function.run
        type_hints = get_type_hints(extract_method)
        return {
            k: v
            for k, v in type_hints.items()
            if k != "return" and not is_pydantic_model_from_annotation(v)
        }

    def _run_fn(
        self,
        ctx: GraphInvocationContext,
        input: Union[Dict, Type[BaseModel], List, Tuple],
        acc: Optional[Type[Any]] = None,
    ) -> Tuple[List[Any], Optional[Exception], Optional[List[str]]]:
        """Invokes the wrapped function.

        Returns a tuple of results, containing:
            The function output
            The exception traceback if there's an exception, else None
            The router edges produced by the function if any, else None
        """

        args = []
        kwargs = {}

        if acc is not None:
            args.append(acc)

        # tuple and list are considered positional arguments, list is used for compatibility
        # with json encoding which won't deserialize in tuple.
        if isinstance(input, tuple) or isinstance(input, list):
            args += input
        elif isinstance(input, dict):
            kwargs.update(input)
        else:
            args.append(input)

        edges = self.indexify_function.next

        if self.indexify_function.inject_ctx:
            args.insert(0, ctx)
        try:
            extracted_data = self.indexify_function._call_run(*args, **kwargs)
            if isinstance(extracted_data, RouteTo):
                edges = extracted_data.edges
                extracted_data = extracted_data.value
        except Exception as e:
            return [], e, None
        if extracted_data is None:
            return [], None, edges

        output = (
            extracted_data if isinstance(extracted_data, list) else [extracted_data]
        )

        if edges is None:
            routes = None
        elif isinstance(edges, list):
            routes = [edge.name for edge in edges]
        else:
            routes = [edges.name]

        return output, None, routes

    def invoke_fn_ser(
        self,
        ctx: GraphInvocationContext,
        input: TensorlakeData,
        acc: Optional[Any] = None,
    ) -> FunctionCallResult:
        input = self.deserialize_input(input)
        input_serializer = get_serializer(self.indexify_function.input_encoder)
        output_serializer = get_serializer(self.indexify_function.output_encoder)
        if acc is not None:
            acc = input_serializer.deserialize(acc.payload)
        if acc is None and self.indexify_function.accumulate is not None:
            acc = self.indexify_function.accumulate()
        outputs, exception, edges = self._run_fn(ctx, input, acc=acc)

        metrics = Metrics(
            timers=ctx.invocation_state.timers,
            counters=ctx.invocation_state.counters,
        )

        ser_outputs = [
            TensorlakeData(
                payload=output_serializer.serialize(output),
                encoder=self.indexify_function.output_encoder,
            )
            for output in outputs
        ]
        return FunctionCallResult(
            ser_outputs=ser_outputs,
            metrics=metrics,
            edges=edges,
            exception=exception,
        )

    def deserialize_input(self, indexify_data: TensorlakeData) -> Any:
        serializer = get_serializer(self.indexify_function.input_encoder)
        return serializer.deserialize(indexify_data.payload)
