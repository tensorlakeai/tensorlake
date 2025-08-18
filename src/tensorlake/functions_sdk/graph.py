import importlib
import inspect
import re
import sys
import time
import traceback
from collections import defaultdict
from queue import deque
from typing import (
    Annotated,
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    Union,
    get_args,
    get_origin,
)

from pydantic import BaseModel
from typing_extensions import get_args, get_origin, get_type_hints

from tensorlake.vendor.nanoid import generate as nanoid

from .data_objects import Metrics, TensorlakeData
from .exceptions import RequestException
from .functions import (
    FunctionCallResult,
    GraphRequestContext,
    TensorlakeCompute,
    TensorlakeFunctionWrapper,
)
from .graph_definition import (
    ComputeGraphMetadata,
    FunctionMetadata,
    ParameterMetadata,
    PlacementConstraints,
    RetryPolicyMetadata,
    RuntimeInformation,
)
from .graph_validation import validate_node
from .invocation_state.local_invocation_state import LocalInvocationState
from .object_serializer import get_serializer
from .resources import resource_metadata_for_graph_node
from .retries import Retries

GraphNode = Annotated[TensorlakeFunctionWrapper, "GraphNode"]


def is_pydantic_model_from_annotation(type_annotation):
    if isinstance(type_annotation, str):
        class_name = type_annotation.split("'")[-2].split(".")[-1]
        return False  # Default to False if we can't evaluate

    origin = get_origin(type_annotation)
    if origin is not None:
        args = get_args(type_annotation)
        if args:
            return is_pydantic_model_from_annotation(args[0])

    if isinstance(type_annotation, type):
        return issubclass(type_annotation, BaseModel)

    return False


def _parse_docstring_parameters(docstring: str) -> Dict[str, str]:
    """Parse parameter descriptions from docstring.

    Supports Google-style, NumPy-style, and simple parameter descriptions.

    Args:
        docstring: The function's docstring

    Returns:
        Dictionary mapping parameter names to their descriptions
    """
    if not docstring:
        return {}

    param_descriptions = {}
    lines = docstring.strip().split("\n")

    # Try Google-style docstring (Args: section)
    in_args_section = False
    for line in lines:
        stripped = line.strip()

        if stripped.lower() in ["args:", "arguments:", "parameters:"]:
            in_args_section = True
            continue
        elif stripped.lower().endswith(":") and in_args_section:
            # New section started, exit args section
            break
        elif in_args_section and stripped:
            # Parse parameter line: "param_name: description" or "param_name (type): description"
            if ":" in stripped:
                parts = stripped.split(":", 1)
                param_part = parts[0].strip()
                description = parts[1].strip()

                # Remove type annotation if present: "param_name (type)" -> "param_name"
                if "(" in param_part and ")" in param_part:
                    param_name = param_part.split("(")[0].strip()
                else:
                    param_name = param_part

                param_descriptions[param_name] = description

    # If no Args section found, try simple line-by-line parsing
    if not param_descriptions:
        for line in lines:
            stripped = line.strip()
            if ":" in stripped and not stripped.endswith(":"):
                parts = stripped.split(":", 1)
                if len(parts) == 2:
                    param_part = parts[0].strip()
                    description = parts[1].strip()

                    # Remove type annotation if present
                    if "(" in param_part and ")" in param_part:
                        param_name = param_part.split("(")[0].strip()
                    else:
                        param_name = param_part

                    param_descriptions[param_name] = description

    return param_descriptions


def _extract_function_parameters(
    node: TensorlakeCompute,
) -> Tuple[List[ParameterMetadata], Optional[str]]:
    """Extract parameter names, types, and return type from TensorlakeCompute function."""
    signature = inspect.signature(node.run)
    type_hints = get_type_hints(node.run)

    # Extract parameter descriptions from docstring
    docstring = inspect.getdoc(node.run) or ""
    param_descriptions = _parse_docstring_parameters(docstring)

    parameters = []
    for param_name, param in signature.parameters.items():
        if param_name == "self":
            continue

        param_type = type_hints.get(param_name, Any)
        schema = _format_type_annotation(param_type)

        is_required = param.default == inspect.Parameter.empty
        if not is_required:
            # Add default value to JSON Schema
            schema["default"] = param.default

        # Get description from docstring
        description = param_descriptions.get(param_name, None)

        parameters.append(
            ParameterMetadata(
                name=param_name,
                data_type=schema,
                description=description,
                required=is_required,
            )
        )

    # Extract return type
    return_type = type_hints.get("return", Any)
    return_type_str = _format_type_annotation(return_type)

    return parameters, return_type_str


def _format_type_annotation(type_annotation) -> dict:
    """Format type annotation as JSON Schema for MCP compatibility."""
    if type_annotation == Any:
        return {"type": "string", "description": "Any type"}

    # Handle typing generics like List, Dict, etc. first
    origin = get_origin(type_annotation)
    args = get_args(type_annotation)

    if origin:
        if origin is list:
            if args:
                return {"type": "array", "items": _format_type_annotation(args[0])}
            else:
                return {"type": "array", "items": {"type": "string"}}
        elif origin is dict:
            if len(args) >= 2:
                return {
                    "type": "object",
                    "additionalProperties": _format_type_annotation(args[1]),
                }
            else:
                return {"type": "object"}
        elif origin is Union:
            # Handle Union types like Union[str, int]
            non_none_types = [arg for arg in args if arg is not type(None)]
            if len(non_none_types) == 1:
                # Optional type (Union[T, None])
                schema = _format_type_annotation(non_none_types[0])
                return schema
            else:
                # Multiple types - use anyOf
                return {
                    "anyOf": [_format_type_annotation(arg) for arg in non_none_types]
                }

    # Handle simple types
    if type_annotation is str:
        return {"type": "string"}
    elif type_annotation is int:
        return {"type": "integer"}
    elif type_annotation is float:
        return {"type": "number"}
    elif type_annotation is bool:
        return {"type": "boolean"}
    elif hasattr(type_annotation, "__name__"):
        # For custom classes, assume object type
        return {"type": "object", "description": f"{type_annotation.__name__} object"}
    else:
        return {"type": "string", "description": str(type_annotation)}


class Graph:
    def __init__(
        self,
        name: str,
        start_node: TensorlakeCompute,
        description: Optional[str] = None,
        tags: Dict[str, str] = {},
        version: Optional[str] = None,
        retries: Retries = Retries(),
        default_region: Optional[str] = None,
    ):
        if version is None:
            # Update graph on every deployment unless user wants to manage the version manually.
            # nanoid() should be called inside function body, otherwise it'll be called
            # only once during module loading.
            version = nanoid()

        _validate_identifier(version, "version")
        _validate_identifier(name, "name")
        self.name = name
        self.description = description
        self.nodes: Dict[str, TensorlakeCompute] = {}
        self.edges: Dict[str, List[str]] = defaultdict(list)
        self.accumulator_zero_values: Dict[str, Any] = {}
        self.tags = tags
        self.version = version
        self.retries = retries
        self.default_region = default_region

        self._fn_cache: Dict[str, TensorlakeFunctionWrapper] = {}

        self.add_node(start_node)
        self._start_node: str = start_node.name

        # Storage for local execution
        self._results: Dict[str, Dict[str, List[TensorlakeData]]] = {}
        self._accumulator_values: Dict[str, TensorlakeData] = {}
        self._local_graph_ctx: Optional[GraphRequestContext] = None
        self._invocation_error: Optional[RequestException] = None

        # Invocation ID -> Metrics
        # For local graphs
        self._metrics: Dict[str, Metrics] = {}

    def get_function(self, name: str) -> TensorlakeFunctionWrapper:
        if name not in self.nodes:
            raise ValueError(f"Function {name} not found in graph")
        return TensorlakeFunctionWrapper(self.nodes[name])

    def get_accumulators(self) -> Dict[str, Any]:
        return self.accumulator_zero_values

    def add_node(self, indexify_fn: Type[TensorlakeCompute]) -> "Graph":
        validate_node(indexify_fn=indexify_fn)

        if indexify_fn.name in self.nodes:
            return self

        if issubclass(indexify_fn, TensorlakeCompute) and indexify_fn.accumulate:
            self.accumulator_zero_values[indexify_fn.name] = indexify_fn.accumulate()

        self.nodes[indexify_fn.name] = indexify_fn

        if indexify_fn.next:
            if isinstance(indexify_fn.next, list):
                for node in indexify_fn.next:
                    self.add_node(node)
                    self.add_edge(indexify_fn, node)
            else:
                self.add_node(indexify_fn.next)
                self.add_edge(indexify_fn, indexify_fn.next)

        return self

    def add_edge(
        self,
        from_node: Type[TensorlakeCompute],
        to_node: Type[TensorlakeCompute],
    ) -> "Graph":
        self.add_edges(from_node, [to_node])
        return self

    def add_edges(
        self,
        from_node: Type[TensorlakeCompute],
        to_node: List[Type[TensorlakeCompute]],
    ) -> "Graph":
        self.add_node(from_node)
        from_node_name = from_node.name
        for node in to_node:
            self.add_node(node)
            self.edges[from_node_name].append(node.name)
        return self

    def definition(self) -> ComputeGraphMetadata:
        start_node = self.nodes[self._start_node]
        is_reducer = False
        if hasattr(start_node, "accumulate"):
            is_reducer = start_node.accumulate is not None
        graph_retry_policy: RetryPolicyMetadata = RetryPolicyMetadata(
            max_retries=self.retries.max_retries,
            initial_delay_sec=self.retries.initial_delay,
            max_delay_sec=self.retries.max_delay,
            delay_multiplier=self.retries.delay_multiplier,
        )

        # Extract parameter information for start node
        start_node_params, start_node_return_type = _extract_function_parameters(
            start_node
        )

        start_node = FunctionMetadata(
            name=start_node.name,
            fn_name=start_node.name,
            description=start_node.description,
            reducer=is_reducer,
            input_encoder=start_node.input_encoder,
            output_encoder=start_node.output_encoder,
            secret_names=start_node.secrets,
            timeout_sec=start_node.timeout,
            resources=resource_metadata_for_graph_node(start_node),
            retry_policy=(
                graph_retry_policy
                if start_node.retries is None
                else RetryPolicyMetadata(
                    max_retries=start_node.retries.max_retries,
                    initial_delay_sec=start_node.retries.initial_delay,
                    max_delay_sec=start_node.retries.max_delay,
                    delay_multiplier=start_node.retries.delay_multiplier,
                )
            ),
            cache_key=(
                f"version_function={self.version}:{start_node.name}"
                if start_node.cacheable
                else None
            ),
            parameters=start_node_params,
            return_type=start_node_return_type,
            placement_constraints=(
                PlacementConstraints(
                    filter_expressions=[f"region=={start_node.region}"]
                )
                if start_node.region is not None
                else (
                    PlacementConstraints(
                        filter_expressions=[f"region=={self.default_region}"]
                    )
                    if self.default_region is not None
                    else None
                )
            ),
            max_concurrency=start_node.max_concurrency,
        )
        metadata_edges = self.edges.copy()
        metadata_nodes = {}
        for node_name, node in self.nodes.items():
            # Extract parameter information for each node
            node_params, node_return_type = _extract_function_parameters(node)

            metadata_nodes[node_name] = FunctionMetadata(
                name=node_name,
                fn_name=node.name,
                description=node.description,
                reducer=node.accumulate is not None,
                input_encoder=node.input_encoder,
                output_encoder=node.output_encoder,
                secret_names=node.secrets,
                timeout_sec=node.timeout,
                resources=resource_metadata_for_graph_node(node),
                retry_policy=(
                    graph_retry_policy
                    if node.retries is None
                    else RetryPolicyMetadata(
                        max_retries=node.retries.max_retries,
                        initial_delay_sec=node.retries.initial_delay,
                        max_delay_sec=node.retries.max_delay,
                        delay_multiplier=node.retries.delay_multiplier,
                    )
                ),
                cache_key=(
                    f"version_function={self.version}:{node.name}"
                    if node.cacheable
                    else None
                ),
                parameters=node_params,
                return_type=node_return_type,
                placement_constraints=(
                    PlacementConstraints(filter_expressions=[f"region=={node.region}"])
                    if node.region is not None
                    else (
                        PlacementConstraints(
                            filter_expressions=[f"region=={self.default_region}"]
                        )
                        if self.default_region is not None
                        else None
                    )
                ),
                max_concurrency=node.max_concurrency,
            )

        return ComputeGraphMetadata(
            name=self.name,
            description=self.description or "",
            entrypoint=start_node,
            functions=metadata_nodes,
            edges=metadata_edges,
            tags=self.tags,
            runtime_information=RuntimeInformation(
                major_version=sys.version_info.major,
                minor_version=sys.version_info.minor,
                sdk_version=importlib.metadata.version("tensorlake"),
            ),
            version=self.version,
        )

    def run(self, block_until_done: bool = False, **kwargs) -> str:
        self.validate_graph()
        start_node = self.nodes[self._start_node]
        serializer = get_serializer(start_node.input_encoder)
        input = TensorlakeData(
            id=nanoid(),
            payload=serializer.serialize(kwargs),
            encoder=start_node.input_encoder,
        )
        print(f"[bold] Invoking {self._start_node}[/bold]")
        outputs = defaultdict(list)
        for k, v in self.accumulator_zero_values.items():
            node = self.nodes[k]
            serializer = get_serializer(node.input_encoder)
            self._accumulator_values[k] = TensorlakeData(
                payload=serializer.serialize(v), encoder=node.input_encoder
            )
        self._results[input.id] = outputs
        self._local_graph_ctx = GraphRequestContext(
            request_id=input.id,
            graph_name=self.name,
            graph_version=self.version,
            request_state=LocalInvocationState(),
        )
        self._invocation_error = None
        self._run(input, outputs)
        return input.id

    def validate_graph(self) -> None:
        """
        A method to validate that each node in the graph is
        reachable from start node using BFS.

        Raises ValueError if the graph is not valid.
        """
        total_number_of_nodes = len(self.nodes)
        queue = deque([self._start_node])
        visited = {self._start_node}

        while queue:
            current_node_name = queue.popleft()
            neighbours = (
                self.edges[current_node_name] if current_node_name in self.edges else []
            )

            for neighbour in neighbours:
                if neighbour in visited:
                    continue
                else:
                    visited.add(neighbour)
                    queue.append(neighbour)

        if total_number_of_nodes != len(visited):
            # all the nodes are not reachable from the start_node.
            raise ValueError(
                "Some nodes in the graph are not reachable from start node"
            )

    def _run(
        self,
        initial_input: TensorlakeData,
        outputs: Dict[str, List[bytes]],
    ) -> None:
        queue = deque([(self._start_node, initial_input)])
        while queue:
            function_name, input = queue.popleft()
            function_outputs: FunctionCallResult = self._invoke_fn_with_retries(
                function_name, input
            )
            # Store metrics for local graph execution
            if function_outputs.metrics is not None:
                metrics = self._metrics.get(
                    self._local_graph_ctx.request_id, Metrics(timers={}, counters={})
                )
                metrics.timers.update(function_outputs.metrics.timers)
                metrics.counters.update(function_outputs.metrics.counters)
                self._metrics[self._local_graph_ctx.request_id] = metrics

            if isinstance(function_outputs.exception, RequestException):
                self._invocation_error = function_outputs.exception
                print(
                    f'InvocationError in function {function_name}: "{function_outputs.exception.message}"'
                )
                return

            self._log_local_exec_tracebacks(function_outputs)

            fn_outputs = function_outputs.ser_outputs
            print(f"ran {function_name}: num outputs: {len(fn_outputs)}")
            if self._accumulator_values.get(function_name, None) is not None:
                acc_output = fn_outputs[-1].copy()
                self._accumulator_values[function_name] = acc_output
                outputs[function_name] = []
            if fn_outputs:
                outputs[function_name].extend(fn_outputs)
            if self._accumulator_values.get(function_name, None) is not None and queue:
                print(
                    f"accumulator not none for {function_name}, continuing, len queue: {len(queue)}"
                )
                continue

            if function_outputs.edges is None:
                # Fallback to the graph edges if not provided by the function.
                edges = self.edges[function_name]
            else:
                edges = function_outputs.edges

            for out_edge in edges:
                for output in fn_outputs:
                    queue.append((out_edge, output))

    def _invoke_fn_with_retries(
        self, node_name: str, input: TensorlakeData
    ) -> FunctionCallResult:
        node: TensorlakeCompute = self.nodes[node_name]
        retries: Retries = self.retries if node.retries is None else node.retries
        runs_left: int = 1 + retries.max_retries
        delay: float = retries.initial_delay

        while runs_left > 0:
            last_result = self._invoke_fn(node_name=node_name, input=input)
            if last_result.exception is None:
                break  # successful run

            time.sleep(delay)
            runs_left -= 1
            delay *= retries.delay_multiplier
            delay = min(delay, retries.max_delay)

        # Return the last result if successful or out of retries.
        return last_result

    def _invoke_fn(self, node_name: str, input: TensorlakeData) -> FunctionCallResult:
        # TODO: Implement function timeouts when we start calling Function Executor in local mode.
        node = self.nodes[node_name]
        if node_name not in self._fn_cache:
            self._fn_cache[node_name] = TensorlakeFunctionWrapper(node)
        fn = self._fn_cache[node_name]
        acc_value = self._accumulator_values.get(node_name, None)
        return fn.invoke_fn_ser(self._local_graph_ctx, input, acc_value)

    def _log_local_exec_tracebacks(self, result: FunctionCallResult) -> None:
        if result.exception is None:
            return

        traceback.print_exception(result.exception)
        import os

        print("exiting local execution due to error")
        os._exit(1)

    def output(
        self,
        invocation_id: str,
        fn_name: str,
    ) -> List[Any]:
        if self._invocation_error is not None:
            # Preserves the original error message and traceback
            raise self._invocation_error

        results = self._results[invocation_id]
        if fn_name not in results:
            if fn_name in self.nodes:
                return []
            raise ValueError(f"no results found for fn {fn_name} on graph {self.name}")
        fn = self.nodes[fn_name]
        fn_model = self.get_function(fn_name).get_output_model()
        serializer = get_serializer(fn.output_encoder)
        outputs = []
        for result in results[fn_name]:
            payload_dict = serializer.deserialize(result.payload)
            if issubclass(fn_model, BaseModel) and isinstance(payload_dict, dict):
                payload = fn_model.model_validate(payload_dict)
            else:
                payload = payload_dict
            outputs.append(payload)
        return outputs


def _validate_identifier(value: str, name: str) -> None:
    if len(value) > 200:
        raise ValueError(f"{name} must be at most 200 characters")
    # Following S3 object key naming restrictions.
    if not re.match(r"^[a-zA-Z0-9!_\-.*'()]+$", value):
        raise ValueError(
            f"{name} must only contain alphanumeric characters or ! - _ . * ' ( )"
        )
