import inspect
from typing import (
    Any,
    Dict,
    List,
    Union,
)

from pydantic import BaseModel
from typing_extensions import get_args, get_origin, get_type_hints

from ...interface.application import Application
from ...interface.function import Function
from .function_resources import FunctionResources, resources_for_function


class Parameter(BaseModel):
    name: str
    data_type: Dict[str, Any]  # JSON Schema object with optional "default" property
    description: str | None
    required: bool


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


def _type_hint_json_schema(type_hint) -> Dict[str, str]:
    """Format type hint as JSON Schema for MCP compatibility."""
    if type_hint == Any:
        return {"type": "string", "description": "Any type"}

    # Handle typing generics like List, Dict, etc. first
    origin = get_origin(type_hint)
    args = get_args(type_hint)

    if origin:
        if origin is list:
            if args:
                return {"type": "array", "items": _type_hint_json_schema(args[0])}
            else:
                return {"type": "array", "items": {"type": "string"}}
        elif origin is dict:
            if len(args) >= 2:
                return {
                    "type": "object",
                    "additionalProperties": _type_hint_json_schema(args[1]),
                }
            else:
                return {"type": "object"}
        elif origin is Union:
            # Handle Union types like Union[str, int]
            non_none_types = [arg for arg in args if arg is not type(None)]
            if len(non_none_types) == 1:
                # Optional type (Union[T, None])
                schema = _type_hint_json_schema(non_none_types[0])
                return schema
            else:
                # Multiple types - use anyOf
                return {
                    "anyOf": [_type_hint_json_schema(arg) for arg in non_none_types]
                }

    # Handle simple types
    if type_hint is str:
        return {"type": "string"}
    elif type_hint is int:
        return {"type": "integer"}
    elif type_hint is float:
        return {"type": "number"}
    elif type_hint is bool:
        return {"type": "boolean"}
    elif hasattr(type_hint, "__name__"):
        # For custom classes, assume object type
        return {"type": "object", "description": f"{type_hint.__name__} object"}
    else:
        return {"type": "string", "description": str(type_hint)}


def _function_signature_info(
    function: Function,
) -> tuple[List[Parameter], Dict[str, str]]:
    """Extract parameter names, types, and return type from TensorlakeCompute function."""
    signature = inspect.signature(function.original_function)
    type_hints = get_type_hints(function.original_function)

    # Extract parameter descriptions from docstring
    docstring = inspect.getdoc(function.original_function) or ""
    param_descriptions = _parse_docstring_parameters(docstring)

    parameters: List[Parameter] = []
    for param_name, param in signature.parameters.items():
        if param_name == "self":
            continue

        param_type = type_hints.get(param_name, Any)
        schema = _type_hint_json_schema(param_type)

        is_required = param.default == inspect.Parameter.empty
        if not is_required:
            # Add default value to JSON Schema
            schema["default"] = param.default

        # Get description from docstring
        description = param_descriptions.get(param_name, None)

        parameters.append(
            Parameter(
                name=param_name,
                data_type=schema,
                description=description,
                required=is_required,
            )
        )

    # Extract return type
    return_type: Any = type_hints.get("return", Any)
    return_type_schema: Dict[str, str] = _type_hint_json_schema(return_type)

    return parameters, return_type_schema


class RetryPolicy(BaseModel):
    max_retries: int
    initial_delay_sec: float
    max_delay_sec: float
    delay_multiplier: float


class PlacementConstraints(BaseModel):
    filter_expressions: List[str]


class FunctionManifest(BaseModel):
    name: str
    description: str
    is_api: bool
    secret_names: List[str]
    timeout_sec: int
    resources: FunctionResources
    retry_policy: RetryPolicy
    cache_key: str | None
    parameters: List[Parameter] | None
    return_type: Dict[str, Any] | None  # JSON Schema object
    placement_constraints: PlacementConstraints
    max_concurrency: int


def create_function_manifest(app: Application, function: Function) -> FunctionManifest:
    retry_policy: RetryPolicy = (
        RetryPolicy(
            max_retries=app.retries.max_retries,
            initial_delay_sec=app.retries.initial_delay,
            max_delay_sec=app.retries.max_delay,
            delay_multiplier=app.retries.delay_multiplier,
        )
        if function.function_config.retries is None
        else RetryPolicy(
            max_retries=function.function_config.retries.max_retries,
            initial_delay_sec=function.function_config.retries.initial_delay,
            max_delay_sec=function.function_config.retries.max_delay,
            delay_multiplier=function.function_config.retries.delay_multiplier,
        )
    )

    parameters: List[Parameter]
    return_type_json_schema: Dict[str, str]
    parameters, return_type_json_schema = _function_signature_info(function)

    cache_key: str | None = (
        f"version_function={app.version}:{function.function_config.function_name}"
        if function.function_config.cacheable
        else None
    )

    app_placement_constraints: PlacementConstraints = (
        PlacementConstraints(filter_expressions=[])
        if app.region is None
        else PlacementConstraints(filter_expressions=[f"region=={app.region}"])
    )
    placement_constraints = (
        app_placement_constraints
        if function.function_config.region is None
        else PlacementConstraints(
            filter_expressions=[f"region=={function.function_config.region}"]
        )
    )

    return FunctionManifest(
        name=function.function_config.function_name,
        description=function.function_config.description,
        is_api=function.api_config is not None,
        secret_names=function.function_config.secrets,
        timeout_sec=function.function_config.timeout,
        resources=resources_for_function(function),
        retry_policy=retry_policy,
        cache_key=cache_key,
        parameters=parameters,
        return_type=return_type_json_schema,
        placement_constraints=placement_constraints,
        max_concurrency=function.function_config.max_concurrency,
    )
