import inspect
from typing import Any, Dict, List, Union

from pydantic import BaseModel
from typing_extensions import get_args, get_origin, get_type_hints

from ...interface.function import Function, _ApplicationConfiguration
from .function_manifests import (
    FunctionResourcesManifest,
    ParameterManifest,
    PlacementConstraintsManifest,
    RetryPolicyManifest,
)
from .function_resources import resources_for_function

# JSON Schema Validation specification (RFC 8928, §6.1.1)
# https://json-schema.org/draft/2020-12/json-schema-validation?#name-validation-keywords-for-any
PYTHON_TYPE_TO_JSON_SCHEMA = {
    "str": "string",
    "int": "integer",
    "float": "number",
    "bool": "boolean",
    "list": "array",
    "dict": "object",
    "tuple": "array",
    "set": "array",
    "NoneType": "null",
}


class FunctionManifest(BaseModel):
    name: str
    description: str
    secret_names: List[str]
    initialization_timeout_sec: int
    timeout_sec: int
    resources: FunctionResourcesManifest
    retry_policy: RetryPolicyManifest
    cache_key: str | None
    parameters: List[ParameterManifest] | None
    return_type: Dict[str, Any] | None  # JSON Schema object
    placement_constraints: PlacementConstraintsManifest
    max_concurrency: int


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

    # Handle Pydantic BaseModel first
    if inspect.isclass(type_hint) and issubclass(type_hint, BaseModel):
        if hasattr(type_hint, "model_json_schema"):
            return type_hint.model_json_schema()

    # Handle typing generics like List, Dict, etc.
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
                return _type_hint_json_schema(non_none_types[0])
            else:
                # Multiple types - use anyOf
                return {
                    "anyOf": [_type_hint_json_schema(arg) for arg in non_none_types]
                }

    # Handle simple types
    type_name = getattr(type_hint, "__name__", None)
    if type_name and type_name in PYTHON_TYPE_TO_JSON_SCHEMA:
        schema = {"type": PYTHON_TYPE_TO_JSON_SCHEMA[type_name]}
        if type_name == "dict":
            schema["description"] = "dict object"
        return schema
    elif hasattr(type_hint, "__name__"):
        # For custom classes, assume object type
        return {"type": "object", "description": f"{type_hint.__name__} object"}
    else:
        # Fallback for complex types without __name__
        return {"type": "string", "description": str(type_hint)}


def _function_signature_info(
    function: Function,
) -> tuple[List[ParameterManifest], Dict[str, str]]:
    """Extract parameter names, types, and return type from TensorlakeCompute function."""
    signature = inspect.signature(function._original_function)
    type_hints = get_type_hints(function._original_function)

    # Extract parameter descriptions from docstring
    docstring = inspect.getdoc(function._original_function) or ""
    param_descriptions = _parse_docstring_parameters(docstring)

    parameters: List[ParameterManifest] = []
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
            ParameterManifest(
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


def create_function_manifest(
    application_function: Function, application_version: str, function: Function
) -> FunctionManifest:
    app_config: _ApplicationConfiguration = application_function._application_config
    retry_policy: RetryPolicyManifest = (
        RetryPolicyManifest(
            max_retries=app_config.retries.max_retries,
            initial_delay_sec=app_config.retries.initial_delay,
            max_delay_sec=app_config.retries.max_delay,
            delay_multiplier=app_config.retries.delay_multiplier,
        )
        if function._function_config.retries is None
        else RetryPolicyManifest(
            max_retries=function._function_config.retries.max_retries,
            initial_delay_sec=function._function_config.retries.initial_delay,
            max_delay_sec=function._function_config.retries.max_delay,
            delay_multiplier=function._function_config.retries.delay_multiplier,
        )
    )

    parameters: List[ParameterManifest]
    return_type_json_schema: Dict[str, str]
    parameters, return_type_json_schema = _function_signature_info(function)

    cache_key: str | None = (
        f"version_function={application_version}:{function._function_config.function_name}"
        if function._function_config.cacheable
        else None
    )

    app_placement_constraints: PlacementConstraintsManifest = (
        PlacementConstraintsManifest(filter_expressions=[])
        if app_config.region is None
        else PlacementConstraintsManifest(
            filter_expressions=[f"region=={app_config.region}"]
        )
    )
    placement_constraints = (
        app_placement_constraints
        if function._function_config.region is None
        else PlacementConstraintsManifest(
            filter_expressions=[f"region=={function._function_config.region}"]
        )
    )

    return FunctionManifest(
        name=function._function_config.function_name,
        description=function._function_config.description,
        is_api=function._application_config is not None,
        secret_names=function._function_config.secrets,
        # When a function doesn't have a class_init_timeout set it means it's not a class method.
        # In this case FE initialization timeout should be the same as function timeout.
        initialization_timeout_sec=(
            function._function_config.timeout
            if function._function_config.class_init_timeout is None
            else function._function_config.class_init_timeout
        ),
        timeout_sec=function._function_config.timeout,
        resources=resources_for_function(function),
        retry_policy=retry_policy,
        cache_key=cache_key,
        parameters=parameters,
        return_type=return_type_json_schema,
        placement_constraints=placement_constraints,
        max_concurrency=function._function_config.max_concurrency,
    )
