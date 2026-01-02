import inspect
import json
from typing import Any, Dict, List, Tuple

import pydantic

from ..interface import DeserializationError, Function, SDKUsageError
from ..metadata import ValueMetadata
from .type_hints import _resolve_type_hint, function_arg_type_hint, function_signature
from .user_data_serializer import deserialize_value, function_input_serializer


def _is_class_method(application: Function) -> bool:
    """Returns True if the application function is a class method."""
    return (
        application._function_config is not None
        and application._function_config.class_name is not None
    )


def _get_application_param_count(application: Function) -> int:
    """Returns the number of callable parameters for the application function.

    Excludes:
    - 'self' for class methods
    - VAR_POSITIONAL (*args) and VAR_KEYWORD (**kwargs) - these are optional
    """
    signature: inspect.Signature = function_signature(application)
    params = list(signature.parameters.values())
    # Exclude first parameter for class methods (it's always the instance)
    if _is_class_method(application) and len(params) > 0:
        params = params[1:]
    # Exclude variadic parameters (*args, **kwargs) - they're inherently optional
    params = [
        p
        for p in params
        if p.kind
        not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
    ]
    return len(params)


def _deserialize_param_value(value: Any, type_hint: Any, serializer_name: str) -> Any:
    """Deserializes a parameter value using the serializer if needed.

    For JSON serializer with dict values and Pydantic type hints:
    re-serializes to JSON and deserializes through the serializer.

    For pickle serializer: values are already deserialized, so we use
    Pydantic's model_validate directly (same as JSON serializer does internally).

    Handles Union/Optional types by extracting concrete types and finding
    the first Pydantic model to deserialize to.
    """
    # If no type hint, return as-is
    if type_hint is inspect.Parameter.empty:
        return value

    # Resolve the type hint to get concrete types (handles Union, Optional, etc.)
    resolved_types: List[Any] = _resolve_type_hint(type_hint)

    # Handle None values - if NoneType is in the resolved types, allow None
    if value is None:
        if type(None) in resolved_types:
            return None
        # If None not allowed but value is None, return as-is (let validation handle it)
        return value

    # Check if value is already one of the expected types
    for resolved_type in resolved_types:
        if isinstance(resolved_type, type) and isinstance(value, resolved_type):
            return value

    # For dict values, find a Pydantic model in the resolved types and deserialize
    if isinstance(value, dict):
        for resolved_type in resolved_types:
            if isinstance(resolved_type, type) and issubclass(
                resolved_type, pydantic.BaseModel
            ):
                if serializer_name == "json":
                    # Re-serialize to JSON bytes and deserialize with correct type hint
                    # This reuses the JSON serializer's Pydantic handling
                    json_bytes = json.dumps(value).encode("utf-8")
                    return deserialize_value(
                        serialized_value=json_bytes,
                        metadata=ValueMetadata(
                            id="param_coerce",
                            cls=resolved_type,
                            serializer_name=serializer_name,
                            content_type=None,
                        ),
                    )
                else:
                    # For pickle: values are already Python objects, just validate
                    return resolved_type.model_validate(value)

    return value


def _coerce_payload_to_kwargs(
    application: Function, payload: Dict[str, Any]
) -> Tuple[List[Any], Dict[str, Any]]:
    """Maps payload dict to args and kwargs, deserializing values to their expected types.

    Used for multi-parameter application functions.
    Reuses the serializer for Pydantic conversion.

    Returns (args, kwargs) where:
    - args: positional-only parameters (before /)
    - kwargs: all other parameters
    """
    # Validate payload is a dict
    if not isinstance(payload, dict):
        raise SDKUsageError(
            f"Application function has multiple parameters but received a non-dict payload "
            f"(got {type(payload).__name__}). "
            f"For multi-parameter functions, your JSON payload must be an object with keys "
            f'matching the parameter names. Example: {{"param1": value1, "param2": value2}}'
        )

    signature: inspect.Signature = function_signature(application)
    params = list(signature.parameters.values())

    # Exclude first parameter for class methods (it's always the instance)
    if _is_class_method(application) and len(params) > 0:
        params = params[1:]

    serializer_name = function_input_serializer(application).name
    args: List[Any] = []
    kwargs: Dict[str, Any] = {}

    for param in params:
        # Skip variadic parameters (*args, **kwargs) - they're inherently optional
        if param.kind in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        ):
            continue

        if param.name in payload:
            raw_value = payload[param.name]
            value = _deserialize_param_value(
                raw_value, param.annotation, serializer_name
            )
            # Positional-only parameters go in args, others in kwargs
            if param.kind == inspect.Parameter.POSITIONAL_ONLY:
                args.append(value)
            else:
                kwargs[param.name] = value
        elif param.default is not inspect.Parameter.empty:
            # Use default value
            if param.kind == inspect.Parameter.POSITIONAL_ONLY:
                args.append(param.default)
            else:
                kwargs[param.name] = param.default
        else:
            raise SDKUsageError(
                f"Missing required parameter '{param.name}' in request payload. "
                f"Please include '{param.name}' in your JSON payload."
            )

    return args, kwargs


def deserialize_application_function_call_payload(
    application: Function, payload: bytes, payload_content_type: str | None
) -> Any:
    """Deserializes the API function call payload.

    This is used for API function calls done over HTTP.
    The FunctionCallAwaitable is compliant with API function calling convention.
    The supplied binary payload is deserialized using the input serializer and type hints of the API function.
    The payload_content_type is used as File content type when application function expects a File.

    Handles three cases:
    - Zero parameters: returns empty dict or deserializes as dict
    - Single parameter: deserializes to the parameter type
    - Multiple parameters: deserializes as dict

    raises DeserializationError if deserialization fails.
    """
    param_count = _get_application_param_count(application)

    # Zero parameters - deserialize as dict (empty body should result in empty dict)
    if param_count == 0:
        if len(payload) == 0:
            return {}
        # Try to deserialize as dict in case user sent an empty JSON object
        return _deserialize_to_type(application, payload, payload_content_type, dict)

    # Single parameter
    if param_count == 1:
        payload_type_hints: List[Any] = function_arg_type_hint(application, -1)
        if len(payload_type_hints) == 0:
            payload_type_hints = [object]

        last_error: DeserializationError | None = None
        deserialized_payload: Any = None

        for type_hint in payload_type_hints:
            try:
                deserialized_payload = _deserialize_to_type(
                    application, payload, payload_content_type, type_hint
                )
                last_error = None
                break
            except DeserializationError as e:
                last_error = e

        if last_error is not None:
            raise last_error

        return deserialized_payload

    # Multiple parameters - deserialize as dict
    return _deserialize_to_type(application, payload, payload_content_type, dict)


def _deserialize_to_type(
    application: Function,
    payload: bytes,
    payload_content_type: str | None,
    type_hint: Any,
) -> Any:
    """Deserializes the payload to the specified type."""
    return deserialize_value(
        serialized_value=payload,
        metadata=ValueMetadata(
            id="fake_id",
            cls=type_hint,
            serializer_name=function_input_serializer(application).name,
            content_type=payload_content_type,
        ),
    )
