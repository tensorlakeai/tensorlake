import inspect
from typing import Any, Dict, List

from ..interface import DeserializationError, Function, InternalError
from ..metadata import ValueMetadata
from .type_hints import function_arg_type_hint, function_signature
from .user_data_serializer import deserialize_value, function_input_serializer


def _get_application_param_count(application: Function) -> int:
    """Returns the number of parameters for the application function, excluding 'self'."""
    signature: inspect.Signature = function_signature(application)
    params = list(signature.parameters.values())
    # Exclude 'self' parameter for class methods
    if len(params) > 0 and params[0].name == "self":
        params = params[1:]
    return len(params)


def _coerce_to_type(value: Any, type_hint: Any) -> Any:
    """Coerces a value to the expected type if needed.

    Handles Pydantic models and other types that can be constructed from dicts.
    """
    if type_hint is inspect.Parameter.empty:
        return value

    # If value is already the expected type, return as-is
    if isinstance(value, type_hint) if isinstance(type_hint, type) else False:
        return value

    # Handle Pydantic models - construct from dict
    if isinstance(value, dict) and isinstance(type_hint, type):
        # Check if it's a Pydantic model
        if hasattr(type_hint, "model_validate"):
            # Pydantic v2
            return type_hint.model_validate(value)
        elif hasattr(type_hint, "parse_obj"):
            # Pydantic v1
            return type_hint.parse_obj(value)

    return value


def _coerce_payload_to_kwargs(
    application: Function, payload: Dict[str, Any]
) -> Dict[str, Any]:
    """Coerces payload dict values to their expected types based on function signature.

    Used for multi-parameter application functions.
    """
    signature: inspect.Signature = function_signature(application)
    params = list(signature.parameters.values())

    # Exclude 'self' parameter for class methods
    if len(params) > 0 and params[0].name == "self":
        params = params[1:]

    kwargs: Dict[str, Any] = {}
    for param in params:
        if param.name in payload:
            raw_value = payload[param.name]
            kwargs[param.name] = _coerce_to_type(raw_value, param.annotation)
        elif param.default is not inspect.Parameter.empty:
            kwargs[param.name] = param.default
        else:
            raise InternalError(
                f"Missing required parameter '{param.name}' in application payload"
            )

    return kwargs


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
