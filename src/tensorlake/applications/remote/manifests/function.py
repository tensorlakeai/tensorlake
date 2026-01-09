import inspect
import json
from dataclasses import dataclass
from typing import Any, List

from pydantic import BaseModel

from tensorlake.applications.function.type_hints import (
    function_return_type_hint,
    is_dict_type_hint,
    is_file_type_hint,
    is_list_type_hint,
    is_pydantic_type_hint,
    is_set_type_hint,
    is_tuple_type_hint,
    parameter_type_hints,
    type_hint_arguments,
)
from tensorlake.applications.interface import File, InternalError
from tensorlake.applications.interface.function import (
    Function,
    _ApplicationConfiguration,
    _is_application_function,
)

from .docstring import (
    DocstringStyle,
    detect_docstring_style,
    parameter_docstrings,
    return_value_description,
)
from .function_manifests import (
    FunctionResourcesManifest,
    JSONSchema,
    ParameterManifest,
    PlacementConstraintsManifest,
    RetryPolicyManifest,
)
from .function_resources import resources_for_function


class FunctionManifest(BaseModel):
    name: str
    description: str
    docstring: str = ""
    secret_names: List[str]
    initialization_timeout_sec: int
    timeout_sec: int
    resources: FunctionResourcesManifest
    retry_policy: RetryPolicyManifest
    cache_key: str | None
    # Not empty for application functions only
    parameters: List[ParameterManifest]
    # Not None for application functions only
    return_type: JSONSchema | None
    placement_constraints: PlacementConstraintsManifest
    max_concurrency: int
    min_containers: int | None = None
    max_containers: int | None = None


@dataclass
class _JSONSchemaOptionalFields:
    title: str | None = None
    description: str | None = None
    has_default_value: bool = False
    default_value: Any = None
    parameter_kind: str | None = None


def _json_schema_with_optional_fields(
    schema: JSONSchema,
    fields: _JSONSchemaOptionalFields | None,
) -> JSONSchema:
    # Because we use exclude_unset=True in model_dump_json of ApplicationManifest,
    # we explicitly set only those fields that are not None to keep the JSON schema
    # output clean and minimal.
    if fields is None:
        return schema

    if fields.title is not None:
        schema.title = fields.title
    if fields.description is not None:
        schema.description = fields.description
    if fields.has_default_value:
        try:
            json.dumps(fields.default_value)
            schema.default = fields.default_value
        except Exception:
            # Non-serializable default value.
            # Use str representation. This is the best information we can provide.
            schema.default = str(fields.default_value)
    if fields.parameter_kind is not None:
        schema.parameter_kind = fields.parameter_kind
    return schema


def _json_schema(
    type_hints: list[Any],
    fields: _JSONSchemaOptionalFields | None,
) -> JSONSchema:
    if len(type_hints) == 0 or type_hints[0] is Any:
        # A value without a type hint or Any.
        return _json_schema_with_optional_fields(
            JSONSchema(
                # Allow all json types.
                type=[
                    "null",
                    "boolean",
                    "object",
                    "array",
                    "number",
                    "string",
                    "integer",
                ],
            ),
            fields=fields,
        )

    if len(type_hints) > 1:
        # Multiple type hints for unions - use anyOf which means
        # value can match one or more of the listed types.
        return _json_schema_with_optional_fields(
            JSONSchema(
                anyOf=[
                    _json_schema(
                        type_hints=[th],
                        fields=None,
                    )
                    for th in type_hints
                ],
            ),
            fields=fields,
        )

    # Main case: single type hint where we handle all the logic.
    type_hint: Any = type_hints[0]

    if is_pydantic_type_hint(type_hint):
        return _json_schema_with_optional_fields(
            JSONSchema.model_validate(type_hint.model_json_schema()),
            fields=fields,
        )
    elif is_list_type_hint(type_hint):
        # There's only one type hint for lists, if more than one item
        # then they are from union of the same T in List[T].
        # If no type hints then this is list without [T].
        item_type_hints: List[Any] = type_hint_arguments(type_hint)
        if len(item_type_hints) > 0:
            item_type_hints = item_type_hints[0]
        return _json_schema_with_optional_fields(
            JSONSchema(
                type="array",
                items=_json_schema(
                    type_hints=item_type_hints,
                    fields=None,
                ),
            ),
            fields=fields,
        )
    elif is_tuple_type_hint(type_hint):
        # Each tuple item can have its own type hint or no type hints.
        # It can also have ellipsis ... at the end which means repeat the
        # last type any number of times.
        item_type_hints: List[Any] = type_hint_arguments(type_hint)
        if len(item_type_hints) == 0:
            # Tuple without type hints is similar to a list[Any].
            return _json_schema_with_optional_fields(
                JSONSchema(
                    type="array",
                    items=JSONSchema(
                        # Allow all json types.
                        type=[
                            "null",
                            "boolean",
                            "object",
                            "array",
                            "number",
                            "string",
                            "integer",
                        ],
                    ),
                ),
                fields=fields,
            )
        else:
            # Tuple with type hints for each item.
            schema: JSONSchema = _json_schema_with_optional_fields(
                JSONSchema(
                    type="array",
                    prefixItems=[
                        _json_schema(
                            type_hints=th,
                            fields=None,
                        )
                        for th in item_type_hints
                        if th is not ...
                    ],
                    minItems=(
                        len(item_type_hints)
                        if item_type_hints[-1] is not ...
                        else len(item_type_hints) - 1
                    ),
                ),
                fields=fields,
            )
            if item_type_hints[-1] is not ...:
                schema.maxItems = schema.minItems
            if len(item_type_hints) >= 2 and item_type_hints[-1] is ...:
                # Ellipsis case - allow additional items of the last type.
                schema.items = _json_schema(
                    type_hints=item_type_hints[-2],
                    fields=None,
                )

            return schema
    elif is_set_type_hint(type_hint):
        # Zero or one type hint for sets. Zero means set without [T].
        item_type_hints: List[Any] = type_hint_arguments(type_hint)
        if len(item_type_hints) > 0:
            item_type_hints = item_type_hints[0]
        return _json_schema_with_optional_fields(
            JSONSchema(
                type="array",
                items=_json_schema(
                    type_hints=item_type_hints,
                    fields=None,
                ),
                uniqueItems=True,
            ),
            fields=fields,
        )
    elif is_dict_type_hint(type_hint):
        # Zero or two type hints for dicts. Zero means dict without [K, V].
        key_value_type_hints: List[Any] = type_hint_arguments(type_hint)

        # Non-string keys are converted to strings by JSON serialization.
        if len(key_value_type_hints) >= 1:
            propertyNames: JSONSchema = _json_schema(
                type_hints=key_value_type_hints[0],
                fields=None,
            )
        else:
            # No key type hint - allow any string keys.
            propertyNames = JSONSchema(type="string")

        value_type_hints: List[Any] = []
        if len(key_value_type_hints) >= 2:
            value_type_hints = key_value_type_hints[1]
        return _json_schema_with_optional_fields(
            JSONSchema(
                type="object",
                propertyNames=propertyNames,
                additionalProperties=_json_schema(
                    type_hints=value_type_hints,
                    fields=None,
                ),
            ),
            fields=fields,
        )
    elif is_file_type_hint(type_hint):
        # Files are never serialized to JSON, they are provided to application
        # function as HTTP body or part of a HTTP multipart request.
        # This is a grey area because we don't support deserializing File from JSON
        # but we still want to document it in function manifest so MCP clients and
        # curl commands can render it properly.
        return _json_schema_with_optional_fields(
            JSONSchema(
                type="file",  # Custom type for File, not part of JSON Schema spec
            ),
            fields=fields,
        )
    # Handle all basic JSON serializable types, see https://docs.python.org/3/library/json.html#py-to-json-table
    elif type_hint is str:
        return _json_schema_with_optional_fields(
            JSONSchema(
                type="string",
            ),
            fields=fields,
        )
    elif type_hint is int:
        return _json_schema_with_optional_fields(
            JSONSchema(
                type="integer",
            ),
            fields=fields,
        )
    elif type_hint is float:
        return _json_schema_with_optional_fields(
            JSONSchema(
                type="number",
            ),
            fields=fields,
        )
    elif type_hint is bool:
        return _json_schema_with_optional_fields(
            JSONSchema(
                type="boolean",
            ),
            fields=fields,
        )
    elif type_hint is None:
        return _json_schema_with_optional_fields(
            JSONSchema(
                type="null",
            ),
            fields=fields,
        )
    else:
        # Arbitrary class/object not supported by our JSON serializer.
        # We fail application validation if there are type hints like this.
        raise ValueError(
            f"Cannot generate JSON schema, type hint {type_hint} is not JSON serializable"
        )


def _function_parameter_manifests(
    function: Function,
    docstring: str,
    docstring_style: DocstringStyle,
) -> List[ParameterManifest]:
    """Returns manifest for each function parameter (in parameter definition order)."""
    try:
        param_to_docstring: dict[str, str] = parameter_docstrings(
            docstring, docstring_style
        )
    except Exception:
        # Not a critical error, either docstring is malformed or our parser could be wrong.
        param_to_docstring: dict[str, str] = {}

    signature = inspect.signature(function._original_function)
    parameters: List[ParameterManifest] = []
    for param_ix, (param_name, param) in enumerate(signature.parameters.items()):
        if param_ix == 0 and function._function_config.class_name is not None:
            continue  # Skip 'self' parameter for class methods

        param_has_default: bool = param.default != inspect.Parameter.empty
        param_type_hints: list[Any] = parameter_type_hints(param)
        param_schema: JSONSchema = _json_schema(
            type_hints=param_type_hints,
            fields=_JSONSchemaOptionalFields(
                title=param_name,
                description=param_to_docstring.get(param_name, None),
                parameter_kind=param.kind.name,
                has_default_value=param_has_default,
                default_value=param.default,
            ),
        )

        parameters.append(
            ParameterManifest(
                name=param_name,
                data_type=param_schema,
                description=param_to_docstring.get(param_name, None),
                required=not param_has_default,
            )
        )

    return parameters


def _function_return_type_schema(
    function: Function,
    docstring: str,
    docstring_style: DocstringStyle,
) -> JSONSchema:
    """Returns JSON schema for function return type.

    Raises Exception on error.
    """
    description: str | None = None
    try:
        description = return_value_description(docstring, docstring_style)
    except Exception:
        pass  # Not a critical error, either docstring is malformed or our parser could be wrong.

    return_type_hints: list[Any] = function_return_type_hint(function)
    return _json_schema(
        type_hints=return_type_hints,
        fields=_JSONSchemaOptionalFields(
            title="Return value",
            description=description,
            parameter_kind=None,
            has_default_value=False,
            default_value=None,
        ),
    )


def create_function_manifest(
    application_function: Function, application_version: str, function: Function
) -> FunctionManifest:
    """Creates FunctionManifest for the supplied function.

    Raises TensorlakeError on error.
    """
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
    docstring: str = inspect.getdoc(function._original_function) or ""
    try:
        docstring_style: DocstringStyle = detect_docstring_style(docstring)
    except Exception as e:
        pass  # Not a critical error, either docstring is malformed or our parser could be wrong.

    # parameters and return type json schemas are only set for application functions
    # because this is only functions that use JSON serializable parameters and return
    # values sent via HTTP (or MCP).
    parameters: List[ParameterManifest] = []
    return_type: JSONSchema | None = None
    if _is_application_function(function):
        try:
            parameters = _function_parameter_manifests(
                function, docstring, docstring_style
            )
        except Exception as e:
            raise InternalError(
                f"Failed to extract function parameter manifests for {function}: {e}"
            )
        try:
            return_type = _function_return_type_schema(
                function, docstring, docstring_style
            )
        except Exception as e:
            raise InternalError(
                f"Failed to extract function return type schema for {function}: {e}"
            )

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
        docstring=docstring,
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
        return_type=return_type,
        placement_constraints=placement_constraints,
        max_concurrency=function._function_config.max_concurrency,
        min_containers=function._function_config.min_containers,
        max_containers=function._function_config.max_containers,
    )
