import inspect
import json
from dataclasses import dataclass
from typing import Any, List

import pydantic

from tensorlake.applications.function.type_hints import (
    function_parameters,
    function_signature,
    is_file_type_hint,
)
from tensorlake.applications.interface import File, InternalError
from tensorlake.applications.interface.function import (
    Function,
    _ApplicationConfiguration,
    _is_application_function,
)
from tensorlake.applications.user_data_serializer import (
    JSONUserDataSerializer,
    create_type_adapter,
    generate_json_schema,
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


class FunctionManifest(pydantic.BaseModel):
    name: str
    description: str
    docstring: str = ""
    secret_names: List[str]
    initialization_timeout_sec: int
    timeout_sec: int
    resources: FunctionResourcesManifest
    retry_policy: RetryPolicyManifest
    cache_key: str | None
    # Parameter manifests in parameter definition order.
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
    default_value_type_hint: Any = None
    parameter_kind: str | None = None


def _json_schema_with_optional_fields(
    schema: JSONSchema,
    fields: _JSONSchemaOptionalFields,
) -> JSONSchema:
    if fields.title is not None:
        schema.title = fields.title
    if fields.description is not None:
        schema.description = fields.description
    if fields.has_default_value:
        if is_file_type_hint(fields.default_value_type_hint):
            # Just indicate that there is a default value for File type hint. The actual
            # default value is not serialized into JSON schema because File is not JSON serializable.
            # This indication will help us to generate curl command.
            schema.default = True
        else:
            # We already checked at pre-deployment validation that the default value is
            # JSON serializable and matches the type hint.
            serializer: JSONUserDataSerializer = JSONUserDataSerializer()
            schema.default = json.loads(
                serializer.serialize(
                    fields.default_value, fields.default_value_type_hint
                )
            )  # convert back to simple json object tree
    if fields.parameter_kind is not None:
        schema.parameter_kind = fields.parameter_kind
    return schema


def _json_schema(
    type_hint: Any,
    fields: _JSONSchemaOptionalFields,
) -> JSONSchema:
    """Returns JSON schema for the provided type hint.

    Raises Exception if the supplied type hint is not supported for JSON schema generation.
    """
    # Note: we only check here for exact match, i.e. we don't support Union[...] or Optional[...]
    # to simplify our implementation for now. We have pre-deployment validation that checks for File
    # type hints to be used without any other type hints.
    if is_file_type_hint(type_hint):
        # Files are never serialized to JSON, they are provided to application
        # function as HTTP body or part of a HTTP multipart request.
        # We're using JSON schema to generate curl commands so we have to preserve information
        # about File parameter in it even though File is not part of JSON Schema spec.
        return _json_schema_with_optional_fields(
            JSONSchema(
                type="tensorlake_file",  # Custom type for File, not part of JSON Schema spec
            ),
            fields=fields,
        )
    else:
        # We already validated at pre-deployment that Pydantic can create JSON schema for the type hint.
        type_hint_adapter: pydantic.TypeAdapter = create_type_adapter(type_hint)
        type_hint_json_schema: JSONSchema = JSONSchema.model_validate(
            generate_json_schema(type_hint_adapter)
        )
        return _json_schema_with_optional_fields(
            type_hint_json_schema,
            fields=fields,
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

    parameters: list[inspect.Parameter] = function_parameters(function)
    manifests: List[ParameterManifest] = []
    for param in parameters:
        param_has_default: bool = param.default != inspect.Parameter.empty
        # Application function is already validated to have non-empty type hints for each parameter.
        param_schema: JSONSchema = _json_schema(
            type_hint=param.annotation,
            fields=_JSONSchemaOptionalFields(
                title=param.name,
                description=param_to_docstring.get(param.name, None),
                parameter_kind=param.kind.name,
                has_default_value=param_has_default,
                default_value=param.default,
                default_value_type_hint=param.annotation,
            ),
        )

        manifests.append(
            ParameterManifest(
                name=param.name,
                data_type=param_schema,
                description=param_to_docstring.get(param.name, None),
                required=not param_has_default,
            )
        )

    return manifests


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

    # Application function is already validated to have non-empty return type hints.
    signature: inspect.Signature = function_signature(function)
    return _json_schema(
        type_hint=signature.return_annotation,
        fields=_JSONSchemaOptionalFields(
            title="Return value",
            description=description,
            parameter_kind=None,
            has_default_value=False,
            default_value=None,
            default_value_type_hint=signature.return_annotation,
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
