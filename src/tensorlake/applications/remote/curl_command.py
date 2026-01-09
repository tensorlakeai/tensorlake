import inspect
import json

from tensorlake.applications import Function
from tensorlake.applications.function.type_hints import (
    function_parameters,
    is_file_type_hint,
)

from .fake_json import fake_json
from .manifests.function import FunctionManifest, create_function_manifest
from .manifests.function_manifests import ParameterManifest


def example_application_curl_command(
    api_url: str,
    application: Function,
    file_paths: dict[str, str] | None,
) -> str:
    """Generates an example cURL command to call the deployed application function.

    The file_paths dict maps parameter names to file paths for File type hint parameters.
    If None then the example command will generate placeholder file paths. The dict is
    mainly used for testing at the moment.

    Raises TensorlakeError on error.
    """
    curl_param_definition_lines: list[str] = []
    curl_command_lines: list[str] = [
        "curl",
        f"{api_url}/applications/{application._name}",
        '-H "Authorization: Bearer $TENSORLAKE_API_KEY"',
    ]

    app_func_manifest: FunctionManifest = create_function_manifest(
        application,
        application._application_config.version,
        application,
    )

    parameters: list[inspect.Parameter] = function_parameters(application)
    # NB: Provide simplest possible cURL command for each case.
    # NB: Keep this code in sync with APIClient.run_request().
    if len(parameters) == 0:
        # No application function parameters: empty body.
        curl_command_lines.append("--json ''")
    elif len(parameters) == 1 and not is_file_type_hint(parameters[0].annotation):
        # Use simple JSON body calling convention for single non-File parameter.
        # This is easier to use for users.
        param_value: str = _render_parameter_value(
            parameters[0], app_func_manifest.parameters[0], file_paths
        )
        curl_command_lines.append(f"--json '{param_value}'")
    else:
        # Note: curl guesses file content type automatically when used in multi-part form.
        curl_command_lines.append('-H "Accept: application/json"')
        param_definition: str
        param_reference: str
        for param_ix in range(len(parameters)):
            param: inspect.Parameter = parameters[param_ix]
            param_manifest: ParameterManifest = app_func_manifest.parameters[param_ix]

            param_definition, param_reference = _render_multipart_parameter(
                param, param_manifest, file_paths
            )
            curl_param_definition_lines.append(param_definition)
            curl_command_lines.append(param_reference)

    curl_command: str = ""
    if len(curl_param_definition_lines) > 0:
        curl_command = "\n".join(curl_param_definition_lines) + "\n"
    curl_command += " \\\n".join(curl_command_lines)
    return curl_command


def _render_multipart_parameter(
    param: inspect.Parameter,
    param_manifest: ParameterManifest,
    file_paths: dict[str, str] | None,
) -> tuple[str, str]:
    """Renders parameter value for inclusion in cURL command.

    returns a tuple of (parameter_definition, parameter_reference).
    The parameter_definition is the line that defines the parameter value,
    and the parameter_reference is the line that references the parameter in the cURL command.
    The parameter_reference is only applicable for multi-part body.
    Raises TensorlakeError on error.
    """
    param_definition_name: str = f"{param.name}_value"
    param_value: str = _render_parameter_value(param, param_manifest, file_paths)
    # We do pre-deployment validation that ensures that File type hint
    # is always used in a simple foo: File form.
    if is_file_type_hint(param.annotation):
        # Note: curl guesses file content type automatically.
        return (
            f"{param_definition_name}='{param_value}'",
            f"-F {param.name}=${param_definition_name}",
        )
    else:
        return (
            f"{param_definition_name}='{_curl_escape_json_string_part(param_value)}'",
            f'-F "{param.name}=${param_definition_name};type=application/json"',
        )


def _render_parameter_value(
    param: inspect.Parameter,
    param_manifest: ParameterManifest,
    file_paths: dict[str, str] | None,
) -> str:
    """Renders parameter value for inclusion in cURL command.

    Raises TensorlakeError on error.
    """
    # We do pre-deployment validation that ensures that File type hint
    # is always used in a simple foo: File form.
    if is_file_type_hint(param.annotation):
        if file_paths is not None and param.name in file_paths:
            return f"@{file_paths[param.name]}"
        else:
            # Always render File params even if they have default parameter values
            # because we can't render File default values in curl command.
            # Here we return a placeholder file path for user to fill.
            return "@FILE_PATH"
    else:
        if param_manifest.required:
            return fake_json(param.annotation)
        else:
            # Use default parameter value as example value. This is the most accurate for user.
            return json.dumps(param_manifest.data_type.default)


def _curl_escape_json_string_part(s: str) -> str:
    """Escapes a JSON string for so cURL body part parser parses it correctly."""
    # We need to escape json string to make cURL happy, other json data types like
    # numbers, bools, objects don't need this escaping.
    if s.startswith('"') and s.endswith('"'):
        return '"\\"' + s[1:-1] + '\\""'
    else:
        return s
