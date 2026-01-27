import unittest
from typing import Annotated, Any, List, Union

import validate_all_applications
from pydantic import BaseModel

from tensorlake.applications import File, application, cls, function
from tensorlake.applications.remote.manifests.function import (
    FunctionManifest,
    JSONSchema,
    ParameterManifest,
    create_function_manifest,
)

# Makes the test case discoverable by unittest framework.
ValidateAllApplicationsTest: unittest.TestCase = validate_all_applications.define_test()


@application()
@function()
def application_function_with_nothing() -> None:
    pass


@application()
@function()
def application_function_with_any(arg: Any) -> Any:
    return arg


# Uses Google Style docstring.
@application()
@function()
def application_function_with_files(
    file1: File, file2: File = File(content=b"Test Bytes", content_type="text/plain")
) -> File:
    """Returns the first file out of the two provided.

    Args:
        file1 (File): The first file.
        file2 (File): The second file.

    Returns:
        File: The first file.

    Raises:
        Exception: If something is wrong.
    """
    # This function uses Google Style docstring.
    return file1


# Uses NumPy / SciPy Style docstring.
@application()
@function()
def application_function_with_basic_types(
    text: Annotated[str, "Input text for processing"],
    count: int,
    factor: float = 1.5,
    is_true: bool = False,
) -> str:
    """
    Returns status about the number of processed items.

    Parameters
    ----------
    text : str
        The text passed to the function.
    count : int
        The number of items to process.
    factor : float, optional
        A multiplication factor.
    is_true : bool, optional
        A boolean flag.

    Returns
    -------
    str
        The status message.

    Raises
    ------
    Exception
        If something is wrong.
    """
    # This function uses NumPy / SciPy docstring.
    return f"Processed {count} items"


@cls()
class ApplicationMethodWithBasicTypes:
    @application()
    @function()
    def application_method_with_basic_types(
        self,
        text: Annotated[str, "Input text for processing"],
        count: int,
        factor: float = 1.5,
        is_true: bool = False,
    ) -> str:
        """
        Returns status about the number of processed items.

        Parameters
        ----------
        text : str
            The text passed to the function.
        count : int
            The number of items to process.
        factor : float, optional
            A multiplication factor.
        is_true : bool, optional
            A boolean flag.

        Returns
        -------
        str
            The status message.

        Raises
        ------
        Exception
            If something is wrong.
        """
        # This function uses NumPy / SciPy docstring.
        return f"Processed {count} items"


# Uses reStructuredText / Sphinx Style docstring.
@application()
@function()
def application_function_with_dicts(
    d1: dict[int, str], d2: dict[str, str]
) -> dict[int, str]:
    """Returns the first dict out of the two provided.

    :param d1: The first dict.
    :type d1: dict[int, str]
    :param d2: The second dict.
    :type d2: dict[str, str]
    :return: The first dict.
    :rtype: dict[int, str]
    :raises Exception: If something is wrong.
    """
    # This function uses Sphinx (reStructuredText) Style docstring.
    return d1


@application()
@function()
def application_function_with_untyped_dicts(d: dict) -> dict:
    return d


@application()
@function()
def application_function_with_lists(lst: List[str]) -> List[int]:
    return [len(lst)]


@application()
@function()
def application_function_with_untyped_lists(lst: list) -> list:
    return [list(range(len(lst)))]


@application()
@function()
def application_function_with_sets(st: set[str]) -> set[int]:
    return {len(st)}


@application()
@function()
def application_function_with_untyped_sets(st: set) -> set:
    return {len(st)}


@application()
@function()
def application_function_with_tuples(tp: tuple[str]) -> tuple[int]:
    return (len(tp),)


@application()
@function()
def application_function_with_untyped_tuples(tp: tuple) -> tuple:
    return (len(tp),)


class PydanticModel(BaseModel):
    name: str
    age: int
    email: str
    is_active: bool = True


@application()
@function()
def application_function_with_pydantic_model(payload: PydanticModel) -> PydanticModel:
    return payload


@application()
@function()
def application_function_with_complex_dicts(
    d: dict[int, PydanticModel],
) -> dict[int, PydanticModel]:
    return d


@application()
@function()
def application_function_with_complex_unions(
    input: str | PydanticModel,
) -> str | PydanticModel:
    return input


@application()
@function()
def application_function_with_complex_recursive_types(
    input: dict[
        str,
        Union[str, PydanticModel, list[str | int]]
        | List[PydanticModel]
        | dict[str, Any],
    ],
) -> dict[
    str,
    Union[str, PydanticModel, list[str | int]] | List[PydanticModel] | dict[str, Any],
]:
    return input


@application()
@function()
def application_function_with_default_parameter_value(factor: float = 1.0) -> str:
    return f"Factor is {factor}"


class TestApplicationJSONSchemas(unittest.TestCase):
    def test_application_function_with_nothing(self):
        func_manifest: FunctionManifest = create_function_manifest(
            application_function_with_nothing,
            "test_app_version",
            application_function_with_nothing,
        )
        self.assertEqual(len(func_manifest.parameters), 0)

        return_schema: JSONSchema = func_manifest.return_type
        self.assertEqual(
            return_schema,
            JSONSchema(
                title="Return value",
                type="null",
            ),
        )

    def test_application_function_with_any(self):
        func_manifest: FunctionManifest = create_function_manifest(
            application_function_with_any,
            "test_app_version",
            application_function_with_any,
        )
        self.assertEqual(len(func_manifest.parameters), 1)
        parameter: ParameterManifest = func_manifest.parameters[0]
        self.assertEqual(parameter.name, "arg")
        self.assertIsNone(parameter.description)
        self.assertEqual(
            parameter.data_type,
            JSONSchema(
                title="arg",
                parameter_kind="POSITIONAL_OR_KEYWORD",
            ),
        )
        self.assertTrue(parameter.required)

        return_schema: JSONSchema = func_manifest.return_type
        self.assertEqual(
            return_schema,
            JSONSchema(
                title="Return value",
            ),
        )

    def test_application_function_with_files(self):
        func_manifest: FunctionManifest = create_function_manifest(
            application_function_with_files,
            "test_app_version",
            application_function_with_files,
        )
        self.assertEqual(len(func_manifest.parameters), 2)

        param1: ParameterManifest = func_manifest.parameters[0]
        self.assertEqual(param1.name, "file1")
        self.assertEqual(param1.description, "The first file.")
        self.assertEqual(
            param1.data_type,
            JSONSchema(
                title="file1",
                type="tensorlake_file",
                parameter_kind="POSITIONAL_OR_KEYWORD",
                description="The first file.",
            ),
        )
        self.assertTrue(param1.required)

        param2: ParameterManifest = func_manifest.parameters[1]
        self.assertEqual(param2.name, "file2")
        self.assertEqual(param2.description, "The second file.")
        self.assertEqual(
            param2.data_type,
            JSONSchema(
                title="file2",
                type="tensorlake_file",
                parameter_kind="POSITIONAL_OR_KEYWORD",
                default=True,
                description="The second file.",
            ),
        )
        self.assertFalse(param2.required)

        return_schema: JSONSchema = func_manifest.return_type
        self.assertEqual(
            return_schema,
            JSONSchema(
                title="Return value",
                type="tensorlake_file",
                description="The first file.",
            ),
        )

    def test_application_function_with_basic_types(self):
        func_manifest: FunctionManifest = create_function_manifest(
            application_function_with_basic_types,
            "test_app_version",
            application_function_with_basic_types,
        )
        self.assertEqual(len(func_manifest.parameters), 4)

        param: ParameterManifest = func_manifest.parameters[0]
        self.assertEqual(param.name, "text")
        self.assertEqual(param.description, "The text passed to the function.")
        self.assertEqual(
            param.data_type,
            JSONSchema(
                title="text",
                type="string",
                parameter_kind="POSITIONAL_OR_KEYWORD",
                description="The text passed to the function.",
            ),
        )
        self.assertTrue(param.required)

        param: ParameterManifest = func_manifest.parameters[1]
        self.assertEqual(param.name, "count")
        self.assertEqual(param.description, "The number of items to process.")
        self.assertEqual(
            param.data_type,
            JSONSchema(
                title="count",
                type="integer",
                parameter_kind="POSITIONAL_OR_KEYWORD",
                description="The number of items to process.",
            ),
        )
        self.assertTrue(param.required)

        param: ParameterManifest = func_manifest.parameters[2]
        self.assertEqual(param.name, "factor")
        self.assertEqual(param.description, "A multiplication factor.")
        self.assertEqual(
            param.data_type,
            JSONSchema(
                title="factor",
                type="number",
                parameter_kind="POSITIONAL_OR_KEYWORD",
                default=1.5,
                description="A multiplication factor.",
            ),
        )
        self.assertFalse(param.required)

        param: ParameterManifest = func_manifest.parameters[3]
        self.assertEqual(param.name, "is_true")
        self.assertEqual(param.description, "A boolean flag.")
        self.assertEqual(
            param.data_type,
            JSONSchema(
                title="is_true",
                type="boolean",
                parameter_kind="POSITIONAL_OR_KEYWORD",
                default=False,
                description="A boolean flag.",
            ),
        )
        self.assertFalse(param.required)

        return_schema: JSONSchema = func_manifest.return_type
        self.assertEqual(
            return_schema,
            JSONSchema(
                title="Return value",
                type="string",
                description="The status message.",
            ),
        )

    def test_application_method_with_basic_types(self):
        func_manifest: FunctionManifest = create_function_manifest(
            ApplicationMethodWithBasicTypes().application_method_with_basic_types,
            "test_app_version",
            ApplicationMethodWithBasicTypes().application_method_with_basic_types,
        )
        self.assertEqual(len(func_manifest.parameters), 4)

        param: ParameterManifest = func_manifest.parameters[0]
        self.assertEqual(param.name, "text")
        self.assertEqual(param.description, "The text passed to the function.")
        self.assertEqual(
            param.data_type,
            JSONSchema(
                title="text",
                type="string",
                parameter_kind="POSITIONAL_OR_KEYWORD",
                description="The text passed to the function.",
            ),
        )
        self.assertTrue(param.required)

        param: ParameterManifest = func_manifest.parameters[1]
        self.assertEqual(param.name, "count")
        self.assertEqual(param.description, "The number of items to process.")
        self.assertEqual(
            param.data_type,
            JSONSchema(
                title="count",
                type="integer",
                parameter_kind="POSITIONAL_OR_KEYWORD",
                description="The number of items to process.",
            ),
        )
        self.assertTrue(param.required)

        param: ParameterManifest = func_manifest.parameters[2]
        self.assertEqual(param.name, "factor")
        self.assertEqual(param.description, "A multiplication factor.")
        self.assertEqual(
            param.data_type,
            JSONSchema(
                title="factor",
                type="number",
                parameter_kind="POSITIONAL_OR_KEYWORD",
                default=1.5,
                description="A multiplication factor.",
            ),
        )
        self.assertFalse(param.required)

        param: ParameterManifest = func_manifest.parameters[3]
        self.assertEqual(param.name, "is_true")
        self.assertEqual(param.description, "A boolean flag.")
        self.assertEqual(
            param.data_type,
            JSONSchema(
                title="is_true",
                type="boolean",
                parameter_kind="POSITIONAL_OR_KEYWORD",
                default=False,
                description="A boolean flag.",
            ),
        )
        self.assertFalse(param.required)

        return_schema: JSONSchema = func_manifest.return_type
        self.assertEqual(
            return_schema,
            JSONSchema(
                title="Return value",
                type="string",
                description="The status message.",
            ),
        )

    def test_application_function_with_dicts(self):
        func_manifest: FunctionManifest = create_function_manifest(
            application_function_with_dicts,
            "test_app_version",
            application_function_with_dicts,
        )
        self.assertEqual(len(func_manifest.parameters), 2)

        parameter1: ParameterManifest = func_manifest.parameters[0]
        self.assertEqual(parameter1.name, "d1")
        self.assertEqual(parameter1.description, "The first dict.")
        self.assertTrue(parameter1.required)
        self.assertEqual(
            parameter1.data_type,
            JSONSchema(
                title="d1",
                type="object",
                parameter_kind="POSITIONAL_OR_KEYWORD",
                description="The first dict.",
                additionalProperties=JSONSchema(type="string"),
            ),
        )

        parameter2: ParameterManifest = func_manifest.parameters[1]
        self.assertEqual(parameter2.name, "d2")
        self.assertEqual(parameter2.description, "The second dict.")
        self.assertTrue(parameter2.required)
        self.assertEqual(
            parameter2.data_type,
            JSONSchema(
                title="d2",
                type="object",
                parameter_kind="POSITIONAL_OR_KEYWORD",
                description="The second dict.",
                additionalProperties=JSONSchema(type="string"),
            ),
        )

        return_schema: JSONSchema = func_manifest.return_type
        self.assertEqual(
            return_schema,
            JSONSchema(
                title="Return value",
                type="object",
                description="The first dict.",
                additionalProperties=JSONSchema(type="string"),
            ),
        )

    def test_application_function_with_untyped_dicts(self):
        func_manifest: FunctionManifest = create_function_manifest(
            application_function_with_untyped_dicts,
            "test_app_version",
            application_function_with_untyped_dicts,
        )
        self.assertEqual(len(func_manifest.parameters), 1)

        parameter: ParameterManifest = func_manifest.parameters[0]
        self.assertEqual(parameter.name, "d")
        self.assertIsNone(parameter.description)
        self.assertTrue(parameter.required)
        self.assertEqual(
            parameter.data_type,
            JSONSchema(
                title="d",
                type="object",
                parameter_kind="POSITIONAL_OR_KEYWORD",
                # True schema is equivalent to {} schema (always passes, matches anything).
                additionalProperties=True,
            ),
        )

        self.assertEqual(
            func_manifest.return_type,
            JSONSchema(
                title="Return value",
                type="object",
                # True schema is equivalent to {} schema (always passes, matches anything).
                additionalProperties=True,
            ),
        )

    def test_application_function_with_lists(self):
        func_manifest: FunctionManifest = create_function_manifest(
            application_function_with_lists,
            "test_app_version",
            application_function_with_lists,
        )
        self.assertEqual(len(func_manifest.parameters), 1)

        parameter: ParameterManifest = func_manifest.parameters[0]
        self.assertEqual(parameter.name, "lst")
        self.assertIsNone(parameter.description)
        self.assertTrue(parameter.required)
        self.assertEqual(
            parameter.data_type,
            JSONSchema(
                title="lst",
                type="array",
                items=JSONSchema(type="string"),
                parameter_kind="POSITIONAL_OR_KEYWORD",
            ),
        )

        self.assertEqual(
            func_manifest.return_type,
            JSONSchema(
                title="Return value",
                type="array",
                items=JSONSchema(type="integer"),
            ),
        )

    def test_application_function_with_untyped_lists(self):
        func_manifest: FunctionManifest = create_function_manifest(
            application_function_with_untyped_lists,
            "test_app_version",
            application_function_with_untyped_lists,
        )
        self.assertEqual(len(func_manifest.parameters), 1)

        parameter: ParameterManifest = func_manifest.parameters[0]
        self.assertEqual(parameter.name, "lst")
        self.assertIsNone(parameter.description)
        self.assertTrue(parameter.required)
        self.assertEqual(
            parameter.data_type,
            JSONSchema(
                title="lst",
                type="array",
                # Matches any item type.
                items=JSONSchema(),
                parameter_kind="POSITIONAL_OR_KEYWORD",
            ),
        )

        self.assertEqual(
            func_manifest.return_type,
            JSONSchema(
                title="Return value",
                type="array",
                items=JSONSchema(),
            ),
        )

    def test_application_function_with_sets(self):
        func_manifest: FunctionManifest = create_function_manifest(
            application_function_with_sets,
            "test_app_version",
            application_function_with_sets,
        )
        self.assertEqual(len(func_manifest.parameters), 1)

        parameter: ParameterManifest = func_manifest.parameters[0]
        self.assertEqual(parameter.name, "st")
        self.assertIsNone(parameter.description)
        self.assertTrue(parameter.required)
        self.assertEqual(
            parameter.data_type,
            JSONSchema(
                title="st",
                type="array",
                items=JSONSchema(type="string"),
                parameter_kind="POSITIONAL_OR_KEYWORD",
                uniqueItems=True,
            ),
        )

        self.assertEqual(
            func_manifest.return_type,
            JSONSchema(
                title="Return value",
                type="array",
                items=JSONSchema(type="integer"),
                uniqueItems=True,
            ),
        )

    def test_application_function_with_untyped_sets(self):
        func_manifest: FunctionManifest = create_function_manifest(
            application_function_with_untyped_sets,
            "test_app_version",
            application_function_with_untyped_sets,
        )
        self.assertEqual(len(func_manifest.parameters), 1)

        parameter: ParameterManifest = func_manifest.parameters[0]
        self.assertEqual(parameter.name, "st")
        self.assertIsNone(parameter.description)
        self.assertTrue(parameter.required)
        self.assertEqual(
            parameter.data_type,
            JSONSchema(
                title="st",
                type="array",
                # Matches any item type.
                items=JSONSchema(),
                parameter_kind="POSITIONAL_OR_KEYWORD",
                uniqueItems=True,
            ),
        )

        self.assertEqual(
            func_manifest.return_type,
            JSONSchema(
                title="Return value",
                type="array",
                items=JSONSchema(),
                uniqueItems=True,
            ),
        )

    def test_application_function_with_tuples(self):
        func_manifest: FunctionManifest = create_function_manifest(
            application_function_with_tuples,
            "test_app_version",
            application_function_with_tuples,
        )
        self.assertEqual(len(func_manifest.parameters), 1)

        parameter: ParameterManifest = func_manifest.parameters[0]
        self.assertEqual(parameter.name, "tp")
        self.assertIsNone(parameter.description)
        self.assertTrue(parameter.required)
        self.assertEqual(
            parameter.data_type,
            JSONSchema(
                title="tp",
                type="array",
                prefixItems=[JSONSchema(type="string")],
                parameter_kind="POSITIONAL_OR_KEYWORD",
                minItems=1,
                maxItems=1,
            ),
        )

        self.assertEqual(
            func_manifest.return_type,
            JSONSchema(
                title="Return value",
                type="array",
                prefixItems=[JSONSchema(type="integer")],
                minItems=1,
                maxItems=1,
            ),
        )

    def test_application_function_with_untyped_tuples(self):
        func_manifest: FunctionManifest = create_function_manifest(
            application_function_with_untyped_tuples,
            "test_app_version",
            application_function_with_untyped_tuples,
        )
        self.assertEqual(len(func_manifest.parameters), 1)

        parameter: ParameterManifest = func_manifest.parameters[0]
        self.assertEqual(parameter.name, "tp")
        self.assertIsNone(parameter.description)
        self.assertTrue(parameter.required)
        self.assertEqual(
            parameter.data_type,
            JSONSchema(
                title="tp",
                type="array",
                items=JSONSchema(),
                parameter_kind="POSITIONAL_OR_KEYWORD",
            ),
        )

        self.assertEqual(
            func_manifest.return_type,
            JSONSchema(
                title="Return value",
                type="array",
                items=JSONSchema(),
            ),
        )

    def test_application_function_with_pydantic_model(self):
        func_manifest: FunctionManifest = create_function_manifest(
            application_function_with_pydantic_model,
            "test_app_version",
            application_function_with_pydantic_model,
        )
        self.assertEqual(len(func_manifest.parameters), 1)

        parameter: ParameterManifest = func_manifest.parameters[0]
        self.assertEqual(parameter.name, "payload")
        self.assertIsNone(parameter.description)
        self.assertTrue(parameter.required)
        expected_parameter: JSONSchema = JSONSchema.model_validate(
            PydanticModel.model_json_schema()
        )
        expected_parameter.title = "payload"
        expected_parameter.parameter_kind = "POSITIONAL_OR_KEYWORD"
        self.assertEqual(
            parameter.data_type,
            expected_parameter,
        )

        expected_return: JSONSchema = JSONSchema.model_validate(
            PydanticModel.model_json_schema()
        )
        expected_return.title = "Return value"
        self.assertEqual(
            func_manifest.return_type,
            expected_return,
        )

    def test_application_function_with_complex_dicts(self):
        func_manifest: FunctionManifest = create_function_manifest(
            application_function_with_complex_dicts,
            "test_app_version",
            application_function_with_complex_dicts,
        )
        self.assertEqual(len(func_manifest.parameters), 1)

        parameter: ParameterManifest = func_manifest.parameters[0]
        self.assertEqual(parameter.name, "d")
        self.assertIsNone(parameter.description)
        self.assertTrue(parameter.required)
        dict_values_schema: JSONSchema = JSONSchema.model_validate(
            PydanticModel.model_json_schema()
        )
        self.assertEqual(
            parameter.data_type,
            JSONSchema(
                title="d",
                type="object",
                parameter_kind="POSITIONAL_OR_KEYWORD",
                additionalProperties=JSONSchema(
                    ref="#/$defs/PydanticModel",
                ),
                defs={
                    "PydanticModel": dict_values_schema,
                },
            ),
        )

        self.assertEqual(
            func_manifest.return_type,
            JSONSchema(
                title="Return value",
                type="object",
                additionalProperties=JSONSchema(
                    ref="#/$defs/PydanticModel",
                ),
                defs={
                    "PydanticModel": dict_values_schema,
                },
            ),
        )

    def test_application_function_with_complex_unions(self):
        func_manifest: FunctionManifest = create_function_manifest(
            application_function_with_complex_unions,
            "test_app_version",
            application_function_with_complex_unions,
        )
        self.assertEqual(len(func_manifest.parameters), 1)

        parameter: ParameterManifest = func_manifest.parameters[0]

        self.assertEqual(parameter.name, "input")
        self.assertIsNone(parameter.description)
        self.assertTrue(parameter.required)
        self.assertEqual(
            parameter.data_type,
            JSONSchema(
                title="input",
                anyOf=[
                    JSONSchema(type="string"),
                    JSONSchema(ref="#/$defs/PydanticModel"),
                ],
                parameter_kind="POSITIONAL_OR_KEYWORD",
                defs={
                    "PydanticModel": JSONSchema.model_validate(
                        PydanticModel.model_json_schema()
                    ),
                },
            ),
        )

        self.assertEqual(
            func_manifest.return_type,
            JSONSchema(
                title="Return value",
                anyOf=[
                    JSONSchema(type="string"),
                    JSONSchema(ref="#/$defs/PydanticModel"),
                ],
                defs={
                    "PydanticModel": JSONSchema.model_validate(
                        PydanticModel.model_json_schema()
                    ),
                },
            ),
        )

    def test_application_function_with_complex_recursive_types(self):
        func_manifest: FunctionManifest = create_function_manifest(
            application_function_with_complex_recursive_types,
            "test_app_version",
            application_function_with_complex_recursive_types,
        )
        self.assertEqual(len(func_manifest.parameters), 1)

        parameter: ParameterManifest = func_manifest.parameters[0]
        self.assertEqual(parameter.name, "input")
        self.assertIsNone(parameter.description)
        self.assertTrue(parameter.required)
        # dict[str, Union[str, PydanticModel, list[str | int]] | List[PydanticModel] | dict[str, Any]]
        dict_values_schema: JSONSchema = JSONSchema(
            anyOf=[
                JSONSchema(type="string"),
                JSONSchema(ref="#/$defs/PydanticModel"),
                JSONSchema(
                    type="array",
                    items=JSONSchema(
                        anyOf=[JSONSchema(type="string"), JSONSchema(type="integer")]
                    ),
                ),
                JSONSchema(
                    type="array",
                    items=JSONSchema(ref="#/$defs/PydanticModel"),
                ),
                JSONSchema(
                    type="object",
                    additionalProperties=True,
                ),
            ],
        )
        self.assertEqual(
            parameter.data_type,
            JSONSchema(
                title="input",
                type="object",
                additionalProperties=dict_values_schema,
                parameter_kind="POSITIONAL_OR_KEYWORD",
                defs={
                    "PydanticModel": JSONSchema.model_validate(
                        PydanticModel.model_json_schema()
                    ),
                },
            ),
        )

        self.assertEqual(
            func_manifest.return_type,
            JSONSchema(
                title="Return value",
                type="object",
                additionalProperties=dict_values_schema,
                defs={
                    "PydanticModel": JSONSchema.model_validate(
                        PydanticModel.model_json_schema()
                    ),
                },
            ),
        )

    def test_application_function_with_default_parameter_value(self):
        func_manifest: FunctionManifest = create_function_manifest(
            application_function_with_default_parameter_value,
            "test_app_version",
            application_function_with_default_parameter_value,
        )
        self.assertEqual(len(func_manifest.parameters), 1)

        parameter: ParameterManifest = func_manifest.parameters[0]
        self.assertEqual(parameter.name, "factor")
        self.assertIsNone(parameter.description)
        self.assertFalse(parameter.required)
        self.assertEqual(
            parameter.data_type,
            JSONSchema(
                title="factor",
                type="number",
                parameter_kind="POSITIONAL_OR_KEYWORD",
                default=1.0,
            ),
        )

        self.assertEqual(
            func_manifest.return_type,
            JSONSchema(
                title="Return value",
                type="string",
            ),
        )


if __name__ == "__main__":
    unittest.main()
