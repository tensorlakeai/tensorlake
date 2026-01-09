import json
import os
import subprocess
import tempfile
import unittest
from typing import Annotated, Any, List, Union

import validate_all_applications
from pydantic import BaseModel

from tensorlake.applications import File, Function, application, cls, function
from tensorlake.applications.remote.api_client import APIClient
from tensorlake.applications.remote.curl_command import example_application_curl_command
from tensorlake.applications.remote.deploy import deploy_applications
from tensorlake.applications.remote.manifests.application import (
    create_application_manifest,
)
from tensorlake.applications.remote.request import RemoteRequest

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


@application()
@function()
def application_function_with_str(arg: str) -> str:
    return arg


@application()
@function()
def application_function_with_file(file1: File) -> File:
    return file1


@application()
@function()
def application_function_with_files(
    file1: File, file2: File = File(content=b"Test Bytes", content_type="text/plain")
) -> File:
    return file1


@application()
@function()
def application_function_with_file_content_and_metadata(
    file: File, metadata: dict[str, str]
) -> File:
    return file


@application()
@function()
def application_function_with_basic_types(
    text: Annotated[str, "Input text for processing"],
    count: int,
    factor: float = 1.5,
    is_true: bool = False,
) -> str:
    return f"Got 4 parameters"


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
        return f"Got 4 parameters"


# Uses reStructuredText / Sphinx Style docstring.
@application()
@function()
def application_function_with_dicts(
    d1: dict[int, str], d2: dict[str, str]
) -> dict[int, str]:
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


# Generates example curl command and verifies that the application functions succeed with them.
class TestApplicationCURLCommand(unittest.TestCase):
    def run_curl_request(self, application: Function, curl_command: str) -> Any:
        result: subprocess.CompletedProcess = subprocess.run(
            curl_command, shell=True, capture_output=True, text=True
        )
        request_id: str = json.loads(result.stdout)["request_id"]
        request: RemoteRequest = RemoteRequest(
            application_name=application._function_config.function_name,
            application_manifest=create_application_manifest(
                application_function=application,
                all_functions=[application_function_with_nothing],
            ),
            request_id=request_id,
            client=self._api_client,
        )
        return request.output()

    @classmethod
    def setUpClass(cls):
        deploy_applications(__file__)
        # Add "/v1/namespaces/default". Ingress appends this automatically, so curl command generation code doesn't append this.
        cls._api_url: str = os.environ["TENSORLAKE_API_URL"] + "/v1/namespaces/default"
        cls._api_client: APIClient = APIClient()

    def test_application_function_with_nothing(self):
        curl_command: str = example_application_curl_command(
            api_url=self._api_url,
            application=application_function_with_nothing,
            file_paths=None,
        )
        output: None = self.run_curl_request(
            application_function_with_nothing, curl_command
        )
        self.assertIsNone(output)

    def test_application_function_with_any(self):
        curl_command: str = example_application_curl_command(
            api_url=self._api_url,
            application=application_function_with_any,
            file_paths=None,
        )
        self.run_curl_request(application_function_with_any, curl_command)

    def test_application_function_with_str(self):
        curl_command: str = example_application_curl_command(
            api_url=self._api_url,
            application=application_function_with_str,
            file_paths=None,
        )
        output: str = self.run_curl_request(application_function_with_str, curl_command)
        self.assertIsInstance(output, str)

    def test_application_function_with_file(self):
        with tempfile.NamedTemporaryFile(
            delete=False, mode="w", suffix=".txt"
        ) as tmp_file1:
            tmp_file1.write("This is a test string 1.")
            tmp_file1_path: str = tmp_file1.name

        try:
            curl_command: str = example_application_curl_command(
                api_url=self._api_url,
                application=application_function_with_file,
                file_paths={
                    "file1": tmp_file1_path,
                },
            )
            file1: File = self.run_curl_request(
                application_function_with_file, curl_command
            )
        finally:
            os.remove(tmp_file1_path)

        self.assertIsInstance(file1, File)
        # We don't guess content type ourselfs.
        self.assertEqual(file1.content_type, "text/plain")
        self.assertEqual(file1.content, b"This is a test string 1.")

    def test_application_function_with_files(self):
        with tempfile.NamedTemporaryFile(
            delete=False, mode="w", suffix=".txt"
        ) as tmp_file1:
            tmp_file1.write("This is a test string 1.")
            tmp_file1_path: str = tmp_file1.name

        with tempfile.NamedTemporaryFile(
            delete=False, mode="w", suffix=".txt"
        ) as tmp_file2:
            tmp_file2.write("This is a test string 2.")
            tmp_file2_path: str = tmp_file2.name

        try:
            curl_command: str = example_application_curl_command(
                api_url=self._api_url,
                application=application_function_with_files,
                file_paths={
                    "file1": tmp_file1_path,
                    "file2": tmp_file2_path,
                },
            )
            file1: File = self.run_curl_request(
                application_function_with_files, curl_command
            )
        finally:
            os.remove(tmp_file1_path)
            os.remove(tmp_file2_path)

        self.assertIsInstance(file1, File)
        self.assertEqual(file1.content_type, "text/plain")
        self.assertEqual(file1.content, b"This is a test string 1.")

    def test_application_function_with_file_content_and_metadata(self):
        with tempfile.NamedTemporaryFile(
            delete=False, mode="w", suffix=".txt"
        ) as tmp_file1:
            tmp_file1.write("This is a test string 1.")
            tmp_file1_path: str = tmp_file1.name

        try:
            curl_command: str = example_application_curl_command(
                api_url=self._api_url,
                application=application_function_with_file_content_and_metadata,
                file_paths={
                    "file": tmp_file1_path,
                },
            )
            file1: File = self.run_curl_request(
                application_function_with_file_content_and_metadata, curl_command
            )
        finally:
            os.remove(tmp_file1_path)

        self.assertIsInstance(file1, File)
        self.assertEqual(file1.content_type, "text/plain")
        self.assertEqual(file1.content, b"This is a test string 1.")

    def test_application_function_with_basic_types(self):
        curl_command: str = example_application_curl_command(
            api_url=self._api_url,
            application=application_function_with_basic_types,
            file_paths=None,
        )
        output: str = self.run_curl_request(
            application_function_with_basic_types, curl_command
        )
        self.assertEqual(output, "Got 4 parameters")

    def test_application_method_with_basic_types(self):
        curl_command: str = example_application_curl_command(
            api_url=self._api_url,
            application=ApplicationMethodWithBasicTypes().application_method_with_basic_types,
            file_paths=None,
        )
        output: str = self.run_curl_request(
            ApplicationMethodWithBasicTypes().application_method_with_basic_types,
            curl_command,
        )
        self.assertEqual(output, "Got 4 parameters")

    def test_application_function_with_dicts(self):
        curl_command: str = example_application_curl_command(
            api_url=self._api_url,
            application=application_function_with_dicts,
            file_paths=None,
        )
        output: dict = self.run_curl_request(
            application_function_with_dicts,
            curl_command,
        )
        self.assertIsInstance(output, dict)

    def test_application_function_with_untyped_dicts(self):
        curl_command: str = example_application_curl_command(
            api_url=self._api_url,
            application=application_function_with_untyped_dicts,
            file_paths=None,
        )
        output: dict = self.run_curl_request(
            application_function_with_untyped_dicts,
            curl_command,
        )
        self.assertIsInstance(output, dict)

    def test_application_function_with_lists(self):
        curl_command: str = example_application_curl_command(
            api_url=self._api_url,
            application=application_function_with_lists,
            file_paths=None,
        )
        output: list = self.run_curl_request(
            application_function_with_lists,
            curl_command,
        )
        self.assertIsInstance(output, list)

    def test_application_function_with_untyped_lists(self):
        curl_command: str = example_application_curl_command(
            api_url=self._api_url,
            application=application_function_with_untyped_lists,
            file_paths=None,
        )
        output: list = self.run_curl_request(
            application_function_with_untyped_lists,
            curl_command,
        )
        self.assertIsInstance(output, list)

    def test_application_function_with_sets(self):
        curl_command: str = example_application_curl_command(
            api_url=self._api_url,
            application=application_function_with_sets,
            file_paths=None,
        )
        output: set = self.run_curl_request(
            application_function_with_sets,
            curl_command,
        )
        self.assertIsInstance(output, set)

    def test_application_function_with_untyped_sets(self):
        curl_command: str = example_application_curl_command(
            api_url=self._api_url,
            application=application_function_with_untyped_sets,
            file_paths=None,
        )
        output: set = self.run_curl_request(
            application_function_with_untyped_sets,
            curl_command,
        )
        self.assertIsInstance(output, set)

    def test_application_function_with_tuples(self):
        curl_command: str = example_application_curl_command(
            api_url=self._api_url,
            application=application_function_with_tuples,
            file_paths=None,
        )
        output: tuple = self.run_curl_request(
            application_function_with_tuples,
            curl_command,
        )
        self.assertIsInstance(output, tuple)

    def test_application_function_with_untyped_tuples(self):
        curl_command: str = example_application_curl_command(
            api_url=self._api_url,
            application=application_function_with_untyped_tuples,
            file_paths=None,
        )
        output: tuple = self.run_curl_request(
            application_function_with_untyped_tuples,
            curl_command,
        )
        self.assertIsInstance(output, tuple)

    def test_application_function_with_pydantic_model(self):
        curl_command: str = example_application_curl_command(
            api_url=self._api_url,
            application=application_function_with_pydantic_model,
            file_paths=None,
        )
        output: PydanticModel = self.run_curl_request(
            application_function_with_pydantic_model,
            curl_command,
        )
        self.assertIsInstance(output, PydanticModel)

    def test_application_function_with_complex_dicts(self):
        curl_command: str = example_application_curl_command(
            api_url=self._api_url,
            application=application_function_with_complex_dicts,
            file_paths=None,
        )
        output: dict = self.run_curl_request(
            application_function_with_complex_dicts,
            curl_command,
        )
        self.assertIsInstance(output, dict)

    def test_application_function_with_complex_unions(self):
        curl_command: str = example_application_curl_command(
            api_url=self._api_url,
            application=application_function_with_complex_unions,
            file_paths=None,
        )
        output: PydanticModel = self.run_curl_request(
            application_function_with_complex_unions,
            curl_command,
        )
        self.assertTrue(isinstance(output, PydanticModel) or isinstance(output, str))

    def test_application_function_with_complex_recursive_types(self):
        curl_command: str = example_application_curl_command(
            api_url=self._api_url,
            application=application_function_with_complex_recursive_types,
            file_paths=None,
        )
        output: dict = self.run_curl_request(
            application_function_with_complex_recursive_types,
            curl_command,
        )
        self.assertIsInstance(output, dict)

    def test_application_function_with_default_parameter_value(self):
        curl_command: str = example_application_curl_command(
            api_url=self._api_url,
            application=application_function_with_default_parameter_value,
            file_paths=None,
        )
        output: str = self.run_curl_request(
            application_function_with_default_parameter_value,
            curl_command,
        )
        # We use default value in curl command if available.
        self.assertEqual(output, "Factor is 1.0")


if __name__ == "__main__":
    unittest.main()
