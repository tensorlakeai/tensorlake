import unittest
from typing import Callable, List

import httpx
import respx
from click.testing import CliRunner

from tensorlake.cli import cli


class TestSecrets(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner(env={"TENSORLAKE_API_KEY": "test_key"})

    def setup_introspect_mock(self):
        """Utility function to set up the common introspect mock"""
        m = respx.post("https://api.tensorlake.ai/platform/v1/keys/introspect")
        m.side_effect = self.when_authenticated(
            httpx.Response(
                200, json={"projectId": "projectId", "organizationId": "orgId"}
            )
        )
        return m

    def when_authenticated(
        self, response: httpx.Response
    ) -> Callable[[httpx.Request], httpx.Response]:
        """Validate the authorization header was sent correctly"""
        return lambda req: (
            response
            if req.headers["Authorization"] == "Bearer test_key"
            else httpx.Response(401, json={"message": "Unauthorized (TEST)"})
        )

    @respx.mock
    def test_list_secrets(self):
        """Table-driven test for listing secrets with different responses"""
        test_cases = [
            {
                "name": "empty_list",
                "response": {"items": []},
                "expected_output": "No secrets found",
                "expected_count": None,
            },
            {
                "name": "multiple_secrets",
                "response": {
                    "items": [
                        {
                            "id": "secretId1",
                            "name": "secretName1",
                            "createdAt": "2025-03-01T00:00:00Z",
                        },
                        {
                            "id": "secretId2",
                            "name": "secretName2",
                            "createdAt": "2025-03-02T00:00:00Z",
                        },
                    ]
                },
                "expected_output": ["secretName1", "secretName2"],
                "expected_count": "2 secrets",
            },
        ]

        for case in test_cases:
            with self.subTest(case=case["name"]):
                # Set up mocks
                introspect_mock = self.setup_introspect_mock()
                list_secrets_mock = respx.get(
                    "https://api.tensorlake.ai/platform/v1/organizations/orgId/projects/projectId/secrets"
                )
                list_secrets_mock.side_effect = self.when_authenticated(
                    httpx.Response(
                        200,
                        json=case["response"],
                    )
                )

                # Run the CLI command
                result = self.runner.invoke(
                    cli, ["secrets", "list"], prog_name="tensorlake"
                )

                # Assert results
                self.assertEqual(
                    result.exit_code,
                    0,
                    f"Failed with output: {result} - {result.output}",
                )

                # Verify expected output
                if isinstance(case["expected_output"], list):
                    for expected in case["expected_output"]:
                        self.assertIn(expected, result.output)
                else:
                    self.assertIn(case["expected_output"], result.output)

                if case["expected_count"]:
                    self.assertIn(case["expected_count"], result.output)

                # Verify mocks were called
                self.assertTrue(introspect_mock.called)
                self.assertTrue(list_secrets_mock.called)

    @respx.mock
    def test_set_secrets(self):
        """Table-driven test for setting secrets with different inputs"""
        test_cases = [
            {
                "name": "single_secret",
                "secrets": ["MY_NAME=MY_VALUE"],
                "expected_output": "1 secret set",
                "expected_request_body": [{"name": "MY_NAME", "value": "MY_VALUE"}],
            },
            {
                "name": "multiple_secrets",
                "secrets": ["NAME1=VALUE1", "NAME2=VALUE2"],
                "expected_output": ["2 secrets set"],
                "expected_request_body": [
                    {"name": "NAME1", "value": "VALUE1"},
                    {"name": "NAME2", "value": "VALUE2"},
                ],
            },
            {
                "name": "secret_value_with_spaces",
                "secrets": ["NAME3=VALUE WITH SPACES"],
                "expected_output": "1 secret set",
                "expected_request_body": [
                    {"name": "NAME3", "value": "VALUE WITH SPACES"}
                ],
            },
            {
                "name": "secret_name_with_spaces",
                "secrets": ["NAME SPACE=VALUE"],
                "expected_output": "Invalid secret name NAME SPACE, spaces are not allowed",
                "expected_request_body": None,
                "exit_code": 2,
            },
        ]

        for case in test_cases:
            with self.subTest(case=case["name"]):
                # Set up mocks
                introspect_mock = self.setup_introspect_mock()

                # Mock the create secret endpoint
                create_secret_mock = respx.put(
                    "https://api.tensorlake.ai/platform/v1/organizations/orgId/projects/projectId/secrets",
                    json=case["expected_request_body"],
                )

                # Add side effect to validate request body
                def validate_create_request(req: httpx.Request) -> httpx.Response:
                    if req.headers["Authorization"] != "Bearer test_key":
                        return httpx.Response(
                            401, json={"message": "Unauthorized (TEST)"}
                        )

                    response_body = [
                        {"id": f"secret-{s['name']}", "name": s["name"]}
                        for s in case["expected_request_body"]
                    ]
                    return httpx.Response(200, json=response_body)

                create_secret_mock.side_effect = validate_create_request

                # Run the CLI command
                result = self.runner.invoke(
                    cli, ["secrets", "set"] + case["secrets"], prog_name="tensorlake"
                )

                # Assert results
                self.assertEqual(
                    result.exit_code,
                    case.get("exit_code", 0),
                    f"Failed with output: {result} - {result.output} - {result.exception}",
                )

                # Verify expected output
                if isinstance(case["expected_output"], list):
                    for expected in case["expected_output"]:
                        self.assertIn(expected, result.output)
                else:
                    self.assertIn(case["expected_output"], result.output)

                # Verify mocks were called
                self.assertTrue(introspect_mock.called)
                if case["expected_request_body"]:
                    self.assertTrue(create_secret_mock.called)
                else:
                    self.assertFalse(create_secret_mock.called)

    @respx.mock
    def test_unset_secrets(self):
        """Table-driven test for unsetting secrets with different inputs"""
        test_cases = [
            {
                "name": "single_secret",
                "secret_names": ["MY_NAME"],
                "get_response": {
                    "items": [
                        {
                            "id": "secretId1",
                            "name": "MY_NAME",
                            "createdAt": "2025-03-01T00:00:00Z",
                        }
                    ]
                },
                "expected_output": "1 secret unset",
            },
            {
                "name": "multiple_secrets",
                "secret_names": ["NAME1", "NAME2"],
                "get_response": {
                    "items": [
                        {
                            "id": "secretId1",
                            "name": "NAME1",
                            "createdAt": "2025-03-01T00:00:00Z",
                        },
                        {
                            "id": "secretId2",
                            "name": "NAME2",
                            "createdAt": "2025-03-02T00:00:00Z",
                        },
                    ]
                },
                "expected_output": ["2 secrets unset"],
            },
            {
                "name": "nonexistent_secret",
                "secret_names": ["NONEXISTENT"],
                "get_response": {"items": []},
                "expected_output": "0 secrets unset",
            },
        ]

        for case in test_cases:
            with self.subTest(case=case["name"]):
                # Set up mocks
                introspect_mock = self.setup_introspect_mock()

                # Mock the get secrets endpoint to find secret IDs
                get_secrets_mock = respx.get(
                    "https://api.tensorlake.ai/platform/v1/organizations/orgId/projects/projectId/secrets"
                )
                get_secrets_mock.side_effect = self.when_authenticated(
                    httpx.Response(
                        200,
                        json=case["get_response"],
                    )
                )

                # Mock the delete secret endpoint
                for item in case["get_response"].get("items", []):
                    delete_secret_mock = respx.delete(
                        f"https://api.tensorlake.ai/platform/v1/organizations/orgId/projects/projectId/secrets/{item['id']}"
                    )
                    delete_secret_mock.side_effect = self.when_authenticated(
                        httpx.Response(
                            200,
                        )
                    )

                # Run the CLI command
                result = self.runner.invoke(
                    cli,
                    ["secrets", "unset"] + case["secret_names"],
                    prog_name="tensorlake",
                )

                # Assert results
                self.assertEqual(
                    result.exit_code,
                    0,
                    f"Failed with output: {result} - {result.output}",
                )

                # Verify expected output
                if isinstance(case["expected_output"], list):
                    for expected in case["expected_output"]:
                        self.assertIn(expected, result.output)
                else:
                    self.assertIn(case["expected_output"], result.output)

                # Verify mocks were called
                self.assertTrue(introspect_mock.called)
                self.assertTrue(get_secrets_mock.called)


if __name__ == "__main__":
    unittest.main()
