import json
import os
import tempfile
import unittest
from pathlib import Path
from typing import Callable
from unittest.mock import patch

import httpx
import respx
from click.testing import CliRunner
from tomlkit import parse

from tensorlake.cli import cli
from tensorlake.cli._configuration import load_credentials, save_credentials


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


class TestCredentialSaving(unittest.TestCase):
    """Tests for credential saving functionality"""

    def setUp(self):
        # Create a temporary directory for test config files
        self.temp_dir = tempfile.mkdtemp()
        self.credentials_path = Path(self.temp_dir) / "credentials.toml"
        self.legacy_credentials_path = Path(self.temp_dir) / "credentials.json"

    def tearDown(self):
        # Clean up temporary directory
        import shutil

        shutil.rmtree(self.temp_dir)

    @patch("tensorlake.cli._configuration.CREDENTIALS_PATH")
    @patch("tensorlake.cli._configuration.CONFIG_DIR")
    def test_save_credentials_preserves_existing_endpoints(
        self, mock_config_dir, mock_credentials_path
    ):
        """
        Test that saving credentials for a new endpoint doesn't delete
        existing credentials for other endpoints.
        """
        mock_config_dir.return_value = Path(self.temp_dir)
        mock_credentials_path.return_value = self.credentials_path

        # Mock the actual paths used in the function
        import tensorlake.cli._configuration as config_module

        original_creds_path = config_module.CREDENTIALS_PATH
        original_config_dir = config_module.CONFIG_DIR

        config_module.CREDENTIALS_PATH = self.credentials_path
        config_module.CONFIG_DIR = Path(self.temp_dir)

        try:
            # Save credentials for first endpoint
            endpoint1 = "https://api.dev.tensorlake.ai"
            token1 = "token_for_dev_endpoint"
            save_credentials(endpoint1, token1)

            # Verify first endpoint is saved
            loaded_token1 = load_credentials(endpoint1)
            self.assertEqual(loaded_token1, token1)

            # Save credentials for second endpoint
            endpoint2 = "https://api.prod.tensorlake.ai"
            token2 = "token_for_prod_endpoint"
            save_credentials(endpoint2, token2)

            # Verify both endpoints are preserved (this was the bug)
            loaded_token1_after = load_credentials(endpoint1)
            loaded_token2 = load_credentials(endpoint2)

            self.assertEqual(
                loaded_token1_after,
                token1,
                "First endpoint credentials were deleted when saving second endpoint",
            )
            self.assertEqual(loaded_token2, token2)

            # Verify the file structure is correct
            with open(self.credentials_path, "r") as f:
                creds_data = parse(f.read())
                self.assertIn(endpoint1, creds_data)
                self.assertIn(endpoint2, creds_data)
                self.assertEqual(creds_data[endpoint1]["token"], token1)
                self.assertEqual(creds_data[endpoint2]["token"], token2)

        finally:
            # Restore original paths
            config_module.CREDENTIALS_PATH = original_creds_path
            config_module.CONFIG_DIR = original_config_dir

    @patch("tensorlake.cli._configuration.CREDENTIALS_PATH")
    @patch("tensorlake.cli._configuration.CONFIG_DIR")
    def test_different_credentials_per_endpoint(
        self, mock_config_dir, mock_credentials_path
    ):
        """Test that different endpoints can have different credentials."""
        mock_config_dir.return_value = Path(self.temp_dir)
        mock_credentials_path.return_value = self.credentials_path

        import tensorlake.cli._configuration as config_module

        original_creds_path = config_module.CREDENTIALS_PATH
        original_config_dir = config_module.CONFIG_DIR

        config_module.CREDENTIALS_PATH = self.credentials_path
        config_module.CONFIG_DIR = Path(self.temp_dir)

        try:
            # Set up multiple endpoints with different tokens
            endpoints_and_tokens = [
                ("https://api.dev.tensorlake.ai", "dev_token_123"),
                ("https://api.prod.tensorlake.ai", "prod_token_456"),
                ("https://api.staging.tensorlake.ai", "staging_token_789"),
            ]

            # Save all credentials
            for endpoint, token in endpoints_and_tokens:
                save_credentials(endpoint, token)

            # Verify each endpoint has the correct credential
            for endpoint, expected_token in endpoints_and_tokens:
                loaded_token = load_credentials(endpoint)
                self.assertEqual(
                    loaded_token,
                    expected_token,
                    f"Endpoint {endpoint} has wrong token",
                )

            # Verify non-existent endpoint returns None
            self.assertIsNone(load_credentials("https://api.nonexistent.example.com"))

        finally:
            config_module.CREDENTIALS_PATH = original_creds_path
            config_module.CONFIG_DIR = original_config_dir

    @patch("tensorlake.cli._configuration.CREDENTIALS_PATH")
    @patch("tensorlake.cli._configuration.CONFIG_DIR")
    def test_save_credentials_creates_file_if_not_exists(
        self, mock_config_dir, mock_credentials_path
    ):
        """Test that save_credentials creates the file if it doesn't exist."""
        mock_config_dir.return_value = Path(self.temp_dir)
        mock_credentials_path.return_value = self.credentials_path

        import tensorlake.cli._configuration as config_module

        original_creds_path = config_module.CREDENTIALS_PATH
        original_config_dir = config_module.CONFIG_DIR

        config_module.CREDENTIALS_PATH = self.credentials_path
        config_module.CONFIG_DIR = Path(self.temp_dir)

        try:
            # Ensure file doesn't exist
            self.assertFalse(self.credentials_path.exists())

            endpoint = "https://api.tensorlake.ai"
            token = "test_token"

            # Save credentials
            save_credentials(endpoint, token)

            # Verify file was created with correct permissions
            self.assertTrue(self.credentials_path.exists())
            file_stats = os.stat(self.credentials_path)
            # Check that permissions are 0600 (owner read/write only)
            self.assertEqual(oct(file_stats.st_mode)[-3:], "600")

            # Verify content
            self.assertEqual(load_credentials(endpoint), token)

        finally:
            config_module.CREDENTIALS_PATH = original_creds_path
            config_module.CONFIG_DIR = original_config_dir

    @patch("tensorlake.cli._configuration.LEGACY_CREDENTIALS_PATH")
    @patch("tensorlake.cli._configuration.CREDENTIALS_PATH")
    @patch("tensorlake.cli._configuration.CONFIG_DIR")
    def test_migration_from_legacy_json_to_toml(
        self, mock_config_dir, mock_credentials_path, mock_legacy_path
    ):
        """Test that credentials.json is automatically migrated to credentials.toml"""
        import json

        mock_config_dir.return_value = Path(self.temp_dir)
        mock_credentials_path.return_value = self.credentials_path
        mock_legacy_path.return_value = self.legacy_credentials_path

        import tensorlake.cli._configuration as config_module

        original_creds_path = config_module.CREDENTIALS_PATH
        original_legacy_path = config_module.LEGACY_CREDENTIALS_PATH
        original_config_dir = config_module.CONFIG_DIR

        config_module.CREDENTIALS_PATH = self.credentials_path
        config_module.LEGACY_CREDENTIALS_PATH = self.legacy_credentials_path
        config_module.CONFIG_DIR = Path(self.temp_dir)

        try:
            # Create old-style credentials.json file
            legacy_token = "legacy_token_12345"
            with open(self.legacy_credentials_path, "w") as f:
                json.dump({"token": legacy_token}, f)

            # Verify legacy file exists and new file doesn't
            self.assertTrue(self.legacy_credentials_path.exists())
            self.assertFalse(self.credentials_path.exists())

            # Load credentials (should trigger migration)
            endpoint = "https://api.tensorlake.ai"
            loaded_token = load_credentials(endpoint)

            # Verify token was loaded correctly
            self.assertEqual(loaded_token, legacy_token)

            # Verify new TOML file was created
            self.assertTrue(self.credentials_path.exists())

            # Verify old JSON file was deleted
            self.assertFalse(self.legacy_credentials_path.exists())

            # Verify the TOML file has correct format
            with open(self.credentials_path, "r") as f:
                toml_data = parse(f.read())
                self.assertIn(endpoint, toml_data)
                self.assertEqual(toml_data[endpoint]["token"], legacy_token)

            # Verify subsequent loads work from TOML file
            loaded_token_again = load_credentials(endpoint)
            self.assertEqual(loaded_token_again, legacy_token)

        finally:
            config_module.CREDENTIALS_PATH = original_creds_path
            config_module.LEGACY_CREDENTIALS_PATH = original_legacy_path
            config_module.CONFIG_DIR = original_config_dir

    @patch("tensorlake.cli._configuration.LEGACY_CREDENTIALS_PATH")
    @patch("tensorlake.cli._configuration.CREDENTIALS_PATH")
    @patch("tensorlake.cli._configuration.CONFIG_DIR")
    def test_no_migration_when_toml_already_exists(
        self, mock_config_dir, mock_credentials_path, mock_legacy_path
    ):
        """Test that migration doesn't happen if credentials.toml already exists"""
        import json

        mock_config_dir.return_value = Path(self.temp_dir)
        mock_credentials_path.return_value = self.credentials_path
        mock_legacy_path.return_value = self.legacy_credentials_path

        import tensorlake.cli._configuration as config_module

        original_creds_path = config_module.CREDENTIALS_PATH
        original_legacy_path = config_module.LEGACY_CREDENTIALS_PATH
        original_config_dir = config_module.CONFIG_DIR

        config_module.CREDENTIALS_PATH = self.credentials_path
        config_module.LEGACY_CREDENTIALS_PATH = self.legacy_credentials_path
        config_module.CONFIG_DIR = Path(self.temp_dir)

        try:
            # Create both old and new credential files
            legacy_token = "legacy_token_old"
            toml_token = "toml_token_new"
            endpoint = "https://api.tensorlake.ai"

            # Create legacy file
            with open(self.legacy_credentials_path, "w") as f:
                json.dump({"token": legacy_token}, f)

            # Create new TOML file
            save_credentials(endpoint, toml_token)

            # Load credentials (should NOT trigger migration since TOML exists)
            loaded_token = load_credentials(endpoint)

            # Should use the TOML token, not the legacy one
            self.assertEqual(loaded_token, toml_token)

            # Legacy file should still exist (not deleted)
            self.assertTrue(self.legacy_credentials_path.exists())

        finally:
            config_module.CREDENTIALS_PATH = original_creds_path
            config_module.LEGACY_CREDENTIALS_PATH = original_legacy_path
            config_module.CONFIG_DIR = original_config_dir


if __name__ == "__main__":
    unittest.main()
