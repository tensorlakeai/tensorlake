import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from click.testing import CliRunner
from test_helpers import get_base_url
from tomlkit import document, dumps, table

import tensorlake.cli._configuration as config_module
from tensorlake.cli import cli


class TestWhoamiNotAuthenticated(unittest.TestCase):
    """Test whoami command when user is not authenticated"""

    def setUp(self):
        """Set up test environment with no credentials"""
        self.tmpdir = tempfile.TemporaryDirectory()
        config_dir = Path(self.tmpdir.name) / ".config" / "tensorlake"
        config_dir.mkdir(parents=True)
        credentials_path = config_dir / "credentials.toml"

        # Save original paths
        self.original_credentials_path = config_module.CREDENTIALS_PATH
        self.original_config_dir = config_module.CONFIG_DIR

        # Set to empty test paths
        config_module.CREDENTIALS_PATH = credentials_path
        config_module.CONFIG_DIR = config_dir

    def tearDown(self):
        """Restore original configuration"""
        config_module.CREDENTIALS_PATH = self.original_credentials_path
        config_module.CONFIG_DIR = self.original_config_dir
        self.tmpdir.cleanup()

    def test_whoami_without_authentication_text_output(self):
        """Test whoami with no authentication shows appropriate message in text format"""
        runner = CliRunner()
        result = runner.invoke(cli, ["whoami"], prog_name="tensorlake")

        self.assertEqual(result.exit_code, 0)
        self.assertIn(
            "You are not logged in and have not provided an API key", result.output
        )
        self.assertIn("tensorlake login", result.output)
        self.assertIn("tensorlake --help", result.output)

    def test_whoami_without_authentication_json_output(self):
        """Test whoami with no authentication shows appropriate message in JSON format"""
        runner = CliRunner()
        result = runner.invoke(cli, ["whoami", "-o", "json"], prog_name="tensorlake")

        self.assertEqual(result.exit_code, 0)

        # Parse JSON output
        output_data = json.loads(result.output)
        self.assertEqual(output_data["authenticated"], False)
        self.assertIn("Not logged in", output_data["message"])

    def test_whoami_alias_who_without_authentication(self):
        """Test that 'who' alias works when not authenticated"""
        runner = CliRunner()
        result = runner.invoke(cli, ["who"], prog_name="tensorlake")

        self.assertEqual(result.exit_code, 0)
        self.assertIn("You are not logged in", result.output)


class TestWhoamiWithPAT(unittest.TestCase):
    """Test whoami command with Personal Access Token"""

    def setUp(self):
        """Set up test environment with PAT credentials"""
        self.tmpdir = tempfile.TemporaryDirectory()
        config_dir = Path(self.tmpdir.name) / ".config" / "tensorlake"
        config_dir.mkdir(parents=True)
        credentials_path = config_dir / "credentials.toml"
        local_config_dir = Path(self.tmpdir.name) / ".tensorlake"
        local_config_dir.mkdir(parents=True, exist_ok=True)
        local_config_path = local_config_dir / "config.toml"

        # Save original paths
        self.original_credentials_path = config_module.CREDENTIALS_PATH
        self.original_config_dir = config_module.CONFIG_DIR
        self.original_local_config_file = config_module.LOCAL_CONFIG_FILE

        # Set to test paths
        config_module.CREDENTIALS_PATH = credentials_path
        config_module.CONFIG_DIR = config_dir
        config_module.LOCAL_CONFIG_FILE = local_config_path

        # Get the base_url that will be used at runtime
        base_url = get_base_url()

        # Create credentials file with PAT using the resolved base_url
        self.test_pat = "test_personal_access_token_1234567890"
        config = document()
        section = table()
        section["token"] = self.test_pat
        config[base_url] = section

        with open(credentials_path, "w") as f:
            f.write(dumps(config))

        # Create local config with org and project
        local_config = document()
        local_config["organization"] = "test-org-123"
        local_config["project"] = "test-project-456"

        with open(local_config_path, "w") as f:
            f.write(dumps(local_config))

    def tearDown(self):
        """Restore original configuration"""
        config_module.CREDENTIALS_PATH = self.original_credentials_path
        config_module.CONFIG_DIR = self.original_config_dir
        config_module.LOCAL_CONFIG_FILE = self.original_local_config_file
        self.tmpdir.cleanup()

    def test_whoami_with_pat_text_output(self):
        """Test whoami with PAT shows authentication details in text format"""
        runner = CliRunner()
        result = runner.invoke(cli, ["whoami"], prog_name="tensorlake")

        self.assertEqual(result.exit_code, 0)
        self.assertIn("Dashboard Endpoint", result.output)
        self.assertIn("API Endpoint", result.output)
        self.assertIn("Organization ID", result.output)
        self.assertIn("Project ID", result.output)
        self.assertIn("Personal Access Token", result.output)
        # Should show masked token (last 6 characters)
        self.assertIn("567890", result.output)  # Last 6 chars of test token
        # Should contain asterisks for masking
        self.assertIn("***", result.output)
        # Should not show full token
        self.assertNotIn(self.test_pat, result.output)

    def test_whoami_with_pat_json_output(self):
        """Test whoami with PAT shows authentication details in JSON format"""
        runner = CliRunner()
        result = runner.invoke(cli, ["whoami", "-o", "json"], prog_name="tensorlake")

        self.assertEqual(result.exit_code, 0)

        # Parse JSON output
        output_data = json.loads(result.output)
        self.assertIn("endpoint", output_data)
        self.assertIn("organizationId", output_data)
        self.assertIn("projectId", output_data)
        self.assertIn("personalAccessToken", output_data)
        # Should show masked token
        self.assertIn("*", output_data["personalAccessToken"])
        self.assertNotIn(self.test_pat, result.output)


class TestWhoamiWithAPIKey(unittest.TestCase):
    """Test whoami command with API key"""

    def setUp(self):
        """Set up test environment"""
        self.tmpdir = tempfile.TemporaryDirectory()
        config_dir = Path(self.tmpdir.name) / ".config" / "tensorlake"
        config_dir.mkdir(parents=True)
        credentials_path = config_dir / "credentials.toml"

        # Save original paths
        self.original_credentials_path = config_module.CREDENTIALS_PATH
        self.original_config_dir = config_module.CONFIG_DIR

        # Set to test paths
        config_module.CREDENTIALS_PATH = credentials_path
        config_module.CONFIG_DIR = config_dir

    def tearDown(self):
        """Restore original configuration"""
        config_module.CREDENTIALS_PATH = self.original_credentials_path
        config_module.CONFIG_DIR = self.original_config_dir
        self.tmpdir.cleanup()

    @patch("tensorlake.cli._common.httpx.Client")
    def test_whoami_with_api_key_text_output(self, mock_client_class):
        """Test whoami with API key shows authentication details in text format"""
        # Mock the introspect response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.is_success = True
        mock_response.json.return_value = {
            "id": "api-key-123",
            "organizationId": "org-456",
            "projectId": "proj-789",
        }

        mock_client = MagicMock()
        mock_client.post.return_value = mock_response
        mock_client.headers = {"Authorization": "Bearer test_api_key"}
        mock_client_class.return_value = mock_client

        runner = CliRunner()
        result = runner.invoke(
            cli, ["--api-key", "test_api_key", "whoami"], prog_name="tensorlake"
        )

        self.assertEqual(result.exit_code, 0)
        self.assertIn("Dashboard Endpoint", result.output)
        self.assertIn("API Endpoint", result.output)
        self.assertIn("Organization ID", result.output)
        self.assertIn("Project ID", result.output)
        self.assertIn("API Key ID", result.output)
        self.assertIn("api-key-123", result.output)

    @patch("tensorlake.cli._common.httpx.Client")
    def test_whoami_with_api_key_json_output(self, mock_client_class):
        """Test whoami with API key shows authentication details in JSON format"""
        # Mock the introspect response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.is_success = True
        mock_response.json.return_value = {
            "id": "api-key-123",
            "organizationId": "org-456",
            "projectId": "proj-789",
        }

        mock_client = MagicMock()
        mock_client.post.return_value = mock_response
        mock_client.headers = {"Authorization": "Bearer test_api_key"}
        mock_client_class.return_value = mock_client

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["--api-key", "test_api_key", "whoami", "-o", "json"],
            prog_name="tensorlake",
        )

        self.assertEqual(result.exit_code, 0)

        # Parse JSON output
        output_data = json.loads(result.output)
        self.assertIn("endpoint", output_data)
        self.assertIn("organizationId", output_data)
        self.assertIn("projectId", output_data)
        self.assertIn("apiKeyId", output_data)
        self.assertEqual(output_data["apiKeyId"], "api-key-123")


if __name__ == "__main__":
    unittest.main()
