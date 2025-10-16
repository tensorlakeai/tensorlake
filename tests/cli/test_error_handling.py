import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
from click.testing import CliRunner
from tomlkit import document, dumps

import tensorlake.cli._configuration as config_module
from tensorlake.cli import cli
from tensorlake.cli._common import Context


class TestDebugMode(unittest.TestCase):
    """Test debug mode functionality"""

    def test_debug_flag_in_context(self):
        """Test that debug flag is passed to context"""
        ctx = Context.default(debug=True)
        self.assertTrue(ctx.debug)

    def test_debug_flag_defaults_to_false(self):
        """Test that debug defaults to False"""
        ctx = Context.default()
        self.assertFalse(ctx.debug)

    def test_debug_flag_in_cli(self):
        """Test that --debug flag is accepted by CLI"""
        runner = CliRunner()
        result = runner.invoke(cli, ["--debug", "--help"], prog_name="tensorlake")
        self.assertEqual(result.exit_code, 0)
        self.assertIn("--debug", result.output)

    def test_debug_flag_shows_in_help(self):
        """Test that debug flag shows in help with description"""
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"], prog_name="tensorlake")
        self.assertEqual(result.exit_code, 0)
        self.assertIn("--debug", result.output)
        self.assertIn("detailed error", result.output.lower())


class TestConfigurationSourceTracking(unittest.TestCase):
    """Test configuration source tracking"""

    def setUp(self):
        """Set up test environment"""
        self.tmpdir = tempfile.TemporaryDirectory()
        config_dir = Path(self.tmpdir.name) / ".config" / "tensorlake"
        config_dir.mkdir(parents=True)
        credentials_path = config_dir / "credentials.toml"
        local_config_path = Path(self.tmpdir.name) / ".tensorlake.toml"

        # Save original paths
        self.original_credentials_path = config_module.CREDENTIALS_PATH
        self.original_config_dir = config_module.CONFIG_DIR
        self.original_local_config_file = config_module.LOCAL_CONFIG_FILE

        # Set to test paths
        config_module.CREDENTIALS_PATH = credentials_path
        config_module.CONFIG_DIR = config_dir
        config_module.LOCAL_CONFIG_FILE = local_config_path

        # Create credentials file with PAT
        test_pat = "test_personal_access_token_123"
        config = document()
        from tomlkit import table

        section = table()
        section["token"] = test_pat
        config["https://api.tensorlake.ai"] = section

        with open(credentials_path, "w") as f:
            f.write(dumps(config))

    def tearDown(self):
        """Restore original configuration"""
        config_module.CREDENTIALS_PATH = self.original_credentials_path
        config_module.CONFIG_DIR = self.original_config_dir
        config_module.LOCAL_CONFIG_FILE = self.original_local_config_file
        self.tmpdir.cleanup()

    def test_organization_source_from_cli(self):
        """Test that organization source is tracked from CLI"""
        ctx = Context.default(organization_id="org_123")
        self.assertEqual(
            ctx.get_organization_source(), "CLI flag or environment variable"
        )

    def test_organization_source_from_local_config(self):
        """Test that organization source is tracked from local config"""
        # Create local config
        local_config = document()
        local_config["organization"] = "org_456"
        local_config["project"] = "proj_789"

        with open(config_module.LOCAL_CONFIG_FILE, "w") as f:
            f.write(dumps(local_config))

        ctx = Context.default()
        self.assertEqual(
            ctx.get_organization_source(), "local config (.tensorlake.toml)"
        )

    def test_project_source_from_cli(self):
        """Test that project source is tracked from CLI"""
        ctx = Context.default(project_id="proj_123")
        self.assertEqual(ctx.get_project_source(), "CLI flag or environment variable")

    def test_project_source_from_local_config(self):
        """Test that project source is tracked from local config"""
        # Create local config
        local_config = document()
        local_config["organization"] = "org_456"
        local_config["project"] = "proj_789"

        with open(config_module.LOCAL_CONFIG_FILE, "w") as f:
            f.write(dumps(local_config))

        ctx = Context.default()
        self.assertEqual(ctx.get_project_source(), "local config (.tensorlake.toml)")

    @patch("tensorlake.cli._common.httpx.Client")
    def test_source_from_api_key_introspection(self, mock_client_class):
        """Test that source is API key introspection when using API key"""
        # Mock the introspect response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "id": "api-key-123",
            "organizationId": "org-456",
            "projectId": "proj-789",
        }

        mock_client = MagicMock()
        mock_client.post.return_value = mock_response
        mock_client_class.return_value = mock_client

        ctx = Context.default(api_key="test_api_key")
        self.assertEqual(ctx.get_organization_source(), "API key introspection")
        self.assertEqual(ctx.get_project_source(), "API key introspection")

    def test_source_not_configured(self):
        """Test that source shows 'not configured' when neither CLI nor config"""
        ctx = Context.default()
        self.assertEqual(ctx.get_organization_source(), "not configured")
        self.assertEqual(ctx.get_project_source(), "not configured")


class TestHTTPErrorHandling(unittest.TestCase):
    """Test HTTP error handling"""

    def setUp(self):
        """Set up test environment"""
        self.tmpdir = tempfile.TemporaryDirectory()
        config_dir = Path(self.tmpdir.name) / ".config" / "tensorlake"
        config_dir.mkdir(parents=True)
        credentials_path = config_dir / "credentials.toml"
        local_config_path = Path(self.tmpdir.name) / ".tensorlake.toml"

        # Save original paths
        self.original_credentials_path = config_module.CREDENTIALS_PATH
        self.original_config_dir = config_module.CONFIG_DIR
        self.original_local_config_file = config_module.LOCAL_CONFIG_FILE

        # Set to test paths
        config_module.CREDENTIALS_PATH = credentials_path
        config_module.CONFIG_DIR = config_dir
        config_module.LOCAL_CONFIG_FILE = local_config_path

        # Create credentials file with PAT
        test_pat = "test_personal_access_token_123"
        config = document()
        from tomlkit import table

        section = table()
        section["token"] = test_pat
        config["https://api.tensorlake.ai"] = section

        with open(credentials_path, "w") as f:
            f.write(dumps(config))

        # Create local config
        local_config = document()
        local_config["organization"] = "org_456"
        local_config["project"] = "proj_789"

        with open(local_config_path, "w") as f:
            f.write(dumps(local_config))

    def tearDown(self):
        """Restore original configuration"""
        config_module.CREDENTIALS_PATH = self.original_credentials_path
        config_module.CONFIG_DIR = self.original_config_dir
        config_module.LOCAL_CONFIG_FILE = self.original_local_config_file
        self.tmpdir.cleanup()

    @patch("tensorlake.cli._common.httpx.Client")
    def test_secrets_list_handles_403_error(self, mock_client_class):
        """Test that secrets list handles 403 errors gracefully"""
        # Mock a 403 response
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_response.reason_phrase = "Forbidden"
        mock_response.text = "Permission denied"

        mock_request = MagicMock()
        mock_request.url = "https://api.tensorlake.ai/platform/v1/organizations/org_456/projects/proj_789/secrets"

        http_error = httpx.HTTPStatusError(
            "403 Forbidden", request=mock_request, response=mock_response
        )

        mock_client = MagicMock()
        mock_client.get.side_effect = http_error
        mock_client_class.return_value = mock_client

        runner = CliRunner()
        result = runner.invoke(cli, ["secrets", "list"], prog_name="tensorlake")

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Permission denied", result.output)
        self.assertIn("org_456", result.output)
        self.assertIn("proj_789", result.output)
        # Check that debug hint is shown
        self.assertIn("--debug", result.output)
        self.assertIn("TENSORLAKE_DEBUG", result.output)

    @patch("tensorlake.cli._common.httpx.Client")
    def test_debug_mode_shows_stack_trace(self, mock_client_class):
        """Test that debug mode shows stack traces"""
        # Mock a 403 response
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_response.reason_phrase = "Forbidden"
        mock_response.text = "Permission denied"

        mock_request = MagicMock()
        mock_request.url = "https://api.tensorlake.ai/platform/v1/organizations/org_456/projects/proj_789/secrets"

        http_error = httpx.HTTPStatusError(
            "403 Forbidden", request=mock_request, response=mock_response
        )

        mock_client = MagicMock()
        mock_client.get.side_effect = http_error
        mock_client_class.return_value = mock_client

        runner = CliRunner()
        result = runner.invoke(
            cli, ["--debug", "secrets", "list"], prog_name="tensorlake"
        )

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Technical details", result.output)
        self.assertIn("Stack trace", result.output)
        # Debug hint should NOT be shown when already in debug mode
        self.assertNotIn("For technical details and stack trace", result.output)


if __name__ == "__main__":
    unittest.main()
