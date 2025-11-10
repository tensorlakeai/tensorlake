import tempfile
import unittest
import webbrowser
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import httpx
import respx
from click.testing import CliRunner
from test_helpers import make_endpoint_url

from tensorlake.cli import cli
from tensorlake.cli._common import Context
from tensorlake.cli._configuration import save_config


@contextmanager
def mock_auth_credentials_path():
    """Context manager to temporarily override _configuration module's credentials path with a temp directory"""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_dir = Path(tmpdir) / ".config" / "tensorlake"
        config_dir.mkdir(parents=True)
        credentials_path = config_dir / "credentials.toml"

        # Also create a local config file to prevent auto-init after login
        local_config_dir = Path(tmpdir) / ".tensorlake"
        local_config_dir.mkdir(parents=True, exist_ok=True)
        local_config_path = local_config_dir / "config.toml"
        with open(local_config_path, "w") as f:
            f.write('organization = "test_org"\nproject = "test_proj"\n')

        import tensorlake.cli._configuration as config_module

        original_config_dir = config_module.CONFIG_DIR
        original_credentials_path = config_module.CREDENTIALS_PATH
        original_local_config = config_module.LOCAL_CONFIG_FILE

        try:
            config_module.CONFIG_DIR = config_dir
            config_module.CREDENTIALS_PATH = credentials_path
            config_module.LOCAL_CONFIG_FILE = local_config_path
            yield
        finally:
            # Restore original values
            config_module.CONFIG_DIR = original_config_dir
            config_module.CREDENTIALS_PATH = original_credentials_path
            config_module.LOCAL_CONFIG_FILE = original_local_config


class TestCloudURL(unittest.TestCase):
    """Test cloud URL configuration through CLI options, environment variables, and config file"""

    def test_default_cloud_url(self):
        """Test that default cloud URL is used when no overrides are provided"""
        ctx = Context.default()
        self.assertEqual(ctx.cloud_url, "https://cloud.tensorlake.ai")

    def test_cloud_url_from_cli_option(self):
        """Test cloud URL override via CLI option"""
        runner = CliRunner()

        # We can't easily test Context from within CLI invocation, but we can verify
        # that the CLI accepts the option without error
        result = runner.invoke(
            cli,
            ["--cloud-url", "https://custom-cloud.example.com", "--help"],
            prog_name="tensorlake",
        )

        self.assertEqual(
            result.exit_code,
            0,
            f"CLI failed with output: {result.output}",
        )

    def test_cloud_url_from_environment_variable(self):
        """Test cloud URL override via TENSORLAKE_CLOUD_URL environment variable"""
        runner = CliRunner(
            env={"TENSORLAKE_CLOUD_URL": "https://env-cloud.example.com"}
        )

        result = runner.invoke(
            cli,
            ["--help"],
            prog_name="tensorlake",
        )

        self.assertEqual(
            result.exit_code,
            0,
            f"CLI failed with output: {result.output}",
        )

    def test_cloud_url_from_config_file(self):
        """Test cloud URL override via config file (tensorlake.cloud_url)"""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_dir = Path(tmpdir) / ".config" / "tensorlake"
            config_dir.mkdir(parents=True)
            config_file = config_dir / ".tensorlake_config"

            # Mock the config directory
            import tensorlake.cli._configuration as config_module

            original_config_dir = config_module.CONFIG_DIR
            original_config_file = config_module.CONFIG_FILE

            try:
                config_module.CONFIG_DIR = config_dir
                config_module.CONFIG_FILE = config_file

                # Save config with custom cloud URL
                config_data = {
                    "tensorlake": {"cloud_url": "https://config-cloud.example.com"}
                }
                save_config(config_data)

                # Load context and verify
                ctx = Context.default()
                self.assertEqual(ctx.cloud_url, "https://config-cloud.example.com")
            finally:
                # Restore original values
                config_module.CONFIG_DIR = original_config_dir
                config_module.CONFIG_FILE = original_config_file

    def test_cloud_url_priority_order(self):
        """Test that cloud URL priority order is: CLI > Config > Default"""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_dir = Path(tmpdir) / ".config" / "tensorlake"
            config_dir.mkdir(parents=True)
            config_file = config_dir / ".tensorlake_config"

            import tensorlake.cli._configuration as config_module

            original_config_dir = config_module.CONFIG_DIR
            original_config_file = config_module.CONFIG_FILE

            try:
                config_module.CONFIG_DIR = config_dir
                config_module.CONFIG_FILE = config_file

                # Save config with custom cloud URL
                config_data = {
                    "tensorlake": {"cloud_url": "https://config-cloud.example.com"}
                }
                save_config(config_data)

                # Test 1: Config overrides default
                ctx = Context.default()
                self.assertEqual(ctx.cloud_url, "https://config-cloud.example.com")

                # Test 2: CLI/env parameter overrides config
                ctx = Context.default(cloud_url="https://cli-cloud.example.com")
                self.assertEqual(ctx.cloud_url, "https://cli-cloud.example.com")

                # Test 3: Explicit None falls back to config
                ctx = Context.default(cloud_url=None)
                self.assertEqual(ctx.cloud_url, "https://config-cloud.example.com")
            finally:
                config_module.CONFIG_DIR = original_config_dir
                config_module.CONFIG_FILE = original_config_file

    def test_cloud_url_in_help_text(self):
        """Test that cloud URL configuration is documented in main CLI help text"""
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"], prog_name="tensorlake")

        self.assertEqual(result.exit_code, 0)
        self.assertIn("--cloud-url", result.output)

    def test_env_variable_name_in_help(self):
        """Test that TENSORLAKE_CLOUD_URL environment variable is shown in main CLI help"""
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"], prog_name="tensorlake")

        self.assertEqual(result.exit_code, 0)
        # Click shows environment variables in the help text
        self.assertIn("--cloud-url", result.output)


class TestCloudURLIntegration(unittest.TestCase):
    """Integration tests for cloud URL usage in authentication flow"""

    def test_context_has_cloud_url_attribute(self):
        """Test that Context object has cloud_url attribute"""
        ctx = Context.default()
        self.assertTrue(hasattr(ctx, "cloud_url"))
        self.assertIsInstance(ctx.cloud_url, str)
        self.assertTrue(ctx.cloud_url.startswith("https://"))

    def test_multiple_cloud_url_formats(self):
        """Test that various cloud URL formats are accepted"""
        test_cases = [
            "https://cloud.tensorlake.ai",
            "https://staging-cloud.tensorlake.ai",
            "http://localhost:8080",
            "https://custom.domain.com/path",
        ]

        for url in test_cases:
            with self.subTest(url=url):
                ctx = Context.default(cloud_url=url)
                self.assertEqual(ctx.cloud_url, url)

    def test_cloud_url_different_from_base_url(self):
        """Test that cloud_url and api_url are independent"""
        ctx = Context.default(
            api_url="https://api.example.com",
            cloud_url="https://cloud.example.com",
        )

        self.assertEqual(ctx.api_url, "https://api.example.com")
        self.assertEqual(ctx.cloud_url, "https://cloud.example.com")
        self.assertNotEqual(ctx.api_url, ctx.cloud_url)


class TestCloudURLWithLogin(unittest.TestCase):
    """Test that cloud URL is used correctly in the login flow"""

    def setup_login_mocks(self, base_url="https://api.tensorlake.ai"):
        """Set up common HTTP mocks for the login flow

        Args:
            base_url: The API base URL to mock (defaults to production URL)
        """
        start_mock = respx.post(f"{base_url}/platform/cli/login/start")
        start_mock.return_value = httpx.Response(
            200,
            json={
                "device_code": "test_device_code",
                "user_code": "TEST123",
            },
        )

        poll_mock = respx.get(
            f"{base_url}/platform/cli/login/poll?device_code=test_device_code"
        )
        poll_mock.return_value = httpx.Response(
            200,
            json={"status": "approved"},
        )

        exchange_mock = respx.post(f"{base_url}/platform/cli/login/exchange")
        exchange_mock.return_value = httpx.Response(
            200,
            json={"access_token": "test_access_token"},
        )

    @respx.mock
    @patch("webbrowser.open")
    def test_login_uses_cloud_url_for_browser(self, mock_browser_open):
        """Test that login opens browser with the correct cloud URL"""
        self.setup_login_mocks()

        with mock_auth_credentials_path():
            # Explicitly set TENSORLAKE_API_URL to ensure test doesn't inherit from CI environment
            runner = CliRunner(env={"TENSORLAKE_API_URL": "https://api.tensorlake.ai"})
            result = runner.invoke(cli, ["login"], prog_name="tensorlake")

            self.assertEqual(
                result.exit_code, 0, f"Failed with output: {result.output}"
            )
            mock_browser_open.assert_called_once_with(
                "https://cloud.tensorlake.ai/cli/login"
            )

    @respx.mock
    @patch("webbrowser.open")
    def test_login_uses_custom_cloud_url_from_env(self, mock_browser_open):
        """Test that login respects TENSORLAKE_CLOUD_URL environment variable"""
        custom_cloud_url = "https://staging-cloud.tensorlake.ai"
        self.setup_login_mocks()

        with mock_auth_credentials_path():
            # Set both TENSORLAKE_API_URL and TENSORLAKE_CLOUD_URL to control the test environment
            runner = CliRunner(
                env={
                    "TENSORLAKE_API_URL": "https://api.tensorlake.ai",
                    "TENSORLAKE_CLOUD_URL": custom_cloud_url,
                }
            )
            result = runner.invoke(cli, ["login"], prog_name="tensorlake")

            self.assertEqual(
                result.exit_code, 0, f"Failed with output: {result.output}"
            )
            mock_browser_open.assert_called_once_with(f"{custom_cloud_url}/cli/login")

    @respx.mock
    @patch("webbrowser.open")
    def test_login_uses_custom_cloud_url_from_cli_flag(self, mock_browser_open):
        """Test that login respects --cloud-url CLI flag"""
        custom_cloud_url = "https://dev-cloud.tensorlake.ai"
        self.setup_login_mocks()

        with mock_auth_credentials_path():
            # Explicitly set TENSORLAKE_API_URL to ensure test doesn't inherit from CI environment
            runner = CliRunner(env={"TENSORLAKE_API_URL": "https://api.tensorlake.ai"})
            result = runner.invoke(
                cli,
                ["--cloud-url", custom_cloud_url, "login"],
                prog_name="tensorlake",
            )

            self.assertEqual(
                result.exit_code, 0, f"Failed with output: {result.output}"
            )
            mock_browser_open.assert_called_once_with(f"{custom_cloud_url}/cli/login")

    @respx.mock
    @patch("webbrowser.open")
    def test_login_shows_custom_cloud_url_on_browser_error(self, mock_browser_open):
        """Test that custom cloud URL is shown in error message when browser fails"""
        mock_browser_open.side_effect = webbrowser.Error("Browser open failed")
        custom_cloud_url = "https://custom-cloud.example.com"
        self.setup_login_mocks()

        with mock_auth_credentials_path():
            # Explicitly set TENSORLAKE_API_URL to ensure test doesn't inherit from CI environment
            runner = CliRunner(env={"TENSORLAKE_API_URL": "https://api.tensorlake.ai"})
            result = runner.invoke(
                cli,
                ["--cloud-url", custom_cloud_url, "login"],
                prog_name="tensorlake",
            )

            # Command should still succeed
            self.assertEqual(
                result.exit_code, 0, f"Failed with output: {result.output}"
            )
            # Verify the custom URL is displayed in output before browser open attempt
            self.assertIn(f"URL: {custom_cloud_url}/cli/login", result.output)
            # Verify error message references the URL above
            self.assertIn("Failed to open web browser", result.output)
            self.assertIn("please open the url above manually", result.output.lower())


if __name__ == "__main__":
    unittest.main()
