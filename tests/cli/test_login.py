"""Tests for the login command"""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
import respx
from click.testing import CliRunner
from tomlkit import parse

import tensorlake.cli._configuration as config_module
from tensorlake.cli import cli


class TestLoginSuccessFlow(unittest.TestCase):
    """Test successful login flow"""

    def setUp(self):
        """Set up test environment with temporary config directories"""
        self.tmpdir = tempfile.TemporaryDirectory()
        config_dir = Path(self.tmpdir.name) / ".config" / "tensorlake"
        config_dir.mkdir(parents=True)
        credentials_path = config_dir / "credentials.toml"
        local_config_path = (
            Path(self.tmpdir.name) / "project" / ".tensorlake" / "config.toml"
        )
        local_config_path.parent.mkdir(parents=True, exist_ok=True)

        # Create local config to skip auto-init
        with open(local_config_path, "w") as f:
            f.write('organization = "test_org"\nproject = "test_proj"\n')

        # Save original paths
        self.original_credentials_path = config_module.CREDENTIALS_PATH
        self.original_config_dir = config_module.CONFIG_DIR
        self.original_local_config_file = config_module.LOCAL_CONFIG_FILE

        # Set to test paths
        config_module.CREDENTIALS_PATH = credentials_path
        config_module.CONFIG_DIR = config_dir
        config_module.LOCAL_CONFIG_FILE = local_config_path

        self.credentials_path = credentials_path

    def tearDown(self):
        """Restore original configuration"""
        config_module.CREDENTIALS_PATH = self.original_credentials_path
        config_module.CONFIG_DIR = self.original_config_dir
        config_module.LOCAL_CONFIG_FILE = self.original_local_config_file
        self.tmpdir.cleanup()

    @respx.mock
    def test_successful_login_flow(self):
        """Test complete successful login flow"""
        # Mock login start
        respx.post("https://api.tensorlake.ai/platform/cli/login/start").mock(
            return_value=httpx.Response(
                200,
                json={"device_code": "test_device_123", "user_code": "ABCD-1234"},
            )
        )

        # Mock poll with immediate success
        respx.get(
            "https://api.tensorlake.ai/platform/cli/login/poll?device_code=test_device_123"
        ).mock(
            return_value=httpx.Response(
                200,
                json={"status": "approved"},
            )
        )

        # Mock token exchange
        respx.post("https://api.tensorlake.ai/platform/cli/login/exchange").mock(
            return_value=httpx.Response(
                200,
                json={"access_token": "test_access_token_xyz"},
            )
        )

        runner = CliRunner()

        with patch("webbrowser.open") as mock_browser, patch("time.sleep"):
            result = runner.invoke(cli, ["login"], prog_name="tensorlake")

        # Verify command succeeded
        self.assertEqual(result.exit_code, 0, f"Failed: {result.output}")

        # Verify output messages
        self.assertIn("ABCD-1234", result.output)  # User code displayed
        self.assertIn("Login successful", result.output)

        # Verify browser was opened
        mock_browser.assert_called_once_with("https://cloud.tensorlake.ai/cli/login")

        # Verify credentials were saved
        self.assertTrue(self.credentials_path.exists())
        with open(self.credentials_path, "r") as f:
            credentials = parse(f.read())
            self.assertEqual(
                credentials["https://api.tensorlake.ai"]["token"],
                "test_access_token_xyz",
            )

    @respx.mock
    def test_login_displays_user_code(self):
        """Test that user code is displayed during login"""
        respx.post("https://api.tensorlake.ai/platform/cli/login/start").mock(
            return_value=httpx.Response(
                200,
                json={"device_code": "device_code", "user_code": "TEST-9999"},
            )
        )

        respx.get(
            "https://api.tensorlake.ai/platform/cli/login/poll?device_code=device_code"
        ).mock(return_value=httpx.Response(200, json={"status": "approved"}))

        respx.post("https://api.tensorlake.ai/platform/cli/login/exchange").mock(
            return_value=httpx.Response(200, json={"access_token": "token"})
        )

        runner = CliRunner()

        with patch("webbrowser.open"), patch("time.sleep"):
            result = runner.invoke(cli, ["login"], prog_name="tensorlake")

        self.assertEqual(result.exit_code, 0)
        self.assertIn("TEST-9999", result.output)
        self.assertIn("Your code is:", result.output)

    @respx.mock
    def test_login_polls_until_approved(self):
        """Test that login polls multiple times before approval"""
        respx.post("https://api.tensorlake.ai/platform/cli/login/start").mock(
            return_value=httpx.Response(
                200,
                json={"device_code": "device_code", "user_code": "CODE-1234"},
            )
        )

        # Mock poll responses: pending, pending, then approved
        poll_responses = [
            httpx.Response(200, json={"status": "pending"}),
            httpx.Response(200, json={"status": "pending"}),
            httpx.Response(200, json={"status": "approved"}),
        ]
        respx.get(
            "https://api.tensorlake.ai/platform/cli/login/poll?device_code=device_code"
        ).mock(side_effect=poll_responses)

        respx.post("https://api.tensorlake.ai/platform/cli/login/exchange").mock(
            return_value=httpx.Response(200, json={"access_token": "token"})
        )

        runner = CliRunner()

        with patch("webbrowser.open"), patch("time.sleep") as mock_sleep:
            result = runner.invoke(cli, ["login"], prog_name="tensorlake")

        self.assertEqual(result.exit_code, 0)
        self.assertIn("Login successful", result.output)
        # Verify sleep was called for pending responses
        self.assertGreaterEqual(mock_sleep.call_count, 2)


class TestLoginErrorHandling(unittest.TestCase):
    """Test error handling during login"""

    def setUp(self):
        """Set up test environment"""
        self.tmpdir = tempfile.TemporaryDirectory()
        config_dir = Path(self.tmpdir.name) / ".config" / "tensorlake"
        config_dir.mkdir(parents=True)
        credentials_path = config_dir / "credentials.toml"
        local_config_dir = Path(self.tmpdir.name) / ".tensorlake"
        local_config_dir.mkdir(parents=True, exist_ok=True)
        local_config_path = local_config_dir / "config.toml"

        self.original_credentials_path = config_module.CREDENTIALS_PATH
        self.original_config_dir = config_module.CONFIG_DIR
        self.original_local_config_file = config_module.LOCAL_CONFIG_FILE

        config_module.CREDENTIALS_PATH = credentials_path
        config_module.CONFIG_DIR = config_dir
        config_module.LOCAL_CONFIG_FILE = local_config_path

    def tearDown(self):
        """Restore original configuration"""
        config_module.CREDENTIALS_PATH = self.original_credentials_path
        config_module.CONFIG_DIR = self.original_config_dir
        config_module.LOCAL_CONFIG_FILE = self.original_local_config_file
        self.tmpdir.cleanup()

    @respx.mock
    def test_login_start_failure(self):
        """Test login failure when start endpoint fails"""
        respx.post("https://api.tensorlake.ai/platform/cli/login/start").mock(
            return_value=httpx.Response(500, text="Internal server error")
        )

        runner = CliRunner()
        result = runner.invoke(cli, ["login"], prog_name="tensorlake")

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Failed to start login process", result.output)

    @respx.mock
    def test_login_poll_failure(self):
        """Test login failure when poll endpoint fails"""
        respx.post("https://api.tensorlake.ai/platform/cli/login/start").mock(
            return_value=httpx.Response(
                200,
                json={"device_code": "device_code", "user_code": "CODE"},
            )
        )

        respx.get(
            "https://api.tensorlake.ai/platform/cli/login/poll?device_code=device_code"
        ).mock(return_value=httpx.Response(500, text="Internal server error"))

        runner = CliRunner()

        with patch("webbrowser.open"), patch("time.sleep"):
            result = runner.invoke(cli, ["login"], prog_name="tensorlake")

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Failed to poll login status", result.output)

    @respx.mock
    def test_login_exchange_failure(self):
        """Test login failure when token exchange fails"""
        respx.post("https://api.tensorlake.ai/platform/cli/login/start").mock(
            return_value=httpx.Response(
                200,
                json={"device_code": "device_code", "user_code": "CODE"},
            )
        )

        respx.get(
            "https://api.tensorlake.ai/platform/cli/login/poll?device_code=device_code"
        ).mock(return_value=httpx.Response(200, json={"status": "approved"}))

        respx.post("https://api.tensorlake.ai/platform/cli/login/exchange").mock(
            return_value=httpx.Response(500, text="Internal server error")
        )

        runner = CliRunner()

        with patch("webbrowser.open"), patch("time.sleep"):
            result = runner.invoke(cli, ["login"], prog_name="tensorlake")

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Failed to exchange token", result.output)

    @respx.mock
    def test_login_expired(self):
        """Test login failure when request expires"""
        respx.post("https://api.tensorlake.ai/platform/cli/login/start").mock(
            return_value=httpx.Response(
                200,
                json={"device_code": "device_code", "user_code": "CODE"},
            )
        )

        respx.get(
            "https://api.tensorlake.ai/platform/cli/login/poll?device_code=device_code"
        ).mock(return_value=httpx.Response(200, json={"status": "expired"}))

        runner = CliRunner()

        with patch("webbrowser.open"), patch("time.sleep"):
            result = runner.invoke(cli, ["login"], prog_name="tensorlake")

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Login request has expired", result.output)
        self.assertIn("Please try again", result.output)

    @respx.mock
    def test_login_failed(self):
        """Test login failure when request fails"""
        respx.post("https://api.tensorlake.ai/platform/cli/login/start").mock(
            return_value=httpx.Response(
                200,
                json={"device_code": "device_code", "user_code": "CODE"},
            )
        )

        respx.get(
            "https://api.tensorlake.ai/platform/cli/login/poll?device_code=device_code"
        ).mock(return_value=httpx.Response(200, json={"status": "failed"}))

        runner = CliRunner()

        with patch("webbrowser.open"), patch("time.sleep"):
            result = runner.invoke(cli, ["login"], prog_name="tensorlake")

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Login request has failed", result.output)
        self.assertIn("Please try again", result.output)

    @respx.mock
    def test_login_unknown_status(self):
        """Test login failure with unknown status from poll endpoint"""
        respx.post("https://api.tensorlake.ai/platform/cli/login/start").mock(
            return_value=httpx.Response(
                200,
                json={"device_code": "device_code", "user_code": "CODE"},
            )
        )

        respx.get(
            "https://api.tensorlake.ai/platform/cli/login/poll?device_code=device_code"
        ).mock(return_value=httpx.Response(200, json={"status": "unknown_status"}))

        runner = CliRunner()

        with patch("webbrowser.open"), patch("time.sleep"):
            result = runner.invoke(cli, ["login"], prog_name="tensorlake")

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Unknown status", result.output)


class TestLoginBrowserHandling(unittest.TestCase):
    """Test browser opening during login"""

    def setUp(self):
        """Set up test environment"""
        self.tmpdir = tempfile.TemporaryDirectory()
        config_dir = Path(self.tmpdir.name) / ".config" / "tensorlake"
        config_dir.mkdir(parents=True)
        credentials_path = config_dir / "credentials.toml"
        local_config_dir = Path(self.tmpdir.name) / ".tensorlake"
        local_config_dir.mkdir(parents=True, exist_ok=True)
        local_config_path = local_config_dir / "config.toml"

        # Create local config to skip auto-init
        with open(local_config_path, "w") as f:
            f.write('organization = "test_org"\nproject = "test_proj"\n')

        self.original_credentials_path = config_module.CREDENTIALS_PATH
        self.original_config_dir = config_module.CONFIG_DIR
        self.original_local_config_file = config_module.LOCAL_CONFIG_FILE

        config_module.CREDENTIALS_PATH = credentials_path
        config_module.CONFIG_DIR = config_dir
        config_module.LOCAL_CONFIG_FILE = local_config_path

    def tearDown(self):
        """Restore original configuration"""
        config_module.CREDENTIALS_PATH = self.original_credentials_path
        config_module.CONFIG_DIR = self.original_config_dir
        config_module.LOCAL_CONFIG_FILE = self.original_local_config_file
        self.tmpdir.cleanup()

    @respx.mock
    def test_browser_open_failure_shows_manual_url(self):
        """Test that manual URL is displayed when browser fails to open"""
        respx.post("https://api.tensorlake.ai/platform/cli/login/start").mock(
            return_value=httpx.Response(
                200,
                json={"device_code": "device_code", "user_code": "MANUAL-CODE"},
            )
        )

        respx.get(
            "https://api.tensorlake.ai/platform/cli/login/poll?device_code=device_code"
        ).mock(return_value=httpx.Response(200, json={"status": "approved"}))

        respx.post("https://api.tensorlake.ai/platform/cli/login/exchange").mock(
            return_value=httpx.Response(200, json={"access_token": "token"})
        )

        runner = CliRunner()

        # Mock browser.open to raise an error
        import webbrowser

        with patch("webbrowser.open", side_effect=webbrowser.Error("No browser")):
            with patch("time.sleep"):
                result = runner.invoke(cli, ["login"], prog_name="tensorlake")

        self.assertEqual(result.exit_code, 0)
        self.assertIn("Failed to open web browser", result.output)
        self.assertIn("please open the following url manually", result.output.lower())
        self.assertIn("https://cloud.tensorlake.ai/cli/login", result.output)
        self.assertIn("MANUAL-CODE", result.output)

    @respx.mock
    def test_browser_opens_with_correct_url(self):
        """Test that browser opens with correct verification URL"""
        respx.post("https://api.tensorlake.ai/platform/cli/login/start").mock(
            return_value=httpx.Response(
                200,
                json={"device_code": "device_code", "user_code": "CODE"},
            )
        )

        respx.get(
            "https://api.tensorlake.ai/platform/cli/login/poll?device_code=device_code"
        ).mock(return_value=httpx.Response(200, json={"status": "approved"}))

        respx.post("https://api.tensorlake.ai/platform/cli/login/exchange").mock(
            return_value=httpx.Response(200, json={"access_token": "token"})
        )

        runner = CliRunner()

        with patch("webbrowser.open") as mock_browser, patch("time.sleep"):
            result = runner.invoke(cli, ["login"], prog_name="tensorlake")

        self.assertEqual(result.exit_code, 0)
        mock_browser.assert_called_once_with("https://cloud.tensorlake.ai/cli/login")


class TestLoginCredentialStorage(unittest.TestCase):
    """Test credential storage after successful login"""

    def setUp(self):
        """Set up test environment"""
        self.tmpdir = tempfile.TemporaryDirectory()
        config_dir = Path(self.tmpdir.name) / ".config" / "tensorlake"
        config_dir.mkdir(parents=True)
        self.credentials_path = config_dir / "credentials.toml"
        local_config_dir = Path(self.tmpdir.name) / ".tensorlake"
        local_config_dir.mkdir(parents=True, exist_ok=True)
        local_config_path = local_config_dir / "config.toml"

        # Create local config to skip auto-init
        with open(local_config_path, "w") as f:
            f.write('organization = "test_org"\nproject = "test_proj"\n')

        self.original_credentials_path = config_module.CREDENTIALS_PATH
        self.original_config_dir = config_module.CONFIG_DIR
        self.original_local_config_file = config_module.LOCAL_CONFIG_FILE

        config_module.CREDENTIALS_PATH = self.credentials_path
        config_module.CONFIG_DIR = config_dir
        config_module.LOCAL_CONFIG_FILE = local_config_path

    def tearDown(self):
        """Restore original configuration"""
        config_module.CREDENTIALS_PATH = self.original_credentials_path
        config_module.CONFIG_DIR = self.original_config_dir
        config_module.LOCAL_CONFIG_FILE = self.original_local_config_file
        self.tmpdir.cleanup()

    @respx.mock
    def test_credentials_saved_in_correct_format(self):
        """Test that credentials are saved in endpoint-scoped TOML format"""
        test_token = "test_access_token_12345"

        respx.post("https://api.tensorlake.ai/platform/cli/login/start").mock(
            return_value=httpx.Response(
                200,
                json={"device_code": "device_code", "user_code": "CODE"},
            )
        )

        respx.get(
            "https://api.tensorlake.ai/platform/cli/login/poll?device_code=device_code"
        ).mock(return_value=httpx.Response(200, json={"status": "approved"}))

        respx.post("https://api.tensorlake.ai/platform/cli/login/exchange").mock(
            return_value=httpx.Response(200, json={"access_token": test_token})
        )

        runner = CliRunner()

        with patch("webbrowser.open"), patch("time.sleep"):
            result = runner.invoke(cli, ["login"], prog_name="tensorlake")

        self.assertEqual(result.exit_code, 0)

        # Verify credentials file format
        self.assertTrue(self.credentials_path.exists())
        with open(self.credentials_path, "r") as f:
            credentials = parse(f.read())

        # Check endpoint-scoped format
        self.assertIn("https://api.tensorlake.ai", credentials)
        self.assertEqual(credentials["https://api.tensorlake.ai"]["token"], test_token)

    @respx.mock
    def test_credentials_file_has_secure_permissions(self):
        """Test that credentials file has restrictive permissions (0600)"""
        import os
        import stat

        respx.post("https://api.tensorlake.ai/platform/cli/login/start").mock(
            return_value=httpx.Response(
                200,
                json={"device_code": "device_code", "user_code": "CODE"},
            )
        )

        respx.get(
            "https://api.tensorlake.ai/platform/cli/login/poll?device_code=device_code"
        ).mock(return_value=httpx.Response(200, json={"status": "approved"}))

        respx.post("https://api.tensorlake.ai/platform/cli/login/exchange").mock(
            return_value=httpx.Response(200, json={"access_token": "token"})
        )

        runner = CliRunner()

        with patch("webbrowser.open"), patch("time.sleep"):
            result = runner.invoke(cli, ["login"], prog_name="tensorlake")

        self.assertEqual(result.exit_code, 0)

        # Check file permissions
        file_mode = os.stat(self.credentials_path).st_mode
        # Check that only owner has read/write permissions (0600)
        self.assertEqual(stat.S_IMODE(file_mode), 0o600)

    @respx.mock
    def test_credentials_can_be_loaded_after_save(self):
        """Test that saved credentials can be loaded by Context"""
        test_token = "test_token_xyz_789"

        respx.post("https://api.tensorlake.ai/platform/cli/login/start").mock(
            return_value=httpx.Response(
                200,
                json={"device_code": "device_code", "user_code": "CODE"},
            )
        )

        respx.get(
            "https://api.tensorlake.ai/platform/cli/login/poll?device_code=device_code"
        ).mock(return_value=httpx.Response(200, json={"status": "approved"}))

        respx.post("https://api.tensorlake.ai/platform/cli/login/exchange").mock(
            return_value=httpx.Response(200, json={"access_token": test_token})
        )

        runner = CliRunner()

        with patch("webbrowser.open"), patch("time.sleep"):
            result = runner.invoke(cli, ["login"], prog_name="tensorlake")

        self.assertEqual(result.exit_code, 0)

        # Verify credentials can be loaded
        from tensorlake.cli._configuration import load_credentials

        loaded_token = load_credentials("https://api.tensorlake.ai")
        self.assertEqual(loaded_token, test_token)


if __name__ == "__main__":
    unittest.main()
