"""Tests for automatic authentication and configuration flow"""
import tempfile
import unittest
from pathlib import Path

import httpx
import respx
from click.testing import CliRunner
from tomlkit import dumps

import tensorlake.cli._configuration as config_module
from tensorlake.cli import cli


class TestAutoInitFlow(unittest.TestCase):
    """Test that commands automatically trigger init when needed"""

    @respx.mock
    def test_secrets_list_auto_init_with_pat(self):
        """Test that secrets list automatically runs init if PAT exists but no local config"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Setup paths
            config_dir = Path(tmpdir) / ".config" / "tensorlake"
            config_dir.mkdir(parents=True)
            credentials_path = config_dir / "credentials.toml"
            local_config_path = Path(tmpdir) / "project" / ".tensorlake.toml"
            local_config_path.parent.mkdir(parents=True)

            original_config_dir = config_module.CONFIG_DIR
            original_credentials_path = config_module.CREDENTIALS_PATH
            original_local_config = config_module.LOCAL_CONFIG_FILE

            try:
                config_module.CONFIG_DIR = config_dir
                config_module.CREDENTIALS_PATH = credentials_path
                config_module.LOCAL_CONFIG_FILE = local_config_path

                # Save credentials (PAT)
                from tensorlake.cli._configuration import save_credentials

                save_credentials("https://api.tensorlake.ai", "test_pat")

                # Mock init flow API calls
                respx.get("https://api.tensorlake.ai/platform/v1/organizations").mock(
                    return_value=httpx.Response(
                        200,
                        json={"items": [{"id": "org_123", "name": "Test Org"}]},
                    )
                )

                respx.get(
                    "https://api.tensorlake.ai/platform/v1/organizations/org_123/projects"
                ).mock(
                    return_value=httpx.Response(
                        200,
                        json={"items": [{"id": "proj_456", "name": "Test Project"}]},
                    )
                )

                # Mock secrets API call (the actual command after init completes)
                respx.get(
                    "https://api.tensorlake.ai/platform/v1/organizations/org_123/projects/proj_456/secrets?pageSize=100"
                ).mock(
                    return_value=httpx.Response(
                        200,
                        json={"items": []},
                    )
                )

                runner = CliRunner()

                # Mock find_project_root to return the temp directory
                from unittest.mock import patch

                with patch(
                    "tensorlake.cli._project_detection.find_project_root",
                    return_value=local_config_path.parent,
                ):
                    result = runner.invoke(
                        cli, ["secrets", "list"], prog_name="tensorlake"
                    )

                # Should succeed after auto-init
                self.assertEqual(result.exit_code, 0, f"Failed: {result.output}")
                self.assertIn("Running initialization flow", result.output)
                self.assertIn("Test Org", result.output)
                self.assertIn("Test Project", result.output)
                self.assertIn("Configuration saved", result.output)

                # Verify local config was created
                self.assertTrue(local_config_path.exists())
                with open(local_config_path, "r") as f:
                    content = f.read()
                    self.assertIn("org_123", content)
                    self.assertIn("proj_456", content)

            finally:
                config_module.CONFIG_DIR = original_config_dir
                config_module.CREDENTIALS_PATH = original_credentials_path
                config_module.LOCAL_CONFIG_FILE = original_local_config

    @respx.mock
    def test_command_auto_login_without_auth(self):
        """Test that commands automatically trigger login flow when no authentication exists"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Setup paths with no credentials
            config_dir = Path(tmpdir) / ".config" / "tensorlake"
            config_dir.mkdir(parents=True)
            credentials_path = config_dir / "credentials.toml"
            local_config_path = Path(tmpdir) / "project" / ".tensorlake.toml"
            local_config_path.parent.mkdir(parents=True)

            original_config_dir = config_module.CONFIG_DIR
            original_credentials_path = config_module.CREDENTIALS_PATH
            original_local_config = config_module.LOCAL_CONFIG_FILE

            try:
                config_module.CONFIG_DIR = config_dir
                config_module.CREDENTIALS_PATH = credentials_path
                config_module.LOCAL_CONFIG_FILE = local_config_path

                # Mock login flow
                respx.post("https://api.tensorlake.ai/platform/cli/login/start").mock(
                    return_value=httpx.Response(
                        200,
                        json={"device_code": "test_device", "user_code": "TEST123"},
                    )
                )

                respx.get(
                    "https://api.tensorlake.ai/platform/cli/login/poll?device_code=test_device"
                ).mock(
                    return_value=httpx.Response(
                        200,
                        json={"status": "approved"},
                    )
                )

                respx.post(
                    "https://api.tensorlake.ai/platform/cli/login/exchange"
                ).mock(
                    return_value=httpx.Response(
                        200,
                        json={"access_token": "test_token"},
                    )
                )

                # Mock init flow
                respx.get("https://api.tensorlake.ai/platform/v1/organizations").mock(
                    return_value=httpx.Response(
                        200,
                        json={"items": [{"id": "org_auto", "name": "Auto Org"}]},
                    )
                )

                respx.get(
                    "https://api.tensorlake.ai/platform/v1/organizations/org_auto/projects"
                ).mock(
                    return_value=httpx.Response(
                        200,
                        json={"items": [{"id": "proj_auto", "name": "Auto Project"}]},
                    )
                )

                # Mock secrets API call (the actual command after login/init completes)
                respx.get(
                    "https://api.tensorlake.ai/platform/v1/organizations/org_auto/projects/proj_auto/secrets?pageSize=100"
                ).mock(
                    return_value=httpx.Response(
                        200,
                        json={"items": []},
                    )
                )

                runner = CliRunner()
                from unittest.mock import patch

                with patch("webbrowser.open"), patch(
                    "tensorlake.cli._project_detection.find_project_root",
                    return_value=local_config_path.parent,
                ):
                    result = runner.invoke(
                        cli, ["secrets", "list"], prog_name="tensorlake"
                    )

                # Should succeed after auto-login and auto-init
                self.assertEqual(result.exit_code, 0, f"Failed: {result.output}")
                self.assertIn("It seems like you're not logged in", result.output)
                self.assertIn("Let's log you in", result.output)
                self.assertIn("Login successful", result.output)
                self.assertIn("Configuration saved", result.output)

                # Verify credentials and local config were created
                self.assertTrue(credentials_path.exists())
                self.assertTrue(local_config_path.exists())

            finally:
                config_module.CONFIG_DIR = original_config_dir
                config_module.CREDENTIALS_PATH = original_credentials_path
                config_module.LOCAL_CONFIG_FILE = original_local_config

    def test_command_succeeds_with_local_config(self):
        """Test that commands succeed when local config exists"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Setup paths
            config_dir = Path(tmpdir) / ".config" / "tensorlake"
            config_dir.mkdir(parents=True)
            credentials_path = config_dir / "credentials.toml"
            local_config_path = Path(tmpdir) / "project" / ".tensorlake.toml"
            local_config_path.parent.mkdir(parents=True)

            original_config_dir = config_module.CONFIG_DIR
            original_credentials_path = config_module.CREDENTIALS_PATH
            original_local_config = config_module.LOCAL_CONFIG_FILE

            try:
                config_module.CONFIG_DIR = config_dir
                config_module.CREDENTIALS_PATH = credentials_path
                config_module.LOCAL_CONFIG_FILE = local_config_path

                # Save credentials and local config
                from tensorlake.cli._configuration import (
                    save_credentials,
                    save_local_config,
                )

                save_credentials("https://api.tensorlake.ai", "test_pat")
                save_local_config(
                    {"organization": "org_999", "project": "proj_888"},
                    local_config_path.parent,
                )

                # Mock secrets API call
                import respx

                respx.get(
                    "https://api.tensorlake.ai/platform/v1/organizations/org_999/projects/proj_888/secrets?pageSize=100"
                ).mock(
                    return_value=httpx.Response(
                        200,
                        json={"items": []},
                    )
                )

                runner = CliRunner()
                with respx.mock:
                    result = runner.invoke(
                        cli, ["secrets", "list"], prog_name="tensorlake"
                    )

                # Should succeed without init flow
                self.assertEqual(result.exit_code, 0, f"Failed: {result.output}")
                self.assertNotIn("Running initialization flow", result.output)
                self.assertIn("No secrets found", result.output)

            finally:
                config_module.CONFIG_DIR = original_config_dir
                config_module.CREDENTIALS_PATH = original_credentials_path
                config_module.LOCAL_CONFIG_FILE = original_local_config

    def test_command_succeeds_with_cli_flags(self):
        """Test that commands succeed with --organization and --project flags (no local config needed)"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Setup paths
            config_dir = Path(tmpdir) / ".config" / "tensorlake"
            config_dir.mkdir(parents=True)
            credentials_path = config_dir / "credentials.toml"
            local_config_path = Path(tmpdir) / "project" / ".tensorlake.toml"
            local_config_path.parent.mkdir(parents=True)

            original_config_dir = config_module.CONFIG_DIR
            original_credentials_path = config_module.CREDENTIALS_PATH
            original_local_config = config_module.LOCAL_CONFIG_FILE

            try:
                config_module.CONFIG_DIR = config_dir
                config_module.CREDENTIALS_PATH = credentials_path
                config_module.LOCAL_CONFIG_FILE = local_config_path

                # Save credentials (no local config)
                from tensorlake.cli._configuration import save_credentials

                save_credentials("https://api.tensorlake.ai", "test_pat")

                # Mock secrets API call
                import respx

                respx.get(
                    "https://api.tensorlake.ai/platform/v1/organizations/cli_org/projects/cli_proj/secrets?pageSize=100"
                ).mock(
                    return_value=httpx.Response(
                        200,
                        json={"items": []},
                    )
                )

                runner = CliRunner()
                with respx.mock:
                    result = runner.invoke(
                        cli,
                        [
                            "--organization",
                            "cli_org",
                            "--project",
                            "cli_proj",
                            "secrets",
                            "list",
                        ],
                        prog_name="tensorlake",
                    )

                # Should succeed without init flow
                self.assertEqual(result.exit_code, 0, f"Failed: {result.output}")
                self.assertNotIn("Running initialization flow", result.output)
                self.assertIn("No secrets found", result.output)

                # Local config should NOT be created
                self.assertFalse(local_config_path.exists())

            finally:
                config_module.CONFIG_DIR = original_config_dir
                config_module.CREDENTIALS_PATH = original_credentials_path
                config_module.LOCAL_CONFIG_FILE = original_local_config


class TestLoginInitChaining(unittest.TestCase):
    """Test that login automatically chains to init"""

    @respx.mock
    def test_login_skips_init_with_env_vars(self):
        """Test that login doesn't run init if org/project provided via env vars"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Setup paths
            config_dir = Path(tmpdir) / ".config" / "tensorlake"
            config_dir.mkdir(parents=True)
            credentials_path = config_dir / "credentials.toml"
            local_config_path = Path(tmpdir) / "project" / ".tensorlake.toml"
            local_config_path.parent.mkdir(parents=True)

            original_config_dir = config_module.CONFIG_DIR
            original_credentials_path = config_module.CREDENTIALS_PATH
            original_local_config = config_module.LOCAL_CONFIG_FILE

            try:
                config_module.CONFIG_DIR = config_dir
                config_module.CREDENTIALS_PATH = credentials_path
                config_module.LOCAL_CONFIG_FILE = local_config_path

                # Mock login flow
                respx.post("https://api.tensorlake.ai/platform/cli/login/start").mock(
                    return_value=httpx.Response(
                        200,
                        json={"device_code": "test_device", "user_code": "TEST123"},
                    )
                )

                respx.get(
                    "https://api.tensorlake.ai/platform/cli/login/poll?device_code=test_device"
                ).mock(
                    return_value=httpx.Response(
                        200,
                        json={"status": "approved"},
                    )
                )

                respx.post(
                    "https://api.tensorlake.ai/platform/cli/login/exchange"
                ).mock(
                    return_value=httpx.Response(
                        200,
                        json={"access_token": "test_token"},
                    )
                )

                # Provide org/project via env vars
                runner = CliRunner(
                    env={
                        "TENSORLAKE_ORGANIZATION_ID": "env_org",
                        "TENSORLAKE_PROJECT_ID": "env_proj",
                    }
                )

                from unittest.mock import patch

                with patch("webbrowser.open"):
                    result = runner.invoke(cli, ["login"], prog_name="tensorlake")

                # Should succeed without init flow
                self.assertEqual(result.exit_code, 0, f"Failed: {result.output}")
                self.assertIn("Login successful", result.output)
                self.assertNotIn("Let's set up your project", result.output)
                self.assertNotIn("Initializing TensorLake", result.output)

                # Local config should NOT be created
                self.assertFalse(local_config_path.exists())

            finally:
                config_module.CONFIG_DIR = original_config_dir
                config_module.CREDENTIALS_PATH = original_credentials_path
                config_module.LOCAL_CONFIG_FILE = original_local_config

    @respx.mock
    def test_login_skips_init_with_cli_flags(self):
        """Test that login doesn't run init if org/project provided via CLI flags"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Setup paths
            config_dir = Path(tmpdir) / ".config" / "tensorlake"
            config_dir.mkdir(parents=True)
            credentials_path = config_dir / "credentials.toml"
            local_config_path = Path(tmpdir) / "project" / ".tensorlake.toml"
            local_config_path.parent.mkdir(parents=True)

            original_config_dir = config_module.CONFIG_DIR
            original_credentials_path = config_module.CREDENTIALS_PATH
            original_local_config = config_module.LOCAL_CONFIG_FILE

            try:
                config_module.CONFIG_DIR = config_dir
                config_module.CREDENTIALS_PATH = credentials_path
                config_module.LOCAL_CONFIG_FILE = local_config_path

                # Mock login flow
                respx.post("https://api.tensorlake.ai/platform/cli/login/start").mock(
                    return_value=httpx.Response(
                        200,
                        json={"device_code": "test_device", "user_code": "TEST123"},
                    )
                )

                respx.get(
                    "https://api.tensorlake.ai/platform/cli/login/poll?device_code=test_device"
                ).mock(
                    return_value=httpx.Response(
                        200,
                        json={"status": "approved"},
                    )
                )

                respx.post(
                    "https://api.tensorlake.ai/platform/cli/login/exchange"
                ).mock(
                    return_value=httpx.Response(
                        200,
                        json={"access_token": "test_token"},
                    )
                )

                # Provide org/project via CLI flags
                runner = CliRunner()

                from unittest.mock import patch

                with patch("webbrowser.open"):
                    result = runner.invoke(
                        cli,
                        [
                            "--organization",
                            "cli_org",
                            "--project",
                            "cli_proj",
                            "login",
                        ],
                        prog_name="tensorlake",
                    )

                # Should succeed without init flow
                self.assertEqual(result.exit_code, 0, f"Failed: {result.output}")
                self.assertIn("Login successful", result.output)
                self.assertNotIn("Let's set up your project", result.output)
                self.assertNotIn("Initializing TensorLake", result.output)

                # Local config should NOT be created
                self.assertFalse(local_config_path.exists())

            finally:
                config_module.CONFIG_DIR = original_config_dir
                config_module.CREDENTIALS_PATH = original_credentials_path
                config_module.LOCAL_CONFIG_FILE = original_local_config

    @respx.mock
    def test_login_chains_to_init_when_no_local_config(self):
        """Test that successful login automatically runs init if no local config exists"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Setup paths
            config_dir = Path(tmpdir) / ".config" / "tensorlake"
            config_dir.mkdir(parents=True)
            credentials_path = config_dir / "credentials.toml"
            local_config_path = Path(tmpdir) / "project" / ".tensorlake.toml"
            local_config_path.parent.mkdir(parents=True)

            original_config_dir = config_module.CONFIG_DIR
            original_credentials_path = config_module.CREDENTIALS_PATH
            original_local_config = config_module.LOCAL_CONFIG_FILE

            try:
                config_module.CONFIG_DIR = config_dir
                config_module.CREDENTIALS_PATH = credentials_path
                config_module.LOCAL_CONFIG_FILE = local_config_path

                # Mock login flow
                respx.post("https://api.tensorlake.ai/platform/cli/login/start").mock(
                    return_value=httpx.Response(
                        200,
                        json={"device_code": "test_device", "user_code": "TEST123"},
                    )
                )

                respx.get(
                    "https://api.tensorlake.ai/platform/cli/login/poll?device_code=test_device"
                ).mock(
                    return_value=httpx.Response(
                        200,
                        json={"status": "approved"},
                    )
                )

                respx.post(
                    "https://api.tensorlake.ai/platform/cli/login/exchange"
                ).mock(
                    return_value=httpx.Response(
                        200,
                        json={"access_token": "test_token"},
                    )
                )

                # Mock init flow
                respx.get("https://api.tensorlake.ai/platform/v1/organizations").mock(
                    return_value=httpx.Response(
                        200,
                        json={"items": [{"id": "org_auto", "name": "Auto Org"}]},
                    )
                )

                respx.get(
                    "https://api.tensorlake.ai/platform/v1/organizations/org_auto/projects"
                ).mock(
                    return_value=httpx.Response(
                        200,
                        json={"items": [{"id": "proj_auto", "name": "Auto Project"}]},
                    )
                )

                runner = CliRunner()
                from unittest.mock import patch

                with patch("webbrowser.open"), patch(
                    "tensorlake.cli._project_detection.find_project_root",
                    return_value=local_config_path.parent,
                ):
                    result = runner.invoke(cli, ["login"], prog_name="tensorlake")

                # Should succeed and show both login and init messages
                self.assertEqual(result.exit_code, 0, f"Failed: {result.output}")
                self.assertIn("Login successful", result.output)
                self.assertIn(
                    "No organization and project configuration found", result.output
                )
                self.assertIn("Auto Org", result.output)
                self.assertIn("Auto Project", result.output)
                self.assertIn("Configuration saved", result.output)

                # Local config should be created
                self.assertTrue(local_config_path.exists())

            finally:
                config_module.CONFIG_DIR = original_config_dir
                config_module.CREDENTIALS_PATH = original_credentials_path
                config_module.LOCAL_CONFIG_FILE = original_local_config

    @respx.mock
    def test_login_skips_init_when_local_config_exists(self):
        """Test that login doesn't run init if local config already exists"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Setup paths
            config_dir = Path(tmpdir) / ".config" / "tensorlake"
            config_dir.mkdir(parents=True)
            credentials_path = config_dir / "credentials.toml"
            local_config_path = Path(tmpdir) / "project" / ".tensorlake.toml"
            local_config_path.parent.mkdir(parents=True)

            # Create existing local config
            with open(local_config_path, "w") as f:
                f.write('organization = "existing_org"\nproject = "existing_proj"\n')

            original_config_dir = config_module.CONFIG_DIR
            original_credentials_path = config_module.CREDENTIALS_PATH
            original_local_config = config_module.LOCAL_CONFIG_FILE

            try:
                config_module.CONFIG_DIR = config_dir
                config_module.CREDENTIALS_PATH = credentials_path
                config_module.LOCAL_CONFIG_FILE = local_config_path

                # Mock login flow
                respx.post("https://api.tensorlake.ai/platform/cli/login/start").mock(
                    return_value=httpx.Response(
                        200,
                        json={"device_code": "test_device", "user_code": "TEST123"},
                    )
                )

                respx.get(
                    "https://api.tensorlake.ai/platform/cli/login/poll?device_code=test_device"
                ).mock(
                    return_value=httpx.Response(
                        200,
                        json={"status": "approved"},
                    )
                )

                respx.post(
                    "https://api.tensorlake.ai/platform/cli/login/exchange"
                ).mock(
                    return_value=httpx.Response(
                        200,
                        json={"access_token": "test_token"},
                    )
                )

                runner = CliRunner()
                from unittest.mock import patch

                with patch("webbrowser.open"):
                    result = runner.invoke(cli, ["login"], prog_name="tensorlake")

                # Should succeed without init flow
                self.assertEqual(result.exit_code, 0, f"Failed: {result.output}")
                self.assertIn("Login successful", result.output)
                self.assertNotIn("No local configuration found", result.output)
                self.assertNotIn("Initializing TensorLake", result.output)

            finally:
                config_module.CONFIG_DIR = original_config_dir
                config_module.CREDENTIALS_PATH = original_credentials_path
                config_module.LOCAL_CONFIG_FILE = original_local_config


class TestContextHelperMethods(unittest.TestCase):
    """Test Context helper methods for auth/config checks"""

    def test_has_authentication_with_api_key(self):
        """Test has_authentication returns True when API key is set"""
        from tensorlake.cli._common import Context

        ctx = Context.default(api_key="test_api_key")
        self.assertTrue(ctx.has_authentication())

    def test_has_authentication_with_pat(self):
        """Test has_authentication returns True when PAT is set"""
        from tensorlake.cli._common import Context

        ctx = Context.default(personal_access_token="test_pat")
        self.assertTrue(ctx.has_authentication())

    def test_has_authentication_without_auth(self):
        """Test has_authentication returns False when no auth is set"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Setup empty credentials to ensure no auth
            config_dir = Path(tmpdir) / ".config" / "tensorlake"
            config_dir.mkdir(parents=True)
            credentials_path = config_dir / "credentials.toml"

            original_config_dir = config_module.CONFIG_DIR
            original_credentials_path = config_module.CREDENTIALS_PATH

            try:
                config_module.CONFIG_DIR = config_dir
                config_module.CREDENTIALS_PATH = credentials_path

                from tensorlake.cli._common import Context

                ctx = Context.default()
                self.assertFalse(ctx.has_authentication())
            finally:
                config_module.CONFIG_DIR = original_config_dir
                config_module.CREDENTIALS_PATH = original_credentials_path

    def test_has_org_and_project_with_both(self):
        """Test has_org_and_project returns True when both are set"""
        from tensorlake.cli._common import Context

        ctx = Context.default(organization_id="org_123", project_id="proj_456")
        self.assertTrue(ctx.has_org_and_project())

    def test_has_org_and_project_with_only_org(self):
        """Test has_org_and_project returns False when only org is set"""
        with tempfile.TemporaryDirectory() as tmpdir:
            local_config_path = Path(tmpdir) / ".tensorlake.toml"

            original_local_config = config_module.LOCAL_CONFIG_FILE
            try:
                config_module.LOCAL_CONFIG_FILE = local_config_path

                from tensorlake.cli._common import Context

                ctx = Context.default(organization_id="org_123")
                self.assertFalse(ctx.has_org_and_project())
            finally:
                config_module.LOCAL_CONFIG_FILE = original_local_config

    def test_has_org_and_project_with_only_project(self):
        """Test has_org_and_project returns False when only project is set"""
        with tempfile.TemporaryDirectory() as tmpdir:
            local_config_path = Path(tmpdir) / ".tensorlake.toml"

            original_local_config = config_module.LOCAL_CONFIG_FILE
            try:
                config_module.LOCAL_CONFIG_FILE = local_config_path

                from tensorlake.cli._common import Context

                ctx = Context.default(project_id="proj_456")
                self.assertFalse(ctx.has_org_and_project())
            finally:
                config_module.LOCAL_CONFIG_FILE = original_local_config

    def test_needs_init_with_pat_no_config(self):
        """Test needs_init returns True when PAT exists but no org/project"""
        with tempfile.TemporaryDirectory() as tmpdir:
            local_config_path = Path(tmpdir) / ".tensorlake.toml"

            original_local_config = config_module.LOCAL_CONFIG_FILE
            try:
                config_module.LOCAL_CONFIG_FILE = local_config_path

                from tensorlake.cli._common import Context

                ctx = Context.default(personal_access_token="test_pat")
                self.assertTrue(ctx.needs_init())
            finally:
                config_module.LOCAL_CONFIG_FILE = original_local_config

    def test_needs_init_with_api_key(self):
        """Test needs_init returns False when using API key (introspection provides org/project)"""
        from tensorlake.cli._common import Context

        ctx = Context.default(api_key="test_api_key")
        self.assertFalse(ctx.needs_init())

    def test_needs_init_with_pat_and_config(self):
        """Test needs_init returns False when PAT and org/project both exist"""
        from tensorlake.cli._common import Context

        ctx = Context.default(
            personal_access_token="test_pat",
            organization_id="org_123",
            project_id="proj_456",
        )
        self.assertFalse(ctx.needs_init())


if __name__ == "__main__":
    unittest.main()
