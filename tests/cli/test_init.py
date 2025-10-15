import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import respx
from click.testing import CliRunner
from tomlkit import document, dumps, parse, table

import tensorlake.cli._configuration as config_module
from tensorlake.cli import cli
from tensorlake.cli._common import Context
from tensorlake.cli._configuration import (
    load_local_config,
    save_config,
    save_credentials,
    save_local_config,
)


class TestLocalConfigFile(unittest.TestCase):
    """Test local .tensorlake.toml configuration file functionality"""

    def test_load_local_config_when_file_exists(self):
        """Test loading local config from .tensorlake.toml"""
        with tempfile.TemporaryDirectory() as tmpdir:
            local_config_path = Path(tmpdir) / ".tensorlake.toml"

            # Create a local config file
            config_data = {"organization": "local_org_123", "project": "local_proj_456"}
            with open(local_config_path, "w") as f:
                f.write(dumps(config_data))

            original_local_config = config_module.LOCAL_CONFIG_FILE

            try:
                config_module.LOCAL_CONFIG_FILE = local_config_path

                # Load and verify
                loaded = load_local_config()
                self.assertEqual(loaded["organization"], "local_org_123")
                self.assertEqual(loaded["project"], "local_proj_456")
            finally:
                config_module.LOCAL_CONFIG_FILE = original_local_config

    def test_load_local_config_when_file_missing(self):
        """Test loading local config when file doesn't exist returns empty dict"""
        with tempfile.TemporaryDirectory() as tmpdir:
            local_config_path = Path(tmpdir) / ".tensorlake.toml"

            original_local_config = config_module.LOCAL_CONFIG_FILE

            try:
                config_module.LOCAL_CONFIG_FILE = local_config_path

                loaded = load_local_config()
                self.assertEqual(loaded, {})
            finally:
                config_module.LOCAL_CONFIG_FILE = original_local_config

    def test_save_local_config_creates_file(self):
        """Test saving local config creates .tensorlake.toml"""
        with tempfile.TemporaryDirectory() as tmpdir:
            local_config_path = Path(tmpdir) / ".tensorlake.toml"

            original_local_config = config_module.LOCAL_CONFIG_FILE

            try:
                config_module.LOCAL_CONFIG_FILE = local_config_path

                config_data = {"organization": "test_org", "project": "test_proj"}
                save_local_config(config_data, local_config_path.parent)

                # Verify file was created
                self.assertTrue(local_config_path.exists())

                # Verify content
                with open(local_config_path, "r") as f:
                    loaded = parse(f.read())
                self.assertEqual(loaded["organization"], "test_org")
                self.assertEqual(loaded["project"], "test_proj")
            finally:
                config_module.LOCAL_CONFIG_FILE = original_local_config

    def test_save_local_config_sets_permissions(self):
        """Test that saving local config sets restrictive permissions (0600)"""
        with tempfile.TemporaryDirectory() as tmpdir:
            local_config_path = Path(tmpdir) / ".tensorlake.toml"

            original_local_config = config_module.LOCAL_CONFIG_FILE

            try:
                config_module.LOCAL_CONFIG_FILE = local_config_path

                config_data = {"organization": "test_org"}
                save_local_config(config_data, local_config_path.parent)

                # Check permissions (0600 = owner read/write only)
                import os
                import stat
                file_stat = os.stat(local_config_path)
                permissions = stat.filemode(file_stat.st_mode)
                # Should be -rw------- on Unix-like systems
                self.assertTrue(file_stat.st_mode & stat.S_IRUSR)
                self.assertTrue(file_stat.st_mode & stat.S_IWUSR)
                self.assertFalse(file_stat.st_mode & stat.S_IRGRP)
                self.assertFalse(file_stat.st_mode & stat.S_IROTH)
            finally:
                config_module.LOCAL_CONFIG_FILE = original_local_config


class TestLocalConfigPriority(unittest.TestCase):
    """Test that local config takes priority over global config"""

    def test_local_config_overrides_global_config(self):
        """Test that local .tensorlake.toml overrides global config"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Setup global config
            global_config_dir = Path(tmpdir) / ".config" / "tensorlake"
            global_config_dir.mkdir(parents=True)
            global_config_file = global_config_dir / ".tensorlake_config"

            global_config_data = {
                "default": {"organization": "global_org", "project": "global_proj"}
            }
            with open(global_config_file, "w") as f:
                f.write(dumps(global_config_data))

            # Setup local config
            local_config_path = Path(tmpdir) / ".tensorlake.toml"
            local_config_data = {"organization": "local_org", "project": "local_proj"}
            with open(local_config_path, "w") as f:
                f.write(dumps(local_config_data))

            original_config_dir = config_module.CONFIG_DIR
            original_config_file = config_module.CONFIG_FILE
            original_local_config = config_module.LOCAL_CONFIG_FILE

            try:
                config_module.CONFIG_DIR = global_config_dir
                config_module.CONFIG_FILE = global_config_file
                config_module.LOCAL_CONFIG_FILE = local_config_path

                # Load context - local should win
                ctx = Context.default()
                self.assertEqual(ctx.organization_id, "local_org")
                self.assertEqual(ctx.project_id, "local_proj")
            finally:
                config_module.CONFIG_DIR = original_config_dir
                config_module.CONFIG_FILE = original_config_file
                config_module.LOCAL_CONFIG_FILE = original_local_config

    def test_cli_args_override_local_config(self):
        """Test that CLI arguments take priority over local config"""
        with tempfile.TemporaryDirectory() as tmpdir:
            local_config_path = Path(tmpdir) / ".tensorlake.toml"
            local_config_data = {
                "default": {"organization": "local_org", "project": "local_proj"}
            }
            with open(local_config_path, "w") as f:
                f.write(dumps(local_config_data))

            original_local_config = config_module.LOCAL_CONFIG_FILE

            try:
                config_module.LOCAL_CONFIG_FILE = local_config_path

                # CLI args should override local config
                ctx = Context.default(
                    organization_id="cli_org", project_id="cli_proj"
                )
                self.assertEqual(ctx.organization_id, "cli_org")
                self.assertEqual(ctx.project_id, "cli_proj")
            finally:
                config_module.LOCAL_CONFIG_FILE = original_local_config

    def test_priority_order_complete(self):
        """Test priority order: CLI > local config > None (global config NOT used for org/project)"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Setup global config (should NOT be used for org/project IDs)
            global_config_dir = Path(tmpdir) / ".config" / "tensorlake"
            global_config_dir.mkdir(parents=True)
            global_config_file = global_config_dir / ".tensorlake_config"

            global_config_data = {
                "default": {"organization": "global_org", "project": "global_proj"}
            }
            with open(global_config_file, "w") as f:
                f.write(dumps(global_config_data))

            # Setup local config
            local_config_path = Path(tmpdir) / ".tensorlake.toml"
            local_config_data = {"organization": "local_org", "project": "local_proj"}
            with open(local_config_path, "w") as f:
                f.write(dumps(local_config_data))

            original_config_dir = config_module.CONFIG_DIR
            original_config_file = config_module.CONFIG_FILE
            original_local_config = config_module.LOCAL_CONFIG_FILE

            try:
                config_module.CONFIG_DIR = global_config_dir
                config_module.CONFIG_FILE = global_config_file
                config_module.LOCAL_CONFIG_FILE = local_config_path

                # Test 1: Without local config, org/project are None (not from global)
                local_config_path.unlink()  # Remove local
                ctx = Context.default()
                self.assertIsNone(ctx.organization_id)
                self.assertIsNone(ctx.project_id)

                # Test 2: Local config provides org/project
                with open(local_config_path, "w") as f:
                    f.write(dumps(local_config_data))
                ctx = Context.default()
                self.assertEqual(ctx.organization_id, "local_org")
                self.assertEqual(ctx.project_id, "local_proj")

                # Test 3: CLI args override local config
                ctx = Context.default(organization_id="cli_org", project_id="cli_proj")
                self.assertEqual(ctx.organization_id, "cli_org")
                self.assertEqual(ctx.project_id, "cli_proj")
            finally:
                config_module.CONFIG_DIR = original_config_dir
                config_module.CONFIG_FILE = original_config_file
                config_module.LOCAL_CONFIG_FILE = original_local_config


class TestInitCommand(unittest.TestCase):
    """Test the tensorlake init command"""

    @respx.mock
    def test_init_command_single_org_single_project(self):
        """Test init command with single organization and single project"""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            # Setup credentials
            credentials_dir = Path(tmpdir) / ".config" / "tensorlake"
            credentials_dir.mkdir(parents=True)
            credentials_path = credentials_dir / "credentials.toml"

            # Setup local config path
            local_config_path = Path(tmpdir) / "project" / ".tensorlake.toml"
            local_config_path.parent.mkdir(parents=True)

            original_config_dir = config_module.CONFIG_DIR
            original_credentials_path = config_module.CREDENTIALS_PATH
            original_local_config = config_module.LOCAL_CONFIG_FILE

            try:
                config_module.CONFIG_DIR = credentials_dir
                config_module.CREDENTIALS_PATH = credentials_path
                config_module.LOCAL_CONFIG_FILE = local_config_path

                # Save credentials AFTER setting the paths
                save_credentials("https://api.tensorlake.ai", "test_token_123")

                # Mock API responses
                respx.get("https://api.tensorlake.ai/platform/v1/organizations").mock(
                    return_value=httpx.Response(
                        200,
                        json={
                            "items": [
                                {"id": "org_123", "name": "Test Organization"}
                            ]
                        },
                    )
                )

                respx.get("https://api.tensorlake.ai/platform/v1/organizations/org_123/projects").mock(
                    return_value=httpx.Response(
                        200,
                        json={
                            "items": [
                                {"id": "proj_456", "name": "Test Project"}
                            ]
                        },
                    )
                )

                # Run init command
                result = runner.invoke(
                    cli,
                    ["init", "--no-confirm", "--directory", str(local_config_path.parent)],
                    prog_name="tensorlake",
                )

                # Verify success
                self.assertEqual(result.exit_code, 0, f"CLI failed: {result.output}")
                self.assertIn("Test Organization", result.output)
                self.assertIn("Test Project", result.output)
                self.assertIn("Configuration saved to", result.output)

                # Verify file was created
                self.assertTrue(local_config_path.exists())

                # Verify content
                with open(local_config_path, "r") as f:
                    config = parse(f.read())
                self.assertEqual(config["organization"], "org_123")
                self.assertEqual(config["project"], "proj_456")
            finally:
                config_module.CONFIG_DIR = original_config_dir
                config_module.CREDENTIALS_PATH = original_credentials_path
                config_module.LOCAL_CONFIG_FILE = original_local_config

    @respx.mock
    def test_init_command_multiple_orgs_multiple_projects(self):
        """Test init command with multiple organizations and projects (requires user input)"""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            # Setup credentials
            credentials_dir = Path(tmpdir) / ".config" / "tensorlake"
            credentials_dir.mkdir(parents=True)
            credentials_path = credentials_dir / "credentials.toml"

            # Setup local config path
            local_config_path = Path(tmpdir) / "project" / ".tensorlake.toml"
            local_config_path.parent.mkdir(parents=True)

            original_config_dir = config_module.CONFIG_DIR
            original_credentials_path = config_module.CREDENTIALS_PATH
            original_local_config = config_module.LOCAL_CONFIG_FILE

            try:
                config_module.CONFIG_DIR = credentials_dir
                config_module.CREDENTIALS_PATH = credentials_path
                config_module.LOCAL_CONFIG_FILE = local_config_path

                # Save credentials AFTER setting the paths
                save_credentials("https://api.tensorlake.ai", "test_token_123")

                # Mock API responses
                respx.get("https://api.tensorlake.ai/platform/v1/organizations").mock(
                    return_value=httpx.Response(
                        200,
                        json={
                            "items": [
                                {"id": "org_1", "name": "Organization One"},
                                {"id": "org_2", "name": "Organization Two"},
                            ]
                        },
                    )
                )

                respx.get("https://api.tensorlake.ai/platform/v1/organizations/org_2/projects").mock(
                    return_value=httpx.Response(
                        200,
                        json={
                            "items": [
                                {"id": "proj_1", "name": "Project Alpha"},
                                {"id": "proj_2", "name": "Project Beta"},
                            ]
                        },
                    )
                )

                # Run init command with simulated user input
                # User selects: 2 (Organization Two), then 1 (Project Alpha)
                result = runner.invoke(
                    cli,
                    ["init", "--no-confirm", "--directory", str(local_config_path.parent)],
                    prog_name="tensorlake",
                    input="2\n1\n",  # Select org 2, then project 1
                )

                # Verify success
                self.assertEqual(result.exit_code, 0, f"CLI failed: {result.output}")
                self.assertIn("Organization Two", result.output)
                self.assertIn("Project Alpha", result.output)

                # Verify file content
                with open(local_config_path, "r") as f:
                    config = parse(f.read())
                self.assertEqual(config["organization"], "org_2")
                self.assertEqual(config["project"], "proj_1")
            finally:
                config_module.CONFIG_DIR = original_config_dir
                config_module.CREDENTIALS_PATH = original_credentials_path
                config_module.LOCAL_CONFIG_FILE = original_local_config

    def test_init_command_without_credentials(self):
        """Test init command fails gracefully when no credentials exist"""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            credentials_dir = Path(tmpdir) / ".config" / "tensorlake"
            credentials_dir.mkdir(parents=True)
            credentials_path = credentials_dir / "credentials.toml"

            local_config_path = Path(tmpdir) / "project" / ".tensorlake.toml"
            local_config_path.parent.mkdir(parents=True)

            original_config_dir = config_module.CONFIG_DIR
            original_credentials_path = config_module.CREDENTIALS_PATH
            original_local_config = config_module.LOCAL_CONFIG_FILE

            try:
                config_module.CONFIG_DIR = credentials_dir
                config_module.CREDENTIALS_PATH = credentials_path
                config_module.LOCAL_CONFIG_FILE = local_config_path

                # Run init command without credentials
                result = runner.invoke(
                    cli,
                    ["init", "--no-confirm", "--directory", str(local_config_path.parent)],
                    prog_name="tensorlake",
                )

                # Should fail with helpful message
                self.assertNotEqual(result.exit_code, 0)
                self.assertIn("No valid credentials found", result.output)
                self.assertIn("tensorlake login", result.output)
            finally:
                config_module.CONFIG_DIR = original_config_dir
                config_module.CREDENTIALS_PATH = original_credentials_path
                config_module.LOCAL_CONFIG_FILE = original_local_config

    @respx.mock
    def test_init_command_api_error(self):
        """Test init command handles API errors gracefully"""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            # Setup credentials
            credentials_dir = Path(tmpdir) / ".config" / "tensorlake"
            credentials_dir.mkdir(parents=True)
            credentials_path = credentials_dir / "credentials.toml"

            local_config_path = Path(tmpdir) / "project" / ".tensorlake.toml"
            local_config_path.parent.mkdir(parents=True)

            original_config_dir = config_module.CONFIG_DIR
            original_credentials_path = config_module.CREDENTIALS_PATH
            original_local_config = config_module.LOCAL_CONFIG_FILE

            try:
                config_module.CONFIG_DIR = credentials_dir
                config_module.CREDENTIALS_PATH = credentials_path
                config_module.LOCAL_CONFIG_FILE = local_config_path

                # Save credentials AFTER setting the paths
                save_credentials("https://api.tensorlake.ai", "test_token_123")

                # Mock API error
                respx.get("https://api.tensorlake.ai/platform/v1/organizations").mock(
                    return_value=httpx.Response(500, json={"error": "Internal Server Error"})
                )

                # Run init command
                result = runner.invoke(
                    cli,
                    ["init", "--no-confirm", "--directory", str(local_config_path.parent)],
                    prog_name="tensorlake",
                )

                # Should fail with error message
                self.assertNotEqual(result.exit_code, 0)
                self.assertIn("Failed to fetch organizations", result.output)
            finally:
                config_module.CONFIG_DIR = original_config_dir
                config_module.CREDENTIALS_PATH = original_credentials_path
                config_module.LOCAL_CONFIG_FILE = original_local_config


class TestInitWithIncompleteConfig(unittest.TestCase):
    """Test that init command handles incomplete/corrupt local config files"""

    @respx.mock
    def test_init_repairs_config_missing_organization(self):
        """Test that init overwrites config file that's missing organization"""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            credentials_dir = Path(tmpdir) / ".config" / "tensorlake"
            credentials_dir.mkdir(parents=True)
            credentials_path = credentials_dir / "credentials.toml"

            local_config_path = Path(tmpdir) / "project" / ".tensorlake.toml"
            local_config_path.parent.mkdir(parents=True)

            # Create incomplete config (missing organization)
            with open(local_config_path, "w") as f:
                f.write('project = "old_proj"\n')

            original_config_dir = config_module.CONFIG_DIR
            original_credentials_path = config_module.CREDENTIALS_PATH
            original_local_config = config_module.LOCAL_CONFIG_FILE

            try:
                config_module.CONFIG_DIR = credentials_dir
                config_module.CREDENTIALS_PATH = credentials_path
                config_module.LOCAL_CONFIG_FILE = local_config_path

                save_credentials("https://api.tensorlake.ai", "test_token_123")

                # Mock API responses
                respx.get("https://api.tensorlake.ai/platform/v1/organizations").mock(
                    return_value=httpx.Response(
                        200,
                        json={"items": [{"id": "new_org", "name": "New Org"}]},
                    )
                )

                respx.get(
                    "https://api.tensorlake.ai/platform/v1/organizations/new_org/projects"
                ).mock(
                    return_value=httpx.Response(
                        200,
                        json={"items": [{"id": "new_proj", "name": "New Project"}]},
                    )
                )

                result = runner.invoke(
                    cli,
                    ["init", "--no-confirm", "--directory", str(local_config_path.parent)],
                    prog_name="tensorlake",
                )

                # Should succeed
                self.assertEqual(result.exit_code, 0, f"CLI failed: {result.output}")
                self.assertIn("New Org", result.output)
                self.assertIn("New Project", result.output)

                # Verify config was repaired
                with open(local_config_path, "r") as f:
                    config = parse(f.read())
                self.assertEqual(config["organization"], "new_org")
                self.assertEqual(config["project"], "new_proj")

            finally:
                config_module.CONFIG_DIR = original_config_dir
                config_module.CREDENTIALS_PATH = original_credentials_path
                config_module.LOCAL_CONFIG_FILE = original_local_config

    @respx.mock
    def test_init_repairs_config_missing_project(self):
        """Test that init overwrites config file that's missing project"""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            credentials_dir = Path(tmpdir) / ".config" / "tensorlake"
            credentials_dir.mkdir(parents=True)
            credentials_path = credentials_dir / "credentials.toml"

            local_config_path = Path(tmpdir) / "project" / ".tensorlake.toml"
            local_config_path.parent.mkdir(parents=True)

            # Create incomplete config (missing project)
            with open(local_config_path, "w") as f:
                f.write('organization = "old_org"\n')

            original_config_dir = config_module.CONFIG_DIR
            original_credentials_path = config_module.CREDENTIALS_PATH
            original_local_config = config_module.LOCAL_CONFIG_FILE

            try:
                config_module.CONFIG_DIR = credentials_dir
                config_module.CREDENTIALS_PATH = credentials_path
                config_module.LOCAL_CONFIG_FILE = local_config_path

                save_credentials("https://api.tensorlake.ai", "test_token_123")

                # Mock API responses
                respx.get("https://api.tensorlake.ai/platform/v1/organizations").mock(
                    return_value=httpx.Response(
                        200,
                        json={"items": [{"id": "new_org", "name": "New Org"}]},
                    )
                )

                respx.get(
                    "https://api.tensorlake.ai/platform/v1/organizations/new_org/projects"
                ).mock(
                    return_value=httpx.Response(
                        200,
                        json={"items": [{"id": "new_proj", "name": "New Project"}]},
                    )
                )

                result = runner.invoke(
                    cli,
                    ["init", "--no-confirm", "--directory", str(local_config_path.parent)],
                    prog_name="tensorlake",
                )

                # Should succeed
                self.assertEqual(result.exit_code, 0, f"CLI failed: {result.output}")
                self.assertIn("New Org", result.output)
                self.assertIn("New Project", result.output)

                # Verify config was repaired
                with open(local_config_path, "r") as f:
                    config = parse(f.read())
                self.assertEqual(config["organization"], "new_org")
                self.assertEqual(config["project"], "new_proj")

            finally:
                config_module.CONFIG_DIR = original_config_dir
                config_module.CREDENTIALS_PATH = original_credentials_path
                config_module.LOCAL_CONFIG_FILE = original_local_config

    @respx.mock
    def test_init_repairs_config_with_empty_values(self):
        """Test that init overwrites config file with empty string values"""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            credentials_dir = Path(tmpdir) / ".config" / "tensorlake"
            credentials_dir.mkdir(parents=True)
            credentials_path = credentials_dir / "credentials.toml"

            local_config_path = Path(tmpdir) / "project" / ".tensorlake.toml"
            local_config_path.parent.mkdir(parents=True)

            # Create config with empty values
            with open(local_config_path, "w") as f:
                f.write('organization = ""\nproject = ""\n')

            original_config_dir = config_module.CONFIG_DIR
            original_credentials_path = config_module.CREDENTIALS_PATH
            original_local_config = config_module.LOCAL_CONFIG_FILE

            try:
                config_module.CONFIG_DIR = credentials_dir
                config_module.CREDENTIALS_PATH = credentials_path
                config_module.LOCAL_CONFIG_FILE = local_config_path

                save_credentials("https://api.tensorlake.ai", "test_token_123")

                # Mock API responses
                respx.get("https://api.tensorlake.ai/platform/v1/organizations").mock(
                    return_value=httpx.Response(
                        200,
                        json={"items": [{"id": "new_org", "name": "New Org"}]},
                    )
                )

                respx.get(
                    "https://api.tensorlake.ai/platform/v1/organizations/new_org/projects"
                ).mock(
                    return_value=httpx.Response(
                        200,
                        json={"items": [{"id": "new_proj", "name": "New Project"}]},
                    )
                )

                result = runner.invoke(
                    cli,
                    ["init", "--no-confirm", "--directory", str(local_config_path.parent)],
                    prog_name="tensorlake",
                )

                # Should succeed
                self.assertEqual(result.exit_code, 0, f"CLI failed: {result.output}")
                self.assertIn("New Org", result.output)
                self.assertIn("New Project", result.output)

                # Verify config was repaired
                with open(local_config_path, "r") as f:
                    config = parse(f.read())
                self.assertEqual(config["organization"], "new_org")
                self.assertEqual(config["project"], "new_proj")

            finally:
                config_module.CONFIG_DIR = original_config_dir
                config_module.CREDENTIALS_PATH = original_credentials_path
                config_module.LOCAL_CONFIG_FILE = original_local_config


class TestInitCommandInHelp(unittest.TestCase):
    """Test that init command appears in CLI help"""

    def test_init_command_listed_in_help(self):
        """Test that 'init' command is listed in main help"""
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"], prog_name="tensorlake")

        self.assertEqual(result.exit_code, 0)
        self.assertIn("init", result.output)
        self.assertIn("Initialize TensorLake configuration", result.output)

    def test_init_command_help(self):
        """Test that 'tensorlake init --help' works"""
        runner = CliRunner()
        result = runner.invoke(cli, ["init", "--help"], prog_name="tensorlake")

        self.assertEqual(result.exit_code, 0)
        self.assertIn("Initialize TensorLake configuration for this project", result.output)


if __name__ == "__main__":
    unittest.main()
