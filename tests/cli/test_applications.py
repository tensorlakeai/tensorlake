"""Tests for the applications (ls) command"""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

import tensorlake.cli._configuration as config_module
from tensorlake.applications.remote.api_client import ApplicationListItem
from tensorlake.cli import cli
from tensorlake.cli._common import Context


class TestApplicationsList(unittest.TestCase):
    """Test listing applications"""

    def setUp(self):
        """Set up test environment with temporary config"""
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

        # Create credentials file with PAT to skip auto-login
        # Format: [base_url]\ntoken = "value"
        with open(credentials_path, "w") as f:
            f.write('["https://api.tensorlake.ai"]\ntoken = "test_token"\n')

        # Save original paths
        self.original_credentials_path = config_module.CREDENTIALS_PATH
        self.original_config_dir = config_module.CONFIG_DIR
        self.original_local_config_file = config_module.LOCAL_CONFIG_FILE

        # Set to test paths
        config_module.CREDENTIALS_PATH = credentials_path
        config_module.CONFIG_DIR = config_dir
        config_module.LOCAL_CONFIG_FILE = local_config_path

        # Save original cwd and change to temp dir
        import os

        self.original_cwd = os.getcwd()
        os.chdir(self.tmpdir.name)

    def tearDown(self):
        """Restore original configuration"""
        import os

        os.chdir(self.original_cwd)
        config_module.CREDENTIALS_PATH = self.original_credentials_path
        config_module.CONFIG_DIR = self.original_config_dir
        config_module.LOCAL_CONFIG_FILE = self.original_local_config_file
        self.tmpdir.cleanup()

    def test_list_multiple_applications(self):
        """Test listing multiple applications"""
        runner = CliRunner()

        applications = [
            ApplicationListItem(
                name="app1",
                description="First app",
                tags={},
                version="v1",
                tombstoned=False,
                created_at=1697539200000,  # 2023-10-17 12:00:00 UTC in milliseconds
            ),
            ApplicationListItem(
                name="app2",
                description="Second app",
                tags={},
                version="v1",
                tombstoned=False,
                created_at=1697625600000,  # 2023-10-18 12:00:00 UTC in milliseconds
            ),
        ]

        with patch(
            "tensorlake.applications.remote.api_client.APIClient.applications"
        ) as mock_apps:
            mock_apps.return_value = applications
            result = runner.invoke(cli, ["ls"])

        self.assertEqual(result.exit_code, 0, f"Failed: {result.output}")
        self.assertIn("app1", result.output)
        self.assertIn("app2", result.output)
        self.assertIn("First app", result.output)
        self.assertIn("Second app", result.output)
        self.assertIn("2 applications", result.output)

    def test_list_single_application(self):
        """Test listing a single application shows singular form"""
        runner = CliRunner()

        applications = [
            ApplicationListItem(
                name="single_app",
                description="Only app",
                tags={},
                version="v1",
                tombstoned=False,
                created_at=1697539200000,
            ),
        ]

        with patch(
            "tensorlake.applications.remote.api_client.APIClient.applications"
        ) as mock_apps:
            mock_apps.return_value = applications
            result = runner.invoke(cli, ["ls"])

        self.assertEqual(result.exit_code, 0)
        self.assertIn("single_app", result.output)
        self.assertIn("1 application", result.output)
        self.assertNotIn("1 applications", result.output)

    def test_list_no_applications(self):
        """Test listing when no applications exist"""
        runner = CliRunner()

        with patch(
            "tensorlake.applications.remote.api_client.APIClient.applications"
        ) as mock_apps:
            mock_apps.return_value = []
            result = runner.invoke(cli, ["ls"])

        self.assertEqual(result.exit_code, 0)
        self.assertIn("No applications found", result.output)

    def test_filters_tombstoned_applications(self):
        """Test that tombstoned applications are filtered out"""
        runner = CliRunner()

        applications = [
            ApplicationListItem(
                name="active_app",
                description="Active",
                tags={},
                version="v1",
                tombstoned=False,
                created_at=1697539200000,
            ),
            ApplicationListItem(
                name="deleted_app",
                description="Deleted",
                tags={},
                version="v1",
                tombstoned=True,
                created_at=1697539200000,
            ),
        ]

        with patch(
            "tensorlake.applications.remote.api_client.APIClient.applications"
        ) as mock_apps:
            mock_apps.return_value = applications
            result = runner.invoke(cli, ["ls"])

        self.assertEqual(result.exit_code, 0)
        self.assertIn("active_app", result.output)
        self.assertNotIn("deleted_app", result.output)
        self.assertIn("1 application", result.output)

    def test_shows_dashboard_link(self):
        """Test that dashboard link is displayed"""
        runner = CliRunner()

        applications = [
            ApplicationListItem(
                name="test_app",
                description="Test",
                tags={},
                version="v1",
                tombstoned=False,
                created_at=1697539200000,
            ),
        ]

        with patch(
            "tensorlake.applications.remote.api_client.APIClient.applications"
        ) as mock_apps:
            mock_apps.return_value = applications
            result = runner.invoke(cli, ["ls"])

        self.assertEqual(result.exit_code, 0)
        self.assertIn("View all applications:", result.output)
        self.assertIn(
            "https://cloud.tensorlake.ai/organizations/test_org/projects/test_proj/applications",
            result.output,
        )

    def test_formats_timestamp_correctly(self):
        """Test that timestamps are formatted correctly"""
        runner = CliRunner()

        applications = [
            ApplicationListItem(
                name="test_app",
                description="Test",
                tags={},
                version="v1",
                tombstoned=False,
                created_at=1697539200000,  # 2023-10-17 12:00:00 UTC in milliseconds
            ),
        ]

        with patch(
            "tensorlake.applications.remote.api_client.APIClient.applications"
        ) as mock_apps:
            mock_apps.return_value = applications
            result = runner.invoke(cli, ["ls"])

        self.assertEqual(result.exit_code, 0)
        # Check that output contains a properly formatted date (YYYY-MM-DD format)
        import re

        date_pattern = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"
        self.assertIsNotNone(re.search(date_pattern, result.output))

    def test_handles_missing_timestamp(self):
        """Test handling of applications without created_at timestamp"""
        runner = CliRunner()

        applications = [
            ApplicationListItem(
                name="test_app",
                description="Test",
                tags={},
                version="v1",
                tombstoned=False,
                created_at=None,
            ),
        ]

        with patch(
            "tensorlake.applications.remote.api_client.APIClient.applications"
        ) as mock_apps:
            mock_apps.return_value = applications
            result = runner.invoke(cli, ["ls"])

        self.assertEqual(result.exit_code, 0)
        self.assertIn("test_app", result.output)

    def test_handles_empty_description(self):
        """Test handling of applications with no description"""
        runner = CliRunner()

        applications = [
            ApplicationListItem(
                name="test_app",
                description="",
                tags={},
                version="v1",
                tombstoned=False,
                created_at=1697539200000,
            ),
        ]

        with patch(
            "tensorlake.applications.remote.api_client.APIClient.applications"
        ) as mock_apps:
            mock_apps.return_value = applications
            result = runner.invoke(cli, ["ls"])

        self.assertEqual(result.exit_code, 0)
        self.assertIn("test_app", result.output)

    def test_table_has_correct_columns(self):
        """Test that the table has the correct column headers"""
        runner = CliRunner()

        applications = [
            ApplicationListItem(
                name="test_app",
                description="Test",
                tags={},
                version="v1",
                tombstoned=False,
                created_at=1697539200000,
            ),
        ]

        with patch(
            "tensorlake.applications.remote.api_client.APIClient.applications"
        ) as mock_apps:
            mock_apps.return_value = applications
            result = runner.invoke(cli, ["ls"])

        self.assertEqual(result.exit_code, 0)
        self.assertIn("Name", result.output)
        self.assertIn("Description", result.output)
        self.assertIn("Deployed At", result.output)


if __name__ == "__main__":
    unittest.main()
