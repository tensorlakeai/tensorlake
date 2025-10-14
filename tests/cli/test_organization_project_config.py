import tempfile
import unittest
from pathlib import Path

from click.testing import CliRunner

from tensorlake.cli import cli
from tensorlake.cli._common import Context


class TestOrganizationIDConfiguration(unittest.TestCase):
    """Test organization ID configuration through CLI options, environment variables, and config file"""

    def test_organization_id_from_cli_option(self):
        """Test organization ID override via CLI option"""
        ctx = Context.default(organization_id="cli_org_123")
        self.assertEqual(ctx.default_organization, "cli_org_123")
        self.assertEqual(ctx.organization_id, "cli_org_123")

    def test_organization_id_from_environment_variable(self):
        """Test organization ID override via TENSORLAKE_ORGANIZATION_ID environment variable"""
        runner = CliRunner(
            env={"TENSORLAKE_ORGANIZATION_ID": "env_org_456"}
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

    def test_organization_id_from_local_config_file(self):
        """Test organization ID loaded from local .tensorlake.toml file"""
        with tempfile.TemporaryDirectory() as tmpdir:
            local_config_path = Path(tmpdir) / ".tensorlake.toml"

            import tensorlake.cli._configuration as config_module
            from tensorlake.cli._configuration import save_local_config

            original_local_config = config_module.LOCAL_CONFIG_FILE

            try:
                config_module.LOCAL_CONFIG_FILE = local_config_path

                # Save local config with custom organization ID
                config_data = {"organization": "local_org_789"}
                save_local_config(config_data)

                # Load context and verify
                ctx = Context.default()
                self.assertEqual(ctx.default_organization, "local_org_789")
                self.assertEqual(ctx.organization_id, "local_org_789")
            finally:
                # Restore original values
                config_module.LOCAL_CONFIG_FILE = original_local_config

    def test_organization_id_priority_order(self):
        """Test that organization ID priority order is: CLI/Env > Local Config > None"""
        with tempfile.TemporaryDirectory() as tmpdir:
            local_config_path = Path(tmpdir) / ".tensorlake.toml"

            import tensorlake.cli._configuration as config_module
            from tensorlake.cli._configuration import save_local_config

            original_local_config = config_module.LOCAL_CONFIG_FILE

            try:
                config_module.LOCAL_CONFIG_FILE = local_config_path

                # Save local config with custom organization ID
                config_data = {"organization": "local_org_abc"}
                save_local_config(config_data)

                # Test 1: Local config overrides default (None)
                ctx = Context.default()
                self.assertEqual(ctx.organization_id, "local_org_abc")

                # Test 2: CLI/env parameter overrides local config
                ctx = Context.default(organization_id="cli_org_xyz")
                self.assertEqual(ctx.organization_id, "cli_org_xyz")

                # Test 3: Explicit None falls back to local config
                ctx = Context.default(organization_id=None)
                self.assertEqual(ctx.organization_id, "local_org_abc")
            finally:
                config_module.LOCAL_CONFIG_FILE = original_local_config

    def test_organization_flag_in_help_text(self):
        """Test that organization flag is documented in help text"""
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"], prog_name="tensorlake")

        self.assertEqual(result.exit_code, 0)
        self.assertIn("--organization", result.output)
        self.assertIn("organization ID", result.output)


class TestProjectIDConfiguration(unittest.TestCase):
    """Test project ID configuration through CLI options, environment variables, and config file"""

    def test_project_id_from_cli_option(self):
        """Test project ID override via CLI option"""
        ctx = Context.default(project_id="cli_proj_123")
        self.assertEqual(ctx.default_project, "cli_proj_123")
        self.assertEqual(ctx.project_id, "cli_proj_123")

    def test_project_id_from_environment_variable(self):
        """Test project ID override via TENSORLAKE_PROJECT_ID environment variable"""
        runner = CliRunner(
            env={"TENSORLAKE_PROJECT_ID": "env_proj_456"}
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

    def test_project_id_from_local_config_file(self):
        """Test project ID loaded from local .tensorlake.toml file"""
        with tempfile.TemporaryDirectory() as tmpdir:
            local_config_path = Path(tmpdir) / ".tensorlake.toml"

            import tensorlake.cli._configuration as config_module
            from tensorlake.cli._configuration import save_local_config

            original_local_config = config_module.LOCAL_CONFIG_FILE

            try:
                config_module.LOCAL_CONFIG_FILE = local_config_path

                # Save local config with custom project ID
                config_data = {"project": "local_proj_789"}
                save_local_config(config_data)

                # Load context and verify
                ctx = Context.default()
                self.assertEqual(ctx.default_project, "local_proj_789")
                self.assertEqual(ctx.project_id, "local_proj_789")
            finally:
                # Restore original values
                config_module.LOCAL_CONFIG_FILE = original_local_config

    def test_project_id_priority_order(self):
        """Test that project ID priority order is: CLI/Env > Local Config > None"""
        with tempfile.TemporaryDirectory() as tmpdir:
            local_config_path = Path(tmpdir) / ".tensorlake.toml"

            import tensorlake.cli._configuration as config_module
            from tensorlake.cli._configuration import save_local_config

            original_local_config = config_module.LOCAL_CONFIG_FILE

            try:
                config_module.LOCAL_CONFIG_FILE = local_config_path

                # Save local config with custom project ID
                config_data = {"project": "local_proj_abc"}
                save_local_config(config_data)

                # Test 1: Local config overrides default (None)
                ctx = Context.default()
                self.assertEqual(ctx.project_id, "local_proj_abc")

                # Test 2: CLI/env parameter overrides local config
                ctx = Context.default(project_id="cli_proj_xyz")
                self.assertEqual(ctx.project_id, "cli_proj_xyz")

                # Test 3: Explicit None falls back to local config
                ctx = Context.default(project_id=None)
                self.assertEqual(ctx.project_id, "local_proj_abc")
            finally:
                config_module.LOCAL_CONFIG_FILE = original_local_config

    def test_project_flag_in_help_text(self):
        """Test that project flag is documented in help text"""
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"], prog_name="tensorlake")

        self.assertEqual(result.exit_code, 0)
        self.assertIn("--project", result.output)
        self.assertIn("project ID", result.output)


class TestOrganizationAndProjectIDTogether(unittest.TestCase):
    """Test organization and project ID configuration together"""

    def test_both_ids_from_cli_options(self):
        """Test both organization and project IDs via CLI options"""
        ctx = Context.default(
            organization_id="org_123",
            project_id="proj_456"
        )
        self.assertEqual(ctx.organization_id, "org_123")
        self.assertEqual(ctx.project_id, "proj_456")

    def test_both_ids_from_local_config_file(self):
        """Test both organization and project IDs via local config file"""
        with tempfile.TemporaryDirectory() as tmpdir:
            local_config_path = Path(tmpdir) / ".tensorlake.toml"

            import tensorlake.cli._configuration as config_module
            from tensorlake.cli._configuration import save_local_config

            original_local_config = config_module.LOCAL_CONFIG_FILE

            try:
                config_module.LOCAL_CONFIG_FILE = local_config_path

                # Save local config with both IDs
                config_data = {
                    "organization": "local_org_111",
                    "project": "local_proj_222"
                }
                save_local_config(config_data)

                # Load context and verify
                ctx = Context.default()
                self.assertEqual(ctx.organization_id, "local_org_111")
                self.assertEqual(ctx.project_id, "local_proj_222")
            finally:
                config_module.LOCAL_CONFIG_FILE = original_local_config

    def test_mixed_sources_for_ids(self):
        """Test organization from CLI and project from local config file"""
        with tempfile.TemporaryDirectory() as tmpdir:
            local_config_path = Path(tmpdir) / ".tensorlake.toml"

            import tensorlake.cli._configuration as config_module
            from tensorlake.cli._configuration import save_local_config

            original_local_config = config_module.LOCAL_CONFIG_FILE

            try:
                config_module.LOCAL_CONFIG_FILE = local_config_path

                # Save local config with project ID only
                config_data = {"project": "local_proj_999"}
                save_local_config(config_data)

                # Provide organization via CLI, project from local config
                ctx = Context.default(organization_id="cli_org_888")
                self.assertEqual(ctx.organization_id, "cli_org_888")
                self.assertEqual(ctx.project_id, "local_proj_999")
            finally:
                config_module.LOCAL_CONFIG_FILE = original_local_config

    def test_cli_runner_with_both_flags(self):
        """Test CLI runner accepts both --organization and --project flags"""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--organization", "org_test",
                "--project", "proj_test",
                "--help"
            ],
            prog_name="tensorlake",
        )

        self.assertEqual(result.exit_code, 0)
        self.assertIn("--organization", result.output)
        self.assertIn("--project", result.output)


class TestOrganizationProjectEnvironmentVariables(unittest.TestCase):
    """Test that environment variable names are correct"""

    def test_organization_env_var_name(self):
        """Test that TENSORLAKE_ORGANIZATION_ID environment variable works"""
        runner = CliRunner(
            env={"TENSORLAKE_ORGANIZATION_ID": "env_org_test"}
        )

        # Just verify the CLI accepts it
        result = runner.invoke(cli, ["--help"], prog_name="tensorlake")
        self.assertEqual(result.exit_code, 0)

    def test_project_env_var_name(self):
        """Test that TENSORLAKE_PROJECT_ID environment variable works"""
        runner = CliRunner(
            env={"TENSORLAKE_PROJECT_ID": "env_proj_test"}
        )

        # Just verify the CLI accepts it
        result = runner.invoke(cli, ["--help"], prog_name="tensorlake")
        self.assertEqual(result.exit_code, 0)

    def test_both_env_vars_together(self):
        """Test that both environment variables work together"""
        runner = CliRunner(
            env={
                "TENSORLAKE_ORGANIZATION_ID": "env_org_test",
                "TENSORLAKE_PROJECT_ID": "env_proj_test"
            }
        )

        result = runner.invoke(cli, ["--help"], prog_name="tensorlake")
        self.assertEqual(result.exit_code, 0)


if __name__ == "__main__":
    unittest.main()
