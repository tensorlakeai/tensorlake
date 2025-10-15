import tempfile
import unittest
from pathlib import Path

from click.testing import CliRunner
from tomlkit import document, dumps, table

import tensorlake.cli._configuration as config_module
from tensorlake.cli import cli
from tensorlake.cli._common import Context


class TestPATEnvironmentVariable(unittest.TestCase):
    """Test that TENSORLAKE_PAT environment variable works correctly"""

    def test_pat_from_env_variable(self):
        """Test that PAT can be provided via TENSORLAKE_PAT environment variable"""
        test_pat = "test_personal_access_token_12345"

        # Create context with PAT from environment variable simulation
        ctx = Context.default(personal_access_token=test_pat)

        self.assertEqual(ctx.personal_access_token, test_pat)

    def test_pat_priority_env_over_file(self):
        """Test that PAT from env var takes priority over credentials file"""
        env_pat = "env_token_12345"

        with tempfile.TemporaryDirectory() as tmpdir:
            config_dir = Path(tmpdir) / ".config" / "tensorlake"
            config_dir.mkdir(parents=True)
            credentials_path = config_dir / "credentials.toml"

            # Write a PAT to the credentials file in TOML format (endpoint-scoped)
            config = document()
            section = table()
            section["token"] = "file_token_67890"
            config["https://api.tensorlake.ai"] = section

            with open(credentials_path, "w") as f:
                f.write(dumps(config))

            original_credentials_path = config_module.CREDENTIALS_PATH
            original_config_dir = config_module.CONFIG_DIR

            try:
                config_module.CREDENTIALS_PATH = credentials_path
                config_module.CONFIG_DIR = config_dir

                # PAT from parameter (simulating env var) should take priority
                ctx = Context.default(personal_access_token=env_pat)
                self.assertEqual(ctx.personal_access_token, env_pat)
            finally:
                config_module.CREDENTIALS_PATH = original_credentials_path
                config_module.CONFIG_DIR = original_config_dir

    def test_pat_falls_back_to_file(self):
        """Test that PAT falls back to credentials file when env var not provided"""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_dir = Path(tmpdir) / ".config" / "tensorlake"
            config_dir.mkdir(parents=True)
            credentials_path = config_dir / "credentials.toml"

            # Write a PAT to the credentials file in TOML format (endpoint-scoped)
            file_pat = "file_token_67890"
            config = document()
            section = table()
            section["token"] = file_pat
            config["https://api.tensorlake.ai"] = section

            with open(credentials_path, "w") as f:
                f.write(dumps(config))

            original_credentials_path = config_module.CREDENTIALS_PATH
            original_config_dir = config_module.CONFIG_DIR

            try:
                config_module.CREDENTIALS_PATH = credentials_path
                config_module.CONFIG_DIR = config_dir

                # Without env var, should use file PAT
                ctx = Context.default()
                self.assertEqual(ctx.personal_access_token, file_pat)
            finally:
                config_module.CREDENTIALS_PATH = original_credentials_path
                config_module.CONFIG_DIR = original_config_dir

    def test_cli_accepts_pat_flag(self):
        """Test that CLI accepts --pat flag"""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["--pat", "test_token_123", "--help"],
            prog_name="tensorlake",
        )

        self.assertEqual(result.exit_code, 0)
        # Verify help text mentions PAT
        self.assertIn("--pat", result.output)

    def test_cli_help_mentions_tensorlake_pat(self):
        """Test that CLI help text mentions TENSORLAKE_PAT environment variable"""
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"], prog_name="tensorlake")

        self.assertEqual(result.exit_code, 0)
        self.assertIn("TENSORLAKE_PAT", result.output)
        self.assertIn("Personal Access Token", result.output)


class TestTokenPriority(unittest.TestCase):
    """Test token priority when both API key and PAT are provided"""

    def test_api_key_takes_priority_over_pat(self):
        """Test that API key is used when both API key and PAT are provided"""
        from tensorlake.cli._common import Context

        # Create context with both API key and PAT
        ctx = Context.default(
            api_key="test_api_key_123",
            personal_access_token="test_pat_456"
        )

        # Verify both are set
        self.assertEqual(ctx.api_key, "test_api_key_123")
        self.assertEqual(ctx.personal_access_token, "test_pat_456")

        # Check that API key is prioritized in client headers
        headers = ctx.client.headers
        self.assertEqual(headers["Authorization"], "Bearer test_api_key_123")

    def test_pat_used_when_no_api_key(self):
        """Test that PAT is used when only PAT is provided"""
        from tensorlake.cli._common import Context

        # Create context with only PAT
        ctx = Context.default(personal_access_token="test_pat_789")

        # Verify PAT is set and API key is not
        self.assertIsNone(ctx.api_key)
        self.assertEqual(ctx.personal_access_token, "test_pat_789")

        # Check that PAT is used in client headers
        headers = ctx.client.headers
        self.assertEqual(headers["Authorization"], "Bearer test_pat_789")

    def test_api_key_used_when_no_pat(self):
        """Test that API key is used when only API key is provided"""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_dir = Path(tmpdir) / ".config" / "tensorlake"
            config_dir.mkdir(parents=True)
            credentials_path = config_dir / "credentials.toml"

            original_credentials_path = config_module.CREDENTIALS_PATH
            original_config_dir = config_module.CONFIG_DIR

            try:
                config_module.CREDENTIALS_PATH = credentials_path
                config_module.CONFIG_DIR = config_dir

                from tensorlake.cli._common import Context

                # Create context with only API key
                ctx = Context.default(api_key="test_api_key_999")

                # Verify API key is set and PAT is not
                self.assertEqual(ctx.api_key, "test_api_key_999")
                self.assertIsNone(ctx.personal_access_token)

                # Check that API key is used in client headers
                headers = ctx.client.headers
                self.assertEqual(headers["Authorization"], "Bearer test_api_key_999")
            finally:
                config_module.CREDENTIALS_PATH = original_credentials_path
                config_module.CONFIG_DIR = original_config_dir


if __name__ == "__main__":
    unittest.main()
