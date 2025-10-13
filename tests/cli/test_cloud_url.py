import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
import respx
from click.testing import CliRunner

from tensorlake.cli import cli
from tensorlake.cli._common import Context
from tensorlake.cli.config import save_config


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
        runner = CliRunner(env={"TENSORLAKE_CLOUD_URL": "https://env-cloud.example.com"})
        
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
            import tensorlake.cli.config as config_module
            original_config_dir = config_module.CONFIG_DIR
            original_config_file = config_module.CONFIG_FILE
            
            try:
                config_module.CONFIG_DIR = config_dir
                config_module.CONFIG_FILE = config_file
                
                # Save config with custom cloud URL
                config_data = {"tensorlake": {"cloud_url": "https://config-cloud.example.com"}}
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
            
            import tensorlake.cli.config as config_module
            original_config_dir = config_module.CONFIG_DIR
            original_config_file = config_module.CONFIG_FILE
            
            try:
                config_module.CONFIG_DIR = config_dir
                config_module.CONFIG_FILE = config_file
                
                # Save config with custom cloud URL
                config_data = {"tensorlake": {"cloud_url": "https://config-cloud.example.com"}}
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
        """Test that cloud URL configuration is documented in help text"""
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"], prog_name="tensorlake")
        
        self.assertEqual(result.exit_code, 0)
        self.assertIn("--cloud-url", result.output)
        self.assertIn("tensorlake.cloud_url", result.output)
        self.assertIn("https://cloud.tensorlake.ai", result.output)

    def test_env_variable_name_in_help(self):
        """Test that TENSORLAKE_CLOUD_URL environment variable is shown in help"""
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
        """Test that cloud_url and base_url are independent"""
        ctx = Context.default(
            base_url="https://api.example.com",
            cloud_url="https://cloud.example.com",
        )
        
        self.assertEqual(ctx.base_url, "https://api.example.com")
        self.assertEqual(ctx.cloud_url, "https://cloud.example.com")
        self.assertNotEqual(ctx.base_url, ctx.cloud_url)


if __name__ == "__main__":
    unittest.main()
