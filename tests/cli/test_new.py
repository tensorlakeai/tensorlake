"""Tests for the new command"""

import tempfile
import unittest
from pathlib import Path

from click.testing import CliRunner

from tensorlake.cli import cli


class TestNewCommandSuccess(unittest.TestCase):
    """Test successful application creation"""

    def setUp(self):
        """Set up test environment with temporary directory"""
        self.tmpdir = tempfile.TemporaryDirectory()
        self.test_dir = Path(self.tmpdir.name)

    def tearDown(self):
        """Clean up temporary directory"""
        self.tmpdir.cleanup()

    def test_create_simple_application(self):
        """Test creating a simple application with default settings"""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["new", "my-test-app", "--path", str(self.test_dir)]
        )

        self.assertEqual(result.exit_code, 0, f"Failed: {result.output}")
        self.assertIn("Application created successfully!", result.output)

        # Check files were created
        python_file = self.test_dir / "my_test_app.py"
        readme_file = self.test_dir / "README.md"

        self.assertTrue(python_file.exists())
        self.assertTrue(readme_file.exists())

    def test_python_file_content(self):
        """Test that generated Python file has correct content"""
        runner = CliRunner()
        runner.invoke(cli, ["new", "test-app", "--path", str(self.test_dir)])

        python_file = self.test_dir / "test_app.py"
        content = python_file.read_text()

        # Check for key elements
        self.assertIn("from tensorlake.applications import", content)
        self.assertIn("@application()", content)
        self.assertIn("@function(description=", content)
        self.assertIn("def test_app(name: str) -> str:", content)

    def test_readme_file_content(self):
        """Test that generated README has correct content"""
        runner = CliRunner()
        runner.invoke(cli, ["new", "my-app", "--path", str(self.test_dir)])

        readme_file = self.test_dir / "README.md"
        content = readme_file.read_text()

        # Check for key sections
        self.assertIn("# my-app", content)
        self.assertIn("## Quick Start", content)
        self.assertIn("tensorlake deploy my_app.py", content)
        self.assertIn("https://api.tensorlake.ai/applications/my_app", content)
        self.assertIn("from my_app import my_app", content)

    def test_success_message_format(self):
        """Test that success message includes next steps"""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["new", "test-app", "--path", str(self.test_dir)]
        )

        self.assertIn("Next steps:", result.output)
        self.assertIn("Deploy:", result.output)
        self.assertIn("tensorlake deploy test_app.py", result.output)


class TestNameConversion(unittest.TestCase):
    """Test application name conversion to snake_case"""

    def setUp(self):
        """Set up test environment"""
        self.tmpdir = tempfile.TemporaryDirectory()
        self.test_dir = Path(self.tmpdir.name)

    def tearDown(self):
        """Clean up"""
        self.tmpdir.cleanup()

    def test_kebab_case_conversion(self):
        """Test kebab-case is converted to snake_case"""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["new", "my-cool-app", "--path", str(self.test_dir)]
        )

        self.assertEqual(result.exit_code, 0)
        self.assertTrue((self.test_dir / "my_cool_app.py").exists())

    def test_camel_case_conversion(self):
        """Test camelCase is converted to snake_case"""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["new", "myCoolApp", "--path", str(self.test_dir)]
        )

        self.assertEqual(result.exit_code, 0)
        self.assertTrue((self.test_dir / "my_cool_app.py").exists())

    def test_pascal_case_conversion(self):
        """Test PascalCase is converted to snake_case"""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["new", "MyCoolApp", "--path", str(self.test_dir)]
        )

        self.assertEqual(result.exit_code, 0)
        self.assertTrue((self.test_dir / "my_cool_app.py").exists())

    def test_spaces_conversion(self):
        """Test spaces are converted to underscores"""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["new", "my cool app", "--path", str(self.test_dir)]
        )

        self.assertEqual(result.exit_code, 0)
        self.assertTrue((self.test_dir / "my_cool_app.py").exists())

    def test_mixed_format_conversion(self):
        """Test mixed formats are converted correctly"""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["new", "My-Cool_App", "--path", str(self.test_dir)]
        )

        self.assertEqual(result.exit_code, 0)
        self.assertTrue((self.test_dir / "my_cool_app.py").exists())


class TestNameValidation(unittest.TestCase):
    """Test application name validation"""

    def setUp(self):
        """Set up test environment"""
        self.tmpdir = tempfile.TemporaryDirectory()
        self.test_dir = Path(self.tmpdir.name)

    def tearDown(self):
        """Clean up"""
        self.tmpdir.cleanup()

    def test_empty_name_rejected(self):
        """Test that empty name is rejected"""
        runner = CliRunner()
        result = runner.invoke(cli, ["new", "", "--path", str(self.test_dir)])

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Error", result.output)

    def test_invalid_characters_rejected(self):
        """Test that names with invalid characters are rejected"""
        runner = CliRunner()
        result = runner.invoke(cli, ["new", "my@app!", "--path", str(self.test_dir)])

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("can only contain", result.output.lower())

    def test_python_keyword_rejected(self):
        """Test that Python keywords are rejected"""
        runner = CliRunner()
        result = runner.invoke(cli, ["new", "class", "--path", str(self.test_dir)])

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("keyword", result.output.lower())

    def test_valid_names_accepted(self):
        """Test that valid names in various formats are accepted"""
        valid_names = ["myapp", "my_app", "my-app", "MyApp", "app123", "app_123"]

        for name in valid_names:
            with self.subTest(name=name):
                tmpdir = tempfile.TemporaryDirectory()
                test_dir = Path(tmpdir.name)

                runner = CliRunner()
                result = runner.invoke(cli, ["new", name, "--path", str(test_dir)])

                self.assertEqual(
                    result.exit_code, 0, f"Failed for name '{name}': {result.output}"
                )

                tmpdir.cleanup()


class TestConflictHandling(unittest.TestCase):
    """Test handling of existing files"""

    def setUp(self):
        """Set up test environment"""
        self.tmpdir = tempfile.TemporaryDirectory()
        self.test_dir = Path(self.tmpdir.name)

    def tearDown(self):
        """Clean up"""
        self.tmpdir.cleanup()

    def test_existing_python_file_without_force(self):
        """Test that existing Python file prevents creation without --force"""
        # Create the app first
        runner = CliRunner()
        result = runner.invoke(cli, ["new", "myapp", "--path", str(self.test_dir)])
        self.assertEqual(result.exit_code, 0)

        # Try to create again without --force
        result = runner.invoke(cli, ["new", "myapp", "--path", str(self.test_dir)])

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("already exists", result.output)

    def test_existing_python_file_with_force(self):
        """Test that existing Python file can be overwritten with --force"""
        # Create the app first
        runner = CliRunner()
        result = runner.invoke(cli, ["new", "myapp", "--path", str(self.test_dir)])
        self.assertEqual(result.exit_code, 0)

        # Modify the file
        python_file = self.test_dir / "myapp.py"
        python_file.write_text("# Modified content")

        # Create again with --force
        result = runner.invoke(
            cli, ["new", "myapp", "--path", str(self.test_dir), "--force"]
        )

        self.assertEqual(result.exit_code, 0)
        # Check that file was overwritten
        content = python_file.read_text()
        self.assertIn("from tensorlake.applications import", content)
        self.assertNotIn("# Modified content", content)

    def test_existing_readme_prompts_confirmation(self):
        """Test that existing README prompts for confirmation"""
        # Create a README
        readme_file = self.test_dir / "README.md"
        readme_file.write_text("# Existing README")

        runner = CliRunner()
        # Answer 'n' to the confirmation prompt
        result = runner.invoke(
            cli, ["new", "myapp", "--path", str(self.test_dir)], input="n\n"
        )

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Aborted", result.output)


class TestCustomPath(unittest.TestCase):
    """Test creating applications in custom paths"""

    def setUp(self):
        """Set up test environment"""
        self.tmpdir = tempfile.TemporaryDirectory()
        self.test_dir = Path(self.tmpdir.name)

    def tearDown(self):
        """Clean up"""
        self.tmpdir.cleanup()

    def test_create_in_custom_path(self):
        """Test creating application in a custom directory"""
        custom_dir = self.test_dir / "custom"
        custom_dir.mkdir()

        runner = CliRunner()
        result = runner.invoke(cli, ["new", "myapp", "--path", str(custom_dir)])

        self.assertEqual(result.exit_code, 0)
        self.assertTrue((custom_dir / "myapp.py").exists())
        self.assertTrue((custom_dir / "README.md").exists())

    def test_create_in_current_directory(self):
        """Test creating application in current directory (default)"""
        runner = CliRunner()
        # CliRunner uses isolated filesystem, so we can test default behavior
        with runner.isolated_filesystem(temp_dir=self.test_dir):
            result = runner.invoke(cli, ["new", "myapp"])

            self.assertEqual(result.exit_code, 0)
            self.assertTrue(Path("myapp.py").exists())
            self.assertTrue(Path("README.md").exists())


class TestGeneratedApplicationStructure(unittest.TestCase):
    """Test that generated application has correct structure"""

    def setUp(self):
        """Set up test environment"""
        self.tmpdir = tempfile.TemporaryDirectory()
        self.test_dir = Path(self.tmpdir.name)

    def tearDown(self):
        """Clean up"""
        self.tmpdir.cleanup()

    def test_application_has_decorators(self):
        """Test that generated application has required decorators"""
        runner = CliRunner()
        runner.invoke(cli, ["new", "myapp", "--path", str(self.test_dir)])

        content = (self.test_dir / "myapp.py").read_text()
        self.assertIn("@application()", content)
        self.assertIn("@function(description=", content)

    def test_application_has_docstrings(self):
        """Test that generated application has docstrings"""
        runner = CliRunner()
        runner.invoke(cli, ["new", "myapp", "--path", str(self.test_dir)])

        content = (self.test_dir / "myapp.py").read_text()
        # Check for docstring markers
        self.assertIn('"""', content)
        self.assertIn("Args:", content)
        self.assertIn("Returns:", content)

    def test_application_has_type_hints(self):
        """Test that generated application has type hints"""
        runner = CliRunner()
        runner.invoke(cli, ["new", "myapp", "--path", str(self.test_dir)])

        content = (self.test_dir / "myapp.py").read_text()
        self.assertIn("def myapp(name: str) -> str:", content)


if __name__ == "__main__":
    unittest.main()
