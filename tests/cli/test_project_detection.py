"""Tests for project root detection functionality."""

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tensorlake.cli._configuration import save_local_config
from tensorlake.cli._project_detection import (
    add_to_gitignore,
    check_for_nested_configs,
    find_gitignore_path,
    find_project_root,
    find_project_root_interactive,
    get_detection_reason,
)


class TestFindProjectRoot(unittest.TestCase):
    """Test find_project_root function."""

    def test_find_existing_tensorlake_toml_in_current_dir(self):
        """Test detection of .tensorlake/config.toml in current directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir).resolve().resolve()
            config_dir = project_dir / ".tensorlake"
            config_dir.mkdir(parents=True, exist_ok=True)
            config_file = config_dir / "config.toml"
            config_file.touch()

            result = find_project_root(project_dir)
            self.assertEqual(result, project_dir)

    def test_find_existing_tensorlake_toml_in_parent_dir(self):
        """Test detection of .tensorlake/config.toml in parent directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir).resolve()
            config_dir = project_dir / ".tensorlake"
            config_dir.mkdir(parents=True, exist_ok=True)
            config_file = config_dir / "config.toml"
            config_file.touch()

            # Create subdirectory
            subdir = project_dir / "src" / "tensorlake"
            subdir.mkdir(parents=True)

            result = find_project_root(subdir)
            self.assertEqual(result, project_dir)

    def test_find_git_directory(self):
        """Test detection of .git directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir).resolve()
            git_dir = project_dir / ".git"
            git_dir.mkdir()

            result = find_project_root(project_dir)
            self.assertEqual(result, project_dir)

    def test_find_git_directory_in_parent(self):
        """Test detection of .git directory in parent."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir).resolve()
            git_dir = project_dir / ".git"
            git_dir.mkdir()

            subdir = project_dir / "tests"
            subdir.mkdir()

            result = find_project_root(subdir)
            self.assertEqual(result, project_dir)

    def test_find_pyproject_toml(self):
        """Test detection of pyproject.toml."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir).resolve()
            pyproject = project_dir / "pyproject.toml"
            pyproject.touch()

            result = find_project_root(project_dir)
            self.assertEqual(result, project_dir)

    def test_find_setup_py(self):
        """Test detection of setup.py."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir).resolve()
            setup_py = project_dir / "setup.py"
            setup_py.touch()

            result = find_project_root(project_dir)
            self.assertEqual(result, project_dir)

    def test_find_requirements_txt(self):
        """Test detection of requirements.txt."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir).resolve()
            requirements = project_dir / "requirements.txt"
            requirements.touch()

            result = find_project_root(project_dir)
            self.assertEqual(result, project_dir)

    def test_fallback_to_current_directory(self):
        """Test fallback to current directory when no markers found."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir).resolve()
            # No marker files

            result = find_project_root(project_dir)
            self.assertEqual(result, project_dir)

    def test_priority_tensorlake_toml_over_git(self):
        """Test that existing .tensorlake/config.toml takes priority over .git."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir).resolve()

            # Create git root
            git_root = root / "git_project"
            git_root.mkdir()
            (git_root / ".git").mkdir()

            # Create tensorlake config in subdirectory
            subproject = git_root / "subproject"
            subproject.mkdir()
            config_dir = subproject / ".tensorlake"
            config_dir.mkdir(parents=True, exist_ok=True)
            (config_dir / "config.toml").touch()

            # Should find subproject, not git_root
            result = find_project_root(subproject)
            self.assertEqual(result, subproject)

    def test_priority_git_over_python_markers(self):
        """Test that .git takes priority over Python project files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir).resolve()

            # Create Python project root
            python_root = root / "python_project"
            python_root.mkdir()
            (python_root / "pyproject.toml").touch()

            # Create git root in subdirectory
            git_subdir = python_root / "monorepo"
            git_subdir.mkdir()
            (git_subdir / ".git").mkdir()

            # Should find git_subdir, not python_root
            result = find_project_root(git_subdir)
            self.assertEqual(result, git_subdir)

    def test_uses_current_directory_as_default_start_path(self):
        """Test that current directory is used when start_path is None."""
        # This test ensures the function works with Path.cwd()
        result = find_project_root(None)
        # Should return some path (at least current directory)
        self.assertIsInstance(result, Path)


class TestGetDetectionReason(unittest.TestCase):
    """Test get_detection_reason function."""

    def test_reason_for_tensorlake_toml(self):
        """Test reason for .tensorlake/config.toml detection."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir).resolve()
            config_dir = project_dir / ".tensorlake"
            config_dir.mkdir(parents=True, exist_ok=True)
            (config_dir / "config.toml").touch()

            reason = get_detection_reason(project_dir)
            self.assertEqual(reason, "Found existing .tensorlake/config.toml")

    def test_reason_for_git_directory(self):
        """Test reason for .git detection."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir).resolve()
            (project_dir / ".git").mkdir()

            reason = get_detection_reason(project_dir)
            self.assertEqual(reason, "Found .git directory")

    def test_reason_for_pyproject_toml(self):
        """Test reason for pyproject.toml detection."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir).resolve()
            (project_dir / "pyproject.toml").touch()

            reason = get_detection_reason(project_dir)
            self.assertEqual(reason, "Found pyproject.toml")

    def test_reason_for_setup_py(self):
        """Test reason for setup.py detection."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir).resolve()
            (project_dir / "setup.py").touch()

            reason = get_detection_reason(project_dir)
            self.assertEqual(reason, "Found setup.py")

    def test_reason_for_no_markers(self):
        """Test reason when no markers found."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir).resolve()

            reason = get_detection_reason(project_dir)
            self.assertEqual(
                reason, "Using current directory (no project markers found)"
            )


class TestFindProjectRootInteractive(unittest.TestCase):
    """Test find_project_root_interactive function."""

    def test_user_confirms_detected_directory(self):
        """Test when user confirms the detected directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir).resolve()
            (project_dir / ".git").mkdir()

            with patch("click.echo"), patch("click.confirm", return_value=True):
                result = find_project_root_interactive(project_dir)
                self.assertEqual(result, project_dir)

    def test_user_provides_custom_directory(self):
        """Test when user provides a custom directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            detected_dir = Path(tmpdir).resolve()
            (detected_dir / ".git").mkdir()

            custom_dir = detected_dir / "custom"
            custom_dir.mkdir()

            with patch("click.echo"), patch("click.confirm", return_value=False), patch(
                "click.prompt", return_value=str(custom_dir)
            ):
                result = find_project_root_interactive(detected_dir)
                self.assertEqual(result.resolve(), custom_dir.resolve())


class TestCheckForNestedConfigs(unittest.TestCase):
    """Test check_for_nested_configs function."""

    def test_no_configs_found(self):
        """Test when no configs exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir).resolve()

            # Change to temp directory so check doesn't find parent configs
            original_cwd = os.getcwd()
            try:
                os.chdir(project_dir)
                configs = check_for_nested_configs(project_dir)
                self.assertEqual(configs, [])
            finally:
                os.chdir(original_cwd)

    def test_single_config_in_current_directory(self):
        """Test when single config exists in current directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir).resolve()
            config_dir = project_dir / ".tensorlake"
            config_dir.mkdir(parents=True, exist_ok=True)
            config_file = config_dir / "config.toml"
            config_file.touch()

            # Change to this directory for the test
            original_cwd = os.getcwd()
            try:
                os.chdir(project_dir)
                configs = check_for_nested_configs(project_dir)
                self.assertEqual(len(configs), 1)
                self.assertEqual(configs[0], config_file)
            finally:
                os.chdir(original_cwd)

    def test_multiple_configs_in_hierarchy(self):
        """Test when multiple configs exist in directory hierarchy."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir).resolve()
            config_dir1 = root / ".tensorlake"
            config_dir1.mkdir(parents=True, exist_ok=True)
            config1 = config_dir1 / "config.toml"
            config1.touch()

            subdir = root / "project"
            subdir.mkdir()
            config_dir2 = subdir / ".tensorlake"
            config_dir2.mkdir(parents=True, exist_ok=True)
            config2 = config_dir2 / "config.toml"
            config2.touch()

            # Change to subdirectory for the test
            original_cwd = os.getcwd()
            try:
                os.chdir(subdir)
                configs = check_for_nested_configs(subdir)
                # Should find both configs
                self.assertGreaterEqual(len(configs), 1)
                # First one should be in subdir/.tensorlake
                self.assertEqual(configs[0].parent.parent, subdir)
            finally:
                os.chdir(original_cwd)


class TestFindGitignorePath(unittest.TestCase):
    """Test find_gitignore_path function."""

    def test_find_gitignore_in_git_repository(self):
        """Test finding .gitignore in a git repository."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir).resolve()
            git_dir = project_dir / ".git"
            git_dir.mkdir()

            result = find_gitignore_path(project_dir)
            expected = project_dir / ".gitignore"
            self.assertEqual(result, expected)

    def test_find_gitignore_in_parent_git_repository(self):
        """Test finding .gitignore in parent directory's git repository."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir).resolve()
            git_dir = project_dir / ".git"
            git_dir.mkdir()

            # Start from subdirectory
            subdir = project_dir / "src" / "tensorlake"
            subdir.mkdir(parents=True)

            result = find_gitignore_path(subdir)
            expected = project_dir / ".gitignore"
            self.assertEqual(result, expected)

    def test_find_gitignore_without_git_repository(self):
        """Test that None is returned when no git repository exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir).resolve()
            # No .git directory

            result = find_gitignore_path(project_dir)
            self.assertIsNone(result)

    def test_find_gitignore_uses_current_directory_as_default(self):
        """Test that current directory is used when start_path is None."""
        # This should return a result or None depending on whether we're in a git repo
        result = find_gitignore_path(None)
        # Just verify it returns a Path or None (don't assert specific value)
        self.assertTrue(result is None or isinstance(result, Path))


class TestAddToGitignore(unittest.TestCase):
    """Test add_to_gitignore function."""

    def test_create_new_gitignore_file(self):
        """Test creating a new .gitignore file with an entry."""
        with tempfile.TemporaryDirectory() as tmpdir:
            gitignore_path = Path(tmpdir) / ".gitignore"
            self.assertFalse(gitignore_path.exists())

            add_to_gitignore(gitignore_path, ".tensorlake/")

            # Verify file was created
            self.assertTrue(gitignore_path.exists())

            # Verify content
            with open(gitignore_path, "r") as f:
                content = f.read()
            self.assertEqual(content, ".tensorlake/\n")

    def test_append_to_existing_gitignore(self):
        """Test appending to an existing .gitignore file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            gitignore_path = Path(tmpdir) / ".gitignore"

            # Create existing .gitignore with some content
            with open(gitignore_path, "w") as f:
                f.write("*.pyc\n__pycache__/\n")

            add_to_gitignore(gitignore_path, ".tensorlake/")

            # Verify content was appended
            with open(gitignore_path, "r") as f:
                content = f.read()
            self.assertEqual(content, "*.pyc\n__pycache__/\n.tensorlake/\n")

    def test_skip_duplicate_entry_exact_match(self):
        """Test that duplicate entries are not added."""
        with tempfile.TemporaryDirectory() as tmpdir:
            gitignore_path = Path(tmpdir) / ".gitignore"

            # Create .gitignore with the entry already present
            with open(gitignore_path, "w") as f:
                f.write("*.pyc\n.tensorlake/\n__pycache__/\n")

            add_to_gitignore(gitignore_path, ".tensorlake/")

            # Verify content wasn't duplicated
            with open(gitignore_path, "r") as f:
                content = f.read()
            self.assertEqual(content, "*.pyc\n.tensorlake/\n__pycache__/\n")

    def test_skip_duplicate_entry_with_leading_slash(self):
        """Test that entries with leading slash are detected as duplicates."""
        with tempfile.TemporaryDirectory() as tmpdir:
            gitignore_path = Path(tmpdir) / ".gitignore"

            # Create .gitignore with entry that has leading slash
            with open(gitignore_path, "w") as f:
                f.write("*.pyc\n/.tensorlake/\n")

            add_to_gitignore(gitignore_path, ".tensorlake/")

            # Verify content wasn't duplicated
            with open(gitignore_path, "r") as f:
                content = f.read()
            self.assertEqual(content, "*.pyc\n/.tensorlake/\n")

    def test_append_to_file_without_trailing_newline(self):
        """Test appending when existing file doesn't end with newline."""
        with tempfile.TemporaryDirectory() as tmpdir:
            gitignore_path = Path(tmpdir) / ".gitignore"

            # Create .gitignore without trailing newline
            with open(gitignore_path, "w") as f:
                f.write("*.pyc")

            add_to_gitignore(gitignore_path, ".tensorlake/")

            # Verify newline was added before the new entry
            with open(gitignore_path, "r") as f:
                content = f.read()
            self.assertEqual(content, "*.pyc\n.tensorlake/\n")


class TestSaveLocalConfigGitignoreIntegration(unittest.TestCase):
    """Test that save_local_config updates .gitignore."""

    def test_save_local_config_updates_gitignore_in_git_repo(self):
        """Test that saving config adds .tensorlake/ to .gitignore in git repo."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir).resolve()

            # Create git repository
            git_dir = project_dir / ".git"
            git_dir.mkdir()

            # Save local config
            config_data = {"organization": "test_org", "project": "test_proj"}
            save_local_config(config_data, project_dir)

            # Verify .tensorlake/config.toml was created
            config_path = project_dir / ".tensorlake" / "config.toml"
            self.assertTrue(config_path.exists())

            # Verify .gitignore was created/updated at git root
            gitignore_path = project_dir / ".gitignore"
            self.assertTrue(gitignore_path.exists())

            # Verify .tensorlake/ is in .gitignore
            with open(gitignore_path, "r") as f:
                content = f.read()
            self.assertIn(".tensorlake/", content)

    def test_save_local_config_updates_gitignore_without_git_repo(self):
        """Test that saving config creates .gitignore next to config when no git repo."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir).resolve()
            # No .git directory

            # Save local config
            config_data = {"organization": "test_org", "project": "test_proj"}
            save_local_config(config_data, project_dir)

            # Verify .tensorlake/config.toml was created
            config_path = project_dir / ".tensorlake" / "config.toml"
            self.assertTrue(config_path.exists())

            # Verify .gitignore was created next to config
            gitignore_path = project_dir / ".gitignore"
            self.assertTrue(gitignore_path.exists())

            # Verify .tensorlake/ is in .gitignore
            with open(gitignore_path, "r") as f:
                content = f.read()
            self.assertIn(".tensorlake/", content)

    def test_save_local_config_doesnt_duplicate_gitignore_entry(self):
        """Test that saving config twice doesn't duplicate .gitignore entry."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir).resolve()

            # Create git repository
            git_dir = project_dir / ".git"
            git_dir.mkdir()

            # Save local config twice
            config_data = {"organization": "test_org", "project": "test_proj"}
            save_local_config(config_data, project_dir)
            save_local_config(config_data, project_dir)

            # Verify .gitignore has only one entry
            gitignore_path = project_dir / ".gitignore"
            with open(gitignore_path, "r") as f:
                content = f.read()
            # Count occurrences
            count = content.count(".tensorlake/")
            self.assertEqual(count, 1, "Entry should appear only once")


if __name__ == "__main__":
    unittest.main()
