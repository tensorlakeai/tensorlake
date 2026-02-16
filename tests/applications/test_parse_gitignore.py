"""Tests for the _parse_gitignore fallback parser and ignored_code_paths integration.

Verifies that .gitignore patterns — including leading '/' (absolute anchors),
trailing '/' (directory markers), negation patterns ('!'), comments, wildcards,
and recursive matching — are handled correctly.

Uses nested directory structures to catch real production behavior.
"""

import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from tensorlake.applications.remote.code.ignored_code_paths import (
    _parse_gitignore,
    ignored_code_paths,
)
from tensorlake.applications.remote.code.loader import walk_code


def _make_tree(root: Path, paths: list[str]):
    """Create a file/directory tree under root.

    Paths ending with '/' create directories; everything else creates files
    (parent dirs are created automatically).
    """
    for p in paths:
        full = root / p
        if p.endswith("/"):
            full.mkdir(parents=True, exist_ok=True)
        else:
            full.parent.mkdir(parents=True, exist_ok=True)
            full.touch()


def _abspath(root: Path, rel: str) -> str:
    """Return os.path.abspath for a relative path under root.

    Uses os.path.abspath (NOT Path.resolve) to match how walk_code and
    ignored_code_paths produce paths in production.
    """
    return os.path.abspath(root / rel)


class TestLeadingSlashFix(unittest.TestCase):
    """The original bug (issue #527): leading '/' in .gitignore caused
    NotImplementedError from Path.glob(). These tests prove the fix."""

    def test_leading_slash_directory(self):
        with TemporaryDirectory() as root:
            root = Path(root)
            _make_tree(root, ["build/", "build/output.js"])

            gitignore = root / ".gitignore"
            gitignore.write_text("/build/\n")

            result = _parse_gitignore(root, gitignore)
            self.assertIn(_abspath(root, "build"), result)

    def test_leading_slash_file(self):
        with TemporaryDirectory() as root:
            root = Path(root)
            _make_tree(root, ["config.local"])

            gitignore = root / ".gitignore"
            gitignore.write_text("/config.local\n")

            result = _parse_gitignore(root, gitignore)
            self.assertIn(_abspath(root, "config.local"), result)

    def test_leading_slash_no_crash_on_missing_targets(self):
        with TemporaryDirectory() as root:
            root = Path(root)

            gitignore = root / ".gitignore"
            gitignore.write_text("/nonexistent\n/also_missing/\n")

            # Must not raise NotImplementedError
            result = _parse_gitignore(root, gitignore)
            self.assertEqual(result, set())

    def test_multiple_leading_slashes_stripped(self):
        with TemporaryDirectory() as root:
            root = Path(root)
            _make_tree(root, ["dist/"])

            gitignore = root / ".gitignore"
            gitignore.write_text("///dist\n")

            result = _parse_gitignore(root, gitignore)
            self.assertIn(_abspath(root, "dist"), result)

    def test_leading_slash_anchors_to_root_only(self):
        """'/build' should match root/build but NOT root/src/build."""
        with TemporaryDirectory() as root:
            root = Path(root)
            _make_tree(root, ["build/", "src/build/"])

            gitignore = root / ".gitignore"
            gitignore.write_text("/build/\n")

            result = _parse_gitignore(root, gitignore)
            self.assertIn(_abspath(root, "build"), result)
            self.assertNotIn(_abspath(root, "src/build"), result)


class TestRecursiveMatching(unittest.TestCase):
    """Patterns without '/' should match at any depth — this is the key
    .gitignore semantic that a naive root.glob() misses."""

    def test_wildcard_matches_nested_files(self):
        """'*.pyc' should match at root AND inside subdirectories."""
        with TemporaryDirectory() as root:
            root = Path(root)
            _make_tree(
                root,
                [
                    "top.pyc",
                    "src/nested.pyc",
                    "src/deep/very_nested.pyc",
                    "src/keep.py",
                ],
            )

            gitignore = root / ".gitignore"
            gitignore.write_text("*.pyc\n")

            result = _parse_gitignore(root, gitignore)
            self.assertIn(_abspath(root, "top.pyc"), result)
            self.assertIn(_abspath(root, "src/nested.pyc"), result)
            self.assertIn(_abspath(root, "src/deep/very_nested.pyc"), result)
            self.assertNotIn(_abspath(root, "src/keep.py"), result)

    def test_directory_pattern_matches_nested(self):
        """'__pycache__/' should match at any depth."""
        with TemporaryDirectory() as root:
            root = Path(root)
            _make_tree(
                root,
                [
                    "__pycache__/",
                    "src/__pycache__/",
                    "src/pkg/__pycache__/",
                ],
            )

            gitignore = root / ".gitignore"
            gitignore.write_text("__pycache__/\n")

            result = _parse_gitignore(root, gitignore)
            self.assertIn(_abspath(root, "__pycache__"), result)
            self.assertIn(_abspath(root, "src/__pycache__"), result)
            self.assertIn(_abspath(root, "src/pkg/__pycache__"), result)

    def test_node_modules_matches_nested(self):
        """'node_modules/' should match at any depth."""
        with TemporaryDirectory() as root:
            root = Path(root)
            _make_tree(
                root,
                [
                    "node_modules/",
                    "packages/frontend/node_modules/",
                ],
            )

            gitignore = root / ".gitignore"
            gitignore.write_text("node_modules/\n")

            result = _parse_gitignore(root, gitignore)
            self.assertIn(_abspath(root, "node_modules"), result)
            self.assertIn(
                _abspath(root, "packages/frontend/node_modules"), result
            )

    def test_dotenv_matches_nested(self):
        """'.env' without '/' should match at any depth."""
        with TemporaryDirectory() as root:
            root = Path(root)
            _make_tree(root, [".env", "services/api/.env"])

            gitignore = root / ".gitignore"
            gitignore.write_text(".env\n")

            result = _parse_gitignore(root, gitignore)
            self.assertIn(_abspath(root, ".env"), result)
            self.assertIn(_abspath(root, "services/api/.env"), result)

    def test_slash_in_pattern_anchors_to_root(self):
        """'src/build' contains '/' so it should only match at root, not recursively."""
        with TemporaryDirectory() as root:
            root = Path(root)
            _make_tree(root, ["src/build/", "other/src/build/"])

            gitignore = root / ".gitignore"
            gitignore.write_text("src/build\n")

            result = _parse_gitignore(root, gitignore)
            self.assertIn(_abspath(root, "src/build"), result)
            self.assertNotIn(_abspath(root, "other/src/build"), result)


class TestNegation(unittest.TestCase):
    """Negation patterns ('!') are unsupported and should be skipped."""

    def test_negation_skipped_no_crash(self):
        with TemporaryDirectory() as root:
            root = Path(root)
            _make_tree(root, ["logs/", "logs/important.log"])

            gitignore = root / ".gitignore"
            gitignore.write_text("logs/\n!logs/important.log\n")

            result = _parse_gitignore(root, gitignore)
            self.assertIn(_abspath(root, "logs"), result)


class TestDirectoryOnlyPatterns(unittest.TestCase):
    """Trailing '/' means only match directories, not files."""

    def test_trailing_slash_skips_files(self):
        with TemporaryDirectory() as root:
            root = Path(root)
            # Create both a file and directory with the same name in different places
            (root / "logs").mkdir()
            (root / "src").mkdir()
            (root / "src" / "logs").touch()  # a file named 'logs'

            gitignore = root / ".gitignore"
            gitignore.write_text("logs/\n")

            result = _parse_gitignore(root, gitignore)
            self.assertIn(_abspath(root, "logs"), result)
            self.assertNotIn(_abspath(root, "src/logs"), result)


class TestBasicPatterns(unittest.TestCase):

    def test_comments_ignored(self):
        with TemporaryDirectory() as root:
            root = Path(root)
            _make_tree(root, ["dist/"])

            gitignore = root / ".gitignore"
            gitignore.write_text("# comment\ndist/\n# another\n")

            result = _parse_gitignore(root, gitignore)
            self.assertIn(_abspath(root, "dist"), result)

    def test_blank_lines_ignored(self):
        with TemporaryDirectory() as root:
            root = Path(root)
            _make_tree(root, ["dist/"])

            gitignore = root / ".gitignore"
            gitignore.write_text("\n\ndist/\n\n")

            result = _parse_gitignore(root, gitignore)
            self.assertIn(_abspath(root, "dist"), result)

    def test_wildcard_multiple_matches(self):
        with TemporaryDirectory() as root:
            root = Path(root)
            _make_tree(root, ["app.log", "error.log", "readme.txt"])

            gitignore = root / ".gitignore"
            gitignore.write_text("*.log\n")

            result = _parse_gitignore(root, gitignore)
            self.assertIn(_abspath(root, "app.log"), result)
            self.assertIn(_abspath(root, "error.log"), result)
            self.assertNotIn(_abspath(root, "readme.txt"), result)


class TestEdgeCases(unittest.TestCase):

    def test_empty_gitignore(self):
        with TemporaryDirectory() as root:
            root = Path(root)
            gitignore = root / ".gitignore"
            gitignore.write_text("")
            self.assertEqual(_parse_gitignore(root, gitignore), set())

    def test_only_comments(self):
        with TemporaryDirectory() as root:
            root = Path(root)
            gitignore = root / ".gitignore"
            gitignore.write_text("# comment\n\n# another\n")
            self.assertEqual(_parse_gitignore(root, gitignore), set())

    def test_no_matches(self):
        with TemporaryDirectory() as root:
            root = Path(root)
            gitignore = root / ".gitignore"
            gitignore.write_text("*.xyz\nnonexistent_dir/\n")
            self.assertEqual(_parse_gitignore(root, gitignore), set())

    def test_trailing_whitespace_stripped(self):
        with TemporaryDirectory() as root:
            root = Path(root)
            _make_tree(root, ["dist/"])

            gitignore = root / ".gitignore"
            gitignore.write_text("dist/   \n")

            result = _parse_gitignore(root, gitignore)
            self.assertIn(_abspath(root, "dist"), result)


class TestRealisticGitignore(unittest.TestCase):
    """Test with a .gitignore that looks like a real Python project."""

    def test_python_project_gitignore(self):
        with TemporaryDirectory() as root:
            root = Path(root)
            _make_tree(
                root,
                [
                    # source code (should NOT be ignored)
                    "src/app/main.py",
                    "src/app/utils.py",
                    "tests/test_main.py",
                    "README.md",
                    # build artifacts
                    "dist/",
                    "dist/app-1.0.tar.gz",
                    "build/",
                    "build/lib/app/main.py",
                    # bytecode
                    "src/app/__pycache__/",
                    "src/app/__pycache__/main.cpython-311.pyc",
                    "tests/__pycache__/",
                    "__pycache__/",
                    "top.pyc",
                    # environment
                    ".env",
                    "services/.env",
                    # IDE
                    ".idea/",
                    ".idea/workspace.xml",
                    # virtualenv
                    "venv/",
                    "venv/bin/python",
                ],
            )

            gitignore = root / ".gitignore"
            gitignore.write_text(
                "# bytecode\n"
                "__pycache__/\n"
                "*.pyc\n"
                "\n"
                "# build\n"
                "/dist/\n"
                "/build/\n"
                "\n"
                "# environment\n"
                ".env\n"
                "\n"
                "# IDE\n"
                ".idea/\n"
                "\n"
                "# virtualenv\n"
                "venv/\n"
            )

            result = _parse_gitignore(root, gitignore)

            # Source code must NOT be excluded
            self.assertNotIn(_abspath(root, "src/app/main.py"), result)
            self.assertNotIn(_abspath(root, "src/app/utils.py"), result)
            self.assertNotIn(_abspath(root, "tests/test_main.py"), result)
            self.assertNotIn(_abspath(root, "README.md"), result)

            # Build dirs excluded (anchored with /)
            self.assertIn(_abspath(root, "dist"), result)
            self.assertIn(_abspath(root, "build"), result)

            # __pycache__ excluded at all depths
            self.assertIn(_abspath(root, "__pycache__"), result)
            self.assertIn(_abspath(root, "src/app/__pycache__"), result)
            self.assertIn(_abspath(root, "tests/__pycache__"), result)

            # .pyc files at any depth
            self.assertIn(_abspath(root, "top.pyc"), result)
            self.assertIn(
                _abspath(root, "src/app/__pycache__/main.cpython-311.pyc"),
                result,
            )

            # .env at any depth
            self.assertIn(_abspath(root, ".env"), result)
            self.assertIn(_abspath(root, "services/.env"), result)

            # .idea at root (and nested via **)
            self.assertIn(_abspath(root, ".idea"), result)

            # venv at root (and nested via **)
            self.assertIn(_abspath(root, "venv"), result)


class TestIgnoredCodePathsIntegration(unittest.TestCase):
    """Integration tests for the top-level ignored_code_paths() function."""

    def test_detects_virtualenv_by_pyvenv_cfg(self):
        """A directory with pyvenv.cfg should be excluded even without VIRTUAL_ENV."""
        with TemporaryDirectory() as root:
            root = Path(root)
            _make_tree(
                root,
                [
                    "myenv/pyvenv.cfg",
                    "myenv/bin/python",
                    "src/main.py",
                ],
            )

            with patch.dict(os.environ, {}, clear=False):
                os.environ.pop("VIRTUAL_ENV", None)
                result = ignored_code_paths(str(root))

            self.assertIn(_abspath(root, "myenv"), result)
            self.assertNotIn(_abspath(root, "src"), result)

    def test_fallback_when_not_git_repo(self):
        """Without a git repo, the fallback parser handles leading '/'."""
        with TemporaryDirectory() as root:
            root = Path(root)
            _make_tree(
                root,
                [
                    "dist/",
                    "dist/bundle.js",
                    "src/app.py",
                    "src/__pycache__/",
                    "src/__pycache__/app.cpython-311.pyc",
                ],
            )

            gitignore = root / ".gitignore"
            gitignore.write_text("/dist/\n__pycache__/\n")

            with patch.dict(os.environ, {}, clear=False):
                os.environ.pop("VIRTUAL_ENV", None)
                result = ignored_code_paths(str(root))

            self.assertIn(_abspath(root, "dist"), result)
            self.assertIn(_abspath(root, "src/__pycache__"), result)
            self.assertNotIn(_abspath(root, "src/app.py"), result)


class TestWalkCodeEndToEnd(unittest.TestCase):
    """End-to-end tests: verify walk_code() actually skips excluded paths.

    This is the critical test — it proves the exclusion set from
    ignored_code_paths() actually prevents files from being walked.
    Uses the same path format (os.path.abspath) as production.
    """

    def test_walk_skips_ignored_directories(self):
        """walk_code must not yield .py files inside ignored directories."""
        with TemporaryDirectory() as root:
            root = Path(root)
            _make_tree(
                root,
                [
                    "app.py",
                    "src/main.py",
                    "venv/lib/site.py",
                    "venv/pyvenv.cfg",
                    "__pycache__/cached.pyc",
                    "src/__pycache__/main.cpython-311.pyc",
                ],
            )

            gitignore = root / ".gitignore"
            gitignore.write_text("__pycache__/\n")

            with patch.dict(os.environ, {}, clear=False):
                os.environ.pop("VIRTUAL_ENV", None)
                excluded = ignored_code_paths(str(root))

            walked_files = list(walk_code(str(root), excluded))

            # Source files should be walked
            self.assertIn(_abspath(root, "app.py"), walked_files)
            self.assertIn(_abspath(root, "src/main.py"), walked_files)

            # Files inside venv (detected by pyvenv.cfg) must be skipped
            self.assertNotIn(
                _abspath(root, "venv/lib/site.py"), walked_files
            )

    def test_walk_skips_gitignored_nested_pycache(self):
        """walk_code must skip __pycache__ at all depths when gitignored."""
        with TemporaryDirectory() as root:
            root = Path(root)
            _make_tree(
                root,
                [
                    "app.py",
                    "pkg/__init__.py",
                    "pkg/mod.py",
                    "pkg/__pycache__/mod.cpython-311.pyc",
                    "__pycache__/app.cpython-311.pyc",
                ],
            )

            gitignore = root / ".gitignore"
            gitignore.write_text("__pycache__/\n")

            # Not a git repo — forces fallback parser
            with patch.dict(os.environ, {}, clear=False):
                os.environ.pop("VIRTUAL_ENV", None)
                excluded = ignored_code_paths(str(root))

            walked_files = list(walk_code(str(root), excluded))

            # walk_code only yields .py files, so .pyc won't appear regardless.
            # But the directories themselves should be pruned from the walk.
            # Verify source .py files ARE included:
            self.assertIn(_abspath(root, "app.py"), walked_files)
            self.assertIn(_abspath(root, "pkg/__init__.py"), walked_files)
            self.assertIn(_abspath(root, "pkg/mod.py"), walked_files)

            # Verify no files from __pycache__ dirs are walked (even if .py existed there)
            for f in walked_files:
                self.assertNotIn("__pycache__", f)

    def test_walk_with_leading_slash_gitignore(self):
        """The original bug scenario: deploy with leading '/' in .gitignore."""
        with TemporaryDirectory() as root:
            root = Path(root)
            _make_tree(
                root,
                [
                    "app.py",
                    "src/utils.py",
                    "build/generated.py",
                    "dist/bundle.py",
                ],
            )

            gitignore = root / ".gitignore"
            gitignore.write_text("/build/\n/dist/\n")

            # Not a git repo — forces fallback parser, exercises the leading '/' fix
            with patch.dict(os.environ, {}, clear=False):
                os.environ.pop("VIRTUAL_ENV", None)
                excluded = ignored_code_paths(str(root))

            walked_files = list(walk_code(str(root), excluded))

            self.assertIn(_abspath(root, "app.py"), walked_files)
            self.assertIn(_abspath(root, "src/utils.py"), walked_files)
            self.assertNotIn(_abspath(root, "build/generated.py"), walked_files)
            self.assertNotIn(_abspath(root, "dist/bundle.py"), walked_files)


if __name__ == "__main__":
    unittest.main()
