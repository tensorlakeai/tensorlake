# Contributing to TensorLake

Thank you for your interest in contributing to TensorLake! We welcome contributions from the community and are excited to see what you'll build with us.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Contributing Guidelines](#contributing-guidelines)
- [Pull Request Process](#pull-request-process)
- [Testing](#testing)
- [Documentation](#documentation)
- [Community](#community)

## Code of Conduct

This project and everyone participating in it is governed by our [Code of Conduct](code-of-conduct.md). By participating, you are expected to uphold this code. Please report unacceptable behavior to [support@tensorlake.ai](mailto:support@tensorlake.ai).

## Getting Started

### Types of Contributions

We welcome many types of contributions, including:

- 🐛 **Bug fixes** - Help us squash bugs and improve reliability
- ✨ **New features** - Add new functionality to the SDK
- 📚 **Documentation** - Improve our docs, add examples, or write tutorials
- 🧪 **Tests** - Add test coverage or improve existing tests
- 🎨 **Examples** - Create new examples or improve existing ones
- 🔧 **Developer experience** - Improve tooling, CI/CD, or development workflows

### Before You Start

1. **Search existing issues** - Check if someone else has already reported the bug or requested the feature
2. **Create an issue** - For new features or significant changes, please create an issue first to discuss the approach
3. **Start small** - If you're new to the project, consider starting with a "good first issue"

## Development Setup

### Prerequisites

- Python 3.10 or higher
- [Poetry 2.0.0](https://python-poetry.org/) for Python dependency management
- [Rust](https://rustup.rs/) (stable toolchain) — required to build the CLI binary
- Git

Install Rust via rustup if you don't have it:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### Setup Instructions

1. **Fork and clone the repository**
   ```bash
   git clone https://github.com/YOUR_USERNAME/tensorlake.git
   cd tensorlake
   ```

2. **Choose your install method**

   **Option A — Global install (recommended):** installs into your user Python environment (`~/.local/`) so `tl` and `tensorlake` work from any directory without activating a virtualenv.

   ```bash
   make install-global
   ```

   Then make sure `~/.local/bin` is on your `PATH` (one-time setup):

   ```fish
   # fish
   fish_add_path ~/.local/bin
   ```
   ```bash
   # bash / zsh
   export PATH="$HOME/.local/bin:$PATH"
   ```

   **Option B — Virtualenv install:** installs into the Poetry-managed virtualenv. The binaries are only available while the venv is active.

   ```bash
   make install-dev
   poetry shell   # activate the venv
   ```

   > **Note:** gRPC stubs are pre-generated and committed to the repository. Run `make build_proto` separately only if you modify `.proto` files. The `build_proto` step requires grpcio 1.60.0 (pinned for stub compatibility), which must be compiled from source and needs Python < 3.13.

3. **Verify the installation**
   ```bash
   tl --help
   tensorlake --help
   ```

### Project Structure

```
tensorlake/
├── src/tensorlake/              # Python SDK source
│   ├── applications/            # Serverless applications SDK
│   ├── cli/                     # Python CLI entry points (NDJSON producers)
│   ├── documentai/              # Document AI client
│   └── function_executor/       # gRPC function executor server
├── crates/
│   ├── cli/                     # Rust CLI binary (tl / tensorlake)
│   └── cloud-sdk/               # Rust cloud SDK
├── tensorlake.data/scripts/     # Python wrapper scripts installed alongside the CLI
├── tests/                       # Test suite
├── Makefile                     # Build and development commands
├── pyproject.toml               # Python project configuration (maturin backend)
└── Cargo.toml                   # Rust workspace configuration
```

### How the CLI Works

The CLI is a **Rust binary** (`tl` / `tensorlake`) that delegates work to **Python wrapper scripts** for commands that need to import user application code. This split allows the Rust binary to handle argument parsing, authentication, and output rendering, while Python handles SDK logic.

For example, `tl deploy app.py` spawns `tensorlake-deploy app.py`, reads its NDJSON output on stdout, and renders it as human-readable text.

Wrapper scripts live in `tensorlake.data/scripts/` and are installed into the virtualenv's `bin/` directory during `make install-dev`. They must be on `PATH` (alongside the Rust binary) for the affected CLI commands to work.

| Rust command | Spawns wrapper script |
|---|---|
| `tl deploy` | `tensorlake-deploy` |
| `tl parse` | `tensorlake-parse` |
| `tl generate-dockerfiles` | `tensorlake-generate-dockerfiles` |

`tl sbx image create` is handled directly by the Rust CLI. Programmatic SDK image builds should go through the language-native `Image` DSL APIs rather than passing raw Dockerfile paths into the Python or TypeScript packages.

### Available Makefile Commands

```bash
# Global install: build Rust CLI (release) + install Python package into ~/.local/
make install-global

# Virtualenv install: build Rust CLI (debug) + install into Poetry venv
make install-dev

# Same as install-dev but with release optimisations
make install-dev-release

# Build distributable wheel (does not install the CLI locally)
make build

# Regenerate gRPC stubs from .proto files
make build_proto

# Format Python code (Black + isort)
make fmt

# Check formatting without modifying files
make check

# Run all tests
make test

# Run Document AI tests only
make test_document_ai
```

### Working on the Rust CLI

After editing Rust code in `crates/cli/`, rebuild and reinstall with:

```bash
make install-dev
```

Or use Cargo directly for a faster iteration loop (the binary won't be in the venv until you copy it):

```bash
cargo build -p tensorlake-cli
# binary at target/debug/tl — run it directly or copy to your PATH
```

## Contributing Guidelines

### Code Style

We use several tools to maintain code quality:

- **Black** for code formatting
- **isort** for import sorting
- **flake8** for linting
- **mypy** for type checking

Run all checks before submitting:
```bash
# Format code
make fmt

# Check formatting and linting
make check
```

### Commit Messages

We follow the [Conventional Commits](https://www.conventionalcommits.org/) specification:

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

**Types:**
- `feat`: A new feature
- `fix`: A bug fix
- `docs`: Documentation only changes
- `style`: Changes that do not affect the meaning of the code
- `refactor`: A code change that neither fixes a bug nor adds a feature
- `test`: Adding missing tests or correcting existing tests
- `chore`: Changes to the build process or auxiliary tools

**Examples:**
```
feat(documentai): add signature detection support
fix(cli): resolve authentication error on Windows
docs(examples): add real estate analysis example
test(functions): add unit tests for graph validation
```

### Branch Naming

Use descriptive branch names with prefixes:
- `feature/description` - For new features
- `fix/description` - For bug fixes
- `docs/description` - For documentation changes
- `test/description` - For test improvements

Examples:
- `feature/signature-detection`
- `fix/cli-auth-windows`
- `docs/contributing-guide`

## Pull Request Process

### Before Creating a PR

1. **Update your fork**
   ```bash
   git checkout main
   git pull upstream main
   git push origin main
   ```

2. **Create a feature branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

3. **Make your changes**
   - Write clean, well-documented code
   - Add tests for new functionality
   - Update documentation as needed

4. **Test your changes**
   ```bash
   make test
   make test_document_ai
   make check
   ```

### Creating the PR

1. **Push your branch**
   ```bash
   git push origin feature/your-feature-name
   ```

2. **Create the Pull Request**
   - Use a clear, descriptive title
   - Fill out the PR template completely
   - Link any related issues
   - Add screenshots or examples if applicable

### PR Requirements

- ✅ All tests pass (`make test`)
- ✅ Code follows style guidelines (`make check`)
- ✅ New functionality includes tests
- ✅ Documentation is updated
- ✅ No merge conflicts with main branch
- ✅ PR description clearly explains the changes

### Review Process

1. **Automated checks** - CI/CD will run tests and checks
2. **Code review** - A maintainer will review your code
3. **Feedback** - Address any requested changes
4. **Approval** - Once approved, a maintainer will merge your PR

## Writing Tests

- **Unit tests** - Test individual functions and classes
- **Integration tests** - Test interactions between components
- **Example tests** - Ensure examples work correctly

Test file naming convention:
- `test_*.py` for unit tests
- `test_integration_*.py` for integration tests

### Test Structure

```python
import pytest
from tensorlake import SomeClass

class TestSomeClass:
    def test_method_returns_expected_value(self):
        # Arrange
        instance = SomeClass()

        # Act
        result = instance.some_method()

        # Assert
        assert result == expected_value
```

## Documentation

### Types of Documentation

1. **API Documentation** - Auto-generated from docstrings
2. **User Guides** - Step-by-step tutorials
3. **Examples** - Working code samples
4. **Contributing Guide** - This document

### Writing Documentation

- Use clear, concise language
- Include code examples
- Test all code snippets
- Update both inline docs and separate documentation files

### Docstring Format

We use Google-style docstrings:

```python
def process_document(document_path: str, options: Dict[str, Any]) -> Dict[str, Any]:
    """Process a document using TensorLake AI.

    Args:
        document_path: Path to the document to process.
        options: Configuration options for processing.

    Returns:
        Dictionary containing the processed document data.

    Raises:
        ValueError: If document_path is invalid.
        APIError: If the API request fails.

    Example:
        >>> result = process_document("contract.pdf", {"extract_signatures": True})
        >>> print(result["signatures"])
    """
```

## Community

### Getting Help

- 📖 **Documentation** - Check our [docs](https://docs.tensorlake.ai)
- 💬 **Slack** - Join our [community Slack](https://tensorlakecloud.slack.com/)
- 📧 **Email** - Reach out to [support@tensorlake.ai](mailto:support@tensorlake.ai)
- 🐛 **Issues** - Create a [GitHub issue](https://github.com/tensorlakeai/tensorlake/issues)

### Recognition

Contributors are recognized in several ways:
- Listed in our [Contributors](https://github.com/tensorlakeai/tensorlake/graphs/contributors) page
- Mentioned in release notes for significant contributions
- Invited to our contributor Discord channels

## Release Process

### Versioning

We follow [Semantic Versioning](https://semver.org/):
- **MAJOR** version for incompatible API changes
- **MINOR** version for new functionality in a backwards compatible manner
- **PATCH** version for backwards compatible bug fixes

### Release Checklist

Maintainers follow this checklist for releases:

- [ ] Update version in `pyproject.toml`
- [ ] Update `CHANGELOG.md`
- [ ] Run full test suite (`make test_document_ai`)
- [ ] Check code quality (`make check`)
- [ ] Build the project (`make build`)
- [ ] Create release PR
- [ ] Tag release after merge
- [ ] Publish to PyPI
- [ ] Update documentation
- [ ] Announce release

## Questions?

Don't hesitate to ask questions! We're here to help:

- Open a [GitHub Issue](https://github.com/tensorlakeai/tensorlake/issues)
- Join our [Slack community](https://tensorlakecloud.slack.com/)
- Email us at [support@tensorlake.ai](mailto:support@tensorlake.ai)

Thank you for contributing to TensorLake! 🚀
