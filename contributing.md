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
- [Poetry](https://python-poetry.org/) for dependency management
- Git

### Setup Instructions

1. **Fork and clone the repository**
   ```bash
   git clone https://github.com/YOUR_USERNAME/tensorlake.git
   cd tensorlake
   ```

2. **Install dependencies**
   ```bash
   make build
   ```

3. **Set up environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your API keys and configuration
   ```

4. **Activate the virtual environment**
   ```bash
   poetry shell
   ```

5. **Run tests to verify setup**
   ```bash
   make test_document_ai
   ```

   For more detailed test output, you can also use:
   ```
   cd tests && ./run_tests.sh
   ```

### Project Structure

```
tensorlake/
├── src/tensorlake/          # Main SDK code
│   ├── cli/                 # Command-line interface
│   ├── documentai/          # Document AI functionality
│   ├── functions_sdk/       # Functions SDK
│   └── utils/               # Utility modules
├── examples/                # Usage examples
├── tests/                   # Test suite
├── docs/                    # Documentation
├── Makefile                # Build and development commands
├── pyproject.toml          # Project configuration
└── README.md               # Project overview
```

### Available Makefile Commands

The project includes a Makefile with common development commands:

```bash
# Build the project (installs dependencies and builds package)
make build

# Format code with Black and isort
make fmt

# Run Document AI specific tests
make test_document_ai
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
