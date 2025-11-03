import re
import shutil
import tempfile
from pathlib import Path

import click

# Template for the generated Python application file
PYTHON_TEMPLATE = '''from tensorlake.applications import application, function


@application()
@function(description="A simple Tensorlake application")
def {function_name}(name: str) -> str:
    """
    A simple greeting application.

    This is the entrypoint function for your application. It demonstrates
    the basic structure of a Tensorlake application.

    Args:
        name: The name to greet

    Returns:
        A greeting message
    """
    return f"Hello, {{name}}!"
'''

# Template for the generated README file
README_TEMPLATE = """# {app_name}

A Tensorlake application created with `tensorlake new`.

## Quick Start

### 1. Deploy to Tensorlake

Deploy your application to make it available via HTTP:

```bash
tensorlake deploy {filename}
```

### 2. Call Your Application

Once deployed, call it using curl or the Python SDK:

**Using curl:**
```bash
curl -X POST https://api.tensorlake.ai/applications/{function_name} \\
  -H "Authorization: Bearer $TENSORLAKE_API_KEY" \\
  -d 'World'
```

**Using Python SDK:**
```python
from tensorlake.applications import run_application
from {module_name} import {function_name}

request = run_application({function_name}, 'World', remote=True)
output = request.output()
print(output)
```

## Next Steps

**Customize Your Application:**
- Modify the `{function_name}()` function to process your data
- Add more `@function()` decorated functions for complex workflows
- Update the description in the `@function()` decorator

**Learn More:**
- [Programming Guide](https://docs.tensorlake.ai/applications/compute) - Customize compute resources
- [Dependency Management](https://docs.tensorlake.ai/applications/images) - Add packages
- [Parallel Processing](https://docs.tensorlake.ai/applications/map-reduce) - Scale with map-reduce
- [Complete Documentation](https://docs.tensorlake.ai)
"""


def sanitize(name: str) -> str:
    """
    Convert a string to snake_case.

    Handles kebab-case, camelCase, PascalCase, and spaces.

    Examples:
        my-app -> my_app
        myApp -> my_app
        MyApp -> my_app
        my app -> my_app
    """
    # Replace hyphens and spaces with underscores
    name = name.replace("-", "_").replace(" ", "_")

    # Insert underscores before uppercase letters (for camelCase/PascalCase)
    name = re.sub(r"(?<!^)(?=[A-Z])", "_", name)

    # Convert to lowercase
    name = name.lower()

    # Remove consecutive underscores
    name = re.sub(r"_+", "_", name)

    # Remove leading/trailing underscores
    name = name.strip("_")

    return name


def validate_app_name(name: str) -> tuple[bool, str]:
    """
    Validate that the application name can be used as a Python identifier and filename.

    Returns:
        tuple: (is_valid, error_message)
    """
    if not name:
        return False, "Application name cannot be empty"

    # Check for invalid characters (before conversion)
    if not re.match(r"^[a-zA-Z0-9_\-\s]+$", name):
        return (
            False,
            "Application name can only contain letters, numbers, hyphens, underscores, and spaces",
        )

    # Convert to snake_case for validation
    snake_name = sanitize(name)

    # Check if it's a valid Python identifier
    if not snake_name.isidentifier():
        return (
            False,
            f"'{snake_name}' is not a valid Python identifier. Names must start with a letter or underscore.",
        )

    # Check if it's a Python keyword
    import keyword

    if keyword.iskeyword(snake_name):
        return (
            False,
            f"'{snake_name}' is a Python keyword and cannot be used as an application name",
        )

    return True, ""


@click.command()
@click.argument("name")
@click.option(
    "--force",
    is_flag=True,
    help="Overwrite existing files if they exist",
)
def new(name: str, force: bool):
    """
    Create a new Tensorlake application.

    Creates a new application with a basic structure that can be run locally
    and deployed to Tensorlake.

    Example:
        tensorlake new my-greeting-app
    """
    # Validate the application name
    is_valid, error_msg = validate_app_name(name)
    if not is_valid:
        click.echo(f"Error: {error_msg}", err=True)
        raise click.Abort()

    # Convert name to snake_case for file/module name
    module_name = sanitize(name)
    filename = f"{module_name}.py"

    # Determine target directory
    target_dir = Path(module_name).resolve()

    python_file = target_dir / filename
    readme_file = target_dir / "README.md"

    # Check for existing files
    if not force:
        if python_file.exists():
            click.echo(
                f"Error: {filename} already exists. Use --force to overwrite or choose a different name.",
                err=True,
            )
            raise click.Abort()

        if readme_file.exists():
            click.confirm(
                f"README.md already exists. Overwrite it?",
                abort=True,
            )

    # Generate file contents
    python_content = PYTHON_TEMPLATE.format(
        function_name=module_name,
        filename=filename,
    )

    readme_content = README_TEMPLATE.format(
        app_name=name,
        function_name=module_name,
        filename=filename,
        module_name=module_name,
    )

    # Create the files
    write_dir = target_dir
    try:
        click.echo(f"\nCreating new Tensorlake application in '{module_name}'...\n")

        if force:
            tmp_dir = tempfile.mkdtemp(prefix="tl_")
            tmp_dir = Path(tmp_dir).resolve()
            python_file = tmp_dir / filename
            readme_file = tmp_dir / "README.md"
            write_dir = tmp_dir
        else:
            write_dir.mkdir(parents=True)

        # Write Python file
        with open(python_file, "w") as f:
            f.write(python_content)
        click.echo(f"  ✓ {filename}")

        # Write README
        with open(readme_file, "w") as f:
            f.write(readme_content)
        click.echo(f"  ✓ README.md")

        if write_dir != target_dir:
            if target_dir.exists():
                _ = shutil.rmtree(target_dir)
            _ = shutil.move(write_dir, target_dir)

        # Success message
        click.echo("\n" + "=" * 50)
        click.echo("Application created successfully!")
        click.echo("=" * 50)
        click.echo("\nNext steps:")
        click.echo(f"  Deploy: tensorlake deploy {filename}")
        click.echo("\nLearn more: https://docs.tensorlake.ai/quickstart")

    except Exception as e:
        click.echo(f"Error creating application: {e}", err=True)
        if write_dir.exists():
            click.echo(f"Application temporarily created in '{write_dir}'", err=True)
        raise click.Abort()
