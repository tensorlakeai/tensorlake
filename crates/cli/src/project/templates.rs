pub const PYTHON_TEMPLATE: &str = r#"from tensorlake.applications import application, function


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
"#;

pub const README_TEMPLATE: &str = r#"# {app_name}

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
curl -X POST https://api.tensorlake.ai/applications/{function_name} \
  -H "Authorization: Bearer $TENSORLAKE_API_KEY" \
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
"#;
