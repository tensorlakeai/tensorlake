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
    return f"Hello, {name}!"
"#;

pub const TYPESCRIPT_TEMPLATE: &str = r#"import { registerApplication } from "tensorlake/applications";

export const app = registerApplication(
  "{function_name}",
  async (name: string): Promise<string> => `Hello, ${name}!`,
);
"#;

pub const TYPESCRIPT_PACKAGE_TEMPLATE: &str = r#"{
  "name": "{package_name}",
  "private": true,
  "type": "module",
  "engines": {
    "node": ">=22.0.0"
  },
  "scripts": {
    "typecheck": "tsc --noEmit"
  },
  "dependencies": {
    "tensorlake": "^{sdk_version}"
  },
  "devDependencies": {
    "typescript": "^5.8.0"
  }
}
"#;

pub const TYPESCRIPT_CONFIG_TEMPLATE: &str = r#"{
  "compilerOptions": {
    "target": "ES2023",
    "module": "ESNext",
    "moduleResolution": "bundler",
    "lib": ["ES2023", "DOM", "DOM.Iterable"],
    "strict": true,
    "skipLibCheck": true,
    "noEmit": true
  },
  "include": ["application.ts"]
}
"#;

pub const README_TEMPLATE: &str = r#"# {app_name}

A Tensorlake application created with `tl app new`.

## Quick Start

### 1. Deploy to Tensorlake

Deploy your application to make it available via HTTP:

```bash
tl app deploy {filename}
```

### 2. Call Your Application

Once deployed, call it using curl or the Python SDK:

**Using curl:**
```bash
curl https://api.tensorlake.ai/applications/{function_name} \
  -H "Authorization: Bearer $TENSORLAKE_API_KEY" \
  --json '"World"'
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

pub const TYPESCRIPT_README_TEMPLATE: &str = r#"# {app_name}

A TypeScript Tensorlake application created with `tl app new`.

## Quick Start

Install dependencies:

```bash
npm install
```

Deploy your application:

```bash
tl app deploy application.ts
```

Invoke it with a JSON string:

```bash
curl https://api.tensorlake.ai/applications/{function_name} \
  -H "Authorization: Bearer $TENSORLAKE_API_KEY" \
  --json '"World"'
```

## Next Steps

- Modify `application.ts` to process your own input.
- Add `registerFunction` functions for multi-step or parallel workflows.
- Declare runtime secrets with the function or application's `secrets` option.
- Run `npm run typecheck` during development.

[Read the complete documentation](https://docs.tensorlake.ai/applications/introduction).
"#;
