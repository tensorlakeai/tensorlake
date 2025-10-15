# Tensorlake Python Analyzer

An isolated, self-contained tool for extracting metadata from Python files containing Tensorlake applications.

## Overview

The analyzer is a standalone component that parses Tensorlake application files and extracts structured information about:
- **Images**: Docker image configurations and build operations
- **Functions**: ALL functions with `@function()` decorator (including standalone functions without `@application()`)
- **Applications**: Application-level configurations and associated functions (only for functions with both `@function()` and `@application()`)

## Design Principles

1. **Isolated**: No dependencies on CLI framework or external libraries (except standard library)
2. **Self-contained**: All code is in this directory
3. **Simple**: Uses dataclasses and standard library JSON encoding
4. **Fast**: Minimal overhead, direct conversion to dictionaries

## Architecture

```
analyzer/
├── __init__.py        # Package initialization
├── main.py            # CLI entry point (uses argparse)
├── core.py            # Core analysis logic
├── converter.py       # Convert Tensorlake objects to models
├── models.py          # Data models (using dataclasses)
└── README.md          # This file
```

## Usage

### As a CLI Tool

```bash
# Output to stdout
tensorlake-python-analyzer myapp.py

# Pretty-print JSON
tensorlake-python-analyzer --pretty myapp.py

# Save to file
tensorlake-python-analyzer myapp.py -o output.json --pretty
```

### Programmatic Usage

```python
from tensorlake.analyzer.core import analyze_code

# Analyze an application file
result = analyze_code("myapp.py")

# Get as dictionary
data = result.to_dict()

# Or convert to JSON
import json
json_str = json.dumps(data, indent=2)

# Access specific data
for image_name, image_data in data['images'].items():
    print(f"Image: {image_name}")

for func_name, func_data in data['functions'].items():
    config = func_data['function_config']
    print(f"Function: {func_name} - CPU: {config['cpu']}")
```

## Output Schema

The analyzer outputs a JSON object with three main sections:

```json
{
  "images": {
    "<image-name>": {
      "name": "string",
      "tag": "string",
      "base_image": "string",
      "build_operations": [...]
    }
  },
  "functions": {
    "<function-name>": {
      "function_name": "string",
      "function_config": {...},
      "application_config": {...}
    }
  },
  "applications": {
    "<app-name>": {
      "application_name": "string",
      "version": "string",
      "functions": [...],
      "config": {...}
    }
  }
}
```

## Dependencies

**Standard Library Only:**
- `argparse` - Command line argument parsing
- `json` - JSON encoding/decoding
- `dataclasses` - Data models
- `os` - File path operations
- `sys` - System operations
- `traceback` - Error reporting

**Tensorlake Internal:**
- `tensorlake.applications.registry` - Access registered functions
- `tensorlake.applications.image` - Image utilities
- `tensorlake.applications.remote.code.loader` - Code loading

## Differences from CLI Version

The original CLI version used:
- `click` for command-line parsing → Now uses `argparse` (stdlib)
- `pydantic` for models → Now uses `dataclasses` (stdlib)

This makes the analyzer truly standalone with no external dependencies.

## Testing

Tests are located in `tests/analyzer/test_analyzer.py` and cover:
- Model creation and validation
- JSON serialization
- Backward compatibility
- Integration with Tensorlake components

## Future Enhancements

Potential improvements:
1. Output format options (YAML, TOML)
2. Filter/query capabilities
3. Validation rules
4. Diff between versions
5. Resource statistics

