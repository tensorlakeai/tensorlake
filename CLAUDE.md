# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

TensorLake SDK — a Python SDK for document ingestion (DocumentAI) and serverless agentic applications. Users define applications using decorator-based APIs and deploy them to TensorLake Cloud or run locally.

## Common Commands

```bash
# Install dependencies and build
make build

# Format code (Black + isort)
make fmt

# Check formatting without modifying
make check

# Run all tests (requires TENSORLAKE_API_URL env var)
make test

# Run a specific test suite
cd tests && ./run_tests.sh --applications
cd tests && ./run_tests.sh --function-executor
cd tests && ./run_tests.sh --document-ai
cd tests && ./run_tests.sh --cli

# Run a single test file
cd tests && poetry run python path/to/test_file.py

# Regenerate gRPC stubs from .proto files
make build_proto
```

## Architecture

### Two Public APIs

1. **Applications SDK** (`src/tensorlake/applications/`) — Decorator-based API for defining serverless functions and applications. Public interface is in `applications/interface/`; everything re-exported through `applications/__init__.py` via `from .interface import *`.

2. **DocumentAI SDK** (`src/tensorlake/documentai/`) — Client for document parsing, extraction, and classification.

### Applications: Decorator → Function → Runner

Users decorate functions with `@application` and `@function` (defined in `interface/decorators.py`). Decorators create `Function` objects registered in a global registry (`registry.py`). Functions support `.map()` and `.reduce()` operations and return `Future`/`Awaitable` objects for async execution.

Two execution modes:
- **Local** (`applications/local/`) — Direct in-process execution for development/testing via `run_local_application()`, implemented by LocalRunner class.
- **Remote** (`applications/remote/`) — Cloud deployment via `run_remote_application()` and gRPC communication, implemented by AllocationRunner class.

### Function Executor

`src/tensorlake/function_executor/` — gRPC server that executes user functions in sandboxed environments. Proto definitions in `function_executor/proto/`. Proto files and generated Python stubs must coexist in the same directory (gRPC limitation).

### CLI

`src/tensorlake/cli/` — Click-based CLI (`tensorlake` command). Entry points: `tensorlake deploy`, `tensorlake parse`, `tensorlake login`, `tensorlake secrets`, etc.

### Vendored Dependencies

`src/tensorlake/vendor/` contains vendored `faker` and `nanoid` libraries. Black and isort are configured to skip this directory.

## Key Conventions

- **Python 3.10+**, managed with **Poetry 2.0.0**
- **Formatting**: Black + isort (profile "black"). Pre-commit hooks enforce this.
- **Tests**: Standard `unittest` framework with `parameterized` for parameterized tests. Parameterized tests are main used to run the same test code in local and remote modes and ensure the same results. Claude must not create new Python environments to run test. Instead it should use currrent Poetry environment by i.e. using `poetry run python tests/path_to/test_file.py`. Remote mode tests require defining env var `TENSORLAKE_API_URL=http://localhost:8900` by i.e. prepanding it to the `poetry run python` command. Before running remote mode tests, verify that the Server is running by issueing an http request to `$TENSORLAKE_API_URL`. If it returns status code 200, then it's running and you can run remote mode tests.
- **Pydantic v2** for data models throughout.
- **gRPC stubs** are generated with grpcio-tools 1.60.0 (pinned old version for forward compatibility) and reformatted with Black/isort after generation.

