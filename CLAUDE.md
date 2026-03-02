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

### Three Public APIs

1. **Applications SDK** (`src/tensorlake/applications/`) — Decorator-based API for defining serverless functions and applications. Public interface is in `applications/interface/`; everything re-exported through `applications/__init__.py` via `from .interface import *`.

2. **Sandbox SDK** (`src/tensorlake/sandbox`) - Client for creating Sandboxes where users can run arbitrary code, copy files, install packages and etc.

3. **DocumentAI SDK** (`src/tensorlake/documentai/`) — Client for document parsing, extraction, and classification.

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
- **Tests**: Standard `unittest` framework with `parameterized` for parameterized tests. Parameterized tests are main used to run the same test code in local and remote modes and ensure the same results. Claude must not create new Python environments to run test. Instead it should use currrent Poetry environment by i.e. using `poetry run python tests/path_to/test_file.py`.
- **Pydantic v2** for data models throughout.
- **gRPC stubs** are generated with grpcio-tools 1.60.0 (pinned old version for forward compatibility) and reformatted with Black/isort after generation.

## Running Applications tests

**CRITICAL: Always run tests in BOTH local and remote modes.** Do NOT skip remote mode or run only local mode unless the user explicitly says to run local or remote only.
Most test files use `@parameterized` to run each test case in both modes.

**Never reduce test scope to work around infrastructure failures.** If tests fail due to missing setup (e.g. `deploy_applications` fails in `setUpClass`), this means remote mode is not set up yet — follow the remote mode setup procedure below instead of falling back to running only local tests.

### Running remote mode tests

**Before running a test in remote mode you MUST first ensure all remote mode dependencies are available:**
1. Check if indexify-server is running (HTTP ping `http://localhost:8900`)
2. If not then **stop immediately** and ask the user to run it. **Do not try to run local mode tests to proceed faster**.
3. Ask user for the command that runs indexify-dataplane. Do not assume that indexify-dataplane is running or not running.
   You have to start it yourself to get access to its stdout/stderr so you can investigate test failures yourself.
4. Run indexify-dataplane using the command given by user.

The dataplane command stdout/stderr contain logs from indexify-dataplane service and from Function Executors started by indexify-dataplane. Use these Function Executor logs to investigate failures in remote mode.

When running remote mode tests using `poetry run tests/path_to/test_file.py` you need to also define env var `TENSORLAKE_API_URL=http://localhost:8900` by prepending it to the `poetry run python` command.

### Running Local mode tests

Local mode tests don't have any dependency on indexify-dataplane and indexify-server. The logs and backtraces for LocalRunner and related classes are available in stdout/stderr of the test file
when you run it. Use these backtraces and logs to investigate local mode test failures.