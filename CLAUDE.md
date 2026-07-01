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

## Manual Cloud Sandbox Validations

For validations that must run inside the real Tensorlake builder image, use a throwaway Python virtualenv with the published `tensorlake` package. Do this for live environment checks, not normal repo tests.

Key points:
- Install from PyPI in a fresh venv: `python3 -m venv /tmp/tl-validate-venv && /tmp/tl-validate-venv/bin/python -m pip install -q --upgrade pip tensorlake`.
- Read the token from `~/.config/tensorlake/credentials.toml`, but never print it. The published SDK reads `TENSORLAKE_API_KEY`; `TENSORLAKE_PAT` is not enough for sandbox creation.
- Read `organization` and `project` from `.tensorlake/config.toml` and pass them explicitly to `SandboxClient.for_cloud(...)`. Relying only on environment variables can produce missing scope headers.
- Create the throwaway sandbox from `tensorlake/rootfs-builder`, run validation commands, and always delete it with `client.delete(sandbox_id)` in a `finally` block.
- Commands run as `tl-user` by default. Builder-image checks that start `dockerd` must run as root, e.g. `sudo -n <script>`.

Minimal pattern:

```python
import os
from pathlib import Path
from tensorlake.sandbox import SandboxClient


def token_for(endpoint: str) -> str:
    current = None
    for raw in (Path.home() / ".config/tensorlake/credentials.toml").read_text().splitlines():
        line = raw.strip()
        if line.startswith("["):
            current = line.strip("[]").strip().strip('"')
        elif current == endpoint and line.startswith("token"):
            return line.split("=", 1)[1].strip().strip('"')
    raise RuntimeError(f"no token for {endpoint}")


def local_config() -> dict[str, str]:
    out = {}
    for raw in Path(".tensorlake/config.toml").read_text().splitlines():
        line = raw.strip()
        if "=" in line:
            key, value = line.split("=", 1)
            out[key.strip()] = value.strip().strip('"')
    return out


endpoint = "https://api.tensorlake.ai"
token = token_for(endpoint)
cfg = local_config()
os.environ["TENSORLAKE_API_KEY"] = token

client = SandboxClient.for_cloud(
    api_key=token,
    api_url=endpoint,
    organization_id=cfg["organization"],
    project_id=cfg["project"],
)
sandbox = None
try:
    sandbox = client.create_and_connect(image="tensorlake/rootfs-builder")
    sandbox.write_file("/tmp/validate.sh", b"#!/usr/bin/env bash\nset -euo pipefail\nsudo -n id\n")
    result = sandbox.run(
        "sh",
        ["-lc", "chmod +x /tmp/validate.sh && sudo -n /tmp/validate.sh"],
        timeout=900,
    )
    print(result.stdout)
    if getattr(result, "exit_code", 0) != 0:
        raise RuntimeError(result.stderr)
finally:
    if sandbox is not None:
        client.delete(sandbox.sandbox_id)
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

## Releasing / version bumps

**IMPORTANT: After finishing code changes on a branch, before handing back to the user, remind them to bump the version.** All packages (Python, Rust, CLI, TypeScript) are released in lockstep at a single shared version, so a release bumps everything together.

How to bump:

- Run `python .github/scripts/bump_version.py <new-version>`. A single command updates every version-bearing file used by the PyPI / crates.io / CLI / npm release workflows:
  - `pyproject.toml` (root, `tensorlake` PyPI package)
  - `Cargo.toml` (root, workspace version — all crates inherit via `version.workspace = true`, including `crates/cli`)
  - `crates/rust-cloud-sdk-py/pyproject.toml`
  - `typescript/package.json` (npm package; `publish_npm.yaml` reads the version from here)
- `Cargo.lock` and `typescript/package-lock.json` regenerate on build/install — commit them after they refresh, don't hand-edit.

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
