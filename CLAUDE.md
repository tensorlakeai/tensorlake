# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

TensorLake SDK — Python, TypeScript and Rust SDKs plus the `tl` CLI for Tensorlake sandboxes, sandbox images, filesystems and repositories.

## Common Commands

```bash
# Install dependencies and build
make build

# Format code (Black + isort)
make fmt

# Check formatting without modifying
make check

# Run all tests
make test

# Run a specific test suite
cd tests && ./run_tests.sh --filesystem

# Run a single test file
cd tests && poetry run python path/to/test_file.py
```

## Manual Cloud Sandbox Validations

For validations that must run inside the real Tensorlake builder image, use a throwaway Python virtualenv with the published `tensorlake` package. Do this for live environment checks, not normal repo tests.

Key points:
- Install from PyPI in a fresh venv: `python3 -m venv /tmp/tl-validate-venv && /tmp/tl-validate-venv/bin/python -m pip install -q --upgrade pip tensorlake`.
- Read the token of the current `tl` context, but never print it. `~/.config/tensorlake/contexts.toml` says whether it is in the OS keychain or in `credentials.toml`. The published SDK reads `TENSORLAKE_API_KEY`; `TENSORLAKE_PAT` is not enough for sandbox creation.
- Read `organization` and `project` from `.tensorlake/config.toml` and pass them explicitly to `SandboxClient.for_cloud(...)`. Relying only on environment variables can produce missing scope headers.
- Create the throwaway sandbox from `tensorlake/rootfs-builder`, run validation commands, and always delete it with `client.delete(sandbox_id)` in a `finally` block.
- Commands run as `tl-user` by default. Builder-image checks that start `dockerd` must run as root, e.g. `sudo -n <script>`.

Minimal pattern:

```python
import os
import subprocess
import tomllib
from pathlib import Path
from tensorlake.sandbox import SandboxClient


def token_for(context: str = "default") -> str:
    config = Path.home() / ".config/tensorlake"
    entry = tomllib.loads((config / "contexts.toml").read_text())["contexts"][context]
    if entry.get("storage") == "keychain":
        # macOS. The item belongs to `tl`, so the keychain asks once to allow this.
        return subprocess.run(
            ["security", "find-generic-password", "-s", "tensorlake", "-a", context, "-w"],
            check=True, capture_output=True, text=True,
        ).stdout.strip()
    credentials = tomllib.loads((config / "credentials.toml").read_text())
    return credentials["contexts"][context]["token"]


def local_config() -> dict[str, str]:
    out = {}
    for raw in Path(".tensorlake/config.toml").read_text().splitlines():
        line = raw.strip()
        if "=" in line:
            key, value = line.split("=", 1)
            out[key.strip()] = value.strip().strip('"')
    return out


endpoint = "https://api.tensorlake.ai"
token = token_for()
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

### Public APIs

**Sandbox SDK** (`src/tensorlake/sandbox`) - Client for creating Sandboxes where users can run arbitrary code, copy files, install packages and etc. Sandbox images live in `src/tensorlake/image/`, filesystems in `src/tensorlake/filesystem/`.

### CLI

The `tl` CLI is a Rust binary in `crates/cli/`. `src/tensorlake/cli/` only holds the shared Python auth `Context` helper.

## Key Conventions

- **Typed API responses**: Deserialize response and error bodies into explicit structs/enums (`serde::Deserialize` in Rust) and access named fields. Do not parse a body into `serde_json::Value` and extract known fields with `.get()` / `.as_str()`. Reuse wire models or add a focused response struct; represent supported legacy shapes with types, defaults, and aliases. Reserve dynamic JSON for individual fields whose contract permits arbitrary JSON, such as `error_details`. Preserve raw-body fallback for unrecognized or non-JSON error responses.
- **Python 3.10+**, managed with **Poetry 2.0.0**
- **Formatting**: Black + isort (profile "black"). Pre-commit hooks enforce this.
- **Tests**: Standard `unittest` framework with `parameterized` for parameterized tests. Claude must not create new Python environments to run test. Instead it should use currrent Poetry environment by i.e. using `poetry run python tests/path_to/test_file.py`.
- **Pydantic v2** for data models throughout.

## Releasing / version bumps

**IMPORTANT: After finishing code changes on a branch, before handing back to the user, remind them to bump the version.** All packages (Python, Rust, CLI, TypeScript) are released in lockstep at a single shared version, so a release bumps everything together.

How to bump:

- Run `python .github/scripts/bump_version.py <new-version>`. A single command updates every version-bearing file used by the PyPI / crates.io / CLI / npm release workflows:
  - `pyproject.toml` (root, `tensorlake` PyPI package)
  - `Cargo.toml` (root, workspace version — all crates inherit via `version.workspace = true`, including `crates/cli`)
  - `crates/rust-cloud-sdk-py/pyproject.toml`
  - `typescript/package.json` (npm package; `publish_npm.yaml` reads the version from here)
- `Cargo.lock` and `typescript/package-lock.json` regenerate on build/install — commit them after they refresh, don't hand-edit.
