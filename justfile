# Tensorlake SDK — Rust + Python build tasks

# Default recipe: show available commands
default:
    @just --list

# ─── Rust ────────────────────────────────────────────────────────────────────

# Build all Rust crates (debug)
build:
    cargo build --workspace

# Build all Rust crates (release)
build-release:
    cargo build --workspace --release

# Build just the CLI binary (debug)
build-cli:
    cargo build -p tensorlake-cli

# Build just the CLI binary (release)
build-cli-release:
    cargo build -p tensorlake-cli --release

# Build the CLI with the private `tl fs mount` stack (the `mount` feature + the real gsvc-mount
# core). The mount core is NOT vendored into this public repo — only a resolution placeholder is
# committed at crates/gsvc-mount. This recipe copies the real source over the placeholder for the
# duration of the build and restores it afterward (even on failure), so the private source is never
# committed here. Requires the private artifact_storage repo checked out as a sibling directory.
build-cli-mount *ARGS:
    #!/usr/bin/env bash
    set -euo pipefail
    real="../artifact_storage/crates/gsvc-mount/src"
    placeholder="crates/gsvc-mount/src"
    if [ ! -d "$real" ]; then
        echo "error: $real not found." >&2
        echo "Check out github.com/tensorlakeai/artifact_storage as a sibling of this repo." >&2
        exit 1
    fi
    restore() {
        git checkout -q -- "$placeholder" 2>/dev/null || true
        git clean -fdq "$placeholder" 2>/dev/null || true
    }
    trap restore EXIT
    # Swap in the real mount core, keeping the placeholder's tensorlake-adapted Cargo.toml.
    rm -f "$placeholder"/*.rs
    cp "$real"/*.rs "$placeholder"/
    cargo build -p tensorlake-cli --release --features mount {{ARGS}}

# Run all Rust tests
test-rust:
    cargo test --workspace

# Run Rust tests for a specific crate
test-crate crate:
    cargo test -p {{crate}}


# Run clippy lints on all crates
clippy:
    cargo clippy --workspace --all-targets -- -D warnings

clippy-fix:
    cargo clippy --fix --workspace --allow-dirty

# Format all Rust code
fmt-rust:
    cargo fmt --all

# Check Rust formatting without modifying
check-rust-fmt:
    cargo fmt --all -- --check

# Full Rust CI check: format, clippy, test
check-rust: check-rust-fmt clippy test-rust

# Clean Rust build artifacts
clean-rust:
    cargo clean

# ─── Python ──────────────────────────────────────────────────────────────────

# Format Python code (Black + isort)
fmt-python:
    poetry run black src/tensorlake --extend-exclude vendor
    poetry run isort src/tensorlake --profile black --extend-skip vendor

# Check Python formatting without modifying
check-python-fmt:
    poetry run black --check src/tensorlake --extend-exclude vendor
    poetry run isort --check src/tensorlake --profile black --extend-skip vendor

# ─── Maturin (Python + Rust packaging) ───────────────────────────────────────

# Development install (builds Rust binary + installs Python package)
develop:
    maturin develop

# Development install (release mode)
develop-release:
    maturin develop --release

# Build wheel for current platform
wheel:
    maturin build --release

# ─── Combined ────────────────────────────────────────────────────────────────

# Format everything
fmt: fmt-rust fmt-python

# Full CI check
check: check-rust check-python-fmt
