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
    black src/tensorlake --extend-exclude vendor
    isort src/tensorlake --profile black --extend-skip vendor

# Check Python formatting without modifying
check-python-fmt:
    black --check src/tensorlake --extend-exclude vendor
    isort --check src/tensorlake --profile black --extend-skip vendor

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
