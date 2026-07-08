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

# Build the full official CLI: the private `tl fs mount` stack (`mount` feature + the real
# gsvc-mount core) and the `tl git clone` fast-clone engine (`git-clone` feature + the real
# gsvc-codec). Neither private crate is vendored into this public repo — only resolution
# placeholders are committed under crates/. This recipe copies the real sources over the
# placeholders for the duration of the build and restores them afterward (even on failure), so
# the private source is never committed here. Requires the private artifact_storage repo checked
# out as a sibling directory.
build-cli-full *ARGS:
    #!/usr/bin/env bash
    set -euo pipefail
    for crate in gsvc-mount gsvc-codec; do
        if [ ! -d "../artifact_storage/crates/$crate/src" ]; then
            echo "error: ../artifact_storage/crates/$crate/src not found." >&2
            echo "Check out github.com/tensorlakeai/artifact_storage as a sibling of this repo." >&2
            exit 1
        fi
    done
    restore() {
        for crate in gsvc-mount gsvc-codec; do
            git checkout -q -- "crates/$crate/src" 2>/dev/null || true
            git clean -fdq "crates/$crate/src" 2>/dev/null || true
        done
    }
    trap restore EXIT
    # Swap in the real sources, keeping the placeholders' tensorlake-adapted Cargo.tomls.
    for crate in gsvc-mount gsvc-codec; do
        rm -f "crates/$crate/src"/*.rs
        cp "../artifact_storage/crates/$crate/src"/*.rs "crates/$crate/src"/
    done
    cargo build -p tensorlake-cli --release --features mount,git-clone {{ARGS}}

# Back-compat alias for the old recipe name.
build-cli-mount *ARGS: (build-cli-full ARGS)

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

# ─── Maturin (Python SDK + Rust Cloud SDK packaging) ─────────────────────────

# Development install (builds Rust Cloud SDK extension + installs Python package)
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
