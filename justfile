# Tensorlake SDK — Rust + Python build tasks

# Where the private artifact_storage checkout lives (used by build-cli-full).
# Override with e.g. `ARTIFACT_STORAGE_DIR=~/src/artifact_storage just build-cli-full`.
ARTIFACT_STORAGE_DIR := env("ARTIFACT_STORAGE_DIR", "../artifact_storage")

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

# Build the full official CLI: the private filesystem client/mount stack (`mount` feature) and the
# `tl git clone` fast-clone engine (`git-clone` feature). No private source is committed here — only
# resolution placeholders and host adapters. This recipe copies the real sources over those
# placeholders for the duration of the build and restores them afterward (even on failure), so
# the private source is never committed here. Requires the private artifact_storage repo checked
# out as a sibling directory, or wherever ARTIFACT_STORAGE_DIR points.
build-cli-full *ARGS:
    #!/usr/bin/env bash
    set -euo pipefail
    artifact_storage="{{ARTIFACT_STORAGE_DIR}}"
    for crate in gsvc-mount gsvc-codec gsvc-fs-client; do
        if [ ! -d "$artifact_storage/crates/$crate/src" ]; then
            echo "error: $artifact_storage/crates/$crate/src not found." >&2
            echo "Check out github.com/tensorlakeai/artifact_storage as a sibling of this repo," >&2
            echo "or point ARTIFACT_STORAGE_DIR at an existing checkout." >&2
            exit 1
        fi
    done
    restore() {
        for crate in gsvc-mount gsvc-codec gsvc-fs-client; do
            git checkout -q -- "crates/$crate/src" 2>/dev/null || true
            git clean -fdq "crates/$crate/src" 2>/dev/null || true
        done
        rm -f "crates/gsvc-fs-client/Cargo.lock"
    }
    trap restore EXIT
    # Swap in the real sources, keeping the placeholders' tensorlake-adapted Cargo.tomls.
    for crate in gsvc-mount gsvc-codec gsvc-fs-client; do
        rm -f "crates/$crate/src"/*.rs
        echo "swapping crates/$crate/src with $artifact_storage/crates/$crate/src"
        cp "$artifact_storage/crates/$crate/src"/*.rs "crates/$crate/src"/
    done
    cargo build -p tensorlake-cli --release --features mount,git-clone {{ARGS}}

# Back-compat alias for the old recipe name.
build-cli-mount *ARGS: (build-cli-full ARGS)

# Run the CLI test suite with the same ephemeral private-crate swap as the official full build.
# This is the authoritative local validation for mount/daemon code: the public placeholders are
# deliberately unbuildable when the `mount` and `git-clone` features are enabled directly.
test-cli-full *ARGS:
    #!/usr/bin/env bash
    set -euo pipefail
    artifact_storage="{{ARTIFACT_STORAGE_DIR}}"
    for crate in gsvc-mount gsvc-codec gsvc-fs-client; do
        if [ ! -d "$artifact_storage/crates/$crate/src" ]; then
            echo "error: $artifact_storage/crates/$crate/src not found." >&2
            exit 1
        fi
    done
    restore() {
        for crate in gsvc-mount gsvc-codec gsvc-fs-client; do
            git checkout -q -- "crates/$crate/src" 2>/dev/null || true
            git clean -fdq "crates/$crate/src" 2>/dev/null || true
        done
        rm -f "crates/gsvc-fs-client/Cargo.lock"
    }
    trap restore EXIT
    for crate in gsvc-mount gsvc-codec gsvc-fs-client; do
        rm -f "crates/$crate/src"/*.rs
        cp "$artifact_storage/crates/$crate/src"/*.rs "crates/$crate/src"/
    done
    CARGO_TARGET_DIR="$PWD/target" cargo clippy --manifest-path crates/gsvc-fs-client/Cargo.toml --all-targets -- -D warnings
    CARGO_TARGET_DIR="$PWD/target" cargo test --manifest-path crates/gsvc-fs-client/Cargo.toml {{ARGS}}
    cargo test --workspace --features tensorlake-cli/mount,tensorlake-cli/git-clone {{ARGS}}

# Authoritative validation for the durable `tl fs` mutation journal and generation engine.
#
# The state engine crosses the mount VFS, daemon recovery/publisher, plain-directory binding,
# and CLI control protocol. Run the complete private/full-feature CLI suite rather than a narrow
# module filter so changes cannot accidentally validate only one of those consumers.
test-fs-journal:
    just test-cli-full

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
