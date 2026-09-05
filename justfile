# Tensorlake SDK — Rust + Python build tasks

# Where the private artifact_storage checkout lives (used by build-cli-full).
# Override with e.g. `ARTIFACT_STORAGE_DIR=~/src/artifact_storage just build-cli-full`.
ARTIFACT_STORAGE_DIR := env("ARTIFACT_STORAGE_DIR", "../artifact_storage")

# Canonical Function Agent core checkout used by `with-function-agent-core`. The public repository
# tracks only a fail-closed placeholder; native SDK builds stage this private CEI crate temporarily.
COMPUTE_ENGINE_INTERNAL_DIR := env("COMPUTE_ENGINE_INTERNAL_DIR", "../compute-engine-internal")

# Default recipe: show available commands
default:
    @just --list

# Run one command with the canonical Function Agent core staged from CEI, restoring the public
# placeholder afterward. Trusted CI uses the equivalent vendor-function-agent-core action.
with-function-agent-core *COMMAND:
    COMPUTE_ENGINE_INTERNAL_DIR="{{COMPUTE_ENGINE_INTERNAL_DIR}}" \
        ./scripts/with_function_agent_core.sh bash -c {{quote(COMMAND)}}

# ─── Rust ────────────────────────────────────────────────────────────────────

# Build all Rust crates (debug)
build:
    just with-function-agent-core cargo build --workspace

# Build all Rust crates (release)
build-release:
    just with-function-agent-core cargo build --workspace --release

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
    stage_macos_tlfs=false
    tlfs_backup=""
    if [ "$(uname -s)" = "Darwin" ]; then
        private_tlfs="$artifact_storage/platform/macos/tlfs"
        for directory in Sources Resources; do
            if [ ! -d "$private_tlfs/$directory" ]; then
                echo "error: $private_tlfs/$directory not found." >&2
                echo "The macOS full build needs the private FSKit companion sources." >&2
                exit 1
            fi
        done
        stage_macos_tlfs=true
        tlfs_backup="$(mktemp -d "${TMPDIR:-/tmp}/tensorlake-tlfs.XXXXXX")"
        for directory in Sources Resources; do
            if [ -e "platform/macos/tlfs/$directory" ]; then
                cp -R "platform/macos/tlfs/$directory" "$tlfs_backup/"
            fi
        done
    fi
    restore() {
        for crate in gsvc-mount gsvc-codec gsvc-fs-client; do
            git checkout -q -- "crates/$crate/src" 2>/dev/null || true
            git clean -fdq "crates/$crate/src" 2>/dev/null || true
        done
        if [ "$stage_macos_tlfs" = true ]; then
            rm -rf "platform/macos/tlfs/Sources" "platform/macos/tlfs/Resources"
            for directory in Sources Resources; do
                if [ -e "$tlfs_backup/$directory" ]; then
                    cp -R "$tlfs_backup/$directory" "platform/macos/tlfs/"
                fi
            done
            rm -rf "$tlfs_backup"
        fi
        rm -f "crates/gsvc-fs-client/Cargo.lock"
    }
    trap restore EXIT
    # Swap in the real sources, keeping the placeholders' tensorlake-adapted Cargo.tomls.
    for crate in gsvc-mount gsvc-codec gsvc-fs-client; do
        rm -f "crates/$crate/src"/*.rs
        echo "swapping crates/$crate/src with $artifact_storage/crates/$crate/src"
        cp "$artifact_storage/crates/$crate/src"/*.rs "crates/$crate/src"/
    done
    # macOS-only tests compile a source-level wire/coherence contract against the private FSKit
    # bridge. Sources and resources are staged with the Rust crates and removed by the trap. The
    # app build script stays private and is invoked directly by `build-tlfs-app`.
    if [ "$stage_macos_tlfs" = true ]; then
        rm -rf "platform/macos/tlfs/Sources" "platform/macos/tlfs/Resources"
        cp -R "$private_tlfs/Sources" "$private_tlfs/Resources" "platform/macos/tlfs/"
    fi
    cargo build -p tensorlake-cli --release --features mount,git-clone {{ARGS}}

# Back-compat alias for the old recipe name.
build-cli-mount *ARGS: (build-cli-full ARGS)

# Build only the headless Linux mount daemon used in Tensorlake sandbox images.
# The private source is staged exactly like the full CLI build, but the output
# links no CLI command surface.
build-tlfs-mount-daemon *ARGS:
    #!/usr/bin/env bash
    set -euo pipefail
    artifact_storage="{{ARTIFACT_STORAGE_DIR}}"
    for crate in gsvc-mount gsvc-fs-client; do
        if [ ! -d "$artifact_storage/crates/$crate/src" ]; then
            echo "error: $artifact_storage/crates/$crate/src not found." >&2
            exit 1
        fi
    done
    restore() {
        for crate in gsvc-mount gsvc-fs-client; do
            git checkout -q -- "crates/$crate/src" 2>/dev/null || true
            git clean -fdq "crates/$crate/src" 2>/dev/null || true
        done
        rm -f crates/gsvc-fs-client/Cargo.lock
    }
    trap restore EXIT
    for crate in gsvc-mount gsvc-fs-client; do
        rm -f "crates/$crate/src"/*.rs
        cp "$artifact_storage/crates/$crate/src"/*.rs "crates/$crate/src"/
    done
    cargo build --manifest-path crates/gsvc-fs-client/Cargo.toml \
        --bin tlfs-mount-daemon {{ARGS}}

# Build the macOS TLFS.app from the private artifact_storage checkout (see its
# platform/macos/tlfs/README.md). Flags pass through, e.g. `just build-tlfs-app --release`.
build-tlfs-app *ARGS:
    #!/usr/bin/env bash
    set -euo pipefail
    build_sh="{{ARTIFACT_STORAGE_DIR}}/platform/macos/tlfs/build.sh"
    if [ ! -f "$build_sh" ]; then
        echo "error: $build_sh not found." >&2
        echo "Check out github.com/tensorlakeai/artifact_storage as a sibling of this repo," >&2
        echo "or point ARTIFACT_STORAGE_DIR at an existing checkout." >&2
        exit 1
    fi
    workspace_version="$(sed -n 's/^version = "\([^"]*\)"$/\1/p' Cargo.toml | head -n 1)"
    if [ -z "$workspace_version" ]; then
        echo "error: could not read [workspace.package] version from Cargo.toml" >&2
        exit 1
    fi
    if [ -n "${TLFS_VERSION:-}" ] && [ "$TLFS_VERSION" != "$workspace_version" ]; then
        echo "error: TLFS_VERSION $TLFS_VERSION does not match workspace version $workspace_version" >&2
        exit 1
    fi
    export TLFS_VERSION="$workspace_version"
    exec "$build_sh" {{ARGS}}

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
    stage_macos_tlfs=false
    tlfs_backup=""
    if [ "$(uname -s)" = "Darwin" ]; then
        private_tlfs="$artifact_storage/platform/macos/tlfs"
        for directory in Sources Resources; do
            if [ ! -d "$private_tlfs/$directory" ]; then
                echo "error: $private_tlfs/$directory not found." >&2
                echo "The macOS full test needs the private FSKit companion sources." >&2
                exit 1
            fi
        done
        stage_macos_tlfs=true
        tlfs_backup="$(mktemp -d "${TMPDIR:-/tmp}/tensorlake-tlfs.XXXXXX")"
        for directory in Sources Resources; do
            if [ -e "platform/macos/tlfs/$directory" ]; then
                cp -R "platform/macos/tlfs/$directory" "$tlfs_backup/"
            fi
        done
    fi
    restore() {
        for crate in gsvc-mount gsvc-codec gsvc-fs-client; do
            git checkout -q -- "crates/$crate/src" 2>/dev/null || true
            git clean -fdq "crates/$crate/src" 2>/dev/null || true
        done
        if [ "$stage_macos_tlfs" = true ]; then
            rm -rf "platform/macos/tlfs/Sources" "platform/macos/tlfs/Resources"
            for directory in Sources Resources; do
                if [ -e "$tlfs_backup/$directory" ]; then
                    cp -R "$tlfs_backup/$directory" "platform/macos/tlfs/"
                fi
            done
            rm -rf "$tlfs_backup"
        fi
        rm -f "crates/gsvc-fs-client/Cargo.lock"
    }
    trap restore EXIT
    for crate in gsvc-mount gsvc-codec gsvc-fs-client; do
        rm -f "crates/$crate/src"/*.rs
        cp "$artifact_storage/crates/$crate/src"/*.rs "crates/$crate/src"/
    done
    if [ "$stage_macos_tlfs" = true ]; then
        rm -rf "platform/macos/tlfs/Sources" "platform/macos/tlfs/Resources"
        cp -R "$private_tlfs/Sources" "$private_tlfs/Resources" "platform/macos/tlfs/"
    fi
    CARGO_TARGET_DIR="$PWD/target" cargo clippy --manifest-path crates/gsvc-fs-client/Cargo.toml --all-targets -- -D warnings
    CARGO_TARGET_DIR="$PWD/target" cargo test --manifest-path crates/gsvc-fs-client/Cargo.toml {{ARGS}}
    COMPUTE_ENGINE_INTERNAL_DIR="{{COMPUTE_ENGINE_INTERNAL_DIR}}" \
        ./scripts/with_function_agent_core.sh \
        cargo test --workspace --features tensorlake-cli/mount,tensorlake-cli/git-clone {{ARGS}}

# Validate the server-side filesystem SDK. It deliberately has no dependency on the private mount
# engine: reads use native HTTP routes and writes upload directly to object storage.
test-node-filesystem-full:
    just with-function-agent-core cargo clippy -p tensorlake-rust-cloud-sdk-node --no-deps -- -D warnings
    just with-function-agent-core cargo test -p tensorlake-rust-cloud-sdk-node
    cd typescript && npm run typecheck
    cd typescript && npm test -- tests/filesystem.test.ts
    just with-function-agent-core 'cd typescript && npm run build:native'

# Authoritative validation for the private filesystem client integration. Run the complete
# full-feature CLI suite so changes exercise every consumer of the private crate.
test-fs-journal:
    just test-cli-full

# Run all Rust tests
test-rust:
    just with-function-agent-core cargo test --workspace

# Run Rust tests for a specific crate
test-crate crate:
    just with-function-agent-core cargo test -p {{crate}}

# Artifact Storage transport unit tests; no credentials or live services required.
test-artifact-storage:
    cargo test --locked -p tensorlake --lib artifact_storage::

# Guard every SDK/CLI HTTP constructor, including feature-gated fast-clone paths in full CI.
# This focused gate does not enable unrelated style lints; the existing `clippy` recipe does.
lint-http-transports:
    cargo clippy --locked -p tensorlake -p tensorlake-cli --all-targets -- -A clippy::all -D clippy::disallowed_methods -D warnings

# Requires private sources to have been injected by the CI vendoring action.
lint-http-transports-full:
    cargo clippy -p tensorlake -p tensorlake-cli --features tensorlake-cli/mount,tensorlake-cli/git-clone --all-targets -- -A clippy::all -D clippy::disallowed_methods -D warnings


# Run clippy lints on all crates
clippy:
    just with-function-agent-core cargo clippy --workspace --all-targets -- -D warnings

clippy-fix:
    just with-function-agent-core cargo clippy --fix --workspace --allow-dirty

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
    just with-function-agent-core maturin develop

# Development install (release mode)
develop-release:
    just with-function-agent-core maturin develop --release

# Build wheel for current platform
wheel:
    just with-function-agent-core maturin build --release

# ─── Combined ────────────────────────────────────────────────────────────────

# Format everything
fmt: fmt-rust fmt-python

# Full CI check
check: check-rust check-python-fmt
