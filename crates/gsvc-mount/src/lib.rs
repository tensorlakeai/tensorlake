//! Resolution-only placeholder for the private `gsvc-mount` mount core.
//!
//! The real crate is not vendored into this public repository. This file exists only so the
//! optional `gsvc-mount` dependency of `tensorlake-cli` resolves without access to the private
//! artifact_storage repo. It is never compiled by a valid build — see this crate's `Cargo.toml`.
//!
//! If you are reading this because the build failed: you enabled `--features mount` without the
//! real mount core in place. Build the official mount-enabled binary via the justfile recipe
//! instead:
//!
//! ```text
//! just build-cli-mount
//! ```
//!
//! which copies the real source in from a sibling artifact_storage checkout for the build.

compile_error!(
    "the `mount` feature requires the private gsvc-mount core, which is not vendored into this \
     public repo. Build the official mount-enabled binary with `just build-cli-mount` (it swaps \
     in the real source from a sibling artifact_storage checkout), or build without \
     `--features mount`.",
);
