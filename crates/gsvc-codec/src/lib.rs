//! Resolution-only placeholder for the private `gsvc-codec` packfile codec.
//!
//! The real crate is not vendored into this public repository (it used to be; it is not
//! anymore). This file exists only so the optional `gsvc-codec` dependency of the Rust SDK's
//! `git-clone` feature resolves without access to the private artifact_storage repo. It is never
//! compiled by a valid build — see this crate's `Cargo.toml`.
//!
//! If you are reading this because the build failed: you enabled `--features git-clone` without
//! the real source in place. Build the official binary via the justfile recipe instead:
//!
//! ```text
//! just build-cli-full
//! ```
//!
//! which copies the real source in from a sibling artifact_storage checkout for the build.

compile_error!(
    "the `git-clone` feature requires the private gsvc-codec packfile codec, which is not \
     vendored into this public repo. Build the official binary with `just build-cli-full` (it \
     swaps in the real source from a sibling artifact_storage checkout), or build without \
     `--features git-clone`.",
);
