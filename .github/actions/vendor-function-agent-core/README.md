# vendor-function-agent-core

This action injects the private Rust Function Agent core and its function-specific PyO3/N-API
modules from `tensorlakeai/compute-engine-internal` for trusted Tensorlake builds. The
implementation is never committed to this public repository. Only the core's workspace-adapted
manifest, fail-closed source placeholders, and an immutable CEI revision are tracked here.

Remote mode mints a short-lived GitHub App token scoped to `compute-engine-internal`, sparse-checks
out the pinned crate and binding modules, verifies that the placeholder manifest differs only in
its portable TLS default, and stages the complete core crate plus bindings for the current job.
Release builds run in CEI. They check out the full commit from
`crates/function-agent-core/CEI_REVISION` using CEI's own repository token, then pass that checkout
to this action's local mode. CEI acceptance instead passes its current source checkout to test
the service and SDK runner together.

Public Tensorlake CI does not call this action or need a private-repository credential. It builds
the Python and Node bindings with `--no-default-features`, disabling only the private Function
Agent integration. The `function-agent` feature remains enabled by default for full builds.
