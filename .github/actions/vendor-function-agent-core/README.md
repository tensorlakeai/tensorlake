# vendor-function-agent-core

This action injects the private Rust Function Agent core and its function-specific PyO3/N-API
modules from `tensorlakeai/compute-engine-internal` for trusted Tensorlake builds. The
implementation is never committed to this public repository. Only the core's workspace-adapted
manifest, fail-closed source placeholders, and an immutable CEI revision are tracked here.

Remote mode mints a short-lived GitHub App token scoped to `compute-engine-internal`, sparse-checks
out the pinned crate and binding modules, verifies that the placeholder manifest differs only in
its portable TLS default, and stages the complete core crate plus bindings for the current job.
Configure these repository secrets:

| Secret | Value |
| --- | --- |
| `COMPUTE_ENGINE_APP_ID` | GitHub App ID with `contents: read` on `compute-engine-internal` |
| `COMPUTE_ENGINE_APP_PRIVATE_KEY` | GitHub App private key (PEM) |

Release and Tensorlake CI builds read the full commit from
`crates/function-agent-core/CEI_REVISION`. CEI acceptance uses local mode instead: it passes the
current CEI repository checkout and therefore tests the service and SDK runner from one source
tree without a companion Tensorlake source-copy PR.

Fork pull requests cannot read the GitHub App secrets. Jobs that compile the native Function Agent
are intentionally skipped for forks; pure public-source jobs remain available.
