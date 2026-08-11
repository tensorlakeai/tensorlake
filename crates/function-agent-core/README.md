# Function Agent core placeholder

The implementation is owned by the private CEI crate at `function-service/agent-core`; its
function-specific PyO3 and N-API modules are also owned under CEI's `function-service/bindings`.
None is committed to Tensorlake. This directory keeps the workspace-adapted manifest, immutable
source revision, and a fail-closed placeholder so Cargo can resolve the combined Python and Node
extensions before trusted builds inject the source.

For local development with sibling checkouts:

```bash
just with-function-agent-core 'cargo test -p tensorlake-function-agent-core'
```

Set `COMPUTE_ENGINE_INTERNAL_DIR` when CEI is not at `../compute-engine-internal`. CI uses
`.github/actions/vendor-function-agent-core` and must provide the repository-scoped GitHub App
credentials documented with that action.
