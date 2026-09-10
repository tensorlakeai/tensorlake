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

The Python runner serializes output writes only within an attempt, through the
native durable acknowledgment or error. Independent attempts can enter the native
core concurrently, allowing its bounded WAL group commit to operate. A short
registry mutex protects weak per-attempt ordering locks; every active writer and
waiter holds a strong reference, and idle keys are reclaimed (including when an
exception traceback is retained). The registry grows with concurrently writing
attempts, not historical attempts. There is no extra Python global admission
semaphore: the core owns bounded admission and reserved completion capacity.
Lifecycle output has a separate lock; initialization is still acknowledged before
the runner starts an attempt. Native future creation remains on the owning asyncio
loop. Canceling an asyncio waiter around a writer thread does not release that
thread's ordering lock before native completion. Python does not acknowledge a
write early or suppress native errors.

After building the extension from the exact `CEI_REVISION`, focused bridge checks
use `just test-function-agent-python` and `just check-function-agent-python`.
`just bench-function-agent-python` compares the prior global-lock writer against
the current writer using the same controlled delayed core; it measures bridge
concurrency only, not actual WAL persistence, SDK function execution, or Function
Service capacity. See the root `BENCHMARK.md` for evidence and boundaries.
