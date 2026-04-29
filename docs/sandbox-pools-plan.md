# Sandbox Pools — Python SDK + CLI Gap Closure

Status doc for the Sandbox Pools effort. Captures the current state, what this
change adds, and gaps that depend on backend or other-team work.

## State of the world today

| Layer | Pool support | Notes |
|---|---|---|
| HTTP API (`compute-engine-internal`) | Full CRUD + claim | Some fields not yet exposed (see backend gaps) |
| Rust core SDK (`crates/cloud-sdk`) | Full CRUD + claim | Missing several fields the API supports |
| Rust→Py bridge (`crates/rust-cloud-sdk-py`) | Full passthrough | |
| Python SDK (`src/tensorlake/sandbox`) | Full CRUD + claim | Missing several fields the API supports |
| TypeScript SDK (`typescript/src`) | Full CRUD + claim | Same field gaps as Python |
| **CLI (`tl`)** | **None** | No pool subcommands at all |

## What this change does

### 1. Field gap-fill across Rust core SDK + Python SDK

Adds these fields to `SandboxPoolRequest` and `SandboxPoolInfo` in both layers
(they exist in the HTTP API today but neither SDK exposes them):

- `allow_unauthenticated_access: bool`
- `exposed_ports: list[int]`
- `network: NetworkAccessControl` (`allow_internet_access`, `allow_out`, `deny_out`)
- `min_containers: int` — read-only on `SandboxPoolInfo` only (see backend gap)

Adds `force: bool` parameter to `delete_pool` (maps to `?force=true` query
param) so callers can delete pools that still have active sandboxes.

`update_pool` becomes patch-like: callers pass only the fields they want to
change; the SDK fetches current state, merges, and PUTs.

### 2. CLI: new `tl sbx pool` subcommand group

| Command | Behavior |
|---|---|
| `tl sbx pool create --image ... --cpus 1.0 --memory-mb 1024 --warm-containers N --max-containers N --timeout S --port P --allow-unauthenticated-access --no-internet --network-allow CIDR --network-deny CIDR --entrypoint ...` | Creates pool. Defaults match `tl sbx create` (cpus=1.0, memory=1024). |
| `tl sbx pool ls [--quiet]` | Lists pools. Columns: ID, Image, Warm, Max, Timeout, Created. |
| `tl sbx pool get <pool-id>` | Shows pool detail + container table. |
| `tl sbx pool update <pool-id> [any subset of create flags]` | Patch-like: fetch-merge-put. |
| `tl sbx pool rm <pool-id> [--force]` | Confirmation prompt if pool has active sandboxes; `--force` skips prompt and terminates them. |
| `tl sbx pool claim <pool-id> [--no-wait]` | Claims a sandbox from the pool. **Waits for `running` status by default**; `--no-wait` returns immediately with `pending`. |

Defaults and conventions:
- All flag names match `tl sbx create` exactly (e.g. `--memory-mb`, `--cpus`,
  `--port`, `--network-allow/deny`, `--no-internet`).
- `tl sbx pool claim` waits by default because pools exist for fast claims —
  inconsistent with `tl sbx create`'s opt-in `--wait`, but defensible because
  the warm-container path is sub-second. Consider matching this default later
  if `tl sbx create` ever gets warm-pool fallback.
- `tl sbx pool rm` confirms only when sandboxes are active. No prompt for empty
  pools.
- `tl sbx pool claim` does NOT support per-claim flags (`--name`, `--ports`,
  etc.) — the API takes an empty body. Proxy/network/access settings are
  inherited from the pool.

### 3. Tests

Three patterns, all of which already exist — no new infrastructure.

- **Python SDK unit tests** ([tests/sandbox/test_client_rust_backend.py](tests/sandbox/test_client_rust_backend.py)) — uses `_FakeRustClient` mock. Extend with cases that capture the create/update JSON payload and assert the new fields (`allow_unauthenticated_access`, `exposed_ports`, `network`, `min_containers`) round-trip through the Python models. Add a unit test for `update_pool` patch-merge: only-passed fields override, untouched fields preserved.
- **Python SDK integration tests** ([tests/sandbox/test_lifecycle.py:190 `TestPoolLifecycle`](tests/sandbox/test_lifecycle.py:190)) — already runs full pool CRUD against a live server. Extend `test_1_create_pool` to set the new fields, extend `test_2_get_pool` to assert they read back, add a `test_N_update_pool_partial` that exercises the patch-merge path, and add a `test_N_delete_pool_force` for the force flag.
- **Rust CLI command-level unit tests** — match the inline `#[test]` pattern used in [crates/cli/src/commands/sbx/ls.rs](crates/cli/src/commands/sbx/ls.rs), [tunnel.rs](crates/cli/src/commands/sbx/tunnel.rs), [exec.rs](crates/cli/src/commands/sbx/exec.rs). For each pool command, add inline tests for body construction (`build_pool_create_body` etc.) and the merge logic in `update`. No end-to-end binary tests — that infra doesn't exist for any sbx command and is out of scope.

### 5. Documentation updates (last step of the change)

- Python SDK reference (sandbox client + models)
- CLI reference (new `tl sbx pool` group)
- docs.tensorlake.ai sandbox guide — add Python pool examples alongside the
  existing TS examples

## Backend / cross-team gaps (NOT in this change)

These need separate work outside the SDK + CLI:

### 1. `min_containers` is not configurable

The HTTP API exposes `min_containers` on `GET` responses (read-only) but does
NOT accept it on `POST` or `PUT`. The value is server-derived from internal
`ContainerPool.min_containers` state.

- **Source**: `indexify/crates/server/src/routes/sandbox_pools.rs` lines 115–146
  (CreateSandboxPoolRequest), 151–182 (UpdateSandboxPoolRequest), 203–223
  (SandboxPoolInfo)
- **Ask**: add `min_containers: Option<u32>` to both request structs and wire
  it through to the underlying pool config.
- **Until then**: SDK exposes `min_containers` on read only; pool create/update
  cannot set it.

### 2. `claim` response gives no warm-vs-cold hint

`POST /sandbox-pools/{id}/sandboxes` always returns
`status="pending", pending_reason="scheduling"`, regardless of whether a warm
container was assigned. Clients must poll `GET /sandboxes/{id}` to discover
running state.

- **Source**: `indexify/crates/server/src/routes/sandbox_pools.rs` lines
  576–606 (`create_pool_sandbox` handler), lines 601–605 (hardcoded response)
- **Ask**: have the response distinguish warm-claim (status="running" or
  pending_reason="warm_assigned") from cold-claim (pending_reason="scheduling")
  so clients can skip polling on the fast path.
- **Until then**: every `claim` does at least one extra round-trip to confirm
  running state. Acceptable but wasteful for the warm path.

### 3. `secret_names` on pools — drop or wire up?

Both Python and Rust SDKs accept `secret_names: list[str]` on `SandboxPoolRequest`
and surface it on `SandboxPoolInfo`. The API route handler does **not** appear
to read this field (no mention in the Create/Update request structs in
`sandbox_pools.rs`).

- **Action needed**: backend confirms whether `secret_names` is silently
  dropped or actually wired through. If dropped: remove from SDK request
  models. If wired: leave as-is and add CLI `--secret` flag to
  `tl sbx pool create`.
- **Until confirmed**: this change leaves Python SDK's `secret_names` alone
  and does NOT add a `--secret` flag to `tl sbx pool create`.

### 4. GPU support on pools

`gpu_configs` exists on `CreateSandboxPoolRequest` in the API. Neither
`tl sbx create` nor the Python `Sandbox.create()` API expose GPU flags today.

- **Action**: defer pool-level GPU until individual-sandbox GPU lands. When
  it does, mirror the same flags to `tl sbx pool create`.

### 5. Per-claim sandbox naming

`tl sbx create --name foo` works for individual sandboxes. `tl sbx pool claim`
cannot name the resulting sandbox — the API takes an empty body.

- **Ask**: optional API change to accept `{ "name": "..." }` on
  `POST /sandbox-pools/{id}/sandboxes`. Low priority; users can rename via
  `tl sbx name` afterward.

### 6. docs.tensorlake.ai sandbox guide

The public docs site lives in a separate repo. The Sandbox Pools section
there currently has TypeScript examples only; the Python equivalents need
to be added in a parallel PR against that repo, mirroring the README and
the new `docs/sandbox-pools-cli-reference.md` in this PR.

### 7. `tl sbx create --pool <id>` as an alias for claim?

Considered and deferred. TS SDK uses `Sandbox.create({poolId})` as the only
surface; Python SDK has both `Sandbox.create()` and `client.claim()`. Keeping
the CLI to one obvious entry point (`tl sbx pool claim`) for now; can add
`tl sbx create --pool` later if discoverability complaints come in.
