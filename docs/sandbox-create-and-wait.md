# Wait-free sandbox create and readiness by polling

Sandbox create can return as soon as the sandbox is durable; readiness is
observed by polling; a caller may bound the capacity wait
(compute-engine-internal ADR 0086). This page describes the SDK and CLI
surface, the behaviour change it brings to timeouts, and how to request many
sandboxes ahead of capacity.

## What changed

**A create that times out no longer deletes the sandbox.** Previously
`Sandbox.create()` / `create_and_connect()` sent a blocking create, and when the
server's wait ran out (or the SDK's own polling deadline passed) the SDK deleted
the sandbox and raised "did not start within 300s". A sandbox that needed a host
to boot was simply lost, and every retry started a fresh create at the back of
the queue.

Now:

- `Sandbox.create()` sends the create with **`wait: false`**. The server
  acknowledges the sandbox as soon as it is durable (HTTP 202, `state:
  pending`, `pending_reason`), and the SDK polls `GET /sandboxes/{id}` every
  two seconds until it runs. When the wait budget runs out it raises
  **`SandboxPending`** (Python) / throws **`SandboxPending`** (TypeScript). The
  error carries `sandbox_id` and the last `pending_reason`, and the sandbox
  **keeps its place in the queue**.
- `Sandbox.create(..., wait=False)` returns a **`PendingSandbox`** handle at
  once instead of waiting: `pending.ready()` waits for it, `pending.status()`
  looks at it. This is the call for requesting many sandboxes ahead of
  capacity.
- `connect(sandbox_id)` on a sandbox that is still pending waits for it (with
  the caller's request timeout) instead of failing. That is the by-id path,
  from any other process.

If your retry loop was written against the old behaviour ("create failed, so
create again"), it will now accumulate pending sandboxes until they run or are
cancelled. Either `connect` to the id in the error, delete it, or pass
`cancel_on_timeout=True` / `cancelOnTimeout: true` to restore delete-then-raise.

Retries stay safe the way they always were: the SDK replays only requests
that provably never reached the server. When your own retry must not create a
second sandbox, give the sandbox a `name` (unique per namespace; a repeat is
an HTTP 409) and resolve it with `get_or_create`.

## Python

```python
from tensorlake.sandbox import Sandbox, SandboxPending, SandboxError

# Blocking, as before. On timeout: SandboxPending, sandbox still queued.
try:
    sandbox = Sandbox.create(image="tensorlake/ubuntu-minimal", request_timeout=300)
except SandboxPending as still:
    print(still.sandbox_id, still.pending_reason)
    sandbox = Sandbox.connect(still.sandbox_id, request_timeout=1800)

# Non-blocking: request now, collect later.
pending = Sandbox.create(
    name="job-17", image="tensorlake/ubuntu-minimal", max_pending_secs=45 * 60, wait=False
)
print(pending.sandbox_id, pending.state, pending.pending_reason)
while True:
    try:
        sandbox = pending.ready(timeout=30)
        break
    except SandboxPending:
        continue                      # still queued; keep waiting
    except SandboxError as failed:
        print(failed.reason)          # no_capacity, cancelled, or a startup failure
        raise
```

`PendingSandbox` fields: `sandbox_id`, `name`, `state`, `pending_reason`.
Methods: `ready(timeout=None, poll_interval=2.0, cancel_on_timeout=False)`
returns a connected `Sandbox`; `status()` fetches the current `SandboxInfo` with
one `GET`. `SandboxClient.create(..., wait=False)` returns the same handle;
`SandboxClient.create()` without `wait` is the blocking create record, as
before. `AsyncSandbox.create(..., wait=False)` / `AsyncSandboxClient.create(...,
wait=False)` return an `AsyncPendingSandbox` whose `ready()` and `status()` are
coroutines.

For many sandboxes, poll the list instead of each handle: `client.list()`
returns every sandbox of the namespace in one request, with `.status` and,
while pending, `.pending_reason`; a sandbox that dropped out of the list has
settled, and `client.get_archived(id).termination_reason` says why
(`no_capacity`, `cancelled`, ...).

Parameters:

- `max_pending_secs` (create, create_and_connect): longest the sandbox may
  wait for **capacity** before the server fails it with reason `no_capacity`.
  `0` fails at once when it cannot be placed. Unset (the default) waits
  indefinitely. The bound covers capacity waits only, not image pull or boot.
  Metal hosts take up to 20 minutes to boot, so a bound shorter than that
  expires the very demand that made the autoscaler launch a host; use 30 minutes
  or more for capacity waits.
- `cancel_on_timeout` (create, create_and_connect, `PendingSandbox.ready`):
  delete the sandbox when the wait runs out and raise `SandboxError` instead of
  `SandboxPending`. Default `False`.
- `poll_interval` (`PendingSandbox.ready`, create_and_connect): seconds between
  polls, default 2.

Errors:

- `SandboxPending(SandboxError)`: `sandbox_id`, `pending_reason`, `timeout`.
- `SandboxError.reason` is the server's reason when a sandbox failed or
  terminated: `no_capacity`, `cancelled` (a pending sandbox was deleted), or a
  startup reason such as `ConfigurationError`. `SandboxError.sandbox_id` names
  the sandbox.

Models: `PendingSandbox` / `AsyncPendingSandbox`, `SandboxPendingReason`, and
the constants `TERMINATION_REASON_NO_CAPACITY` / `TERMINATION_REASON_CANCELLED`.
`SandboxInfo` and `CreateSandboxResponse` gained `pending_reason`.

## TypeScript

```typescript
import { Sandbox, SandboxPending, SandboxError } from "tensorlake";

const pending = await Sandbox.create({
  name: "job-17", image: "tensorlake/ubuntu-minimal", maxPendingSecs: 45 * 60, wait: false,
});
for (;;) {
  try {
    const sandbox = await pending.ready({ timeout: 30 });
    break;
  } catch (error) {
    if (error instanceof SandboxPending) continue; // still queued
    if (error instanceof SandboxError) console.error(error.reason); // no_capacity, cancelled, ...
    throw error;
  }
}
```

`Sandbox.create({ wait: false })` and `SandboxClient.create({ wait: false })`
return a `PendingSandbox` (the return type follows the `wait` option);
`pending.ready({ timeout, pollInterval, cancelOnTimeout })` returns a connected
`Sandbox` and `pending.status()` fetches the current `SandboxInfo`.
`createAndConnect` accepts `maxPendingSecs`, `cancelOnTimeout` and
`pollInterval`. `SandboxPending` carries `sandboxId`, `pendingReason`,
`timeout`; `SandboxError` carries `reason` and `sandboxId`. A lazy
`Sandbox.connect` handle waits for a pending sandbox on its first request.

## CLI

```bash
tl sbx create                       # one blocking request (server waits up to 5 min); never cancels on timeout
tl sbx create --queue               # prints the id at once; the sandbox starts when capacity is available
tl sbx create --queue --max-pending-secs 1800
tl sbx wait <id> [--timeout 120]    # poll until running; run again to keep waiting
```

A wait that runs out exits non-zero with the sandbox id and its pending reason,
and says how to keep waiting (`tl sbx wait <id>`) or cancel
(`tl sbx terminate <id>`).

## How the wait works

`PendingSandbox.ready()`, `Sandbox.create()` and a `connect()` on a pending
sandbox all poll `GET /sandboxes/{id}` every `poll_interval` seconds (default
2) until the sandbox leaves `pending` or the budget runs out. Transient answers
(502/503/504, a cut connection, a timed-out poll — a pending sandbox is not
routable yet, so the lifecycle gateway can answer a proxy error until it starts)
are retried between observations; a missing sandbox surfaces as the poll's 404.
The budget is honoured closely: it is checked before every poll and every
sleep, no sleep runs past the deadline, each poll's HTTP timeout is capped at
the remaining budget (only the very first attempt gets at least one second so
the state can be observed once), and once the budget is spent the last
observation is returned without another request, so `ready(timeout=t)` returns
within `t` plus one poll's round trip even when a poll stalls. The same holds
when polls fail: a retry is never granted the first-poll floor, its backoff
sleep is capped at the remaining budget, and once the budget is spent the last
error (for example the 503 that started the retries) is raised instead of
another request. The loop lives in the Rust core
(`SandboxesClient::wait_until_settled`) and is
shared by both bindings and the CLI's own poll. Against a server that predates
`wait: false`, the create blocks as before and may answer `running` or the
legacy `timeout` status; the SDK uses the former directly and polls after the
latter, so the no-delete rule holds on every server.
